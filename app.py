"""
Keyword & Part Scanner — Ultra-Lite v5 (Streamlit Edition)
===========================================================
Fixes applied:
  1. Results download button now renders correctly (was using .columns() on st.empty())
  2. Sidebar text colours fixed — labels readable, values contrasted
  3. Stop / Pause / Resume buttons added with threading Events
  4. Auto-save on error / connection cut — partial results written to session_state immediately
"""

import re
import time
import json
import random
import threading
import io
import os
import collections
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import fitz                     # PyMuPDF
import urllib3
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import streamlit as st

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ══════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════

MAX_HTML_BYTES = 300 * 1024
MAX_PDF_BYTES  = 3 * 1024 * 1024
HEAD_TIMEOUT   = 6
MAX_RETRIES    = 3
RETRY_BACKOFF  = [5, 15, 30]
RETRY_ON_CODES = {429, 503, 502, 504}

DEFAULT_KW     = "AEC-Q100\nAEC-Q200\nAEC-Q101"
DEFAULT_MIL_KW = "Military\nMIL-PRF\nMIL-C\nMIL-R\nMIL-DTL"

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
]

_BASE_HEADERS = {
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection":      "keep-alive",
    "DNT":             "1",
}

_ua_lock  = threading.Lock()
_ua_index = 0


def _get_headers() -> dict:
    global _ua_index
    with _ua_lock:
        ua = _USER_AGENTS[_ua_index % len(_USER_AGENTS)]
        _ua_index += 1
    return {**_BASE_HEADERS, "User-Agent": ua}


# ══════════════════════════════════════════════════════════════════════
#  RATE LIMITER & CIRCUIT BREAKER
# ══════════════════════════════════════════════════════════════════════

class RateLimiter:
    def __init__(self, max_per_minute: int):
        self.max_per_minute = max_per_minute
        self._lock          = threading.Lock()
        self._timestamps    = collections.deque()

    def wait(self):
        while True:
            with self._lock:
                now = time.time()
                while self._timestamps and now - self._timestamps[0] > 60:
                    self._timestamps.popleft()
                if len(self._timestamps) < self.max_per_minute:
                    self._timestamps.append(now)
                    return
                oldest = self._timestamps[0]
                wait_s = 60 - (now - oldest) + 0.1
            time.sleep(wait_s)


class CircuitBreaker:
    def __init__(self, error_threshold: int, pause_seconds: int):
        self.error_threshold = error_threshold
        self.pause_seconds   = pause_seconds
        self._lock           = threading.Lock()
        self._errors         = 0
        self._tripped        = False
        self._trip_time      = 0

    def record_success(self):
        with self._lock:
            self._errors  = 0
            self._tripped = False

    def record_error(self):
        with self._lock:
            self._errors += 1
            if self._errors >= self.error_threshold and not self._tripped:
                self._tripped   = True
                self._trip_time = time.time()

    def wait_if_tripped(self):
        with self._lock:
            if not self._tripped:
                return
            elapsed   = time.time() - self._trip_time
            remaining = self.pause_seconds - elapsed
            if remaining <= 0:
                self._tripped = False
                self._errors  = 0
                return
        time.sleep(max(0, remaining))
        with self._lock:
            self._tripped = False
            self._errors  = 0

    @property
    def error_count(self):
        with self._lock:
            return self._errors


# ══════════════════════════════════════════════════════════════════════
#  SCRAPER HELPERS
# ══════════════════════════════════════════════════════════════════════

def _safe_str(val) -> str:
    if val is None:
        return ""
    if isinstance(val, float):
        if val != val:
            return ""
        if val == int(val):
            return str(int(val))
        return str(val)
    return str(val).strip()


def _normalize_kw(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower().replace("-", " "))


def _kw_variants(kw: str) -> list:
    k = kw.lower()
    return list({k, k.replace("-", " "), k.replace("-", "")})


def _normalize_cmp(text) -> str:
    if pd.isna(text):
        return ""
    t = str(text).lower().strip()
    t = t.replace("–", "-").replace("—", "-").replace("°c", "c").replace("°", "")
    t = re.sub(r"\s+", "", t)
    return re.sub(r"[^0-9a-z\-\+to]", "", t)


def _all_terms_found(text_lower, kw_variants_list, part_lower):
    for variants in kw_variants_list:
        if not any(v in text_lower for v in variants):
            return False
    if part_lower and part_lower not in text_lower:
        return False
    return True


def _probe_content_type(url: str, session: requests.Session) -> str:
    ct = ""
    for attempt in range(MAX_RETRIES):
        try:
            r  = session.head(url, timeout=HEAD_TIMEOUT, verify=False,
                              allow_redirects=True, headers=_get_headers())
            if r.status_code in RETRY_ON_CODES:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                time.sleep(wait + random.uniform(0, 2))
                continue
            ct = r.headers.get("Content-Type", "").lower()
            break
        except Exception:
            ct = ""
            break

    if "pdf" in ct:
        return "pdf"
    if any(x in ct for x in ("html", "xml", "text")):
        return "html"
    low = url.lower().split("?")[0]
    if low.endswith(".pdf"):
        return "pdf"
    if any(low.endswith(e) for e in (".html", ".htm", ".php", ".asp", ".aspx", "/")):
        return "html"
    if ct:
        return "skip"
    return "html"


def _read_pdf(url: str, kw_variants_list, part_lower, session: requests.Session) -> str:
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, timeout=20, verify=False,
                            headers=_get_headers(), stream=True)
            if r.status_code in RETRY_ON_CODES:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                r.close()
                time.sleep(wait + random.uniform(1, 4))
                continue
            content = b""
            for chunk in r.iter_content(chunk_size=65536):
                content += chunk
                if len(content) >= MAX_PDF_BYTES:
                    break
            r.close()
            doc        = fitz.open(stream=content, filetype="pdf")
            parts      = []
            seen_lower = ""
            for page in doc:
                t = page.get_text()
                if t.strip():
                    parts.append(t)
                    seen_lower += t.lower()
                    if _all_terms_found(seen_lower, kw_variants_list, part_lower):
                        break
            doc.close()
            return " ".join(parts)
        except Exception as e:
            last_exc = e
            wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
            time.sleep(wait + random.uniform(1, 3))
    return ""


def _read_html(url: str, timeout: int, kw_variants_list, part_lower,
               session: requests.Session) -> str:
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, timeout=timeout, verify=False,
                            headers=_get_headers(), stream=True)
            if r.status_code in RETRY_ON_CODES:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                r.close()
                time.sleep(wait + random.uniform(1, 4))
                continue
            r.raise_for_status()
            raw_bytes = b""
            for chunk in r.iter_content(chunk_size=32768):
                raw_bytes += chunk
                raw_lower = raw_bytes.decode("utf-8", errors="ignore").lower()
                if _all_terms_found(raw_lower, kw_variants_list, part_lower):
                    break
                if len(raw_bytes) >= MAX_HTML_BYTES:
                    break
            r.close()
            html = raw_bytes.decode("utf-8", errors="ignore")
            soup = BeautifulSoup(html, "html.parser")
            for tag in soup(["script", "style", "noscript", "head"]):
                tag.decompose()
            return soup.get_text(separator=" ", strip=True)
        except Exception as e:
            last_exc = e
            wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
            time.sleep(wait + random.uniform(1, 3))
    raise last_exc or RuntimeError("HTML fetch failed after retries")


def _count_keywords(raw: str, keywords: list, mil_keywords: list) -> dict:
    norm   = _normalize_kw(raw)
    result = {}
    for kw in keywords:
        total = sum(len(re.findall(re.escape(v), norm)) for v in _kw_variants(kw))
        result[kw] = 1 if total > 0 else 0
    result["Military"] = 0
    for kw in mil_keywords:
        total = sum(len(re.findall(re.escape(v), norm)) for v in _kw_variants(kw))
        if total > 0:
            result["Military"] = 1
            break
    return result


def _search_part(raw: str, part) -> str:
    part_str = _safe_str(part)
    if not part_str or part_str.lower() == "nan":
        return "FALSE"
    return "TRUE" if part_str.lower() in raw.lower() else "FALSE"


def _process_row(row_index, url, part, keywords, mil_keywords, timeout,
                 rate_limiter, circuit_breaker, session,
                 stop_event=None, pause_event=None) -> dict:
    """Scan one row. Respects stop/pause threading events."""
    url_str  = _safe_str(url)
    part_str = _safe_str(part)

    # ── Pause support ──────────────────────────────────────────────
    if pause_event:
        while pause_event.is_set():
            if stop_event and stop_event.is_set():
                raise InterruptedError("Stopped by user")
            time.sleep(0.5)

    # ── Stop support ───────────────────────────────────────────────
    if stop_event and stop_event.is_set():
        raise InterruptedError("Stopped by user")

    if circuit_breaker:
        circuit_breaker.wait_if_tripped()
    if rate_limiter:
        rate_limiter.wait()

    all_kw        = keywords + mil_keywords
    kw_variants_l = [_kw_variants(k) for k in all_kw]
    part_lower    = part_str.lower() if part_str and part_str.lower() != "nan" else ""

    try:
        ctype = _probe_content_type(url_str, session)
        if ctype == "skip":
            raw = ""
        elif ctype == "pdf":
            raw = _read_pdf(url_str, kw_variants_l, part_lower, session)
        else:
            raw = _read_html(url_str, timeout, kw_variants_l, part_lower, session)

        if circuit_breaker:
            circuit_breaker.record_success()
    except InterruptedError:
        raise
    except Exception as e:
        if circuit_breaker:
            circuit_breaker.record_error()
        raise e

    row = {"_row_index": row_index, "_scan_url": url_str, "_scan_part": part_str}
    row.update(_count_keywords(raw, keywords, mil_keywords))
    row["Part_Scanned"] = _search_part(raw, part_str)
    return row


def _build_ref_map(ref_bytes: bytes) -> dict:
    try:
        ref = pd.read_excel(io.BytesIO(ref_bytes))
        ref.columns = [c.strip().lower() for c in ref.columns]
        if "qualificationrangemapping" not in ref.columns or "ztemperaturegrade" not in ref.columns:
            return {}
        ref["_norm"] = ref["qualificationrangemapping"].apply(_normalize_cmp)
        return dict(zip(ref["_norm"], ref["ztemperaturegrade"]))
    except Exception:
        return {}


def _compare_ztemp(qual, ztemp, ref_map: dict) -> str:
    v = ref_map.get(_normalize_cmp(qual))
    if v is None:
        return "NOT FOUND"
    return "TRUE" if str(v).strip() == str(ztemp).strip() else "FALSE"


def _compare_aec(kw: str, scanned, feat) -> str:
    feat = "" if pd.isna(feat) else str(feat).strip()
    return "TRUE" if feat == kw and int(scanned) == 1 else "FALSE"


def _compare_mil(scanned, ztemp) -> str:
    if pd.isna(ztemp):
        return "FALSE"
    return "TRUE" if str(ztemp).strip().lower() == "military" and int(scanned) == 1 else "FALSE"


def _apply_results_to_df(df_work, results, keywords, orig_qual, orig_ztemp,
                          orig_feature, ref_map):
    idx_to_result = {r["_row_index"]: r for r in results}

    def get_r(i, field, default):
        return idx_to_result.get(i, {}).get(field, default)

    for kw in keywords:
        df_work[kw] = [get_r(i, kw, 0) for i in df_work.index]

    df_work["Military"]     = [get_r(i, "Military",     0)       for i in df_work.index]
    df_work["Part_Scanned"] = [get_r(i, "Part_Scanned", "FALSE") for i in df_work.index]

    if ref_map:
        df_work["RESULT"] = [
            _compare_ztemp(q, z, ref_map)
            for q, z in zip(orig_qual, orig_ztemp)
        ]
    for kw in keywords:
        df_work[f"{kw}_RESULT"] = [
            _compare_aec(kw, s, f)
            for s, f in zip(df_work[kw], orig_feature)
        ]
    df_work["Military_RESULT"] = [
        _compare_mil(m, z)
        for m, z in zip(df_work["Military"], orig_ztemp)
    ]
    return df_work


def _highlight_excel(df: pd.DataFrame) -> bytes:
    """Write df to an xlsx buffer and apply green/red/yellow fills."""
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)

    green  = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    red    = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    yellow = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")

    wb = load_workbook(buf)
    ws = wb.active
    header = [c.value for c in ws[1]]
    hl_cols = [c for c in header if c and (
        c.endswith("_RESULT") or c in ("RESULT", "Part_Scanned", "Military_RESULT")
    )]
    for col in hl_cols:
        if col not in header:
            continue
        ci = header.index(col) + 1
        for r in range(2, ws.max_row + 1):
            cell = ws.cell(row=r, column=ci)
            if cell.value == "TRUE":
                cell.fill = green
            elif cell.value == "FALSE":
                cell.fill = red
            elif cell.value == "NOT FOUND":
                cell.fill = yellow

    out = io.BytesIO()
    wb.save(out)
    wb.close()
    return out.getvalue()


def _build_partial_excel(df_work, all_results, keywords,
                          orig_qual, orig_ztemp, orig_feature, ref_map) -> bytes:
    """Build an Excel file from whatever results we have so far (partial save)."""
    try:
        df_partial = _apply_results_to_df(
            df_work.copy(), all_results, keywords,
            orig_qual, orig_ztemp, orig_feature, ref_map
        )
        return _highlight_excel(df_partial)
    except Exception:
        return b""


# ══════════════════════════════════════════════════════════════════════
#  STREAMLIT PAGE CONFIG
# ══════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Keyword & Part Scanner v5",
    page_icon="⬡",
    layout="wide",
)

# ── Custom CSS ────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Sora:wght@400;600;700&display=swap');

html, body, [class*="css"] { font-family: 'Sora', sans-serif; }

/* ── Sidebar base ───────────────────────────────────────────────── */
[data-testid="stSidebar"] {
    background: #0f1117 !important;
    border-right: 1px solid #2d3148;
}

/* Sidebar widget labels — soft grey so they're readable but not harsh */
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stTextInput label,
[data-testid="stSidebar"] .stTextArea label,
[data-testid="stSidebar"] .stSlider label,
[data-testid="stSidebar"] .stNumberInput label,
[data-testid="stSidebar"] .stFileUploader label {
    color: #94a3b8 !important;   /* muted slate — readable on dark bg */
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.04em !important;
}

/* Sidebar heading text (##) */
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 {
    color: #cbd5e1 !important;
}

/* Sidebar input boxes */
[data-testid="stSidebar"] input,
[data-testid="stSidebar"] textarea {
    background: #1a1d27 !important;
    color: #f1f5f9 !important;
    border: 1px solid #2d3148 !important;
    border-radius: 6px !important;
}

/* Slider track + thumb */
[data-testid="stSidebar"] [data-testid="stSlider"] div[role="slider"] {
    background: #4f8ef7 !important;
}

/* Info box inside sidebar */
[data-testid="stSidebar"] .stAlert {
    background: #1a2035 !important;
    border: 1px solid #2d3148 !important;
    color: #94a3b8 !important;
    font-size: 0.72rem !important;
}

/* ── Main area ──────────────────────────────────────────────────── */
.main .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }

/* Metric cards */
[data-testid="metric-container"] {
    background: #1a1d27;
    border: 1px solid #2d3148;
    border-radius: 10px;
    padding: 12px 16px;
}

/* ── Buttons ────────────────────────────────────────────────────── */
/* RUN  — blue */
div[data-testid="column"]:nth-child(1) .stButton > button {
    background: #4f8ef7 !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    font-family: 'Sora', sans-serif !important;
    font-weight: 600 !important;
    padding: 0.5rem 1.4rem !important;
    transition: opacity 0.2s;
}
div[data-testid="column"]:nth-child(1) .stButton > button:hover { opacity: 0.85; }

/* PAUSE — amber */
div[data-testid="column"]:nth-child(2) .stButton > button {
    background: #d97706 !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    font-family: 'Sora', sans-serif !important;
    font-weight: 600 !important;
    padding: 0.5rem 1.4rem !important;
    transition: opacity 0.2s;
}

/* STOP — red */
div[data-testid="column"]:nth-child(3) .stButton > button {
    background: #ef4444 !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    font-family: 'Sora', sans-serif !important;
    font-weight: 600 !important;
    padding: 0.5rem 1.4rem !important;
    transition: opacity 0.2s;
}

/* SAVE partial — teal */
div[data-testid="column"]:nth-child(4) .stButton > button {
    background: #0d9488 !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    font-family: 'Sora', sans-serif !important;
    font-weight: 600 !important;
    padding: 0.5rem 1.4rem !important;
    transition: opacity 0.2s;
}

/* ── Log area ───────────────────────────────────────────────────── */
.log-box {
    background: #1a1d27;
    border: 1px solid #2d3148;
    border-radius: 8px;
    padding: 12px 16px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    color: #e2e8f0;
    height: 320px;
    overflow-y: auto;
    white-space: pre-wrap;
    word-break: break-all;
}
.log-ok   { color: #22c55e; }
.log-err  { color: #ef4444; }
.log-warn { color: #f59e0b; }
.log-info { color: #4f8ef7; }
.log-dim  { color: #64748b; }

/* ── Title bar ──────────────────────────────────────────────────── */
.title-bar {
    background: #1a1d27;
    border: 1px solid #2d3148;
    border-radius: 12px;
    padding: 16px 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 1.2rem;
}
.title-bar h1 {
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.15rem;
    font-weight: 700;
    color: #4f8ef7;
    margin: 0;
}
.title-bar span {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.75rem;
    color: #22c55e;
}

/* ── Section headers ────────────────────────────────────────────── */
.section-head {
    font-family: 'Sora', sans-serif;
    font-size: 0.78rem;
    font-weight: 700;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    margin: 1.1rem 0 0.4rem 0;
    border-bottom: 1px solid #2d3148;
    padding-bottom: 4px;
}
</style>
""", unsafe_allow_html=True)

# ── Title bar ─────────────────────────────────────────────────────────
st.markdown("""
<div class="title-bar">
  <h1>⬡ KEYWORD &amp; PART SCANNER — ULTRA-LITE v5</h1>
  <span>HEAD probe · Stream · Early-Exit · Stop/Pause/Save · Auto-save on error</span>
</div>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════
#  SIDEBAR — SETTINGS
# ══════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("## ⚙️ Configuration")

    st.markdown('<div class="section-head">📁 Files</div>', unsafe_allow_html=True)
    links_file = st.file_uploader("Links Sheet (.xlsx)", type=["xlsx"], key="links")
    ref_file   = st.file_uploader("Reference Sheet (.xlsx)", type=["xlsx"], key="ref")

    st.markdown('<div class="section-head">🗂 Column Names</div>', unsafe_allow_html=True)
    url_col  = st.text_input("URL column name",    value="offlineURL")
    part_col = st.text_input("Part Number column", value="PartNumber")

    if links_file:
        try:
            preview_df = pd.read_excel(io.BytesIO(links_file.read()), nrows=0)
            links_file.seek(0)
            st.info("📋 Columns: " + "  |  ".join(preview_df.columns.tolist()))
        except Exception:
            pass

    st.markdown('<div class="section-head">🔑 Keywords</div>', unsafe_allow_html=True)
    kw_text  = st.text_area("Keywords (one per line)",          value=DEFAULT_KW,     height=100)
    mil_text = st.text_area("Military Keywords (one per line)", value=DEFAULT_MIL_KW, height=110)

    st.markdown('<div class="section-head">⚡ Performance</div>', unsafe_allow_html=True)
    n_workers  = st.slider("Workers (threads)",       1,  50,  4)
    timeout    = st.slider("Page timeout (s)",         5, 120, 15)
    chunk_size = st.number_input("Chunk size (rows)", min_value=50, max_value=50000,
                                  value=500, step=50)
    rpm        = st.slider("Max requests / minute",   1, 300, 30)

    st.markdown('<div class="section-head">🛡 Anti-Detection</div>', unsafe_allow_html=True)
    delay_min  = st.number_input("Min delay (s)", min_value=0.0, max_value=30.0,
                                  value=0.5, step=0.5)
    delay_max  = st.number_input("Max delay (s)", min_value=0.0, max_value=60.0,
                                  value=2.0, step=0.5)
    cb_errors  = st.slider("Circuit breaker error threshold",  1,  50,  5)
    cb_pause   = st.slider("Circuit breaker pause (s)",        5, 600, 60)


# ══════════════════════════════════════════════════════════════════════
#  SESSION STATE
# ══════════════════════════════════════════════════════════════════════

for _key, _default in [
    ("log_lines",     []),
    ("scan_done",     False),
    ("scan_running",  False),
    ("scan_paused",   False),
    ("result_bytes",  None),
    ("failed_bytes",  None),
    ("partial_bytes", None),
    ("stop_event",    None),
    ("pause_event",   None),
    # Carry scan context so Stop/Save can rebuild the file mid-run
    ("_scan_ctx",     None),
]:
    if _key not in st.session_state:
        st.session_state[_key] = _default


def _append_log(msg: str, tag: str = ""):
    css = {"ok": "log-ok", "err": "log-err", "warn": "log-warn",
           "info": "log-info", "dim": "log-dim"}.get(tag, "")
    ts   = time.strftime("%H:%M:%S")
    line = f'<span class="{css}">[{ts}] {msg}</span>'
    st.session_state.log_lines.append(line)
    if len(st.session_state.log_lines) > 300:
        st.session_state.log_lines = st.session_state.log_lines[-300:]


def _render_log():
    html = "\n".join(st.session_state.log_lines)
    log_ph.markdown(f'<div class="log-box">{html}</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════
#  MAIN PANEL LAYOUT
# ══════════════════════════════════════════════════════════════════════

st.markdown('<div class="section-head">📈 Live Stats</div>', unsafe_allow_html=True)
col1, col2, col3, col4, col5, col6 = st.columns(6)
total_ph   = col1.empty()
jobs_ph    = col2.empty()
done_ph    = col3.empty()
failed_ph  = col4.empty()
chunk_ph   = col5.empty()
circuit_ph = col6.empty()
total_ph.metric("Total Rows", "—")
jobs_ph.metric("Jobs",        "—")
done_ph.metric("Completed",   "0")
failed_ph.metric("Failed",    "0")
chunk_ph.metric("Chunk",      "—")
circuit_ph.metric("CB Errors","0")

st.markdown('<div class="section-head">📊 Progress</div>', unsafe_allow_html=True)
prog_bar  = st.progress(0)
prog_text = st.empty()
status_ph = st.empty()

# ── Action buttons ────────────────────────────────────────────────────
st.markdown('<div class="section-head">🚀 Actions</div>', unsafe_allow_html=True)
btn_c1, btn_c2, btn_c3, btn_c4, _rest = st.columns([1, 1, 1, 1, 3])
with btn_c1:
    run_btn   = st.button("▶ RUN SCAN",  use_container_width=True,
                          disabled=st.session_state.scan_running)
with btn_c2:
    pause_lbl = "⏸ PAUSE" if not st.session_state.scan_paused else "▶ RESUME"
    pause_btn = st.button(pause_lbl, use_container_width=True,
                          disabled=not st.session_state.scan_running)
with btn_c3:
    stop_btn  = st.button("⏹ STOP",     use_container_width=True,
                          disabled=not st.session_state.scan_running)
with btn_c4:
    save_btn  = st.button("💾 SAVE NOW", use_container_width=True,
                          disabled=not st.session_state.scan_running)

st.markdown('<div class="section-head">🖥 Log</div>', unsafe_allow_html=True)
log_ph = st.empty()

# ── FIX #1: Results section uses a plain container, not st.empty() ──
# We use st.container() so we can render multiple widgets inside it.
st.markdown('<div class="section-head">✅ Results</div>', unsafe_allow_html=True)
results_container = st.container()

# Re-render log on every rerun
_render_log()

# ── Show download buttons from previous completed scan ────────────────
with results_container:
    if st.session_state.scan_done and st.session_state.result_bytes:
        dl1, dl2 = st.columns([1, 1])
        dl1.download_button(
            label="📥 Download Results (.xlsx)",
            data=st.session_state.result_bytes,
            file_name="scan_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_results_top",
        )
        if st.session_state.failed_bytes:
            dl2.download_button(
                label="⚠️ Download Failed Rows (.xlsx)",
                data=st.session_state.failed_bytes,
                file_name="scan_failed.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_failed_top",
            )

    if st.session_state.partial_bytes and not st.session_state.scan_done:
        st.download_button(
            label="💾 Download Partial Results (.xlsx)",
            data=st.session_state.partial_bytes,
            file_name="scan_partial.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_partial",
        )


# ══════════════════════════════════════════════════════════════════════
#  PAUSE / STOP / SAVE HANDLERS  (before run_btn so they fire first)
# ══════════════════════════════════════════════════════════════════════

if pause_btn and st.session_state.scan_running:
    if st.session_state.pause_event is not None:
        if st.session_state.scan_paused:
            st.session_state.pause_event.clear()   # resume
            st.session_state.scan_paused = False
            _append_log("▶ Resumed by user.", "ok")
        else:
            st.session_state.pause_event.set()     # pause
            st.session_state.scan_paused = True
            _append_log("⏸ Paused by user — workers will finish current request then wait.", "warn")
    st.rerun()

if stop_btn and st.session_state.scan_running:
    if st.session_state.stop_event is not None:
        st.session_state.stop_event.set()
    # Also unpause so threads can see the stop
    if st.session_state.pause_event is not None:
        st.session_state.pause_event.clear()
    _append_log("⏹ Stop requested — saving partial results …", "warn")
    # Trigger a partial save immediately from current context
    ctx = st.session_state._scan_ctx
    if ctx:
        pb = _build_partial_excel(
            ctx["df_work"], ctx["all_results"], ctx["keywords"],
            ctx["orig_qual"], ctx["orig_ztemp"], ctx["orig_feature"], ctx["ref_map"]
        )
        if pb:
            st.session_state.partial_bytes = pb
            _append_log(f"💾 Partial file saved ({len(ctx['all_results'])} rows).", "ok")
    st.rerun()

if save_btn and st.session_state.scan_running:
    ctx = st.session_state._scan_ctx
    if ctx:
        pb = _build_partial_excel(
            ctx["df_work"], ctx["all_results"], ctx["keywords"],
            ctx["orig_qual"], ctx["orig_ztemp"], ctx["orig_feature"], ctx["ref_map"]
        )
        if pb:
            st.session_state.partial_bytes = pb
            _append_log(f"💾 Manual save: {len(ctx['all_results'])} rows saved.", "ok")
    st.rerun()


# ══════════════════════════════════════════════════════════════════════
#  RUN SCAN
# ══════════════════════════════════════════════════════════════════════

if run_btn:
    if not links_file:
        st.error("❌ Please upload a Links Sheet (.xlsx) first.")
        st.stop()

    keywords     = [k.strip() for k in kw_text.splitlines()  if k.strip()]
    mil_keywords = [k.strip() for k in mil_text.splitlines() if k.strip()]

    if not keywords:
        st.error("❌ Enter at least one keyword.")
        st.stop()

    # Reset state
    st.session_state.log_lines    = []
    st.session_state.scan_done    = False
    st.session_state.scan_running = True
    st.session_state.scan_paused  = False
    st.session_state.result_bytes = None
    st.session_state.failed_bytes = None
    st.session_state.partial_bytes = None

    # Fresh stop / pause events
    stop_event  = threading.Event()
    pause_event = threading.Event()
    st.session_state.stop_event  = stop_event
    st.session_state.pause_event = pause_event

    def log(msg, tag=""):
        _append_log(msg, tag)

    log(f"Rate limit : {rpm} req/min | Delay: {delay_min}–{delay_max}s", "info")
    log(f"Circuit    : trip at {cb_errors} errors, pause {cb_pause}s", "info")
    log(f"Workers    : {n_workers}  |  Timeout: {timeout}s", "info")
    log("🚀 Mode: HEAD probe + streaming early-exit (no browser, no OCR)", "ok")
    log(f"📦 HTML cap: {MAX_HTML_BYTES//1024} KB | PDF cap: {MAX_PDF_BYTES//1024//1024} MB", "info")
    log(f"🎭 User-Agent pool: {len(_USER_AGENTS)} agents", "info")
    _render_log()

    # ── Load data ─────────────────────────────────────────────────────
    try:
        df = pd.read_excel(io.BytesIO(links_file.read()), dtype={part_col: str})
    except Exception as e:
        st.error(f"Cannot open links file: {e}")
        st.session_state.scan_running = False
        st.stop()

    df_work = df.copy()
    df_work.columns = [c.strip().lower() for c in df_work.columns]
    url_col_lower  = url_col.strip().lower()
    part_col_lower = part_col.strip().lower()

    if url_col_lower not in df_work.columns:
        available = ", ".join(df.columns.tolist())
        st.error(
            f"❌ Column **{url_col}** not found.\n\n"
            f"Available: `{available}`\n\nFix the URL column name in the sidebar."
        )
        st.session_state.scan_running = False
        st.stop()

    if part_col_lower not in df_work.columns:
        log(f"⚠ Column '{part_col}' not found — Part_Scanned will be FALSE for all.", "warn")
        df_work[part_col_lower] = ""

    df_work[url_col_lower]  = df_work[url_col_lower].apply(_safe_str)
    df_work[part_col_lower] = df_work[part_col_lower].apply(_safe_str)

    orig_ztemp   = df_work.get("ztemperaturegrade",         pd.Series("", index=df_work.index))
    orig_feature = df_work.get("featurevalue",              pd.Series("", index=df_work.index))
    orig_qual    = df_work.get("qualificationrangemapping", pd.Series("", index=df_work.index))

    ref_map = {}
    if ref_file:
        ref_map = _build_ref_map(ref_file.read())
        log(f"Reference entries: {len(ref_map)}", "dim")

    # ── Build jobs list ───────────────────────────────────────────────
    all_jobs = []
    for idx, row in df_work.iterrows():
        url  = row[url_col_lower]
        part = row[part_col_lower]
        if not url or url.lower() == "nan":
            continue
        all_jobs.append((idx, url, part))

    total_rows = len(df_work)
    total_jobs = len(all_jobs)
    total_ph.metric("Total Rows", total_rows)
    jobs_ph.metric("Jobs",        total_jobs)
    log(f"Rows: {total_rows}  |  Jobs: {total_jobs}", "info")

    chunks       = [all_jobs[i:i + chunk_size] for i in range(0, total_jobs, chunk_size)]
    total_chunks = len(chunks)
    log(f"Chunks: {total_chunks}  |  Chunk size: {chunk_size}", "info")
    _render_log()

    # ── Shared objects ────────────────────────────────────────────────
    rate_limiter    = RateLimiter(max_per_minute=rpm)
    circuit_breaker = CircuitBreaker(error_threshold=cb_errors, pause_seconds=cb_pause)
    session         = requests.Session()

    all_results = []
    failed      = []
    done_cnt    = 0

    # Store context so Pause/Stop/Save buttons can access it
    st.session_state._scan_ctx = {
        "df_work":     df_work,
        "all_results": all_results,   # shared list — mutated in-place below
        "keywords":    keywords,
        "orig_qual":   orig_qual,
        "orig_ztemp":  orig_ztemp,
        "orig_feature":orig_feature,
        "ref_map":     ref_map,
    }

    status_ph.info("● RUNNING")
    user_stopped = False

    try:
        for chunk_idx, chunk_jobs in enumerate(chunks):
            if stop_event.is_set():
                user_stopped = True
                break

            chunk_ph.metric("Chunk", f"{chunk_idx+1}/{total_chunks}")
            log(f"── Chunk {chunk_idx+1}/{total_chunks} ({len(chunk_jobs)} jobs) ──", "info")

            with ThreadPoolExecutor(max_workers=n_workers) as pool:
                futures = {
                    pool.submit(
                        _process_row,
                        ri, url, part,
                        keywords, mil_keywords, timeout,
                        rate_limiter, circuit_breaker, session,
                        stop_event, pause_event
                    ): (ri, url, part)
                    for ri, url, part in chunk_jobs
                }

                for fut in as_completed(futures):
                    ri, url, part = futures[fut]
                    try:
                        r = fut.result()
                        all_results.append(r)
                        done_cnt += 1
                        tag   = "ok" if r.get("Part_Scanned") == "TRUE" else "dim"
                        short = url[:60] + "…" if len(url) > 60 else url
                        log(f"✓ [{ri}] {short} | Part={r.get('Part_Scanned')}", tag)
                    except InterruptedError:
                        # Worker was stopped — record as failed so row gets a default
                        failed.append({"row_index": ri, "url": url, "part": part,
                                       "error": "Stopped by user"})
                        done_cnt += 1
                        all_results.append({
                            "_row_index":   ri,
                            "_scan_url":    url,
                            "_scan_part":   part,
                            **{k: 0 for k in keywords},
                            "Military":     0,
                            "Part_Scanned": "FALSE",
                        })
                    except Exception as e:
                        failed.append({"row_index": ri, "url": url, "part": part, "error": str(e)})
                        done_cnt += 1
                        log(f"✗ [{ri}] {url[:60]} | {e}", "err")
                        all_results.append({
                            "_row_index":   ri,
                            "_scan_url":    url,
                            "_scan_part":   part,
                            **{k: 0 for k in keywords},
                            "Military":     0,
                            "Part_Scanned": "FALSE",
                        })
                        # ── FIX #4: auto-save partial on every error ──────
                        pb = _build_partial_excel(
                            df_work, all_results, keywords,
                            orig_qual, orig_ztemp, orig_feature, ref_map
                        )
                        if pb:
                            st.session_state.partial_bytes = pb

                    pct = int(done_cnt / total_jobs * 100) if total_jobs else 0
                    prog_bar.progress(pct)
                    prog_text.text(f"{done_cnt} / {total_jobs} jobs  ({pct}%)")
                    done_ph.metric("Completed", done_cnt)
                    failed_ph.metric("Failed",    len(failed))
                    circuit_ph.metric("CB Errors", circuit_breaker.error_count)

                    # Stop check inside future loop
                    if stop_event.is_set():
                        user_stopped = True
                        break

            _render_log()

            if stop_event.is_set():
                user_stopped = True
                break

            if chunk_idx < total_chunks - 1:
                pause = random.uniform(delay_min * 2, delay_max * 2)
                log(f"⏸ Pausing {pause:.1f}s between chunks …", "dim")
                time.sleep(pause)

    except Exception as outer_exc:
        # ── FIX #4: unexpected outer error — auto-save whatever we have ──
        log(f"💥 Unexpected error: {outer_exc}", "err")
        pb = _build_partial_excel(
            df_work, all_results, keywords,
            orig_qual, orig_ztemp, orig_feature, ref_map
        )
        if pb:
            st.session_state.partial_bytes = pb
            log(f"💾 Auto-saved {len(all_results)} rows due to error.", "warn")

    finally:
        session.close()
        st.session_state.scan_running = False
        st.session_state.scan_paused  = False
        st.session_state._scan_ctx    = None

    # ── Build final output ────────────────────────────────────────────
    if not all_results:
        st.warning("No results to save.")
        st.stop()

    df_final = _apply_results_to_df(
        df_work.copy(), all_results, keywords,
        orig_qual, orig_ztemp, orig_feature, ref_map
    )
    # Restore original column names
    orig_cols = {c.strip().lower(): c.strip() for c in df.columns}
    df_final.rename(columns=orig_cols, inplace=True)

    result_bytes = _highlight_excel(df_final)
    st.session_state.result_bytes = result_bytes

    if user_stopped:
        st.session_state.partial_bytes = result_bytes   # treat as partial
        log("⏹ Scan stopped by user — partial file ready.", "warn")
        status_ph.warning("● STOPPED")
    else:
        st.session_state.scan_done = True
        log("SCAN COMPLETE ✓", "ok")
        status_ph.success("● COMPLETE")

    if failed:
        fail_df = pd.DataFrame(failed)
        buf = io.BytesIO()
        fail_df.to_excel(buf, index=False)
        st.session_state.failed_bytes = buf.getvalue()
        log(f"Failed rows logged: {len(failed)}", "warn")

    # Summary
    true_part = (df_final.get("Part_Scanned",    pd.Series()) == "TRUE").sum()
    true_mil  = (df_final.get("Military_RESULT", pd.Series()) == "TRUE").sum()
    log("═" * 50, "dim")
    log(f"Total rows     : {len(df_final)}", "ok")
    log(f"Part_Scanned T : {true_part}", "ok")
    log(f"Military TRUE  : {true_mil}", "ok")
    for kw in keywords:
        col_name = f"{kw}_RESULT"
        if col_name in df_final.columns:
            log(f"{col_name}: TRUE={(df_final[col_name]=='TRUE').sum()}", "ok")
    _render_log()

    # ── FIX #1: Render download buttons inside the pre-declared container ──
    with results_container:
        dl1, dl2 = st.columns([1, 1])
        dl1.download_button(
            label="📥 Download Results (.xlsx)",
            data=result_bytes,
            file_name="scan_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_results_run",
        )
        if st.session_state.failed_bytes:
            dl2.download_button(
                label="⚠️ Download Failed Rows (.xlsx)",
                data=st.session_state.failed_bytes,
                file_name="scan_failed.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_failed_run",
            )
