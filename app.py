"""
Keyword & Part Scanner — Ultra-Lite v5 (Streamlit Edition)
===========================================================
Streamlit port of the tkinter GUI scanner.
Run with:  streamlit run app.py
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
                 rate_limiter, circuit_breaker, session) -> dict:
    url_str  = _safe_str(url)
    part_str = _safe_str(part)

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


# ══════════════════════════════════════════════════════════════════════
#  STREAMLIT UI
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

/* Sidebar */
[data-testid="stSidebar"] {
    background: #0f1117 !important;
    border-right: 1px solid #2d3148;
}
[data-testid="stSidebar"] * { color: #e2e8f0 !important; }

/* Main area */
.main .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }

/* Metric cards */
[data-testid="metric-container"] {
    background: #1a1d27;
    border: 1px solid #2d3148;
    border-radius: 10px;
    padding: 12px 16px;
}

/* Buttons */
.stButton > button {
    background: #4f8ef7 !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    font-family: 'Sora', sans-serif !important;
    font-weight: 600 !important;
    padding: 0.5rem 1.4rem !important;
    transition: opacity 0.2s;
}
.stButton > button:hover { opacity: 0.85; }

/* Stop button override via key targeting trick */
div[data-testid="column"]:nth-child(2) .stButton > button {
    background: #ef4444 !important;
}

/* Log area */
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

/* Title bar */
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

/* Section headers */
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
  <h1>⬡ KEYWORD & PART SCANNER — ULTRA-LITE v5</h1>
  <span>HEAD probe · Stream · Early-Exit · ~95% less network vs v3</span>
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
    url_col  = st.text_input("URL column name",  value="offlineURL")
    part_col = st.text_input("Part Number column", value="PartNumber")

    if links_file:
        try:
            preview_df = pd.read_excel(io.BytesIO(links_file.read()), nrows=0)
            links_file.seek(0)
            st.info("📋 Columns found:\n" + "  |  ".join(preview_df.columns.tolist()))
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
#  MAIN PANEL
# ══════════════════════════════════════════════════════════════════════

# ── Stats row ─────────────────────────────────────────────────────────
st.markdown('<div class="section-head">📈 Live Stats</div>', unsafe_allow_html=True)
col1, col2, col3, col4, col5, col6 = st.columns(6)
stat_total   = col1.metric("Total Rows",  "—")
stat_jobs    = col2.metric("Jobs",        "—")
stat_done    = col3.metric("Completed",   "0")
stat_failed  = col4.metric("Failed",      "0")
stat_chunk   = col5.metric("Chunk",       "—")
stat_circuit = col6.metric("CB Errors",   "0")

total_ph   = col1.empty()
jobs_ph    = col2.empty()
done_ph    = col3.empty()
failed_ph  = col4.empty()
chunk_ph   = col5.empty()
circuit_ph = col6.empty()

# ── Progress ──────────────────────────────────────────────────────────
st.markdown('<div class="section-head">📊 Progress</div>', unsafe_allow_html=True)
prog_bar  = st.progress(0)
prog_text = st.empty()
status_ph = st.empty()

# ── Buttons ───────────────────────────────────────────────────────────
st.markdown('<div class="section-head">🚀 Actions</div>', unsafe_allow_html=True)
btn_col1, btn_col2 = st.columns([1, 5])
with btn_col1:
    run_btn = st.button("▶  RUN SCAN", use_container_width=True)

# ── Log ───────────────────────────────────────────────────────────────
st.markdown('<div class="section-head">🖥 Log</div>', unsafe_allow_html=True)
log_ph = st.empty()

# ── Results ───────────────────────────────────────────────────────────
st.markdown('<div class="section-head">✅ Results</div>', unsafe_allow_html=True)
results_ph = st.empty()


# ══════════════════════════════════════════════════════════════════════
#  SESSION STATE
# ══════════════════════════════════════════════════════════════════════

if "log_lines" not in st.session_state:
    st.session_state.log_lines = []
if "scan_done" not in st.session_state:
    st.session_state.scan_done = False
if "result_bytes" not in st.session_state:
    st.session_state.result_bytes = None
if "failed_bytes" not in st.session_state:
    st.session_state.failed_bytes = None


def _append_log(msg: str, tag: str = ""):
    css = {"ok": "log-ok", "err": "log-err", "warn": "log-warn",
           "info": "log-info", "dim": "log-dim"}.get(tag, "")
    ts = time.strftime("%H:%M:%S")
    line = f'<span class="{css}">[{ts}] {msg}</span>'
    st.session_state.log_lines.append(line)
    # keep last 300 lines
    if len(st.session_state.log_lines) > 300:
        st.session_state.log_lines = st.session_state.log_lines[-300:]


def _render_log():
    html = "\n".join(st.session_state.log_lines)
    log_ph.markdown(f'<div class="log-box">{html}</div>', unsafe_allow_html=True)


# Re-render log on every rerun
_render_log()

# Show previous download buttons if scan already done
if st.session_state.scan_done and st.session_state.result_bytes:
    results_ph.download_button(
        label="📥 Download Results (.xlsx)",
        data=st.session_state.result_bytes,
        file_name="scan_results.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# ══════════════════════════════════════════════════════════════════════
#  RUN SCAN
# ══════════════════════════════════════════════════════════════════════

if run_btn:
    # Validate inputs
    if not links_file:
        st.error("❌ Please upload a Links Sheet (.xlsx) first.")
        st.stop()

    keywords     = [k.strip() for k in kw_text.splitlines()  if k.strip()]
    mil_keywords = [k.strip() for k in mil_text.splitlines() if k.strip()]

    if not keywords:
        st.error("❌ Enter at least one keyword.")
        st.stop()

    # Reset state
    st.session_state.log_lines   = []
    st.session_state.scan_done   = False
    st.session_state.result_bytes = None
    st.session_state.failed_bytes = None

    def log(msg, tag=""):
        _append_log(msg, tag)

    log(f"Rate limit : {rpm} req/min | Delay: {delay_min}–{delay_max}s", "info")
    log(f"Circuit    : trip at {cb_errors} errors, pause {cb_pause}s", "info")
    log(f"Workers    : {n_workers}  |  Timeout: {timeout}s", "info")
    log("🚀 Mode: HEAD probe + streaming early-exit (no browser, no OCR)", "ok")
    log(f"📦 HTML cap: {MAX_HTML_BYTES//1024} KB | PDF cap: {MAX_PDF_BYTES//1024//1024} MB", "info")
    log(f"🎭 User-Agent pool: {len(_USER_AGENTS)} agents", "info")
    _render_log()

    # Load data
    try:
        df = pd.read_excel(io.BytesIO(links_file.read()), dtype={part_col: str})
    except Exception as e:
        st.error(f"Cannot open links file: {e}")
        st.stop()

    df_work = df.copy()
    df_work.columns = [c.strip().lower() for c in df_work.columns]
    url_col_lower  = url_col.strip().lower()
    part_col_lower = part_col.strip().lower()

    if url_col_lower not in df_work.columns:
        available = ", ".join(df.columns.tolist())
        st.error(
            f"❌ Column **{url_col}** not found in sheet.\n\n"
            f"Available columns: `{available}`\n\n"
            "Fix the URL column name in the sidebar."
        )
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

    # Build jobs list
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
    _render_log()

    chunks       = [all_jobs[i:i + chunk_size] for i in range(0, total_jobs, chunk_size)]
    total_chunks = len(chunks)
    log(f"Chunks: {total_chunks}  |  Chunk size: {chunk_size}", "info")

    # Set up shared objects
    rate_limiter    = RateLimiter(max_per_minute=rpm)
    circuit_breaker = CircuitBreaker(error_threshold=cb_errors, pause_seconds=cb_pause)
    session         = requests.Session()

    all_results = []
    failed      = []
    done_cnt    = 0

    status_ph.info("● RUNNING")

    try:
        for chunk_idx, chunk_jobs in enumerate(chunks):
            chunk_ph.metric("Chunk", f"{chunk_idx+1}/{total_chunks}")
            log(f"── Chunk {chunk_idx+1}/{total_chunks} ({len(chunk_jobs)} jobs) ──", "info")

            with ThreadPoolExecutor(max_workers=n_workers) as pool:
                futures = {
                    pool.submit(
                        _process_row,
                        ri, url, part,
                        keywords, mil_keywords, timeout,
                        rate_limiter, circuit_breaker, session
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
                            "Part_Scanned": "FALSE"
                        })

                    pct = int(done_cnt / total_jobs * 100) if total_jobs else 0
                    prog_bar.progress(pct)
                    prog_text.text(f"{done_cnt} / {total_jobs} jobs  ({pct}%)")
                    done_ph.metric("Completed", done_cnt)
                    failed_ph.metric("Failed", len(failed))
                    circuit_ph.metric("CB Errors", circuit_breaker.error_count)

            _render_log()

            if chunk_idx < total_chunks - 1:
                pause = random.uniform(delay_min * 2, delay_max * 2)
                log(f"⏸ Pausing {pause:.1f}s between chunks …", "dim")
                time.sleep(pause)

    finally:
        session.close()

    log("Scan finished.", "dim")
    _render_log()

    if not all_results:
        st.warning("No results to save.")
        st.stop()

    # Build final dataframe
    df_final = _apply_results_to_df(
        df_work.copy(), all_results, keywords,
        orig_qual, orig_ztemp, orig_feature, ref_map
    )
    # Restore original column names
    orig_cols = {c.strip().lower(): c.strip() for c in df.columns}
    df_final.rename(columns=orig_cols, inplace=True)

    result_bytes = _highlight_excel(df_final)
    st.session_state.result_bytes = result_bytes
    st.session_state.scan_done    = True

    if failed:
        fail_df = pd.DataFrame(failed)
        buf = io.BytesIO()
        fail_df.to_excel(buf, index=False)
        st.session_state.failed_bytes = buf.getvalue()
        log(f"Failed: {len(failed)} rows logged.", "warn")

    # Summary
    true_part = (df_final.get("Part_Scanned",    pd.Series()) == "TRUE").sum()
    true_mil  = (df_final.get("Military_RESULT", pd.Series()) == "TRUE").sum()
    log("═" * 50, "dim")
    log(f"Total rows     : {len(df_final)}", "ok")
    log(f"Part_Scanned T : {true_part}", "ok")
    log(f"Military TRUE  : {true_mil}", "ok")
    for kw in keywords:
        col = f"{kw}_RESULT"
        if col in df_final.columns:
            log(f"{col}: TRUE={(df_final[col]=='TRUE').sum()}", "ok")
    log("SCAN COMPLETE ✓", "ok")
    _render_log()

    status_ph.success("● COMPLETE")

    # Download buttons
    dl_col1, dl_col2 = results_ph.columns([1, 1])
    dl_col1.download_button(
        label="📥 Download Results (.xlsx)",
        data=result_bytes,
        file_name="scan_results.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    if st.session_state.failed_bytes:
        dl_col2.download_button(
            label="⚠️ Download Failed Rows (.xlsx)",
            data=st.session_state.failed_bytes,
            file_name="scan_failed.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
