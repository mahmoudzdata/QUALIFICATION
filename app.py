"""
Keyword & Part Scanner — v6
============================
 ✦  No reference sheet / ZTemp — removed entirely
 ✦  Full PDF text extraction (entire document, no early exit)
 ✦  Flexible regex: AEC-Q100 / AECQ100 / AEC Q100 all match
 ✦  Thread-safe results list
 ✦  Auto-save after EVERY row — partial download always available
 ✦  Graceful fallback if PyMuPDF not installed
 ✦  requirements.txt companion included in comments below
"""

# ── requirements.txt (put this next to the .py file on Streamlit Cloud) ──
# streamlit>=1.32
# requests>=2.31
# pandas>=2.0
# openpyxl>=3.1
# beautifulsoup4>=4.12
# pymupdf>=1.23
# urllib3>=2.0
# lxml>=5.0
# ─────────────────────────────────────────────────────────────────────────

import re
import time
import random
import threading
import io
import collections
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import urllib3
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import streamlit as st

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# PyMuPDF — handle both package names across versions
try:
    import fitz as _fitz
    _FITZ_OK = True
except ImportError:
    try:
        import pymupdf as _fitz
        _FITZ_OK = True
    except ImportError:
        _fitz = None
        _FITZ_OK = False

# ══════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════

MAX_HTML_BYTES = 500 * 1024
MAX_PDF_BYTES  = 10 * 1024 * 1024
HEAD_TIMEOUT   = 8
GET_TIMEOUT    = 25
MAX_RETRIES    = 3
RETRY_BACKOFF  = [5, 15, 30]
RETRY_ON_CODES = {429, 502, 503, 504}

DEFAULT_AUTO_KW = """\
AEC-Q100
AEC-Q101
AEC-Q200
Automotive Grade
Automotive Qualified

DEFAULT_MIL_KW = """\
Military
MIL-PRF
MIL-C
MIL-R
MIL-DTL
MIL-STD
MIL-SPEC
MIL-S
MIL-M
MIL-I
"""

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
]

_BASE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "DNT": "1",
}

_ua_lock  = threading.Lock()
_ua_index = 0

def _get_headers():
    global _ua_index
    with _ua_lock:
        ua = _USER_AGENTS[_ua_index % len(_USER_AGENTS)]
        _ua_index += 1
    return {**_BASE_HEADERS, "User-Agent": ua}


# ══════════════════════════════════════════════════════════════════════
#  RATE LIMITER & CIRCUIT BREAKER
# ══════════════════════════════════════════════════════════════════════

class RateLimiter:
    def __init__(self, max_per_minute):
        self.max_per_minute = max_per_minute
        self._lock = threading.Lock()
        self._timestamps = collections.deque()

    def wait(self):
        while True:
            with self._lock:
                now = time.time()
                while self._timestamps and now - self._timestamps[0] > 60:
                    self._timestamps.popleft()
                if len(self._timestamps) < self.max_per_minute:
                    self._timestamps.append(now)
                    return
                wait_s = 60 - (now - self._timestamps[0]) + 0.1
            time.sleep(wait_s)


class CircuitBreaker:
    def __init__(self, error_threshold, pause_seconds):
        self.error_threshold = error_threshold
        self.pause_seconds   = pause_seconds
        self._lock           = threading.Lock()
        self._errors         = 0
        self._tripped        = False
        self._trip_time      = 0.0

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
            remaining = self.pause_seconds - (time.time() - self._trip_time)
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
#  HELPERS
# ══════════════════════════════════════════════════════════════════════

def _safe_str(val):
    if val is None:
        return ""
    if isinstance(val, float):
        return "" if (val != val) else (str(int(val)) if val == int(val) else str(val))
    return str(val).strip()


def _build_pattern(kw):
    """Case-insensitive pattern; hyphens/spaces interchangeable."""
    escaped  = re.escape(kw)
    flexible = escaped.replace(r"\-", r"[\s\-]?").replace(r"\ ", r"[\s\-]?")
    return re.compile(flexible, re.IGNORECASE)


def _probe_content_type(url, session):
    ct = ""
    for attempt in range(MAX_RETRIES):
        try:
            r  = session.head(url, timeout=HEAD_TIMEOUT, verify=False,
                              allow_redirects=True, headers=_get_headers())
            if r.status_code in RETRY_ON_CODES:
                time.sleep(RETRY_BACKOFF[min(attempt, 2)] + random.uniform(0, 2))
                continue
            ct = r.headers.get("Content-Type", "").lower()
            break
        except Exception:
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
    return "html" if not ct else "skip"


def _read_pdf_full(url, session):
    """Full PDF text — reads entire document, no early exit."""
    if not _FITZ_OK:
        return _read_html_full(url, session)   # fallback
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, timeout=GET_TIMEOUT, verify=False,
                            headers=_get_headers(), stream=True)
            if r.status_code in RETRY_ON_CODES:
                r.close()
                time.sleep(RETRY_BACKOFF[min(attempt, 2)] + random.uniform(1, 4))
                continue
            r.raise_for_status()
            data = b""
            for chunk in r.iter_content(65536):
                data += chunk
                if len(data) >= MAX_PDF_BYTES:
                    break
            r.close()
            doc   = _fitz.open(stream=data, filetype="pdf")
            pages = [p.get_text() for p in doc]
            doc.close()
            return " ".join(pages)
        except Exception:
            time.sleep(RETRY_BACKOFF[min(attempt, 2)] + random.uniform(1, 3))
    return ""


def _read_html_full(url, session):
    """Full HTML text extraction."""
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, timeout=GET_TIMEOUT, verify=False,
                            headers=_get_headers(), stream=True)
            if r.status_code in RETRY_ON_CODES:
                r.close()
                time.sleep(RETRY_BACKOFF[min(attempt, 2)] + random.uniform(1, 4))
                continue
            r.raise_for_status()
            raw = b""
            for chunk in r.iter_content(32768):
                raw += chunk
                if len(raw) >= MAX_HTML_BYTES:
                    break
            r.close()
            soup = BeautifulSoup(raw.decode("utf-8", errors="ignore"), "html.parser")
            for tag in soup(["script", "style", "noscript", "head", "meta", "link"]):
                tag.decompose()
            return soup.get_text(separator=" ", strip=True)
        except Exception:
            time.sleep(RETRY_BACKOFF[min(attempt, 2)] + random.uniform(1, 3))
    return ""


def _count_keywords(raw, auto_patterns, mil_patterns, auto_keywords, mil_keywords):
    result = {}
    for kw, pat in zip(auto_keywords, auto_patterns):
        result[kw] = 1 if pat.search(raw) else 0
    result["Military"] = 0
    for pat in mil_patterns:
        if pat.search(raw):
            result["Military"] = 1
            break
    return result


def _search_part(raw, part):
    p = _safe_str(part)
    if not p or p.lower() in ("nan", ""):
        return "FALSE"
    return "TRUE" if p.lower() in raw.lower() else "FALSE"


# ══════════════════════════════════════════════════════════════════════
#  WORKER
# ══════════════════════════════════════════════════════════════════════

def _process_row(row_index, url, part,
                 auto_keywords, mil_keywords,
                 auto_patterns, mil_patterns,
                 rate_limiter, circuit_breaker, session,
                 stop_event, pause_event):

    if pause_event:
        while pause_event.is_set():
            if stop_event and stop_event.is_set():
                raise InterruptedError("Stopped")
            time.sleep(0.5)
    if stop_event and stop_event.is_set():
        raise InterruptedError("Stopped")

    if circuit_breaker:
        circuit_breaker.wait_if_tripped()
    if rate_limiter:
        rate_limiter.wait()

    url_str  = _safe_str(url)
    part_str = _safe_str(part)

    try:
        ctype = _probe_content_type(url_str, session)
        if ctype == "pdf":
            raw = _read_pdf_full(url_str, session)
        elif ctype == "skip":
            raw = ""
        else:
            raw = _read_html_full(url_str, session)

        if circuit_breaker:
            circuit_breaker.record_success()
    except InterruptedError:
        raise
    except Exception as e:
        if circuit_breaker:
            circuit_breaker.record_error()
        raise

    row = {"_row_index": row_index, "_scan_url": url_str, "_scan_part": part_str}
    row.update(_count_keywords(raw, auto_patterns, mil_patterns,
                               auto_keywords, mil_keywords))
    row["Part_Scanned"] = _search_part(raw, part_str)
    return row


# ══════════════════════════════════════════════════════════════════════
#  EXCEL OUTPUT
# ══════════════════════════════════════════════════════════════════════

def _apply_results(df_work, results, auto_keywords):
    idx_map = {r["_row_index"]: r for r in results}
    def _g(i, f, d):
        return idx_map.get(i, {}).get(f, d)

    for kw in auto_keywords:
        df_work[kw] = [_g(i, kw, 0) for i in df_work.index]
    df_work["Military"]     = [_g(i, "Military",     0)       for i in df_work.index]
    df_work["Part_Scanned"] = [_g(i, "Part_Scanned", "FALSE") for i in df_work.index]
    return df_work


def _to_excel_bytes(df):
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)

    green = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    red   = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")

    wb = load_workbook(buf)
    ws = wb.active
    header = [c.value for c in ws[1]]

    for col_name in header:
        if col_name is None:
            continue
        ci = header.index(col_name) + 1
        for r in range(2, ws.max_row + 1):
            cell = ws.cell(row=r, column=ci)
            if col_name == "Part_Scanned":
                cell.fill = green if cell.value == "TRUE" else red
            elif col_name == "Military":
                if cell.value == 1:
                    cell.fill = green
            elif col_name not in ("_scan_url", "_scan_part", "_row_index"):
                # numeric keyword columns: 1=green
                if cell.value == 1:
                    cell.fill = green

    out = io.BytesIO()
    wb.save(out)
    wb.close()
    return out.getvalue()


def _partial_excel(df_work, results, auto_keywords):
    try:
        df_p = _apply_results(df_work.copy(), results, auto_keywords)
        return _to_excel_bytes(df_p)
    except Exception:
        return b""


# ══════════════════════════════════════════════════════════════════════
#  STREAMLIT APP
# ══════════════════════════════════════════════════════════════════════

st.set_page_config(page_title="AEC / MIL Scanner v6", page_icon="🔬", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600;700&family=IBM+Plex+Sans:wght@400;500;600&display=swap');
html,body,[class*="css"]{font-family:'IBM Plex Sans',sans-serif;}
[data-testid="stSidebar"]{background:#0a0e1a!important;border-right:1px solid #1e2840;}
[data-testid="stSidebar"] label,[data-testid="stSidebar"] .stTextInput label,
[data-testid="stSidebar"] .stTextArea label,[data-testid="stSidebar"] .stSlider label,
[data-testid="stSidebar"] .stNumberInput label,[data-testid="stSidebar"] .stFileUploader label{
    color:#7b9cc4!important;font-size:0.77rem!important;font-weight:600!important;
    letter-spacing:0.06em!important;text-transform:uppercase;}
[data-testid="stSidebar"] h2,[data-testid="stSidebar"] h3{
    color:#c9d8f0!important;font-family:'IBM Plex Mono',monospace!important;}
[data-testid="stSidebar"] input,[data-testid="stSidebar"] textarea{
    background:#111827!important;color:#e2eaf8!important;border:1px solid #1e2840!important;border-radius:4px!important;}
[data-testid="stSidebar"] .stAlert{
    background:#0f1929!important;border:1px solid #1e2840!important;color:#7b9cc4!important;font-size:0.72rem!important;}
.main .block-container{padding-top:1rem;padding-bottom:2rem;}
[data-testid="metric-container"]{background:#0f1929;border:1px solid #1e2840;border-radius:8px;padding:10px 14px;}
.title-bar{background:linear-gradient(135deg,#0a0e1a,#111827);border:1px solid #1e3a5f;
    border-left:4px solid #3b82f6;border-radius:8px;padding:14px 22px;margin-bottom:1.2rem;
    display:flex;align-items:center;justify-content:space-between;}
.title-bar h1{font-family:'IBM Plex Mono',monospace;font-size:1.05rem;font-weight:700;color:#60a5fa;margin:0;}
.title-bar .badge{font-family:'IBM Plex Mono',monospace;font-size:0.68rem;color:#22c55e;
    background:#052e16;border:1px solid #166534;border-radius:4px;padding:2px 8px;}
.sec{font-family:'IBM Plex Mono',monospace;font-size:0.68rem;font-weight:700;color:#3b4f6b;
    text-transform:uppercase;letter-spacing:0.14em;margin:1rem 0 0.35rem 0;
    border-bottom:1px solid #1e2840;padding-bottom:3px;}
div[data-testid="column"]:nth-child(1) .stButton>button{
    background:#2563eb!important;color:#fff!important;border:none!important;border-radius:6px!important;
    font-family:'IBM Plex Mono',monospace!important;font-weight:600!important;font-size:0.82rem!important;padding:0.45rem 1.2rem!important;}
div[data-testid="column"]:nth-child(2) .stButton>button{
    background:#b45309!important;color:#fff!important;border:none!important;border-radius:6px!important;
    font-family:'IBM Plex Mono',monospace!important;font-weight:600!important;font-size:0.82rem!important;padding:0.45rem 1.2rem!important;}
div[data-testid="column"]:nth-child(3) .stButton>button{
    background:#dc2626!important;color:#fff!important;border:none!important;border-radius:6px!important;
    font-family:'IBM Plex Mono',monospace!important;font-weight:600!important;font-size:0.82rem!important;padding:0.45rem 1.2rem!important;}
div[data-testid="column"]:nth-child(4) .stButton>button{
    background:#0f766e!important;color:#fff!important;border:none!important;border-radius:6px!important;
    font-family:'IBM Plex Mono',monospace!important;font-weight:600!important;font-size:0.82rem!important;padding:0.45rem 1.2rem!important;}
.log-box{background:#080c18;border:1px solid #1e2840;border-radius:6px;padding:12px 16px;
    font-family:'IBM Plex Mono',monospace;font-size:11.5px;color:#cbd5e1;
    height:340px;overflow-y:auto;white-space:pre-wrap;word-break:break-all;line-height:1.6;}
.log-ok{color:#4ade80;}.log-err{color:#f87171;}.log-warn{color:#fbbf24;}
.log-info{color:#60a5fa;}.log-dim{color:#374151;}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="title-bar">
  <h1>🔬 AUTOMOTIVE &amp; MILITARY KEYWORD SCANNER — v6</h1>
  <span class="badge">FULL PDF · AUTO-SAVE EVERY ROW · LARGE FILE SAFE</span>
</div>
""", unsafe_allow_html=True)

# ── Sidebar ────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Configuration")
    st.markdown('<div class="sec">📁 File</div>', unsafe_allow_html=True)
    links_file = st.file_uploader("Links Sheet (.xlsx)", type=["xlsx"])

    st.markdown('<div class="sec">🗂 Columns</div>', unsafe_allow_html=True)
    url_col  = st.text_input("URL column name",    value="offlineURL")
    part_col = st.text_input("Part Number column", value="PartNumber")

    if links_file:
        try:
            _pr = pd.read_excel(io.BytesIO(links_file.read()), nrows=0)
            links_file.seek(0)
            st.info("Columns: " + " | ".join(str(c) for c in _pr.columns))
        except Exception:
            pass

    st.markdown('<div class="sec">🚗 Automotive Keywords</div>', unsafe_allow_html=True)
    auto_text = st.text_area("One per line", value=DEFAULT_AUTO_KW, height=220)

    st.markdown('<div class="sec">🎖 Military Keywords</div>', unsafe_allow_html=True)
    mil_text  = st.text_area("One per line — any match → Military=1", value=DEFAULT_MIL_KW, height=200)

    st.markdown('<div class="sec">⚡ Performance</div>', unsafe_allow_html=True)
    n_workers  = st.slider("Workers (threads)", 1, 50, 6)
    chunk_size = st.number_input("Chunk size (rows)", 50, 50000, 500, step=50)
    rpm        = st.slider("Max requests / minute", 1, 300, 40)

    st.markdown('<div class="sec">🛡 Delays</div>', unsafe_allow_html=True)
    delay_min = st.number_input("Min inter-chunk delay (s)", 0.0, 30.0, 0.5, step=0.5)
    delay_max = st.number_input("Max inter-chunk delay (s)", 0.0, 60.0, 2.0, step=0.5)
    cb_errors = st.slider("Circuit breaker threshold",  1,  50,  8)
    cb_pause  = st.slider("Circuit breaker pause (s)",  5, 600, 60)

# ── Session state ──────────────────────────────────────────────────────
for _k, _v in [
    ("log_lines",     []),
    ("scan_done",     False),
    ("scan_running",  False),
    ("scan_paused",   False),
    ("result_bytes",  None),
    ("failed_bytes",  None),
    ("partial_bytes", None),
    ("stop_event",    None),
    ("pause_event",   None),
    ("_scan_ctx",     None),
    ("_run_id",       0),
]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

if not _FITZ_OK:
    st.warning("⚠️ PyMuPDF not installed — PDFs will be read as plain text (install `pymupdf` for full PDF support).")

def _log(msg, tag=""):
    css = {"ok":"log-ok","err":"log-err","warn":"log-warn","info":"log-info","dim":"log-dim"}.get(tag,"")
    ts  = time.strftime("%H:%M:%S")
    st.session_state.log_lines.append(f'<span class="{css}">[{ts}] {msg}</span>')
    if len(st.session_state.log_lines) > 600:
        st.session_state.log_lines = st.session_state.log_lines[-600:]

def _render_log():
    log_ph.markdown('<div class="log-box">' + "\n".join(st.session_state.log_lines) + '</div>',
                    unsafe_allow_html=True)

# ── Layout ─────────────────────────────────────────────────────────────
st.markdown('<div class="sec">📈 Live Stats</div>', unsafe_allow_html=True)
sc1,sc2,sc3,sc4,sc5,sc6 = st.columns(6)
total_ph   = sc1.empty(); total_ph.metric("Total Rows","—")
jobs_ph    = sc2.empty(); jobs_ph.metric("Jobs","—")
done_ph    = sc3.empty(); done_ph.metric("Completed","0")
failed_ph  = sc4.empty(); failed_ph.metric("Failed","0")
chunk_ph   = sc5.empty(); chunk_ph.metric("Chunk","—")
circuit_ph = sc6.empty(); circuit_ph.metric("CB Errors","0")

st.markdown('<div class="sec">📊 Progress</div>', unsafe_allow_html=True)
prog_bar  = st.progress(0)
prog_text = st.empty()
status_ph = st.empty()

st.markdown('<div class="sec">🚀 Actions</div>', unsafe_allow_html=True)
ac1,ac2,ac3,ac4,_ar = st.columns([1,1,1,1,3])
run_btn   = ac1.button("▶ RUN SCAN",  use_container_width=True, disabled=st.session_state.scan_running)
plbl      = "▶ RESUME" if st.session_state.scan_paused else "⏸ PAUSE"
pause_btn = ac2.button(plbl,           use_container_width=True, disabled=not st.session_state.scan_running)
stop_btn  = ac3.button("⏹ STOP",      use_container_width=True, disabled=not st.session_state.scan_running)
save_btn  = ac4.button("💾 SAVE NOW", use_container_width=True, disabled=not st.session_state.scan_running)

st.markdown('<div class="sec">🖥 Log</div>', unsafe_allow_html=True)
log_ph = st.empty()

st.markdown('<div class="sec">✅ Results</div>', unsafe_allow_html=True)
results_box = st.container()
_render_log()

# Persistent download buttons
_rid = st.session_state._run_id
with results_box:
    if st.session_state.result_bytes:
        _d1, _d2 = st.columns(2)
        _d1.download_button("📥 Download Results (.xlsx)",
                            data=st.session_state.result_bytes,
                            file_name="scan_results.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"dl_r_{_rid}")
        if st.session_state.failed_bytes:
            _d2.download_button("⚠️ Download Failed Rows (.xlsx)",
                                data=st.session_state.failed_bytes,
                                file_name="scan_failed.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"dl_f_{_rid}")
    if st.session_state.partial_bytes and not st.session_state.result_bytes:
        st.download_button("💾 Download Partial Results (.xlsx)",
                           data=st.session_state.partial_bytes,
                           file_name="scan_partial.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           key=f"dl_p_{_rid}")


# ── Control buttons ────────────────────────────────────────────────────
def _do_save(label=""):
    ctx = st.session_state._scan_ctx
    if not ctx:
        return
    with ctx["lock"]:
        snap = list(ctx["all_results"])
    if not snap:
        return
    pb = _partial_excel(ctx["df_work"], snap, ctx["auto_keywords"])
    if pb:
        st.session_state.partial_bytes = pb
        _log(f"💾 {label} — {len(snap)} rows saved.", "ok")

if pause_btn and st.session_state.scan_running:
    pe = st.session_state.pause_event
    if pe:
        if st.session_state.scan_paused:
            pe.clear(); st.session_state.scan_paused = False; _log("▶ Resumed.", "ok")
        else:
            pe.set();   st.session_state.scan_paused = True;  _log("⏸ Paused.", "warn")
    st.rerun()

if stop_btn and st.session_state.scan_running:
    se = st.session_state.stop_event
    pe = st.session_state.pause_event
    if se: se.set()
    if pe: pe.clear()
    _log("⏹ Stop requested …", "warn")
    _do_save("Stop save")
    st.rerun()

if save_btn and st.session_state.scan_running:
    _do_save("Manual save")
    st.rerun()


# ══════════════════════════════════════════════════════════════════════
#  RUN
# ══════════════════════════════════════════════════════════════════════

if run_btn:
    if not links_file:
        st.error("❌ Upload a Links Sheet (.xlsx) first.")
        st.stop()

    auto_keywords = [k.strip() for k in auto_text.splitlines() if k.strip()]
    mil_keywords  = [k.strip() for k in mil_text.splitlines()  if k.strip()]
    if not auto_keywords and not mil_keywords:
        st.error("❌ Enter at least one keyword.")
        st.stop()

    auto_patterns = [_build_pattern(k) for k in auto_keywords]
    mil_patterns  = [_build_pattern(k) for k in mil_keywords]

    # Reset
    st.session_state.log_lines     = []
    st.session_state.scan_done     = False
    st.session_state.scan_running  = True
    st.session_state.scan_paused   = False
    st.session_state.result_bytes  = None
    st.session_state.failed_bytes  = None
    st.session_state.partial_bytes = None
    st.session_state._run_id      += 1

    stop_event  = threading.Event()
    pause_event = threading.Event()
    st.session_state.stop_event  = stop_event
    st.session_state.pause_event = pause_event

    _log(f"Workers={n_workers} | RPM={rpm} | CB={cb_errors}err→{cb_pause}s", "info")
    _log(f"HTML={MAX_HTML_BYTES//1024}KB | PDF={MAX_PDF_BYTES//1024//1024}MB | fitz={_FITZ_OK}", "info")
    _log(f"Automotive keywords: {len(auto_keywords)} | Military keywords: {len(mil_keywords)}", "info")

    # Load Excel
    try:
        raw_bytes = links_file.read()
        df = pd.read_excel(io.BytesIO(raw_bytes), dtype=str)
    except Exception as e:
        st.session_state.scan_running = False
        st.error(f"Cannot open file: {e}")
        st.stop()

    # Normalise columns (strip BOM + whitespace)
    df.columns = [str(c).strip().lstrip("\ufeff").lower() for c in df.columns]
    url_cn  = url_col.strip().lower()
    part_cn = part_col.strip().lower()

    if url_cn not in df.columns:
        st.session_state.scan_running = False
        st.error(f"❌ Column '{url_col}' not found. Available: {', '.join(df.columns)}")
        st.stop()

    if part_cn not in df.columns:
        _log(f"⚠ Column '{part_col}' not found — Part_Scanned=FALSE for all.", "warn")
        df[part_cn] = ""

    df[url_cn]  = df[url_cn].apply(_safe_str)
    df[part_cn] = df[part_cn].apply(_safe_str)
    df_work = df.copy()

    all_jobs = [
        (idx, row[url_cn], row[part_cn])
        for idx, row in df_work.iterrows()
        if row[url_cn] and row[url_cn].lower() not in ("nan", "")
    ]

    total_rows = len(df_work)
    total_jobs = len(all_jobs)
    total_ph.metric("Total Rows", total_rows)
    jobs_ph.metric("Jobs", total_jobs)
    _log(f"Rows: {total_rows} | Jobs with URL: {total_jobs}", "info")

    chunks       = [all_jobs[i:i+chunk_size] for i in range(0, total_jobs, chunk_size)]
    total_chunks = len(chunks)
    _log(f"Chunks: {total_chunks} × up to {chunk_size}", "info")
    _render_log()

    rl  = RateLimiter(rpm)
    cb  = CircuitBreaker(cb_errors, cb_pause)
    ses = requests.Session()

    results_lock = threading.Lock()
    all_results  = []
    failed       = []
    done_cnt     = 0

    st.session_state._scan_ctx = {
        "df_work":      df_work,
        "all_results":  all_results,
        "auto_keywords":auto_keywords,
        "lock":         results_lock,
    }

    status_ph.info("● RUNNING")
    user_stopped = False

    try:
        for ci, chunk_jobs in enumerate(chunks):
            if stop_event.is_set():
                user_stopped = True; break

            chunk_ph.metric("Chunk", f"{ci+1}/{total_chunks}")
            _log(f"── Chunk {ci+1}/{total_chunks} ({len(chunk_jobs)} rows) ──", "info")

            with ThreadPoolExecutor(max_workers=n_workers) as pool:
                futs = {
                    pool.submit(
                        _process_row,
                        ri, url, part,
                        auto_keywords, mil_keywords,
                        auto_patterns, mil_patterns,
                        rl, cb, ses,
                        stop_event, pause_event
                    ): (ri, url, part)
                    for ri, url, part in chunk_jobs
                }

                for fut in as_completed(futs):
                    ri, url, part = futs[fut]
                    blank = {"_row_index":ri,"_scan_url":url,"_scan_part":part,
                             **{k:0 for k in auto_keywords},"Military":0,"Part_Scanned":"FALSE"}
                    try:
                        res = fut.result()
                        with results_lock:
                            all_results.append(res)
                        done_cnt += 1
                        short = (url[:55]+"…") if len(url)>55 else url
                        tag   = "ok" if res.get("Part_Scanned")=="TRUE" else "dim"
                        _log(f"✓ [{ri}] {short} | Part={res['Part_Scanned']} Mil={res['Military']}", tag)
                    except InterruptedError:
                        with results_lock: all_results.append(blank)
                        failed.append({"row":ri,"url":url,"error":"Stopped"})
                        done_cnt += 1
                    except Exception as e:
                        with results_lock: all_results.append(blank)
                        failed.append({"row":ri,"url":url,"error":str(e)})
                        done_cnt += 1
                        _log(f"✗ [{ri}] {url[:55]} | {e}", "err")

                    # Auto-save after every row
                    with results_lock:
                        snap = list(all_results)
                    pb = _partial_excel(df_work, snap, auto_keywords)
                    if pb:
                        st.session_state.partial_bytes = pb

                    pct = min(int(done_cnt / total_jobs * 100), 100) if total_jobs else 0
                    prog_bar.progress(pct)
                    prog_text.text(f"{done_cnt} / {total_jobs}  ({pct}%)")
                    done_ph.metric("Completed", done_cnt)
                    failed_ph.metric("Failed",    len(failed))
                    circuit_ph.metric("CB Errors", cb.error_count)

                    if stop_event.is_set():
                        user_stopped = True; break

            _render_log()
            if user_stopped:
                break

            if ci < total_chunks - 1:
                p = random.uniform(delay_min * 2, delay_max * 2)
                _log(f"⏸ Pause {p:.1f}s …", "dim")
                time.sleep(p)

    except Exception as outer:
        _log(f"💥 Outer error: {outer}", "err")
        _log(traceback.format_exc(), "err")
        with results_lock: snap = list(all_results)
        pb = _partial_excel(df_work, snap, auto_keywords)
        if pb:
            st.session_state.partial_bytes = pb
            _log(f"💾 Emergency save: {len(snap)} rows.", "warn")

    finally:
        ses.close()
        st.session_state.scan_running = False
        st.session_state.scan_paused  = False
        st.session_state._scan_ctx    = None

    with results_lock:
        final = list(all_results)

    if not final:
        st.warning("No results collected.")
        st.stop()

    df_final = _apply_results(df_work.copy(), final, auto_keywords)

    # Restore original column capitalisation
    try:
        orig_cols = {str(c).strip().lstrip("\ufeff").lower(): str(c).strip()
                     for c in pd.read_excel(io.BytesIO(raw_bytes), nrows=0).columns}
        df_final.rename(columns=orig_cols, inplace=True)
    except Exception:
        pass

    result_bytes = _to_excel_bytes(df_final)
    st.session_state.result_bytes  = result_bytes
    st.session_state.partial_bytes = None

    if user_stopped:
        status_ph.warning("● STOPPED")
        _log("⏹ Stopped — download partial results below.", "warn")
        st.session_state.partial_bytes = result_bytes
    else:
        st.session_state.scan_done = True
        status_ph.success("● COMPLETE")
        prog_bar.progress(100)
        _log("✅ SCAN COMPLETE", "ok")

    if failed:
        fb = io.BytesIO()
        pd.DataFrame(failed).to_excel(fb, index=False)
        st.session_state.failed_bytes = fb.getvalue()
        _log(f"Failed rows: {len(failed)}", "warn")

    _log("═"*52, "dim")
    _log(f"Total rows     : {len(df_final)}", "ok")
    _log(f"Part_Scanned=T : {(df_final.get('Part_Scanned','')=='TRUE').sum()}", "ok")
    _log(f"Military=1     : {(df_final.get('Military',0)==1).sum()}", "ok")
    for kw in auto_keywords:
        if kw in df_final.columns:
            _log(f"  {kw}: {(df_final[kw]==1).sum()} hits", "ok")
    _render_log()

    rid = st.session_state._run_id
    with results_box:
        _d1, _d2 = st.columns(2)
        _d1.download_button("📥 Download Results (.xlsx)",
                            data=result_bytes,
                            file_name="scan_results.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"dl_r_run_{rid}")
        if st.session_state.failed_bytes:
            _d2.download_button("⚠️ Download Failed Rows (.xlsx)",
                                data=st.session_state.failed_bytes,
                                file_name="scan_failed.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"dl_f_run_{rid}")
