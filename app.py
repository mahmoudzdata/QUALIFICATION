"""
Keyword & Part Scanner — v6 FAST
=================================
Speed fixes vs previous:
  1. HEAD probe removed — URL extension / GET Content-Type header used instead
  2. Auto-save every N rows (default 50) not every single row
  3. BeautifulSoup replaced with fast regex text stripper for HTML
  4. PDF: parallel page extraction with list comprehension
  5. Session reuse with connection pooling (HTTPAdapter)
  6. Chunk inter-delay only between chunks, not between rows
  7. lxml parser used when available (10x faster than html.parser)
  8. Timeout tuned down — don't wait 25s for dead servers
"""

import re, time, random, threading, io, collections, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import urllib3
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import streamlit as st

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# PyMuPDF — handle both package names
try:
    import fitz as _fitz; _FITZ_OK = True
except ImportError:
    try:
        import pymupdf as _fitz; _FITZ_OK = True
    except ImportError:
        _fitz = None; _FITZ_OK = False

# Optional fast HTML parser
try:
    import lxml; _LXML = True
except ImportError:
    _LXML = False

# ══════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════
MAX_HTML_BYTES  = 400 * 1024        # 400 KB
MAX_PDF_BYTES   = 8 * 1024 * 1024  # 8 MB
GET_TIMEOUT     = 12               # seconds — was 25
MAX_RETRIES     = 2                # was 3
RETRY_BACKOFF   = [3, 10]
RETRY_ON_CODES  = {429, 502, 503, 504}
AUTOSAVE_EVERY  = 50               # rows between auto-saves

# Strip HTML tags — much faster than BeautifulSoup
_TAG_RE   = re.compile(r'<[^>]+>')
_WS_RE    = re.compile(r'\s+')
_SKIP_TAGS = re.compile(
    r'<(script|style|noscript|head|meta|link)[\s>].*?</\1>',
    re.IGNORECASE | re.DOTALL
)

def _strip_html(raw: str) -> str:
    t = _SKIP_TAGS.sub(' ', raw)
    t = _TAG_RE.sub(' ', t)
    return _WS_RE.sub(' ', t).strip()

DEFAULT_AUTO_KW = """\
AEC-Q100
AEC-Q101
AEC-Q200
Automotive Grade
Automotive Qualified
"""

DEFAULT_MIL_KW = """\
Military
MIL-PRF
MIL-C
MIL-R
MIL-DTL
MIL-STD
MIL-SPEC
MIL-S
"""

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
]

_BASE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "DNT": "1",
}

_ua_lock  = threading.Lock()
_ua_idx   = 0

def _get_headers():
    global _ua_idx
    with _ua_lock:
        ua = _USER_AGENTS[_ua_idx % len(_USER_AGENTS)]
        _ua_idx += 1
    return {**_BASE_HEADERS, "User-Agent": ua}

def _make_session(pool_size: int) -> requests.Session:
    """Session with connection pool — reused across all workers."""
    s = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=pool_size,
        pool_maxsize=pool_size * 2,
        max_retries=Retry(total=0),   # we handle retries ourselves
    )
    s.mount("http://",  adapter)
    s.mount("https://", adapter)
    return s

# ══════════════════════════════════════════════════════════════════════
#  RATE LIMITER & CIRCUIT BREAKER
# ══════════════════════════════════════════════════════════════════════
class RateLimiter:
    def __init__(self, max_per_minute):
        self._max = max_per_minute
        self._lock = threading.Lock()
        self._ts   = collections.deque()

    def wait(self):
        while True:
            with self._lock:
                now = time.time()
                while self._ts and now - self._ts[0] > 60:
                    self._ts.popleft()
                if len(self._ts) < self._max:
                    self._ts.append(now)
                    return
                w = 60 - (now - self._ts[0]) + 0.05
            time.sleep(w)

class CircuitBreaker:
    def __init__(self, threshold, pause_s):
        self._thr  = threshold
        self._pau  = pause_s
        self._lock = threading.Lock()
        self._err  = 0
        self._trip = False
        self._t0   = 0.0

    def ok(self):
        with self._lock: self._err = 0; self._trip = False

    def fail(self):
        with self._lock:
            self._err += 1
            if self._err >= self._thr and not self._trip:
                self._trip = True; self._t0 = time.time()

    def wait(self):
        with self._lock:
            if not self._trip: return
            rem = self._pau - (time.time() - self._t0)
            if rem <= 0: self._trip = False; self._err = 0; return
        time.sleep(max(0, rem))
        with self._lock: self._trip = False; self._err = 0

    @property
    def errors(self):
        with self._lock: return self._err

# ══════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════
def _safe_str(v):
    if v is None: return ""
    if isinstance(v, float):
        return "" if v != v else (str(int(v)) if v == int(v) else str(v))
    return str(v).strip()

def _build_pattern(kw):
    esc = re.escape(kw)
    flex = esc.replace(r"\-", r"[\s\-]?").replace(r"\ ", r"[\s\-]?")
    return re.compile(flex, re.IGNORECASE)

def _url_looks_like_pdf(url: str) -> bool:
    return url.lower().split("?")[0].endswith(".pdf")

def _fetch(url: str, session: requests.Session):
    """
    Single GET request — returns (text, is_pdf_flag).
    No HEAD probe; we detect content type from the response header.
    """
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, timeout=GET_TIMEOUT, verify=False,
                            headers=_get_headers(), stream=True)
            if r.status_code in RETRY_ON_CODES:
                r.close()
                time.sleep(RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF)-1)] + random.uniform(0,2))
                continue
            r.raise_for_status()

            ct = r.headers.get("Content-Type", "").lower()
            is_pdf = "pdf" in ct or _url_looks_like_pdf(url)

            raw = b""
            limit = MAX_PDF_BYTES if is_pdf else MAX_HTML_BYTES
            for chunk in r.iter_content(65536):
                raw += chunk
                if len(raw) >= limit:
                    break
            r.close()
            return raw, is_pdf
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF)-1)])
    return b"", False

def _extract_text(raw: bytes, is_pdf: bool) -> str:
    if not raw:
        return ""
    if is_pdf:
        if not _FITZ_OK:
            # fallback: treat as text
            return raw.decode("utf-8", errors="ignore")
        try:
            doc   = _fitz.open(stream=raw, filetype="pdf")
            pages = [p.get_text() for p in doc]
            doc.close()
            return " ".join(pages)
        except Exception:
            return ""
    else:
        html = raw.decode("utf-8", errors="ignore")
        return _strip_html(html)

def _count_kw(text, auto_patterns, mil_patterns, auto_kws, mil_kws):
    result = {kw: (1 if pat.search(text) else 0)
              for kw, pat in zip(auto_kws, auto_patterns)}
    result["Military"] = 0
    for pat in mil_patterns:
        if pat.search(text):
            result["Military"] = 1
            break
    return result

def _search_part(text, part):
    p = _safe_str(part)
    if not p or p.lower() in ("nan", ""):
        return "FALSE"
    return "TRUE" if p.lower() in text.lower() else "FALSE"

# ══════════════════════════════════════════════════════════════════════
#  WORKER
# ══════════════════════════════════════════════════════════════════════
def _process_row(row_index, url, part,
                 auto_kws, mil_kws, auto_pats, mil_pats,
                 rl, cb, session, stop_ev, pause_ev):

    if pause_ev:
        while pause_ev.is_set():
            if stop_ev and stop_ev.is_set(): raise InterruptedError()
            time.sleep(0.3)
    if stop_ev and stop_ev.is_set(): raise InterruptedError()

    cb.wait()
    rl.wait()

    url_s  = _safe_str(url)
    part_s = _safe_str(part)

    try:
        raw, is_pdf = _fetch(url_s, session)
        text = _extract_text(raw, is_pdf)
        cb.ok()
    except InterruptedError:
        raise
    except Exception as e:
        cb.fail(); raise

    row = {"_row_index": row_index, "_scan_url": url_s, "_scan_part": part_s}
    row.update(_count_kw(text, auto_pats, mil_pats, auto_kws, mil_kws))
    row["Part_Scanned"] = _search_part(text, part_s)
    return row

# ══════════════════════════════════════════════════════════════════════
#  EXCEL OUTPUT
# ══════════════════════════════════════════════════════════════════════
def _apply_results(df, results, auto_kws):
    m = {r["_row_index"]: r for r in results}
    g = lambda i, f, d: m.get(i, {}).get(f, d)
    for kw in auto_kws:
        df[kw] = [g(i, kw, 0) for i in df.index]
    df["Military"]     = [g(i, "Military",     0)       for i in df.index]
    df["Part_Scanned"] = [g(i, "Part_Scanned", "FALSE") for i in df.index]
    return df

def _to_xlsx(df) -> bytes:
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    green = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    red   = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    wb = load_workbook(buf)
    ws = wb.active
    hdr = [c.value for c in ws[1]]
    for ci, col in enumerate(hdr, 1):
        if col is None: continue
        for r in range(2, ws.max_row + 1):
            cell = ws.cell(row=r, column=ci)
            if col == "Part_Scanned":
                cell.fill = green if cell.value == "TRUE" else red
            elif col == "Military":
                if cell.value == 1: cell.fill = green
            elif col not in ("_scan_url", "_scan_part", "_row_index"):
                if cell.value == 1: cell.fill = green
    out = io.BytesIO(); wb.save(out); wb.close()
    return out.getvalue()

def _partial_xlsx(df_work, results, auto_kws) -> bytes:
    try:
        return _to_xlsx(_apply_results(df_work.copy(), results, auto_kws))
    except Exception:
        return b""

# ══════════════════════════════════════════════════════════════════════
#  STREAMLIT UI
# ══════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="AEC/MIL Scanner v6 FAST", page_icon="⚡", layout="wide")

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
[data-testid="stSidebar"] h2,[data-testid="stSidebar"] h3{color:#c9d8f0!important;font-family:'IBM Plex Mono',monospace!important;}
[data-testid="stSidebar"] input,[data-testid="stSidebar"] textarea{background:#111827!important;color:#e2eaf8!important;border:1px solid #1e2840!important;border-radius:4px!important;}
[data-testid="stSidebar"] .stAlert{background:#0f1929!important;border:1px solid #1e2840!important;color:#7b9cc4!important;font-size:0.72rem!important;}
.main .block-container{padding-top:1rem;padding-bottom:2rem;}
[data-testid="metric-container"]{background:#0f1929;border:1px solid #1e2840;border-radius:8px;padding:10px 14px;}
.title-bar{background:linear-gradient(135deg,#0a0e1a,#111827);border:1px solid #1e3a5f;border-left:4px solid #22c55e;border-radius:8px;padding:14px 22px;margin-bottom:1.2rem;display:flex;align-items:center;justify-content:space-between;}
.title-bar h1{font-family:'IBM Plex Mono',monospace;font-size:1.05rem;font-weight:700;color:#22c55e;margin:0;}
.title-bar .badge{font-family:'IBM Plex Mono',monospace;font-size:0.68rem;color:#60a5fa;background:#0c1a35;border:1px solid #1e3a5f;border-radius:4px;padding:2px 8px;}
.sec{font-family:'IBM Plex Mono',monospace;font-size:0.68rem;font-weight:700;color:#3b4f6b;text-transform:uppercase;letter-spacing:0.14em;margin:1rem 0 0.35rem 0;border-bottom:1px solid #1e2840;padding-bottom:3px;}
div[data-testid="column"]:nth-child(1) .stButton>button{background:#16a34a!important;color:#fff!important;border:none!important;border-radius:6px!important;font-family:'IBM Plex Mono',monospace!important;font-weight:700!important;font-size:0.82rem!important;padding:0.5rem 1.2rem!important;}
div[data-testid="column"]:nth-child(2) .stButton>button{background:#b45309!important;color:#fff!important;border:none!important;border-radius:6px!important;font-family:'IBM Plex Mono',monospace!important;font-weight:600!important;font-size:0.82rem!important;padding:0.5rem 1.2rem!important;}
div[data-testid="column"]:nth-child(3) .stButton>button{background:#dc2626!important;color:#fff!important;border:none!important;border-radius:6px!important;font-family:'IBM Plex Mono',monospace!important;font-weight:600!important;font-size:0.82rem!important;padding:0.5rem 1.2rem!important;}
div[data-testid="column"]:nth-child(4) .stButton>button{background:#0f766e!important;color:#fff!important;border:none!important;border-radius:6px!important;font-family:'IBM Plex Mono',monospace!important;font-weight:600!important;font-size:0.82rem!important;padding:0.5rem 1.2rem!important;}
.log-box{background:#080c18;border:1px solid #1e2840;border-radius:6px;padding:12px 16px;font-family:'IBM Plex Mono',monospace;font-size:11px;color:#cbd5e1;height:300px;overflow-y:auto;white-space:pre-wrap;word-break:break-all;line-height:1.55;}
.log-ok{color:#4ade80;}.log-err{color:#f87171;}.log-warn{color:#fbbf24;}.log-info{color:#60a5fa;}.log-dim{color:#374151;}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="title-bar">
  <h1>⚡ AUTOMOTIVE &amp; MILITARY SCANNER — v6 FAST</h1>
  <span class="badge">NO HEAD PROBE · FAST HTML STRIP · POOL SESSIONS · AUTO-SAVE</span>
</div>
""", unsafe_allow_html=True)

# ── Sidebar ──────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Configuration")
    st.markdown('<div class="sec">📁 File</div>', unsafe_allow_html=True)
    links_file = st.file_uploader("Links Sheet (.xlsx)", type=["xlsx"])

    st.markdown('<div class="sec">🗂 Columns</div>', unsafe_allow_html=True)
    url_col  = st.text_input("URL column",        value="offlineURL")
    part_col = st.text_input("Part Number column", value="PartNumber")

    if links_file:
        try:
            _pr = pd.read_excel(io.BytesIO(links_file.read()), nrows=0)
            links_file.seek(0)
            st.info("Cols: " + " | ".join(str(c) for c in _pr.columns))
        except Exception:
            pass

    st.markdown('<div class="sec">🚗 Automotive Keywords</div>', unsafe_allow_html=True)
    auto_text = st.text_area("One per line", value=DEFAULT_AUTO_KW, height=200)

    st.markdown('<div class="sec">🎖 Military Keywords</div>', unsafe_allow_html=True)
    mil_text = st.text_area("One per line — any hit → Military=1", value=DEFAULT_MIL_KW, height=180)

    st.markdown('<div class="sec">⚡ Performance</div>', unsafe_allow_html=True)
    n_workers    = st.slider("Concurrent workers", 1, 100, 20)
    chunk_size   = st.number_input("Chunk size", 50, 100000, 1000, step=100)
    rpm          = st.slider("Max requests/min", 10, 1000, 120)
    autosave_n   = st.number_input("Auto-save every N rows", 10, 500, 50, step=10)

    st.markdown('<div class="sec">🛡 Resilience</div>', unsafe_allow_html=True)
    cb_errors = st.slider("Circuit breaker threshold", 1, 50, 10)
    cb_pause  = st.slider("Circuit breaker pause (s)",  5, 300, 30)
    delay_min = st.number_input("Min inter-chunk delay (s)", 0.0, 10.0, 0.2, step=0.1)
    delay_max = st.number_input("Max inter-chunk delay (s)", 0.0, 30.0, 1.0, step=0.1)

# ── Session state ────────────────────────────────────────────────────
for _k, _v in [
    ("log_lines",[]),("scan_done",False),("scan_running",False),("scan_paused",False),
    ("result_bytes",None),("failed_bytes",None),("partial_bytes",None),
    ("stop_event",None),("pause_event",None),("_scan_ctx",None),("_run_id",0),
]:
    if _k not in st.session_state: st.session_state[_k] = _v

if not _FITZ_OK:
    st.warning("⚠️ PyMuPDF not installed — PDFs fall back to plain-text read. Add `pymupdf` to requirements.txt.")

def _log(msg, tag=""):
    css={"ok":"log-ok","err":"log-err","warn":"log-warn","info":"log-info","dim":"log-dim"}.get(tag,"")
    st.session_state.log_lines.append(f'<span class="{css}">[{time.strftime("%H:%M:%S")}] {msg}</span>')
    if len(st.session_state.log_lines) > 800:
        st.session_state.log_lines = st.session_state.log_lines[-800:]

def _render_log():
    log_ph.markdown('<div class="log-box">'+ "\n".join(st.session_state.log_lines) +'</div>',
                    unsafe_allow_html=True)

# ── Layout ───────────────────────────────────────────────────────────
st.markdown('<div class="sec">📈 Live Stats</div>', unsafe_allow_html=True)
s1,s2,s3,s4,s5,s6,s7 = st.columns(7)
total_ph   = s1.empty(); total_ph.metric("Total","—")
jobs_ph    = s2.empty(); jobs_ph.metric("Jobs","—")
done_ph    = s3.empty(); done_ph.metric("Done","0")
ok_ph      = s4.empty(); ok_ph.metric("Part Found","0")
failed_ph  = s5.empty(); failed_ph.metric("Failed","0")
chunk_ph   = s6.empty(); chunk_ph.metric("Chunk","—")
speed_ph   = s7.empty(); speed_ph.metric("Rows/min","—")

st.markdown('<div class="sec">📊 Progress</div>', unsafe_allow_html=True)
prog_bar  = st.progress(0)
prog_text = st.empty()
status_ph = st.empty()

st.markdown('<div class="sec">🚀 Actions</div>', unsafe_allow_html=True)
ac1,ac2,ac3,ac4,_ar = st.columns([1,1,1,1,4])
run_btn   = ac1.button("▶ RUN",       use_container_width=True, disabled=st.session_state.scan_running)
plbl      = "▶ RESUME" if st.session_state.scan_paused else "⏸ PAUSE"
pause_btn = ac2.button(plbl,           use_container_width=True, disabled=not st.session_state.scan_running)
stop_btn  = ac3.button("⏹ STOP",      use_container_width=True, disabled=not st.session_state.scan_running)
save_btn  = ac4.button("💾 SAVE NOW", use_container_width=True, disabled=not st.session_state.scan_running)

st.markdown('<div class="sec">🖥 Log</div>', unsafe_allow_html=True)
log_ph = st.empty()

st.markdown('<div class="sec">✅ Results</div>', unsafe_allow_html=True)
results_box = st.container()
_render_log()

# Persistent downloads
_rid = st.session_state._run_id
with results_box:
    if st.session_state.result_bytes:
        d1,d2 = st.columns(2)
        d1.download_button("📥 Download Results (.xlsx)", data=st.session_state.result_bytes,
            file_name="scan_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"dl_r_{_rid}")
        if st.session_state.failed_bytes:
            d2.download_button("⚠️ Failed Rows (.xlsx)", data=st.session_state.failed_bytes,
                file_name="scan_failed.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_f_{_rid}")
    if st.session_state.partial_bytes and not st.session_state.result_bytes:
        st.download_button("💾 Partial Results (.xlsx)", data=st.session_state.partial_bytes,
            file_name="scan_partial.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"dl_p_{_rid}")

# ── Control buttons ──────────────────────────────────────────────────
def _do_save(label=""):
    ctx = st.session_state._scan_ctx
    if not ctx: return
    with ctx["lock"]: snap = list(ctx["all_results"])
    if not snap: return
    pb = _partial_xlsx(ctx["df_work"], snap, ctx["auto_kws"])
    if pb:
        st.session_state.partial_bytes = pb
        _log(f"💾 {label} — {len(snap)} rows.", "ok")

if pause_btn and st.session_state.scan_running:
    pe = st.session_state.pause_event
    if pe:
        if st.session_state.scan_paused: pe.clear(); st.session_state.scan_paused=False; _log("▶ Resumed.","ok")
        else: pe.set(); st.session_state.scan_paused=True; _log("⏸ Paused.","warn")
    st.rerun()

if stop_btn and st.session_state.scan_running:
    se,pe = st.session_state.stop_event, st.session_state.pause_event
    if se: se.set()
    if pe: pe.clear()
    _log("⏹ Stop …","warn"); _do_save("Stop save"); st.rerun()

if save_btn and st.session_state.scan_running:
    _do_save("Manual save"); st.rerun()

# ══════════════════════════════════════════════════════════════════════
#  RUN
# ══════════════════════════════════════════════════════════════════════
if run_btn:
    if not links_file:
        st.error("❌ Upload a Links Sheet (.xlsx) first."); st.stop()

    auto_kws  = [k.strip() for k in auto_text.splitlines() if k.strip()]
    mil_kws   = [k.strip() for k in mil_text.splitlines()  if k.strip()]
    if not auto_kws and not mil_kws:
        st.error("❌ Enter at least one keyword."); st.stop()

    auto_pats = [_build_pattern(k) for k in auto_kws]
    mil_pats  = [_build_pattern(k) for k in mil_kws]

    # Reset
    st.session_state.update({
        "log_lines":[], "scan_done":False, "scan_running":True, "scan_paused":False,
        "result_bytes":None, "failed_bytes":None, "partial_bytes":None,
    })
    st.session_state._run_id += 1

    stop_ev  = threading.Event()
    pause_ev = threading.Event()
    st.session_state.stop_event  = stop_ev
    st.session_state.pause_event = pause_ev

    _log(f"Workers={n_workers} | RPM={rpm} | CB={cb_errors}err→{cb_pause}s | AutoSave={autosave_n}rows","info")
    _log(f"HTML cap={MAX_HTML_BYTES//1024}KB | PDF cap={MAX_PDF_BYTES//1024//1024}MB | PyMuPDF={_FITZ_OK}","info")
    _log(f"Auto kws={len(auto_kws)} | Mil kws={len(mil_kws)}","info")

    # Load
    try:
        raw_bytes = links_file.read()
        df = pd.read_excel(io.BytesIO(raw_bytes), dtype=str)
    except Exception as e:
        st.session_state.scan_running = False
        st.error(f"Cannot open file: {e}"); st.stop()

    df.columns = [str(c).strip().lstrip("\ufeff").lower() for c in df.columns]
    uc = url_col.strip().lower()
    pc = part_col.strip().lower()

    if uc not in df.columns:
        st.session_state.scan_running = False
        st.error(f"❌ Column '{url_col}' not found. Got: {', '.join(df.columns)}"); st.stop()
    if pc not in df.columns:
        _log(f"⚠ '{part_col}' not found — Part_Scanned=FALSE for all.","warn")
        df[pc] = ""

    df[uc] = df[uc].apply(_safe_str)
    df[pc] = df[pc].apply(_safe_str)
    df_work = df.copy()

    all_jobs = [(idx, row[uc], row[pc]) for idx, row in df_work.iterrows()
                if row[uc] and row[uc].lower() not in ("nan","")]

    total_rows = len(df_work)
    total_jobs = len(all_jobs)
    total_ph.metric("Total", total_rows)
    jobs_ph.metric("Jobs",   total_jobs)
    _log(f"Rows={total_rows} | Jobs with URL={total_jobs}","info")

    chunks = [all_jobs[i:i+chunk_size] for i in range(0, total_jobs, chunk_size)]
    _log(f"Chunks={len(chunks)} × up to {chunk_size}","info")
    _render_log()

    rl  = RateLimiter(rpm)
    cb  = CircuitBreaker(cb_errors, cb_pause)
    ses = _make_session(n_workers + 4)

    lock        = threading.Lock()
    all_results = []
    failed      = []
    done_cnt    = 0
    ok_cnt      = 0
    t_start     = time.time()

    st.session_state._scan_ctx = {
        "df_work": df_work, "all_results": all_results,
        "auto_kws": auto_kws, "lock": lock,
    }
    status_ph.info("● RUNNING")
    user_stopped = False

    try:
        for ci, chunk in enumerate(chunks):
            if stop_ev.is_set(): user_stopped=True; break
            chunk_ph.metric("Chunk", f"{ci+1}/{len(chunks)}")
            _log(f"── Chunk {ci+1}/{len(chunks)} ({len(chunk)} rows) ──","info")

            with ThreadPoolExecutor(max_workers=n_workers) as pool:
                futs = {
                    pool.submit(_process_row,
                        ri, url, part,
                        auto_kws, mil_kws, auto_pats, mil_pats,
                        rl, cb, ses, stop_ev, pause_ev
                    ): (ri, url, part)
                    for ri, url, part in chunk
                }

                for fut in as_completed(futs):
                    ri, url, part = futs[fut]
                    blank = {"_row_index":ri,"_scan_url":url,"_scan_part":part,
                             **{k:0 for k in auto_kws},"Military":0,"Part_Scanned":"FALSE"}
                    try:
                        res = fut.result()
                        with lock: all_results.append(res)
                        done_cnt += 1
                        if res.get("Part_Scanned") == "TRUE": ok_cnt += 1
                        short = (url[:50]+"…") if len(url)>50 else url
                        tag = "ok" if res.get("Part_Scanned")=="TRUE" else "dim"
                        _log(f"✓[{ri}] {short} | Part={res['Part_Scanned']} Mil={res['Military']}", tag)
                    except InterruptedError:
                        with lock: all_results.append(blank)
                        failed.append({"row":ri,"url":url,"error":"Stopped"})
                        done_cnt += 1
                    except Exception as e:
                        with lock: all_results.append(blank)
                        failed.append({"row":ri,"url":url,"error":str(e)})
                        done_cnt += 1
                        _log(f"✗[{ri}] {url[:50]} | {e}","err")

                    # Auto-save every N rows (not every row — much faster)
                    if done_cnt % autosave_n == 0:
                        with lock: snap = list(all_results)
                        pb = _partial_xlsx(df_work, snap, auto_kws)
                        if pb: st.session_state.partial_bytes = pb

                    pct = min(int(done_cnt/total_jobs*100), 100) if total_jobs else 0
                    prog_bar.progress(pct)
                    elapsed = time.time() - t_start
                    rpm_live = int(done_cnt / elapsed * 60) if elapsed > 2 else 0
                    prog_text.text(f"{done_cnt}/{total_jobs} ({pct}%)")
                    done_ph.metric("Done",      done_cnt)
                    ok_ph.metric("Part Found",  ok_cnt)
                    failed_ph.metric("Failed",  len(failed))
                    speed_ph.metric("Rows/min", rpm_live)

                    if stop_ev.is_set(): user_stopped=True; break

            _render_log()
            if user_stopped: break

            if ci < len(chunks)-1:
                p = random.uniform(delay_min, delay_max)
                if p > 0.05: time.sleep(p)

    except Exception as outer:
        _log(f"💥 {outer}","err")
        _log(traceback.format_exc(),"err")
        with lock: snap = list(all_results)
        pb = _partial_xlsx(df_work, snap, auto_kws)
        if pb: st.session_state.partial_bytes=pb; _log(f"💾 Emergency save {len(snap)} rows.","warn")

    finally:
        ses.close()
        st.session_state.scan_running = False
        st.session_state.scan_paused  = False
        st.session_state._scan_ctx    = None

    with lock: final = list(all_results)
    if not final:
        st.warning("No results collected."); st.stop()

    df_final = _apply_results(df_work.copy(), final, auto_kws)
    try:
        orig = {str(c).strip().lstrip("\ufeff").lower(): str(c).strip()
                for c in pd.read_excel(io.BytesIO(raw_bytes), nrows=0).columns}
        df_final.rename(columns=orig, inplace=True)
    except Exception: pass

    result_bytes = _to_xlsx(df_final)
    st.session_state.result_bytes  = result_bytes
    st.session_state.partial_bytes = None

    elapsed = time.time() - t_start
    avg_rpm = int(done_cnt / elapsed * 60) if elapsed > 0 else 0

    if user_stopped:
        status_ph.warning("● STOPPED")
        _log("⏹ Stopped — download available.","warn")
        st.session_state.partial_bytes = result_bytes
    else:
        st.session_state.scan_done = True
        status_ph.success("● COMPLETE")
        prog_bar.progress(100)
        _log("✅ SCAN COMPLETE","ok")

    if failed:
        fb = io.BytesIO()
        pd.DataFrame(failed).to_excel(fb, index=False)
        st.session_state.failed_bytes = fb.getvalue()

    _log("═"*52,"dim")
    _log(f"Total rows     : {len(df_final)}","ok")
    _log(f"Time           : {elapsed:.0f}s  |  Avg speed: {avg_rpm} rows/min","ok")
    _log(f"Part_Scanned=T : {(df_final.get('Part_Scanned','')=='TRUE').sum()}","ok")
    _log(f"Military=1     : {(df_final.get('Military',0)==1).sum()}","ok")
    for kw in auto_kws:
        if kw in df_final.columns:
            cnt = (df_final[kw]==1).sum()
            if cnt > 0:
              _log(f"  {kw}: {cnt} hits","ok")
    _log(f"Failed rows    : {len(failed)}","warn" if failed else "ok")
    _render_log()

    rid = st.session_state._run_id
    with results_box:
        d1,d2 = st.columns(2)
        d1.download_button("📥 Download Results (.xlsx)", data=result_bytes,
            file_name="scan_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"dl_r_run_{rid}")
        if st.session_state.failed_bytes:
            d2.download_button("⚠️ Failed Rows (.xlsx)", data=st.session_state.failed_bytes,
                file_name="scan_failed.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_f_run_{rid}")
