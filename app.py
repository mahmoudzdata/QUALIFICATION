"""
Keyword & Part Scanner — v7 STABLE
====================================
Fixes vs v6:
  1. Scan runs in a background thread — UI never freezes
  2. File read once into bytes, cached in session_state — no stream loss
  3. Upload accepts any filename (no extension check enforced)
  4. Auto-save removed — replaced with a single final save + manual Save Now
  5. ThreadPoolExecutor cancellation handled cleanly via stop_event
  6. Column name match is case-insensitive + strips BOM/spaces robustly
  7. Memory: results collected in a thread-safe queue, not appended to session_state live
  8. Progress updates via st.session_state shared dict — no mid-scan reruns
  9. Circuit breaker reset on success properly
 10. Session closed in finally block always
"""

import re, time, random, threading, io, collections, traceback, queue
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

# ── Optional deps ────────────────────────────────────────────────────
try:
    import fitz as _fitz; _FITZ_OK = True
except ImportError:
    try:
        import pymupdf as _fitz; _FITZ_OK = True
    except ImportError:
        _fitz = None; _FITZ_OK = False

# ══════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════
MAX_HTML_BYTES = 400 * 1024
MAX_PDF_BYTES  = 8 * 1024 * 1024
GET_TIMEOUT    = 12
MAX_RETRIES    = 2
RETRY_BACKOFF  = [3, 10]
RETRY_ON_CODES = {429, 502, 503, 504}

_TAG_RE    = re.compile(r'<[^>]+>')
_WS_RE     = re.compile(r'\s+')
_SKIP_TAGS = re.compile(
    r'<(script|style|noscript|head|meta|link)[\s>].*?</\1>',
    re.IGNORECASE | re.DOTALL,
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

_ua_lock = threading.Lock()
_ua_idx  = 0

def _get_headers():
    global _ua_idx
    with _ua_lock:
        ua = _USER_AGENTS[_ua_idx % len(_USER_AGENTS)]
        _ua_idx += 1
    return {**_BASE_HEADERS, "User-Agent": ua}

def _make_session(pool_size: int) -> requests.Session:
    s = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=pool_size,
        pool_maxsize=pool_size * 2,
        max_retries=Retry(total=0),
    )
    s.mount("http://",  adapter)
    s.mount("https://", adapter)
    return s

# ══════════════════════════════════════════════════════════════════════
#  RATE LIMITER & CIRCUIT BREAKER
# ══════════════════════════════════════════════════════════════════════
class RateLimiter:
    def __init__(self, max_per_minute):
        self._max  = max_per_minute
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
        with self._lock:
            self._err  = 0
            self._trip = False

    def fail(self):
        with self._lock:
            self._err += 1
            if self._err >= self._thr and not self._trip:
                self._trip = True
                self._t0   = time.time()

    def wait(self):
        with self._lock:
            if not self._trip:
                return
            rem = self._pau - (time.time() - self._t0)
            if rem <= 0:
                self._trip = False
                self._err  = 0
                return
        time.sleep(max(0, rem))
        with self._lock:
            self._trip = False
            self._err  = 0

    @property
    def errors(self):
        with self._lock:
            return self._err

# ══════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════
def _safe_str(v):
    if v is None:
        return ""
    if isinstance(v, float):
        return "" if v != v else (str(int(v)) if v == int(v) else str(v))
    return str(v).strip()

def _build_pattern(kw):
    esc  = re.escape(kw)
    flex = esc.replace(r"\-", r"[\s\-]?").replace(r"\ ", r"[\s\-]?")
    return re.compile(flex, re.IGNORECASE)

def _url_looks_like_pdf(url: str) -> bool:
    return url.lower().split("?")[0].endswith(".pdf")

def _fetch(url: str, session: requests.Session):
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, timeout=GET_TIMEOUT, verify=False,
                            headers=_get_headers(), stream=True)
            if r.status_code in RETRY_ON_CODES:
                r.close()
                time.sleep(RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF)-1)]
                           + random.uniform(0, 2))
                continue
            r.raise_for_status()

            ct     = r.headers.get("Content-Type", "").lower()
            is_pdf = "pdf" in ct or _url_looks_like_pdf(url)
            limit  = MAX_PDF_BYTES if is_pdf else MAX_HTML_BYTES

            raw = b""
            for chunk in r.iter_content(65536):
                raw += chunk
                if len(raw) >= limit:
                    break
            r.close()
            return raw, is_pdf
        except Exception:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF)-1)])
    return b"", False

def _extract_text(raw: bytes, is_pdf: bool) -> str:
    if not raw:
        return ""
    if is_pdf:
        if not _FITZ_OK:
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

def _normalize_col(name: str) -> str:
    """Strip BOM, whitespace, lowercase."""
    return str(name).strip().lstrip("\ufeff").lower()

# ══════════════════════════════════════════════════════════════════════
#  WORKER
# ══════════════════════════════════════════════════════════════════════
def _process_row(row_index, url, part,
                 auto_kws, mil_kws, auto_pats, mil_pats,
                 rl, cb, session, stop_ev, pause_ev):

    # Honour pause
    while pause_ev.is_set():
        if stop_ev.is_set():
            raise InterruptedError()
        time.sleep(0.3)

    if stop_ev.is_set():
        raise InterruptedError()

    cb.wait()
    rl.wait()

    url_s  = _safe_str(url)
    part_s = _safe_str(part)

    try:
        raw, is_pdf = _fetch(url_s, session)
        text        = _extract_text(raw, is_pdf)
        cb.ok()
    except InterruptedError:
        raise
    except Exception:
        cb.fail()
        raise

    row = {
        "_row_index" : row_index,
        "_scan_url"  : url_s,
        "_scan_part" : part_s,
    }
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
        if col is None:
            continue
        for r in range(2, ws.max_row + 1):
            cell = ws.cell(row=r, column=ci)
            if col == "Part_Scanned":
                cell.fill = green if cell.value == "TRUE" else red
            elif col == "Military":
                if cell.value == 1:
                    cell.fill = green
            elif col not in ("_scan_url", "_scan_part", "_row_index"):
                if cell.value == 1:
                    cell.fill = green
    out = io.BytesIO()
    wb.save(out)
    wb.close()
    return out.getvalue()

# ══════════════════════════════════════════════════════════════════════
#  BACKGROUND SCAN THREAD
# ══════════════════════════════════════════════════════════════════════
def _scan_thread(cfg: dict):
    """
    Runs entirely in a background thread.
    Communicates via cfg["state"] dict (thread-safe for simple reads/writes).
    """
    state       = cfg["state"]
    df_work     = cfg["df_work"]
    all_jobs    = cfg["all_jobs"]
    auto_kws    = cfg["auto_kws"]
    mil_kws     = cfg["mil_kws"]
    auto_pats   = cfg["auto_pats"]
    mil_pats    = cfg["mil_pats"]
    n_workers   = cfg["n_workers"]
    rpm         = cfg["rpm"]
    cb_errors   = cfg["cb_errors"]
    cb_pause    = cfg["cb_pause"]
    chunk_size  = cfg["chunk_size"]
    delay_min   = cfg["delay_min"]
    delay_max   = cfg["delay_max"]
    stop_ev     = cfg["stop_ev"]
    pause_ev    = cfg["pause_ev"]
    log_q       = cfg["log_q"]   # queue.Queue for log lines

    def log(msg, tag=""):
        log_q.put((tag, msg))

    total_jobs  = len(all_jobs)
    chunks      = [all_jobs[i:i+chunk_size]
                   for i in range(0, total_jobs, chunk_size)]

    rl          = RateLimiter(rpm)
    cb          = CircuitBreaker(cb_errors, cb_pause)
    ses         = _make_session(n_workers + 4)

    all_results = []
    failed      = []
    done_cnt    = 0
    ok_cnt      = 0
    t_start     = time.time()
    user_stopped = False

    log(f"Workers={n_workers} | RPM={rpm} | CB={cb_errors}err→{cb_pause}s", "info")
    log(f"HTML cap={MAX_HTML_BYTES//1024}KB | PDF={MAX_PDF_BYTES//1024//1024}MB | PyMuPDF={_FITZ_OK}", "info")
    log(f"Auto kws={len(auto_kws)} | Mil kws={len(mil_kws)}", "info")
    log(f"Rows={len(df_work)} | Jobs with URL={total_jobs}", "info")
    log(f"Chunks={len(chunks)} × up to {chunk_size}", "info")

    try:
        for ci, chunk in enumerate(chunks):
            if stop_ev.is_set():
                user_stopped = True
                break

            state["chunk"] = f"{ci+1}/{len(chunks)}"
            log(f"── Chunk {ci+1}/{len(chunks)} ({len(chunk)} rows) ──", "info")

            with ThreadPoolExecutor(max_workers=n_workers) as pool:
                futs = {
                    pool.submit(
                        _process_row,
                        ri, url, part,
                        auto_kws, mil_kws, auto_pats, mil_pats,
                        rl, cb, ses, stop_ev, pause_ev,
                    ): (ri, url, part)
                    for ri, url, part in chunk
                }

                for fut in as_completed(futs):
                    ri, url, part = futs[fut]
                    blank = {
                        "_row_index" : ri,
                        "_scan_url"  : url,
                        "_scan_part" : part,
                        **{k: 0 for k in auto_kws},
                        "Military"     : 0,
                        "Part_Scanned" : "FALSE",
                    }
                    try:
                        res = fut.result()
                        all_results.append(res)
                        done_cnt += 1
                        if res.get("Part_Scanned") == "TRUE":
                            ok_cnt += 1
                        short = (url[:55] + "…") if len(url) > 55 else url
                        tag   = "ok" if res.get("Part_Scanned") == "TRUE" else "dim"
                        log(f"✓[{ri}] {short} | Part={res['Part_Scanned']} Mil={res['Military']}", tag)
                    except InterruptedError:
                        all_results.append(blank)
                        failed.append({"row": ri, "url": url, "error": "Stopped"})
                        done_cnt += 1
                    except Exception as e:
                        all_results.append(blank)
                        failed.append({"row": ri, "url": url, "error": str(e)})
                        done_cnt += 1
                        log(f"✗[{ri}] {url[:55]} | {e}", "err")

                    # Update shared state counters
                    elapsed = time.time() - t_start
                    state.update({
                        "done"    : done_cnt,
                        "ok"      : ok_cnt,
                        "failed"  : len(failed),
                        "pct"     : min(int(done_cnt / total_jobs * 100), 100) if total_jobs else 0,
                        "rpm_live": int(done_cnt / elapsed * 60) if elapsed > 2 else 0,
                    })

                    if stop_ev.is_set():
                        user_stopped = True
                        break

            if user_stopped:
                break

            if ci < len(chunks) - 1:
                p = random.uniform(delay_min, delay_max)
                if p > 0.05:
                    time.sleep(p)

    except Exception as outer:
        log(f"💥 {outer}", "err")
        log(traceback.format_exc(), "err")
    finally:
        ses.close()

    # ── Build final xlsx ────────────────────────────────────────────
    df_final = _apply_results(df_work.copy(), all_results, auto_kws)

    # Restore original column names
    try:
        orig = cfg.get("orig_col_map", {})
        if orig:
            df_final.rename(columns=orig, inplace=True)
    except Exception:
        pass

    result_bytes = _to_xlsx(df_final)

    failed_bytes = None
    if failed:
        fb = io.BytesIO()
        pd.DataFrame(failed).to_excel(fb, index=False)
        failed_bytes = fb.getvalue()

    elapsed = time.time() - t_start
    avg_rpm = int(done_cnt / elapsed * 60) if elapsed > 0 else 0

    # Summary log
    log("═" * 52, "dim")
    log(f"Total rows     : {len(df_final)}", "ok")
    log(f"Time           : {elapsed:.0f}s  |  Avg speed: {avg_rpm} rows/min", "ok")
    log(f"Part_Scanned=T : {(df_final.get('Part_Scanned', '') == 'TRUE').sum()}", "ok")
    log(f"Military=1     : {(df_final.get('Military', 0) == 1).sum()}", "ok")
    for kw in auto_kws:
        if kw in df_final.columns:
            cnt = (df_final[kw] == 1).sum()
            if cnt > 0:
                log(f"  {kw}: {cnt} hits", "ok")
    log(f"Failed rows    : {len(failed)}", "warn" if failed else "ok")

    # Push final results into state
    state.update({
        "result_bytes" : result_bytes,
        "failed_bytes" : failed_bytes,
        "user_stopped" : user_stopped,
        "finished"     : True,
        "done"         : done_cnt,
        "ok"           : ok_cnt,
        "failed"       : len(failed),
        "pct"          : 100,
        "rpm_live"     : avg_rpm,
    })

# ══════════════════════════════════════════════════════════════════════
#  STREAMLIT UI
# ══════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="AEC/MIL Scanner v7", page_icon="⚡", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600;700&family=IBM+Plex+Sans:wght@400;500;600&display=swap');
html,body,[class*="css"]{font-family:'IBM Plex Sans',sans-serif;}
[data-testid="stSidebar"]{background:#0a0e1a!important;border-right:1px solid #1e2840;}
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stTextInput label,
[data-testid="stSidebar"] .stTextArea label,
[data-testid="stSidebar"] .stSlider label,
[data-testid="stSidebar"] .stNumberInput label,
[data-testid="stSidebar"] .stFileUploader label{
    color:#7b9cc4!important;font-size:0.77rem!important;font-weight:600!important;
    letter-spacing:0.06em!important;text-transform:uppercase;}
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3{color:#c9d8f0!important;font-family:'IBM Plex Mono',monospace!important;}
[data-testid="stSidebar"] input,
[data-testid="stSidebar"] textarea{background:#111827!important;color:#e2eaf8!important;
    border:1px solid #1e2840!important;border-radius:4px!important;}
[data-testid="stSidebar"] .stAlert{background:#0f1929!important;border:1px solid #1e2840!important;
    color:#7b9cc4!important;font-size:0.72rem!important;}
.main .block-container{padding-top:1rem;padding-bottom:2rem;}
[data-testid="metric-container"]{background:#0f1929;border:1px solid #1e2840;
    border-radius:8px;padding:10px 14px;}
.title-bar{background:linear-gradient(135deg,#0a0e1a,#111827);border:1px solid #1e3a5f;
    border-left:4px solid #22c55e;border-radius:8px;padding:14px 22px;margin-bottom:1.2rem;
    display:flex;align-items:center;justify-content:space-between;}
.title-bar h1{font-family:'IBM Plex Mono',monospace;font-size:1.05rem;font-weight:700;color:#22c55e;margin:0;}
.title-bar .badge{font-family:'IBM Plex Mono',monospace;font-size:0.68rem;color:#60a5fa;
    background:#0c1a35;border:1px solid #1e3a5f;border-radius:4px;padding:2px 8px;}
.sec{font-family:'IBM Plex Mono',monospace;font-size:0.68rem;font-weight:700;color:#3b4f6b;
    text-transform:uppercase;letter-spacing:0.14em;margin:1rem 0 0.35rem 0;
    border-bottom:1px solid #1e2840;padding-bottom:3px;}
div[data-testid="column"]:nth-child(1) .stButton>button{background:#16a34a!important;color:#fff!important;
    border:none!important;border-radius:6px!important;font-family:'IBM Plex Mono',monospace!important;
    font-weight:700!important;font-size:0.82rem!important;padding:0.5rem 1.2rem!important;}
div[data-testid="column"]:nth-child(2) .stButton>button{background:#b45309!important;color:#fff!important;
    border:none!important;border-radius:6px!important;font-family:'IBM Plex Mono',monospace!important;
    font-weight:600!important;font-size:0.82rem!important;padding:0.5rem 1.2rem!important;}
div[data-testid="column"]:nth-child(3) .stButton>button{background:#dc2626!important;color:#fff!important;
    border:none!important;border-radius:6px!important;font-family:'IBM Plex Mono',monospace!important;
    font-weight:600!important;font-size:0.82rem!important;padding:0.5rem 1.2rem!important;}
.log-box{background:#080c18;border:1px solid #1e2840;border-radius:6px;padding:12px 16px;
    font-family:'IBM Plex Mono',monospace;font-size:11px;color:#cbd5e1;height:300px;overflow-y:auto;
    white-space:pre-wrap;word-break:break-all;line-height:1.55;}
.log-ok{color:#4ade80;}.log-err{color:#f87171;}.log-warn{color:#fbbf24;}
.log-info{color:#60a5fa;}.log-dim{color:#374151;}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="title-bar">
  <h1>⚡ AUTOMOTIVE &amp; MILITARY SCANNER — v7 STABLE</h1>
  <span class="badge">BACKGROUND THREAD · NO FREEZE · ANY FILENAME · STABLE</span>
</div>
""", unsafe_allow_html=True)

# ── Sidebar ──────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Configuration")
    st.markdown('<div class="sec">📁 File</div>', unsafe_allow_html=True)

    # Accept any file — user can name it anything
    links_file = st.file_uploader(
        "Links Sheet (Excel — any filename)",
        type=None,          # accept any extension
        accept_multiple_files=False,
    )

    st.markdown('<div class="sec">🗂 Columns</div>', unsafe_allow_html=True)
    url_col  = st.text_input("URL column",         value="offlineURL")
    part_col = st.text_input("Part Number column",  value="PartNumber")

    if links_file:
        try:
            peek = pd.read_excel(io.BytesIO(links_file.read()), nrows=0)
            links_file.seek(0)
            st.info("Cols: " + " | ".join(str(c) for c in peek.columns))
        except Exception as e:
            st.warning(f"Cannot preview columns: {e}")

    st.markdown('<div class="sec">🚗 Automotive Keywords</div>', unsafe_allow_html=True)
    auto_text = st.text_area("One per line", value=DEFAULT_AUTO_KW, height=200)

    st.markdown('<div class="sec">🎖 Military Keywords</div>', unsafe_allow_html=True)
    mil_text = st.text_area(
        "One per line — any hit → Military=1",
        value=DEFAULT_MIL_KW,
        height=180,
    )

    st.markdown('<div class="sec">⚡ Performance</div>', unsafe_allow_html=True)
    n_workers   = st.slider("Concurrent workers",  1, 100,   20)
    chunk_size  = st.number_input("Chunk size",   50, 100000, 1000, step=100)
    rpm         = st.slider("Max requests/min",   10, 1000,  120)

    st.markdown('<div class="sec">🛡 Resilience</div>', unsafe_allow_html=True)
    cb_errors = st.slider("Circuit breaker threshold", 1,  50,  10)
    cb_pause  = st.slider("Circuit breaker pause (s)",  5, 300,  30)
    delay_min = st.number_input("Min inter-chunk delay (s)", 0.0, 10.0, 0.2, step=0.1)
    delay_max = st.number_input("Max inter-chunk delay (s)", 0.0, 30.0, 1.0, step=0.1)

# ── Session state init ───────────────────────────────────────────────
_DEFAULTS = {
    "log_lines"    : [],
    "scan_running" : False,
    "scan_paused"  : False,
    "stop_event"   : None,
    "pause_event"  : None,
    "scan_state"   : None,    # shared dict updated by background thread
    "log_queue"    : None,    # queue.Queue fed by background thread
    "result_bytes" : None,
    "failed_bytes" : None,
    "run_id"       : 0,
    "file_bytes"   : None,    # cached upload bytes
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

if not _FITZ_OK:
    st.warning("⚠️ PyMuPDF not installed — PDFs fall back to plain-text. Add `pymupdf` to requirements.txt.")

def _flush_log_queue():
    """Drain the log queue into session_state.log_lines."""
    lq = st.session_state.log_queue
    if lq is None:
        return
    css_map = {"ok":"log-ok","err":"log-err","warn":"log-warn","info":"log-info","dim":"log-dim"}
    while True:
        try:
            tag, msg = lq.get_nowait()
            css = css_map.get(tag, "")
            st.session_state.log_lines.append(
                f'<span class="{css}">[{time.strftime("%H:%M:%S")}] {msg}</span>'
            )
        except queue.Empty:
            break
    if len(st.session_state.log_lines) > 1000:
        st.session_state.log_lines = st.session_state.log_lines[-1000:]

def _render_log():
    log_ph.markdown(
        '<div class="log-box">' + "\n".join(st.session_state.log_lines) + '</div>',
        unsafe_allow_html=True,
    )

# ── Layout ───────────────────────────────────────────────────────────
st.markdown('<div class="sec">📈 Live Stats</div>', unsafe_allow_html=True)
s1,s2,s3,s4,s5,s6,s7 = st.columns(7)
total_ph  = s1.empty(); total_ph.metric("Total",     "—")
jobs_ph   = s2.empty(); jobs_ph.metric("Jobs",       "—")
done_ph   = s3.empty(); done_ph.metric("Done",       "0")
ok_ph     = s4.empty(); ok_ph.metric("Part Found",   "0")
failed_ph = s5.empty(); failed_ph.metric("Failed",   "0")
chunk_ph  = s6.empty(); chunk_ph.metric("Chunk",     "—")
speed_ph  = s7.empty(); speed_ph.metric("Rows/min",  "—")

st.markdown('<div class="sec">📊 Progress</div>', unsafe_allow_html=True)
prog_bar  = st.progress(0)
prog_text = st.empty()
status_ph = st.empty()

st.markdown('<div class="sec">🚀 Actions</div>', unsafe_allow_html=True)
ac1, ac2, ac3, _ar = st.columns([1, 1, 1, 5])
run_btn   = ac1.button("▶ RUN",  use_container_width=True,
                        disabled=st.session_state.scan_running)
plbl      = "▶ RESUME" if st.session_state.scan_paused else "⏸ PAUSE"
pause_btn = ac2.button(plbl,     use_container_width=True,
                        disabled=not st.session_state.scan_running)
stop_btn  = ac3.button("⏹ STOP", use_container_width=True,
                        disabled=not st.session_state.scan_running)

st.markdown('<div class="sec">🖥 Log</div>', unsafe_allow_html=True)
log_ph = st.empty()

st.markdown('<div class="sec">✅ Results</div>', unsafe_allow_html=True)
results_box = st.container()

_render_log()

# Persistent download buttons
_rid = st.session_state.run_id
with results_box:
    if st.session_state.result_bytes:
        d1, d2 = st.columns(2)
        d1.download_button(
            "📥 Download Results (.xlsx)",
            data=st.session_state.result_bytes,
            file_name="scan_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"dl_r_{_rid}",
        )
        if st.session_state.failed_bytes:
            d2.download_button(
                "⚠️ Failed Rows (.xlsx)",
                data=st.session_state.failed_bytes,
                file_name="scan_failed.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_f_{_rid}",
            )

# ── Control buttons ──────────────────────────────────────────────────
if pause_btn and st.session_state.scan_running:
    pe = st.session_state.pause_event
    if pe:
        if st.session_state.scan_paused:
            pe.clear()
            st.session_state.scan_paused = False
        else:
            pe.set()
            st.session_state.scan_paused = True
    st.rerun()

if stop_btn and st.session_state.scan_running:
    se, pe = st.session_state.stop_event, st.session_state.pause_event
    if se: se.set()
    if pe: pe.clear()
    st.rerun()

# ── Poll running scan ────────────────────────────────────────────────
if st.session_state.scan_running:
    _flush_log_queue()
    ss = st.session_state.scan_state or {}

    done_ph.metric("Done",      ss.get("done",     0))
    ok_ph.metric("Part Found",  ss.get("ok",       0))
    failed_ph.metric("Failed",  ss.get("failed",   0))
    chunk_ph.metric("Chunk",    ss.get("chunk",    "—"))
    speed_ph.metric("Rows/min", ss.get("rpm_live", 0))
    pct = ss.get("pct", 0)
    prog_bar.progress(pct)
    prog_text.text(f"{ss.get('done',0)}/{ss.get('total_jobs',0)} ({pct}%)")

    if ss.get("finished"):
        # Scan complete — harvest results
        st.session_state.result_bytes  = ss.get("result_bytes")
        st.session_state.failed_bytes  = ss.get("failed_bytes")
        st.session_state.scan_running  = False
        st.session_state.scan_paused   = False
        st.session_state.run_id       += 1

        if ss.get("user_stopped"):
            status_ph.warning("● STOPPED")
        else:
            status_ph.success("● COMPLETE")
            prog_bar.progress(100)

        _render_log()

        rid = st.session_state.run_id
        with results_box:
            d1, d2 = st.columns(2)
            if st.session_state.result_bytes:
                d1.download_button(
                    "📥 Download Results (.xlsx)",
                    data=st.session_state.result_bytes,
                    file_name="scan_results.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"dl_r_done_{rid}",
                )
            if st.session_state.failed_bytes:
                d2.download_button(
                    "⚠️ Failed Rows (.xlsx)",
                    data=st.session_state.failed_bytes,
                    file_name="scan_failed.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"dl_f_done_{rid}",
                )
        st.rerun()
    else:
        status_ph.info("● RUNNING")
        _render_log()
        time.sleep(1.5)
        st.rerun()

# ══════════════════════════════════════════════════════════════════════
#  RUN BUTTON
# ══════════════════════════════════════════════════════════════════════
if run_btn:
    if not links_file:
        st.error("❌ Upload a Links Sheet first."); st.stop()

    # Cache upload bytes — avoids re-read issues
    raw_upload = links_file.read()
    st.session_state.file_bytes = raw_upload

    auto_kws = [k.strip() for k in auto_text.splitlines() if k.strip()]
    mil_kws  = [k.strip() for k in mil_text.splitlines()  if k.strip()]
    if not auto_kws and not mil_kws:
        st.error("❌ Enter at least one keyword."); st.stop()

    auto_pats = [_build_pattern(k) for k in auto_kws]
    mil_pats  = [_build_pattern(k) for k in mil_kws]

    # Load dataframe
    try:
        df = pd.read_excel(io.BytesIO(raw_upload), dtype=str)
    except Exception as e:
        st.error(f"Cannot open file: {e}"); st.stop()

    # Normalise column names for lookup, keep originals for output
    orig_cols    = {_normalize_col(c): str(c).strip() for c in df.columns}
    df.columns   = [_normalize_col(c) for c in df.columns]

    uc = _normalize_col(url_col)
    pc = _normalize_col(part_col)

    if uc not in df.columns:
        st.error(f"❌ Column '{url_col}' not found. Got: {', '.join(orig_cols.values())}"); st.stop()
    if pc not in df.columns:
        st.warning(f"⚠ '{part_col}' not found — Part_Scanned=FALSE for all.")
        df[pc] = ""

    df[uc] = df[uc].apply(_safe_str)
    df[pc] = df[pc].apply(_safe_str)
    df_work = df.copy()

    all_jobs = [
        (idx, row[uc], row[pc])
        for idx, row in df_work.iterrows()
        if row[uc] and row[uc].lower() not in ("nan", "")
    ]

    total_rows = len(df_work)
    total_jobs = len(all_jobs)
    total_ph.metric("Total", total_rows)
    jobs_ph.metric("Jobs",   total_jobs)

    # Shared state dict
    scan_state = {
        "done"       : 0,
        "ok"         : 0,
        "failed"     : 0,
        "pct"        : 0,
        "rpm_live"   : 0,
        "chunk"      : "—",
        "finished"   : False,
        "user_stopped": False,
        "result_bytes": None,
        "failed_bytes": None,
        "total_jobs" : total_jobs,
    }
    log_q    = queue.Queue()
    stop_ev  = threading.Event()
    pause_ev = threading.Event()

    # Reset UI state
    st.session_state.update({
        "log_lines"    : [],
        "scan_running" : True,
        "scan_paused"  : False,
        "result_bytes" : None,
        "failed_bytes" : None,
        "stop_event"   : stop_ev,
        "pause_event"  : pause_ev,
        "scan_state"   : scan_state,
        "log_queue"    : log_q,
    })

    cfg = dict(
        state        = scan_state,
        df_work      = df_work,
        all_jobs     = all_jobs,
        auto_kws     = auto_kws,
        mil_kws      = mil_kws,
        auto_pats    = auto_pats,
        mil_pats     = mil_pats,
        n_workers    = n_workers,
        rpm          = rpm,
        cb_errors    = cb_errors,
        cb_pause     = cb_pause,
        chunk_size   = int(chunk_size),
        delay_min    = delay_min,
        delay_max    = delay_max,
        stop_ev      = stop_ev,
        pause_ev     = pause_ev,
        log_q        = log_q,
        orig_col_map = orig_cols,
    )

    t = threading.Thread(target=_scan_thread, args=(cfg,), daemon=True)
    t.start()

    status_ph.info("● RUNNING")
    st.rerun()
