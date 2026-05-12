"""
Keyword & Part Scanner — Ultra-Lite v6 (Streamlit Edition) — FIXED
====================================================================
Bugs fixed in this version:
  1. Page reload / start needs two clicks  → scan_running set BEFORE rerun;
     file bytes read from session_state (never from widget on rerun).
  2. File lost on rerun  → _links_bytes / _ref_bytes cached in session_state
     the moment the uploader fires; run_btn reads from cache, not widget.
  3. PDF no size limit  → MAX_PDF_BYTES = 50 MB; reads ALL pages.
  4. Data lost on disconnect  → background thread writes partial results to
     session_state every AUTOSAVE_EVERY rows AND after every chunk.
  5. Stop/Pause/Resume  → preserved and improved.
"""

import re, time, json, random, threading, io, os, collections, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import fitz                       # PyMuPDF
import urllib3
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import streamlit as st
try:
    from streamlit_autorefresh import st_autorefresh
    _HAS_AUTOREFRESH = True
except ImportError:
    _HAS_AUTOREFRESH = False

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ══════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════

MAX_HTML_BYTES  = 300  * 1024
MAX_PDF_BYTES   = 50   * 1024 * 1024
HEAD_TIMEOUT    = 8
MAX_RETRIES     = 3
RETRY_BACKOFF   = [5, 15, 30]
RETRY_ON_CODES  = {429, 503, 502, 504}
AUTOSAVE_EVERY  = 20

DEFAULT_KW      = "AEC-Q100\nAEC-Q200\nAEC-Q101"
DEFAULT_MIL_KW  = "Military\nMIL-PRF\nMIL-C\nMIL-R\nMIL-DTL"

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
            r = session.get(url, timeout=30, verify=False,
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

            doc   = fitz.open(stream=content, filetype="pdf")
            parts = []
            for page in doc:
                t = page.get_text()
                if t.strip():
                    parts.append(t)
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
    url_str  = _safe_str(url)
    part_str = _safe_str(part)

    if pause_event:
        while pause_event.is_set():
            if stop_event and stop_event.is_set():
                raise InterruptedError("Stopped by user")
            time.sleep(0.5)

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
    try:
        df_partial = _apply_results_to_df(
            df_work.copy(), all_results, keywords,
            orig_qual, orig_ztemp, orig_feature, ref_map
        )
        return _highlight_excel(df_partial)
    except Exception:
        return b""


# ══════════════════════════════════════════════════════════════════════
#  BACKGROUND SCAN THREAD
# ══════════════════════════════════════════════════════════════════════

def _run_scan_thread(
    df, df_work, url_col_lower, part_col_lower,
    orig_qual, orig_ztemp, orig_feature,
    keywords, mil_keywords,
    ref_map, chunk_size, n_workers, timeout,
    rpm, delay_min, delay_max, cb_errors, cb_pause,
    stop_event, pause_event,
):
    ss = st.session_state

    def log(msg, tag=""):
        css = {"ok": "log-ok", "err": "log-err", "warn": "log-warn",
               "info": "log-info", "dim": "log-dim"}.get(tag, "")
        ts   = time.strftime("%H:%M:%S")
        line = f'<span class="{css}">[{ts}] {msg}</span>'
        ss.log_lines.append(line)
        if len(ss.log_lines) > 500:
            ss.log_lines = ss.log_lines[-500:]

    def do_autosave(results, label=""):
        pb = _build_partial_excel(
            df_work.copy(), results, keywords,
            orig_qual, orig_ztemp, orig_feature, ref_map
        )
        if pb:
            ss.partial_bytes = pb
            if label:
                log(f"💾 Auto-saved {len(results)} rows {label}", "dim")

    log(f"Rate limit : {rpm} req/min | Delay: {delay_min}–{delay_max}s", "info")
    log(f"Circuit    : trip at {cb_errors} errors, pause {cb_pause}s", "info")
    log(f"Workers    : {n_workers}  |  Timeout: {timeout}s", "info")
    log(f"PDF cap    : {MAX_PDF_BYTES//1024//1024} MB (reads ALL pages)", "info")
    log(f"HTML cap   : {MAX_HTML_BYTES//1024} KB", "info")
    log(f"🎭 User-Agent pool: {len(_USER_AGENTS)} agents", "info")

    all_jobs = []
    for idx, row in df_work.iterrows():
        url  = row[url_col_lower]
        part = row[part_col_lower]
        if not url or url.lower() == "nan":
            continue
        all_jobs.append((idx, url, part))

    total_rows = len(df_work)
    total_jobs = len(all_jobs)
    ss.stat_total_rows = total_rows
    ss.stat_total_jobs = total_jobs
    log(f"Rows: {total_rows}  |  Jobs: {total_jobs}", "info")

    chunks       = [all_jobs[i:i + chunk_size] for i in range(0, total_jobs, chunk_size)]
    total_chunks = len(chunks)
    log(f"Chunks: {total_chunks}  |  Chunk size: {chunk_size}", "info")

    rate_limiter    = RateLimiter(max_per_minute=rpm)
    circuit_breaker = CircuitBreaker(error_threshold=cb_errors, pause_seconds=cb_pause)
    session         = requests.Session()

    all_results  = []
    failed       = []
    done_cnt     = 0
    user_stopped = False

    try:
        for chunk_idx, chunk_jobs in enumerate(chunks):
            if stop_event.is_set():
                user_stopped = True
                break

            ss.stat_chunk = f"{chunk_idx+1}/{total_chunks}"
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

                    if done_cnt % AUTOSAVE_EVERY == 0:
                        do_autosave(all_results, f"({done_cnt}/{total_jobs})")

                    ss.stat_done    = done_cnt
                    ss.stat_failed  = len(failed)
                    ss.stat_cb      = circuit_breaker.error_count
                    ss.stat_pct     = int(done_cnt / total_jobs * 100) if total_jobs else 0

                    if stop_event.is_set():
                        user_stopped = True
                        break

            do_autosave(all_results, f"after chunk {chunk_idx+1}")

            if stop_event.is_set():
                user_stopped = True
                break

            if chunk_idx < total_chunks - 1:
                pause = random.uniform(delay_min * 2, delay_max * 2)
                log(f"⏸ Pausing {pause:.1f}s between chunks …", "dim")
                time.sleep(pause)

    except Exception as outer_exc:
        log(f"💥 Unexpected error: {outer_exc}", "err")
        do_autosave(all_results, "(error recovery)")

    finally:
        session.close()

    if not all_results:
        ss.scan_status  = "no_results"
        ss.scan_running = False
        return

    df_final = _apply_results_to_df(
        df_work.copy(), all_results, keywords,
        orig_qual, orig_ztemp, orig_feature, ref_map
    )
    orig_cols = {c.strip().lower(): c.strip() for c in df.columns}
    df_final.rename(columns=orig_cols, inplace=True)

    result_bytes = _highlight_excel(df_final)
    ss.result_bytes = result_bytes
    ss.stat_done    = done_cnt
    ss.stat_pct     = 100

    if user_stopped:
        ss.partial_bytes = result_bytes
        log("⏹ Scan stopped — partial file ready.", "warn")
        ss.scan_status = "stopped"
    else:
        ss.scan_done   = True
        ss.scan_status = "complete"
        log("SCAN COMPLETE ✓", "ok")

    if failed:
        fail_df = pd.DataFrame(failed)
        buf = io.BytesIO()
        fail_df.to_excel(buf, index=False)
        ss.failed_bytes = buf.getvalue()
        log(f"Failed rows logged: {len(failed)}", "warn")

    true_part = (df_final.get("Part_Scanned",    pd.Series()) == "TRUE").sum()
    true_mil  = (df_final.get("Military_RESULT", pd.Series()) == "TRUE").sum()
    log("═" * 50, "dim")
    log(f"Total rows     : {len(df_final)}", "ok")
    log(f"Part_Scanned T : {true_part}",     "ok")
    log(f"Military TRUE  : {true_mil}",       "ok")
    for kw in keywords:
        col_name = f"{kw}_RESULT"
        if col_name in df_final.columns:
            log(f"{col_name}: TRUE={(df_final[col_name]=='TRUE').sum()}", "ok")

    ss.scan_running = False


# ══════════════════════════════════════════════════════════════════════
#  STREAMLIT PAGE CONFIG
# ══════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Keyword & Part Scanner v6",
    page_icon="⬡",
    layout="wide",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Sora:wght@400;600;700&display=swap');

html, body, [class*="css"] { font-family: 'Sora', sans-serif; }

[data-testid="stSidebar"] {
    background: #0f1117 !important;
    border-right: 1px solid #2d3148;
}
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stTextInput label,
[data-testid="stSidebar"] .stTextArea label,
[data-testid="stSidebar"] .stSlider label,
[data-testid="stSidebar"] .stNumberInput label,
[data-testid="stSidebar"] .stFileUploader label {
    color: #94a3b8 !important;
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.04em !important;
}
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 { color: #cbd5e1 !important; }
[data-testid="stSidebar"] input,
[data-testid="stSidebar"] textarea {
    background: #1a1d27 !important;
    color: #f1f5f9 !important;
    border: 1px solid #2d3148 !important;
    border-radius: 6px !important;
}
[data-testid="stSidebar"] .stAlert {
    background: #1a2035 !important;
    border: 1px solid #2d3148 !important;
    color: #94a3b8 !important;
    font-size: 0.72rem !important;
}
.main .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
[data-testid="metric-container"] {
    background: #1a1d27;
    border: 1px solid #2d3148;
    border-radius: 10px;
    padding: 12px 16px;
}
div[data-testid="column"]:nth-child(1) .stButton > button {
    background: #4f8ef7 !important; color: white !important;
    border: none !important; border-radius: 8px !important;
    font-family: 'Sora', sans-serif !important; font-weight: 600 !important;
    padding: 0.5rem 1.4rem !important; transition: opacity 0.2s;
}
div[data-testid="column"]:nth-child(1) .stButton > button:hover { opacity: 0.85; }
div[data-testid="column"]:nth-child(2) .stButton > button {
    background: #d97706 !important; color: white !important;
    border: none !important; border-radius: 8px !important;
    font-family: 'Sora', sans-serif !important; font-weight: 600 !important;
    padding: 0.5rem 1.4rem !important; transition: opacity 0.2s;
}
div[data-testid="column"]:nth-child(3) .stButton > button {
    background: #ef4444 !important; color: white !important;
    border: none !important; border-radius: 8px !important;
    font-family: 'Sora', sans-serif !important; font-weight: 600 !important;
    padding: 0.5rem 1.4rem !important; transition: opacity 0.2s;
}
div[data-testid="column"]:nth-child(4) .stButton > button {
    background: #0d9488 !important; color: white !important;
    border: none !important; border-radius: 8px !important;
    font-family: 'Sora', sans-serif !important; font-weight: 600 !important;
    padding: 0.5rem 1.4rem !important; transition: opacity 0.2s;
}
.log-box {
    background: #1a1d27; border: 1px solid #2d3148; border-radius: 8px;
    padding: 12px 16px; font-family: 'JetBrains Mono', monospace;
    font-size: 12px; color: #e2e8f0; height: 320px;
    overflow-y: auto; white-space: pre-wrap; word-break: break-all;
}
.log-ok   { color: #22c55e; }
.log-err  { color: #ef4444; }
.log-warn { color: #f59e0b; }
.log-info { color: #4f8ef7; }
.log-dim  { color: #64748b; }
.title-bar {
    background: #1a1d27; border: 1px solid #2d3148; border-radius: 12px;
    padding: 16px 24px; display: flex; align-items: center;
    justify-content: space-between; margin-bottom: 1.2rem;
}
.title-bar h1 {
    font-family: 'JetBrains Mono', monospace; font-size: 1.15rem;
    font-weight: 700; color: #4f8ef7; margin: 0;
}
.title-bar span { font-family: 'JetBrains Mono', monospace; font-size: 0.75rem; color: #22c55e; }
.section-head {
    font-family: 'Sora', sans-serif; font-size: 0.78rem; font-weight: 700;
    color: #64748b; text-transform: uppercase; letter-spacing: 0.12em;
    margin: 1.1rem 0 0.4rem 0; border-bottom: 1px solid #2d3148; padding-bottom: 4px;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="title-bar">
  <h1>⬡ KEYWORD &amp; PART SCANNER — ULTRA-LITE v6</h1>
  <span>Background thread · Persistent state · No-limit PDF · Stop/Pause/Save · Auto-save</span>
</div>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════
#  SESSION STATE  (initialise once)
# ══════════════════════════════════════════════════════════════════════

_DEFAULTS = dict(
    log_lines       = [],
    scan_done       = False,
    scan_running    = False,
    scan_paused     = False,
    scan_status     = "",
    result_bytes    = None,
    failed_bytes    = None,
    partial_bytes   = None,
    stop_event      = None,
    pause_event     = None,
    stat_total_rows = 0,
    stat_total_jobs = 0,
    stat_done       = 0,
    stat_failed     = 0,
    stat_chunk      = "—",
    stat_cb         = 0,
    stat_pct        = 0,
    # ── FIX: file bytes cached here survive ALL reruns ──
    _links_bytes    = None,
    _ref_bytes      = None,
)
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ══════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ══════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("## ⚙️ Configuration")

    st.markdown('<div class="section-head">📁 Files</div>', unsafe_allow_html=True)
    links_file = st.file_uploader("Links Sheet (.xlsx)", type=["xlsx"], key="links")
    ref_file   = st.file_uploader("Reference Sheet (.xlsx)", type=["xlsx"], key="ref")

    # ── Cache bytes into session_state THE MOMENT a file is uploaded.
    # These survive every future rerun; the widget itself becomes None on rerun.
    if links_file is not None:
        st.session_state._links_bytes = links_file.read()
        links_file.seek(0)
    if ref_file is not None:
        st.session_state._ref_bytes = ref_file.read()
        ref_file.seek(0)

    # Persistent status indicators (survive after widget clears on rerun)
    if st.session_state._links_bytes:
        try:
            _prev    = pd.read_excel(io.BytesIO(st.session_state._links_bytes), nrows=0)
            cols_str = "  |  ".join(_prev.columns.tolist())
            st.success(f"✅ Links file loaded — Columns: {cols_str}")
        except Exception:
            st.success("✅ Links file loaded")
    else:
        st.warning("⚠️ No links file yet")

    if st.session_state._ref_bytes:
        st.success("✅ Reference file loaded")

    st.markdown('<div class="section-head">🗂 Column Names</div>', unsafe_allow_html=True)
    url_col  = st.text_input("URL column name",    value="offlineURL")
    part_col = st.text_input("Part Number column", value="PartNumber")

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

    st.markdown('<div class="section-head">🔄 Auto-save</div>', unsafe_allow_html=True)
    st.info(f"Partial results saved every {AUTOSAVE_EVERY} completed rows and after each chunk.")


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

ss = st.session_state
total_ph.metric("Total Rows", ss.stat_total_rows or "—")
jobs_ph.metric("Jobs",        ss.stat_total_jobs or "—")
done_ph.metric("Completed",   ss.stat_done)
failed_ph.metric("Failed",    ss.stat_failed)
chunk_ph.metric("Chunk",      ss.stat_chunk)
circuit_ph.metric("CB Errors",ss.stat_cb)

st.markdown('<div class="section-head">📊 Progress</div>', unsafe_allow_html=True)
prog_bar  = st.progress(ss.stat_pct)
prog_text = st.empty()
prog_text.text(f"{ss.stat_done} / {ss.stat_total_jobs} jobs  ({ss.stat_pct}%)")

status_ph = st.empty()
if ss.scan_running:
    status_ph.info("● RUNNING" + (" — PAUSED" if ss.scan_paused else ""))
elif ss.scan_status == "complete":
    status_ph.success("● COMPLETE")
elif ss.scan_status == "stopped":
    status_ph.warning("● STOPPED")
elif ss.scan_status == "no_results":
    status_ph.warning("● NO RESULTS")

# ── Action buttons ────────────────────────────────────────────────────
st.markdown('<div class="section-head">🚀 Actions</div>', unsafe_allow_html=True)
btn_c1, btn_c2, btn_c3, btn_c4, _rest = st.columns([1, 1, 1, 1, 3])
with btn_c1:
    run_btn   = st.button("▶ RUN SCAN",  use_container_width=True,
                          disabled=ss.scan_running)
with btn_c2:
    pause_lbl = "▶ RESUME" if ss.scan_paused else "⏸ PAUSE"
    pause_btn = st.button(pause_lbl, use_container_width=True,
                          disabled=not ss.scan_running)
with btn_c3:
    stop_btn  = st.button("⏹ STOP",     use_container_width=True,
                          disabled=not ss.scan_running)
with btn_c4:
    save_btn  = st.button("💾 SAVE NOW", use_container_width=True,
                          disabled=not ss.scan_running)

# Auto-refresh every 2 s while scan is running — NO meta refresh (causes full reload)
if ss.scan_running:
    if _HAS_AUTOREFRESH:
        st_autorefresh(interval=2000, limit=None, key="scan_autorefresh")
    else:
        st.markdown(
            """<script>
            setTimeout(function(){
                window.parent.postMessage({type:'streamlit:rerun'}, '*');
            }, 2000);
            </script>""",
            unsafe_allow_html=True,
        )

st.markdown('<div class="section-head">🖥 Log</div>', unsafe_allow_html=True)
log_ph = st.empty()

st.markdown('<div class="section-head">✅ Results</div>', unsafe_allow_html=True)
results_container = st.container()

# Render log
html_log = "\n".join(ss.log_lines)
log_ph.markdown(f'<div class="log-box">{html_log}</div>', unsafe_allow_html=True)

# Download buttons
with results_container:
    if ss.scan_done and ss.result_bytes:
        dl1, dl2 = st.columns([1, 1])
        dl1.download_button(
            "📥 Download Results (.xlsx)",
            data=ss.result_bytes,
            file_name="scan_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_results_top",
        )
        if ss.failed_bytes:
            dl2.download_button(
                "⚠️ Download Failed Rows (.xlsx)",
                data=ss.failed_bytes,
                file_name="scan_failed.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_failed_top",
            )

    if ss.partial_bytes and not ss.scan_done:
        st.download_button(
            "💾 Download Partial Results (.xlsx)",
            data=ss.partial_bytes,
            file_name="scan_partial.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_partial",
        )


# ══════════════════════════════════════════════════════════════════════
#  PAUSE / STOP / SAVE HANDLERS
# ══════════════════════════════════════════════════════════════════════

if pause_btn and ss.scan_running and ss.pause_event is not None:
    if ss.scan_paused:
        ss.pause_event.clear()
        ss.scan_paused = False
        ss.log_lines.append('<span class="log-ok">[--:--:--] ▶ Resumed by user.</span>')
    else:
        ss.pause_event.set()
        ss.scan_paused = True
        ss.log_lines.append('<span class="log-warn">[--:--:--] ⏸ Paused — workers will finish current request then wait.</span>')
    st.rerun()

if stop_btn and ss.scan_running and ss.stop_event is not None:
    ss.stop_event.set()
    if ss.pause_event is not None:
        ss.pause_event.clear()
    ss.log_lines.append('<span class="log-warn">[--:--:--] ⏹ Stop requested …</span>')
    st.rerun()

if save_btn and ss.scan_running:
    if ss.partial_bytes:
        ss.log_lines.append(
            f'<span class="log-ok">[--:--:--] 💾 Manual save triggered — '
            f'{ss.stat_done} rows available.</span>'
        )
    st.rerun()


# ══════════════════════════════════════════════════════════════════════
#  RUN SCAN  ← ALL FIXES APPLIED HERE
# ══════════════════════════════════════════════════════════════════════

if run_btn:
    # ── FIX: check session_state cache, NOT the widget (widget is None on rerun)
    if not st.session_state._links_bytes:
        st.error("❌ Please upload a Links Sheet (.xlsx) first.")
        st.stop()

    keywords     = [k.strip() for k in kw_text.splitlines()  if k.strip()]
    mil_keywords = [k.strip() for k in mil_text.splitlines() if k.strip()]

    if not keywords:
        st.error("❌ Enter at least one keyword.")
        st.stop()

    # ── FIX: file bytes are ALREADY in session_state — do NOT call .read()
    # on the widget here; it may be None after any rerun.
    # ss._links_bytes and ss._ref_bytes were saved by the sidebar uploader block.

    # Reset state for new run
    ss.log_lines     = []
    ss.scan_done     = False
    ss.scan_paused   = False
    ss.scan_status   = ""
    ss.result_bytes  = None
    ss.failed_bytes  = None
    ss.partial_bytes = None
    ss.stat_total_rows = 0
    ss.stat_total_jobs = 0
    ss.stat_done       = 0
    ss.stat_failed     = 0
    ss.stat_chunk      = "—"
    ss.stat_cb         = 0
    ss.stat_pct        = 0

    stop_event  = threading.Event()
    pause_event = threading.Event()
    ss.stop_event  = stop_event
    ss.pause_event = pause_event

    # ── FIX: mark running BEFORE rerun so UI shows RUNNING immediately
    ss.scan_running = True

    # Load & validate dataframe (fast, synchronous — no network)
    try:
        df = pd.read_excel(io.BytesIO(ss._links_bytes), dtype={part_col: str})
    except Exception as e:
        st.error(f"Cannot open links file: {e}")
        ss.scan_running = False
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
        ss.scan_running = False
        st.stop()

    if part_col_lower not in df_work.columns:
        ss.log_lines.append(
            f'<span class="log-warn">[--:--:--] ⚠ Column \'{part_col}\' not found — '
            f'Part_Scanned will be FALSE for all.</span>'
        )
        df_work[part_col_lower] = ""

    df_work[url_col_lower]  = df_work[url_col_lower].apply(_safe_str)
    df_work[part_col_lower] = df_work[part_col_lower].apply(_safe_str)

    orig_ztemp   = df_work.get("ztemperaturegrade",         pd.Series("", index=df_work.index))
    orig_feature = df_work.get("featurevalue",              pd.Series("", index=df_work.index))
    orig_qual    = df_work.get("qualificationrangemapping", pd.Series("", index=df_work.index))

    ref_map = {}
    if ss._ref_bytes:
        ref_map = _build_ref_map(ss._ref_bytes)

    # Launch background daemon thread (survives Streamlit reruns)
    t = threading.Thread(
        target=_run_scan_thread,
        args=(
            df, df_work, url_col_lower, part_col_lower,
            orig_qual, orig_ztemp, orig_feature,
            keywords, mil_keywords,
            ref_map, int(chunk_size), n_workers, timeout,
            rpm, delay_min, delay_max, cb_errors, cb_pause,
            stop_event, pause_event,
        ),
        daemon=True,
    )
    t.start()

    # Rerun immediately so UI flips to RUNNING state
    st.rerun()
