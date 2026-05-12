"""
Keyword & Part Scanner — Flask Edition
=======================================
- Zero page refresh: كل حاجة بتشتغل بـ AJAX في نفس الصفحة
- الفايلات بتتحفظ في الـ server memory ومش بتروح
- السكان بيشتغل في background thread
- Live stats عن طريق SSE (Server-Sent Events)
- تشغيل: python scanner_flask.py
- افتح المتصفح على: http://localhost:5000
"""

import re, time, random, threading, io, os, collections, base64, uuid, json
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import fitz
import urllib3
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from flask import Flask, request, jsonify, send_file, Response, render_template_string

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ══════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════
MAX_HTML_BYTES = 300 * 1024
MAX_PDF_BYTES  = 50  * 1024 * 1024
HEAD_TIMEOUT   = 8
MAX_RETRIES    = 3
RETRY_BACKOFF  = [5, 15, 30]
RETRY_ON_CODES = {429, 503, 502, 504}
AUTOSAVE_EVERY = 20

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 Version/17.4.1 Safari/605.1.15",
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
#  GLOBAL STATE  (lives in process memory — never lost on browser action)
# ══════════════════════════════════════════════════════════════════════
state = {
    "links_bytes":   None,   # bytes of uploaded links xlsx
    "ref_bytes":     None,   # bytes of uploaded ref xlsx
    "links_name":    "",
    "ref_name":      "",
    "links_cols":    [],     # column names for preview
    "scan_running":  False,
    "scan_paused":   False,
    "stop_event":    None,
    "pause_event":   None,
    "log":           [],     # list of {"ts","msg","tag"}
    "stat": {
        "total_rows": 0, "total_jobs": 0, "done": 0,
        "failed": 0, "chunk": "—", "cb_errors": 0, "pct": 0,
    },
    "result_bytes":  None,   # final xlsx
    "failed_bytes":  None,
    "partial_bytes": None,
    "status":        "",     # "running"|"paused"|"complete"|"stopped"|"error"|""
    "log_lock":      threading.Lock(),
    "state_lock":    threading.Lock(),
    # SSE subscribers
    "sse_queues":    [],
    "sse_lock":      threading.Lock(),
}

def _push_sse(event_type: str, data: dict):
    """Push an event to all SSE subscribers."""
    msg = f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
    with state["sse_lock"]:
        dead = []
        for q in state["sse_queues"]:
            try:
                q.append(msg)
            except Exception:
                dead.append(q)
        for q in dead:
            state["sse_queues"].remove(q)

def _log(msg: str, tag: str = ""):
    ts = time.strftime("%H:%M:%S")
    entry = {"ts": ts, "msg": msg, "tag": tag}
    with state["log_lock"]:
        state["log"].append(entry)
        if len(state["log"]) > 500:
            state["log"] = state["log"][-500:]
    _push_sse("log", entry)

def _push_stats():
    _push_sse("stats", {**state["stat"], "status": state["status"]})

# ══════════════════════════════════════════════════════════════════════
#  SCRAPER HELPERS  (unchanged logic from v6)
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
        self._lock    = threading.Lock()
        self._errors  = 0
        self._tripped = False
        self._trip_time = 0
    def record_success(self):
        with self._lock:
            self._errors = 0; self._tripped = False
    def record_error(self):
        with self._lock:
            self._errors += 1
            if self._errors >= self.error_threshold and not self._tripped:
                self._tripped = True; self._trip_time = time.time()
    def wait_if_tripped(self):
        with self._lock:
            if not self._tripped: return
            remaining = self.pause_seconds - (time.time() - self._trip_time)
            if remaining <= 0:
                self._tripped = False; self._errors = 0; return
        time.sleep(max(0, remaining))
        with self._lock:
            self._tripped = False; self._errors = 0
    @property
    def error_count(self):
        with self._lock: return self._errors

def _safe_str(val):
    if val is None: return ""
    if isinstance(val, float):
        if val != val: return ""
        if val == int(val): return str(int(val))
        return str(val)
    return str(val).strip()

def _normalize_kw(text): return re.sub(r"\s+", " ", text.lower().replace("-", " "))
def _kw_variants(kw):
    k = kw.lower()
    return list({k, k.replace("-", " "), k.replace("-", "")})
def _normalize_cmp(text):
    if pd.isna(text): return ""
    t = str(text).lower().strip().replace("–","-").replace("—","-").replace("°c","c").replace("°","")
    return re.sub(r"[^0-9a-z\-\+to]","", re.sub(r"\s+","",t))
def _all_terms_found(text_lower, kw_variants_list, part_lower):
    for variants in kw_variants_list:
        if not any(v in text_lower for v in variants): return False
    if part_lower and part_lower not in text_lower: return False
    return True

def _probe_content_type(url, session):
    ct = ""
    for attempt in range(MAX_RETRIES):
        try:
            r = session.head(url, timeout=HEAD_TIMEOUT, verify=False, allow_redirects=True, headers=_get_headers())
            if r.status_code in RETRY_ON_CODES:
                time.sleep(RETRY_BACKOFF[min(attempt, 2)] + random.uniform(0,2)); continue
            ct = r.headers.get("Content-Type","").lower(); break
        except: ct = ""; break
    if "pdf" in ct: return "pdf"
    if any(x in ct for x in ("html","xml","text")): return "html"
    low = url.lower().split("?")[0]
    if low.endswith(".pdf"): return "pdf"
    if any(low.endswith(e) for e in (".html",".htm",".php",".asp",".aspx","/")):return "html"
    if ct: return "skip"
    return "html"

def _read_pdf(url, kw_variants_list, part_lower, session):
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, timeout=30, verify=False, headers=_get_headers(), stream=True)
            if r.status_code in RETRY_ON_CODES:
                r.close(); time.sleep(RETRY_BACKOFF[min(attempt,2)] + random.uniform(1,4)); continue
            content = b""
            for chunk in r.iter_content(65536):
                content += chunk
                if len(content) >= MAX_PDF_BYTES: break
            r.close()
            doc   = fitz.open(stream=content, filetype="pdf")
            parts = [page.get_text() for page in doc if page.get_text().strip()]
            doc.close()
            return " ".join(parts)
        except Exception as e:
            time.sleep(RETRY_BACKOFF[min(attempt,2)] + random.uniform(1,3))
    return ""

def _read_html(url, timeout, kw_variants_list, part_lower, session):
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, timeout=timeout, verify=False, headers=_get_headers(), stream=True)
            if r.status_code in RETRY_ON_CODES:
                r.close(); time.sleep(RETRY_BACKOFF[min(attempt,2)] + random.uniform(1,4)); continue
            r.raise_for_status()
            raw_bytes = b""
            for chunk in r.iter_content(32768):
                raw_bytes += chunk
                if _all_terms_found(raw_bytes.decode("utf-8","ignore").lower(), kw_variants_list, part_lower): break
                if len(raw_bytes) >= MAX_HTML_BYTES: break
            r.close()
            soup = BeautifulSoup(raw_bytes.decode("utf-8","ignore"), "html.parser")
            for tag in soup(["script","style","noscript","head"]): tag.decompose()
            return soup.get_text(separator=" ", strip=True)
        except Exception as e:
            last_exc = e; time.sleep(RETRY_BACKOFF[min(attempt,2)] + random.uniform(1,3))
    raise last_exc or RuntimeError("HTML fetch failed")

def _count_keywords(raw, keywords, mil_keywords):
    norm = _normalize_kw(raw)
    result = {}
    for kw in keywords:
        total = sum(len(re.findall(re.escape(v), norm)) for v in _kw_variants(kw))
        result[kw] = 1 if total > 0 else 0
    result["Military"] = 0
    for kw in mil_keywords:
        if sum(len(re.findall(re.escape(v), norm)) for v in _kw_variants(kw)) > 0:
            result["Military"] = 1; break
    return result

def _search_part(raw, part):
    p = _safe_str(part)
    if not p or p.lower() == "nan": return "FALSE"
    return "TRUE" if p.lower() in raw.lower() else "FALSE"

def _process_row(row_index, url, part, keywords, mil_keywords, timeout,
                 rate_limiter, circuit_breaker, session, stop_event, pause_event):
    url_str = _safe_str(url); part_str = _safe_str(part)
    if pause_event:
        while pause_event.is_set():
            if stop_event and stop_event.is_set(): raise InterruptedError("Stopped")
            time.sleep(0.5)
    if stop_event and stop_event.is_set(): raise InterruptedError("Stopped")
    if circuit_breaker: circuit_breaker.wait_if_tripped()
    if rate_limiter:    rate_limiter.wait()
    kw_variants_l = [_kw_variants(k) for k in keywords + mil_keywords]
    part_lower    = part_str.lower() if part_str and part_str.lower() != "nan" else ""
    try:
        ctype = _probe_content_type(url_str, session)
        if   ctype == "skip": raw = ""
        elif ctype == "pdf":  raw = _read_pdf(url_str, kw_variants_l, part_lower, session)
        else:                 raw = _read_html(url_str, timeout, kw_variants_l, part_lower, session)
        if circuit_breaker: circuit_breaker.record_success()
    except InterruptedError: raise
    except Exception as e:
        if circuit_breaker: circuit_breaker.record_error()
        raise
    row = {"_row_index": row_index, "_scan_url": url_str, "_scan_part": part_str}
    row.update(_count_keywords(raw, keywords, mil_keywords))
    row["Part_Scanned"] = _search_part(raw, part_str)
    return row

def _build_ref_map(ref_bytes):
    try:
        ref = pd.read_excel(io.BytesIO(ref_bytes))
        ref.columns = [c.strip().lower() for c in ref.columns]
        if "qualificationrangemapping" not in ref.columns or "ztemperaturegrade" not in ref.columns: return {}
        ref["_norm"] = ref["qualificationrangemapping"].apply(_normalize_cmp)
        return dict(zip(ref["_norm"], ref["ztemperaturegrade"]))
    except: return {}

def _compare_ztemp(qual, ztemp, ref_map):
    v = ref_map.get(_normalize_cmp(qual))
    if v is None: return "NOT FOUND"
    return "TRUE" if str(v).strip() == str(ztemp).strip() else "FALSE"

def _compare_aec(kw, scanned, feat):
    feat = "" if pd.isna(feat) else str(feat).strip()
    return "TRUE" if feat == kw and int(scanned) == 1 else "FALSE"

def _compare_mil(scanned, ztemp):
    if pd.isna(ztemp): return "FALSE"
    return "TRUE" if str(ztemp).strip().lower() == "military" and int(scanned) == 1 else "FALSE"

def _apply_results_to_df(df_work, results, keywords, orig_qual, orig_ztemp, orig_feature, ref_map):
    idx_map = {r["_row_index"]: r for r in results}
    def get_r(i, f, d): return idx_map.get(i, {}).get(f, d)
    for kw in keywords:
        df_work[kw] = [get_r(i, kw, 0) for i in df_work.index]
    df_work["Military"]     = [get_r(i, "Military", 0)       for i in df_work.index]
    df_work["Part_Scanned"] = [get_r(i, "Part_Scanned","FALSE") for i in df_work.index]
    if ref_map:
        df_work["RESULT"] = [_compare_ztemp(q,z,ref_map) for q,z in zip(orig_qual,orig_ztemp)]
    for kw in keywords:
        df_work[f"{kw}_RESULT"] = [_compare_aec(kw,s,f) for s,f in zip(df_work[kw],orig_feature)]
    df_work["Military_RESULT"] = [_compare_mil(m,z) for m,z in zip(df_work["Military"],orig_ztemp)]
    return df_work

def _highlight_excel(df):
    buf = io.BytesIO(); df.to_excel(buf, index=False); buf.seek(0)
    green  = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    red    = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    yellow = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
    wb = load_workbook(buf); ws = wb.active
    header  = [c.value for c in ws[1]]
    hl_cols = [c for c in header if c and (c.endswith("_RESULT") or c in ("RESULT","Part_Scanned","Military_RESULT"))]
    for col in hl_cols:
        ci = header.index(col) + 1
        for r in range(2, ws.max_row+1):
            cell = ws.cell(row=r, column=ci)
            if   cell.value == "TRUE":      cell.fill = green
            elif cell.value == "FALSE":     cell.fill = red
            elif cell.value == "NOT FOUND": cell.fill = yellow
    out = io.BytesIO(); wb.save(out); wb.close(); return out.getvalue()

def _build_partial_excel(df_work, results, keywords, orig_qual, orig_ztemp, orig_feature, ref_map):
    try:
        return _highlight_excel(_apply_results_to_df(df_work.copy(), results, keywords, orig_qual, orig_ztemp, orig_feature, ref_map))
    except: return b""

# ══════════════════════════════════════════════════════════════════════
#  BACKGROUND SCAN THREAD
# ══════════════════════════════════════════════════════════════════════

def _run_scan(df, df_work, url_col, part_col, orig_qual, orig_ztemp, orig_feature,
              keywords, mil_keywords, ref_map,
              chunk_size, n_workers, timeout, rpm, delay_min, delay_max,
              cb_errors, cb_pause, stop_event, pause_event):

    def autosave(results, label=""):
        pb = _build_partial_excel(df_work.copy(), results, keywords, orig_qual, orig_ztemp, orig_feature, ref_map)
        if pb:
            state["partial_bytes"] = pb
            if label: _log(f"💾 Auto-saved {len(results)} rows {label}", "dim")
        _push_sse("partial_ready", {})

    _log(f"Workers: {n_workers} | Timeout: {timeout}s | RPM: {rpm}", "info")
    _log(f"PDF cap: {MAX_PDF_BYTES//1024//1024} MB (all pages) | HTML cap: {MAX_HTML_BYTES//1024} KB", "info")

    all_jobs = [(idx, row[url_col], row[part_col])
                for idx, row in df_work.iterrows()
                if _safe_str(row[url_col]) and _safe_str(row[url_col]).lower() != "nan"]

    total_rows = len(df_work)
    total_jobs = len(all_jobs)
    state["stat"]["total_rows"] = total_rows
    state["stat"]["total_jobs"] = total_jobs
    _log(f"Rows: {total_rows} | Jobs: {total_jobs}", "info")
    _push_stats()

    chunks = [all_jobs[i:i+chunk_size] for i in range(0, total_jobs, chunk_size)]
    _log(f"Chunks: {len(chunks)} | Chunk size: {chunk_size}", "info")

    rl  = RateLimiter(rpm)
    cb  = CircuitBreaker(cb_errors, cb_pause)
    ses = requests.Session()

    all_results = []
    failed      = []
    done_cnt    = 0
    user_stopped = False

    try:
        for ci, chunk in enumerate(chunks):
            if stop_event.is_set(): user_stopped = True; break
            state["stat"]["chunk"] = f"{ci+1}/{len(chunks)}"
            _log(f"── Chunk {ci+1}/{len(chunks)} ({len(chunk)} jobs) ──", "info")
            _push_stats()

            with ThreadPoolExecutor(max_workers=n_workers) as pool:
                futures = {
                    pool.submit(_process_row, ri, url, part, keywords, mil_keywords,
                                timeout, rl, cb, ses, stop_event, pause_event): (ri, url, part)
                    for ri, url, part in chunk
                }
                for fut in as_completed(futures):
                    ri, url, part = futures[fut]
                    try:
                        r = fut.result()
                        all_results.append(r); done_cnt += 1
                        tag   = "ok" if r.get("Part_Scanned") == "TRUE" else "dim"
                        short = url[:70]+"…" if len(url)>70 else url
                        _log(f"✓ [{ri}] {short} | Part={r.get('Part_Scanned')}", tag)
                    except InterruptedError:
                        done_cnt += 1
                        failed.append({"row_index":ri,"url":url,"part":part,"error":"Stopped"})
                        all_results.append({"_row_index":ri,"_scan_url":url,"_scan_part":part,
                                            **{k:0 for k in keywords},"Military":0,"Part_Scanned":"FALSE"})
                    except Exception as e:
                        done_cnt += 1
                        failed.append({"row_index":ri,"url":url,"part":part,"error":str(e)})
                        _log(f"✗ [{ri}] {url[:70]} | {e}", "err")
                        all_results.append({"_row_index":ri,"_scan_url":url,"_scan_part":part,
                                            **{k:0 for k in keywords},"Military":0,"Part_Scanned":"FALSE"})

                    if done_cnt % AUTOSAVE_EVERY == 0:
                        autosave(all_results, f"({done_cnt}/{total_jobs})")

                    pct = int(done_cnt / total_jobs * 100) if total_jobs else 0
                    state["stat"].update({"done":done_cnt,"failed":len(failed),"cb_errors":cb.error_count,"pct":pct})
                    _push_stats()

                    if stop_event.is_set(): user_stopped = True; break

            autosave(all_results, f"after chunk {ci+1}")
            if stop_event.is_set(): user_stopped = True; break
            if ci < len(chunks)-1:
                p = random.uniform(delay_min*2, delay_max*2)
                _log(f"⏸ Pausing {p:.1f}s …", "dim"); time.sleep(p)

    except Exception as e:
        _log(f"💥 Unexpected: {e}", "err")
        autosave(all_results, "(error recovery)")
    finally:
        ses.close()

    if not all_results:
        state["status"] = "error"; state["scan_running"] = False
        _log("No results.", "warn"); _push_stats(); return

    df_final = _apply_results_to_df(df_work.copy(), all_results, keywords,
                                    orig_qual, orig_ztemp, orig_feature, ref_map)
    orig_cols = {c.strip().lower(): c.strip() for c in df.columns}
    df_final.rename(columns=orig_cols, inplace=True)
    result_bytes = _highlight_excel(df_final)
    state["result_bytes"] = result_bytes
    state["stat"]["pct"]  = 100

    if user_stopped:
        state["partial_bytes"] = result_bytes
        state["status"] = "stopped"
        _log("⏹ Scan stopped — file ready.", "warn")
    else:
        state["status"] = "complete"
        _log("✅ SCAN COMPLETE", "ok")

    if failed:
        buf = io.BytesIO(); pd.DataFrame(failed).to_excel(buf, index=False)
        state["failed_bytes"] = buf.getvalue()
        _log(f"Failed rows: {len(failed)}", "warn")

    tp = (df_final.get("Part_Scanned", pd.Series()) == "TRUE").sum()
    tm = (df_final.get("Military_RESULT", pd.Series()) == "TRUE").sum()
    _log(f"Total: {len(df_final)} | Part TRUE: {tp} | Military TRUE: {tm}", "ok")

    state["scan_running"] = False
    _push_stats()
    _push_sse("done", {"status": state["status"]})

# ══════════════════════════════════════════════════════════════════════
#  FLASK APP
# ══════════════════════════════════════════════════════════════════════

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200 MB upload limit

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>⬡ Keyword & Part Scanner</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Sora:wght@400;600;700&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0b0d14;--sidebar:#0f1117;--card:#1a1d27;--border:#2d3148;
  --text:#e2e8f0;--muted:#94a3b8;--dim:#64748b;
  --blue:#4f8ef7;--green:#22c55e;--red:#ef4444;--amber:#d97706;--teal:#0d9488;
}
body{font-family:'Sora',sans-serif;background:var(--bg);color:var(--text);display:flex;height:100vh;overflow:hidden}

/* ── SIDEBAR ── */
#sidebar{width:300px;min-width:300px;background:var(--sidebar);border-right:1px solid var(--border);
  display:flex;flex-direction:column;overflow-y:auto;padding:16px}
#sidebar h2{font-family:'JetBrains Mono',monospace;font-size:.9rem;color:var(--blue);
  margin-bottom:16px;letter-spacing:.05em}
.sh{font-size:.68rem;font-weight:700;color:var(--dim);text-transform:uppercase;
  letter-spacing:.12em;border-bottom:1px solid var(--border);padding-bottom:4px;margin:14px 0 8px}
label{display:block;font-size:.72rem;font-weight:600;color:var(--muted);margin-bottom:4px}
input[type=text],textarea,select,input[type=number]{
  width:100%;background:var(--card);color:var(--text);border:1px solid var(--border);
  border-radius:6px;padding:6px 8px;font-size:.78rem;font-family:'Sora',sans-serif;outline:none}
input[type=range]{width:100%;accent-color:var(--blue)}
textarea{resize:vertical;line-height:1.5}
.range-row{display:flex;justify-content:space-between;align-items:center;gap:8px}
.range-row input[type=number]{width:64px;flex-shrink:0}

/* file upload zone */
.upload-zone{border:2px dashed var(--border);border-radius:8px;padding:12px;
  text-align:center;cursor:pointer;transition:.2s;position:relative;margin-bottom:6px}
.upload-zone:hover{border-color:var(--blue);background:#1a2035}
.upload-zone input{position:absolute;inset:0;opacity:0;cursor:pointer;width:100%;height:100%}
.upload-zone .uz-label{font-size:.75rem;color:var(--muted)}
.upload-zone .uz-name{font-size:.7rem;color:var(--green);margin-top:4px;word-break:break-all}
.file-ok{background:#0d2218;border-color:var(--green)}

/* ── MAIN ── */
#main{flex:1;display:flex;flex-direction:column;overflow:hidden;padding:16px;gap:12px}

/* title */
.title-bar{background:var(--card);border:1px solid var(--border);border-radius:12px;
  padding:14px 20px;display:flex;align-items:center;justify-content:space-between}
.title-bar h1{font-family:'JetBrains Mono',monospace;font-size:1rem;font-weight:700;color:var(--blue)}
.title-bar span{font-size:.7rem;color:var(--green);font-family:'JetBrains Mono',monospace}

/* stats */
.stats-row{display:grid;grid-template-columns:repeat(6,1fr);gap:8px}
.stat-card{background:var(--card);border:1px solid var(--border);border-radius:10px;
  padding:10px 12px}
.stat-card .sv{font-family:'JetBrains Mono',monospace;font-size:1.1rem;font-weight:700;color:var(--text)}
.stat-card .sk{font-size:.65rem;color:var(--muted);margin-top:2px}

/* progress */
.prog-wrap{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:12px 16px}
progress{width:100%;height:8px;border-radius:4px;border:none;background:var(--border)}
progress::-webkit-progress-bar{background:var(--border);border-radius:4px}
progress::-webkit-progress-value{background:var(--blue);border-radius:4px;transition:width .3s}
.prog-text{font-size:.72rem;color:var(--muted);margin-top:6px;font-family:'JetBrains Mono',monospace}
.status-badge{display:inline-block;padding:3px 10px;border-radius:20px;font-size:.7rem;font-weight:700;
  margin-left:12px;font-family:'JetBrains Mono',monospace}
.s-running{background:#1a2a4a;color:var(--blue)}
.s-paused{background:#2a1a00;color:var(--amber)}
.s-complete{background:#0d2218;color:var(--green)}
.s-stopped{background:#2a1010;color:var(--red)}

/* buttons */
.btn-row{display:flex;gap:8px}
button{padding:9px 20px;border:none;border-radius:8px;font-family:'Sora',sans-serif;
  font-size:.8rem;font-weight:600;cursor:pointer;transition:opacity .2s}
button:hover{opacity:.85}
button:disabled{opacity:.4;cursor:not-allowed}
#btn-run{background:var(--blue);color:#fff}
#btn-pause{background:var(--amber);color:#fff}
#btn-stop{background:var(--red);color:#fff}
#btn-save{background:var(--teal);color:#fff}

/* log */
.log-box{flex:1;background:var(--card);border:1px solid var(--border);border-radius:8px;
  padding:12px 16px;font-family:'JetBrains Mono',monospace;font-size:.7rem;
  overflow-y:auto;white-space:pre-wrap;word-break:break-all;min-height:0}
.log-ok{color:var(--green)}.log-err{color:var(--red)}.log-warn{color:var(--amber)}
.log-info{color:var(--blue)}.log-dim{color:var(--dim)}

/* downloads */
.dl-row{display:flex;gap:8px;flex-wrap:wrap}
.dl-btn{padding:8px 16px;background:#1a2a4a;border:1px solid var(--blue);color:var(--blue);
  border-radius:8px;font-size:.78rem;font-weight:600;text-decoration:none;cursor:pointer;
  font-family:'Sora',sans-serif;display:inline-block;transition:.2s}
.dl-btn:hover{background:#2a3a5a}
.dl-btn.partial{border-color:var(--teal);color:var(--teal);background:#0d1f1e}
.dl-btn.failed{border-color:var(--amber);color:var(--amber);background:#1f1800}
</style>
</head>
<body>

<!-- ══ SIDEBAR ══ -->
<div id="sidebar">
  <h2>⬡ Configuration</h2>

  <div class="sh">📁 Files</div>

  <label>Links Sheet (.xlsx)</label>
  <div class="upload-zone" id="zone-links">
    <input type="file" accept=".xlsx" id="inp-links" onchange="uploadFile('links',this)">
    <div class="uz-label">Click or drag to upload</div>
    <div class="uz-name" id="links-name">No file</div>
  </div>
  <div id="links-cols" style="font-size:.65rem;color:var(--dim);margin-bottom:8px"></div>

  <label>Reference Sheet (.xlsx) — optional</label>
  <div class="upload-zone" id="zone-ref">
    <input type="file" accept=".xlsx" id="inp-ref" onchange="uploadFile('ref',this)">
    <div class="uz-label">Click or drag to upload</div>
    <div class="uz-name" id="ref-name">No file</div>
  </div>

  <div class="sh">🗂 Columns</div>
  <label>URL column</label>
  <input type="text" id="url-col" value="offlineURL">
  <br><br>
  <label>Part Number column</label>
  <input type="text" id="part-col" value="PartNumber">

  <div class="sh">🔑 Keywords</div>
  <label>Keywords (one per line)</label>
  <textarea id="kw-text" rows="4">AEC-Q100
AEC-Q200
AEC-Q101</textarea>
  <br>
  <label>Military Keywords (one per line)</label>
  <textarea id="mil-text" rows="4">Military
MIL-PRF
MIL-C
MIL-R
MIL-DTL</textarea>

  <div class="sh">⚡ Performance</div>
  <label>Workers: <b id="lbl-workers">4</b></label>
  <input type="range" id="workers" min="1" max="50" value="4" oninput="document.getElementById('lbl-workers').textContent=this.value">
  <br><br>
  <label>Page timeout (s): <b id="lbl-timeout">15</b></label>
  <input type="range" id="timeout" min="5" max="120" value="15" oninput="document.getElementById('lbl-timeout').textContent=this.value">
  <br><br>
  <label>Chunk size</label>
  <input type="number" id="chunk-size" value="500" min="50" max="50000" step="50">
  <br><br>
  <label>Max requests/min: <b id="lbl-rpm">30</b></label>
  <input type="range" id="rpm" min="1" max="300" value="30" oninput="document.getElementById('lbl-rpm').textContent=this.value">

  <div class="sh">🛡 Anti-Detection</div>
  <div class="range-row">
    <div><label>Min delay (s)</label><input type="number" id="delay-min" value="0.5" min="0" max="30" step="0.5"></div>
    <div><label>Max delay (s)</label><input type="number" id="delay-max" value="2.0" min="0" max="60" step="0.5"></div>
  </div>
  <br>
  <label>CB error threshold: <b id="lbl-cbe">5</b></label>
  <input type="range" id="cb-errors" min="1" max="50" value="5" oninput="document.getElementById('lbl-cbe').textContent=this.value">
  <br><br>
  <label>CB pause (s): <b id="lbl-cbp">60</b></label>
  <input type="range" id="cb-pause" min="5" max="600" value="60" oninput="document.getElementById('lbl-cbp').textContent=this.value">
</div>

<!-- ══ MAIN ══ -->
<div id="main">
  <div class="title-bar">
    <h1>⬡ KEYWORD &amp; PART SCANNER</h1>
    <span>Flask · Background Thread · Zero Page Refresh · Auto-Save</span>
  </div>

  <!-- Stats -->
  <div class="stats-row">
    <div class="stat-card"><div class="sv" id="s-rows">—</div><div class="sk">Total Rows</div></div>
    <div class="stat-card"><div class="sv" id="s-jobs">—</div><div class="sk">Jobs</div></div>
    <div class="stat-card"><div class="sv" id="s-done">0</div><div class="sk">Completed</div></div>
    <div class="stat-card"><div class="sv" id="s-fail">0</div><div class="sk">Failed</div></div>
    <div class="stat-card"><div class="sv" id="s-chunk">—</div><div class="sk">Chunk</div></div>
    <div class="stat-card"><div class="sv" id="s-cb">0</div><div class="sk">CB Errors</div></div>
  </div>

  <!-- Progress -->
  <div class="prog-wrap">
    <div style="display:flex;align-items:center">
      <progress id="prog" value="0" max="100"></progress>
      <span class="status-badge s-running" id="status-badge" style="display:none"></span>
    </div>
    <div class="prog-text" id="prog-text">Ready</div>
  </div>

  <!-- Buttons -->
  <div class="btn-row">
    <button id="btn-run"   onclick="runScan()">▶ RUN SCAN</button>
    <button id="btn-pause" onclick="togglePause()" disabled>⏸ PAUSE</button>
    <button id="btn-stop"  onclick="stopScan()"    disabled>⏹ STOP</button>
    <button id="btn-save"  onclick="saveNow()"     disabled>💾 SAVE NOW</button>
  </div>

  <!-- Downloads -->
  <div class="dl-row" id="dl-row"></div>

  <!-- Log -->
  <div class="log-box" id="log-box"></div>
</div>

<script>
let paused = false;
let evtSource = null;

// ── SSE connection ──────────────────────────────────────────────────
function connectSSE() {
  if (evtSource) evtSource.close();
  evtSource = new EventSource('/sse');

  evtSource.addEventListener('log', e => {
    const d = JSON.parse(e.data);
    appendLog(d.ts, d.msg, d.tag);
  });
  evtSource.addEventListener('stats', e => {
    const d = JSON.parse(e.data);
    updateStats(d);
  });
  evtSource.addEventListener('done', e => {
    const d = JSON.parse(e.data);
    onDone(d.status);
  });
  evtSource.addEventListener('partial_ready', e => {
    refreshDownloads();
  });
  evtSource.onerror = () => {
    // reconnect after 2s
    setTimeout(connectSSE, 2000);
  };
}

// ── Log ─────────────────────────────────────────────────────────────
function appendLog(ts, msg, tag) {
  const box = document.getElementById('log-box');
  const cls = {ok:'log-ok',err:'log-err',warn:'log-warn',info:'log-info',dim:'log-dim'}[tag]||'';
  const line = document.createElement('div');
  line.className = cls;
  line.textContent = `[${ts}] ${msg}`;
  box.appendChild(line);
  // keep last 400 lines
  while (box.children.length > 400) box.removeChild(box.firstChild);
  box.scrollTop = box.scrollHeight;
}

// ── Stats ────────────────────────────────────────────────────────────
function updateStats(d) {
  document.getElementById('s-rows').textContent  = d.total_rows || '—';
  document.getElementById('s-jobs').textContent  = d.total_jobs || '—';
  document.getElementById('s-done').textContent  = d.done;
  document.getElementById('s-fail').textContent  = d.failed;
  document.getElementById('s-chunk').textContent = d.chunk || '—';
  document.getElementById('s-cb').textContent    = d.cb_errors;
  document.getElementById('prog').value          = d.pct;
  document.getElementById('prog-text').textContent =
    `${d.done} / ${d.total_jobs} jobs  (${d.pct}%)`;

  const badge = document.getElementById('status-badge');
  const statusMap = {
    running:  ['● RUNNING',  's-running'],
    paused:   ['⏸ PAUSED',   's-paused'],
    complete: ['✅ COMPLETE', 's-complete'],
    stopped:  ['⏹ STOPPED',  's-stopped'],
  };
  const s = d.status;
  if (s && statusMap[s]) {
    badge.textContent  = statusMap[s][0];
    badge.className    = 'status-badge ' + statusMap[s][1];
    badge.style.display = 'inline-block';
  }
}

// ── File upload ──────────────────────────────────────────────────────
function uploadFile(which, input) {
  if (!input.files.length) return;
  const file = input.files[0];
  const fd   = new FormData();
  fd.append('file', file);
  fd.append('which', which);

  fetch('/upload', {method:'POST', body:fd})
    .then(r => r.json())
    .then(d => {
      if (d.ok) {
        const nameEl = document.getElementById(`${which}-name`);
        const zone   = document.getElementById(`zone-${which}`);
        nameEl.textContent = '✅ ' + d.filename;
        zone.classList.add('file-ok');
        if (which === 'links' && d.columns) {
          document.getElementById('links-cols').textContent = 'Columns: ' + d.columns.join(' | ');
        }
      } else {
        alert('Upload failed: ' + d.error);
      }
    });
}

// ── Run ──────────────────────────────────────────────────────────────
function runScan() {
  const cfg = {
    url_col:    document.getElementById('url-col').value,
    part_col:   document.getElementById('part-col').value,
    kw_text:    document.getElementById('kw-text').value,
    mil_text:   document.getElementById('mil-text').value,
    n_workers:  +document.getElementById('workers').value,
    timeout:    +document.getElementById('timeout').value,
    chunk_size: +document.getElementById('chunk-size').value,
    rpm:        +document.getElementById('rpm').value,
    delay_min:  +document.getElementById('delay-min').value,
    delay_max:  +document.getElementById('delay-max').value,
    cb_errors:  +document.getElementById('cb-errors').value,
    cb_pause:   +document.getElementById('cb-pause').value,
  };

  // clear log & downloads
  document.getElementById('log-box').innerHTML = '';
  document.getElementById('dl-row').innerHTML  = '';

  fetch('/start', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(cfg)})
    .then(r => r.json())
    .then(d => {
      if (!d.ok) { alert(d.error); return; }
      paused = false;
      document.getElementById('btn-run').disabled   = true;
      document.getElementById('btn-pause').disabled = false;
      document.getElementById('btn-stop').disabled  = false;
      document.getElementById('btn-save').disabled  = false;
      document.getElementById('btn-pause').textContent = '⏸ PAUSE';
      const badge = document.getElementById('status-badge');
      badge.textContent = '● RUNNING'; badge.className = 'status-badge s-running';
      badge.style.display = 'inline-block';
    });
}

// ── Pause / Resume ───────────────────────────────────────────────────
function togglePause() {
  const action = paused ? 'resume' : 'pause';
  fetch('/control', {method:'POST', headers:{'Content-Type':'application/json'},
                     body: JSON.stringify({action})})
    .then(r => r.json()).then(d => {
      if (d.ok) {
        paused = !paused;
        document.getElementById('btn-pause').textContent = paused ? '▶ RESUME' : '⏸ PAUSE';
      }
    });
}

// ── Stop ─────────────────────────────────────────────────────────────
function stopScan() {
  fetch('/control', {method:'POST', headers:{'Content-Type':'application/json'},
                     body: JSON.stringify({action:'stop'})}).then(r=>r.json());
}

// ── Save Now ─────────────────────────────────────────────────────────
function saveNow() {
  fetch('/control', {method:'POST', headers:{'Content-Type':'application/json'},
                     body: JSON.stringify({action:'save'})})
    .then(r=>r.json()).then(d => { if(d.ok) refreshDownloads(); });
}

// ── Downloads ────────────────────────────────────────────────────────
function refreshDownloads() {
  fetch('/download_status').then(r=>r.json()).then(d => {
    const row = document.getElementById('dl-row');
    row.innerHTML = '';
    if (d.result)  row.innerHTML += `<a class="dl-btn" href="/download/result">📥 Download Results (.xlsx)</a>`;
    if (d.partial) row.innerHTML += `<a class="dl-btn partial" href="/download/partial">💾 Download Partial (.xlsx)</a>`;
    if (d.failed)  row.innerHTML += `<a class="dl-btn failed" href="/download/failed">⚠️ Download Failed (.xlsx)</a>`;
  });
}

// ── Scan done ────────────────────────────────────────────────────────
function onDone(status) {
  document.getElementById('btn-run').disabled   = false;
  document.getElementById('btn-pause').disabled = true;
  document.getElementById('btn-stop').disabled  = true;
  document.getElementById('btn-save').disabled  = true;
  refreshDownloads();
}

// ── Init ─────────────────────────────────────────────────────────────
connectSSE();

// Restore any existing state on page load
fetch('/state').then(r=>r.json()).then(d => {
  if (d.links_name) {
    document.getElementById('links-name').textContent = '✅ ' + d.links_name;
    document.getElementById('zone-links').classList.add('file-ok');
    if (d.links_cols) document.getElementById('links-cols').textContent = 'Columns: ' + d.links_cols.join(' | ');
  }
  if (d.ref_name) {
    document.getElementById('ref-name').textContent = '✅ ' + d.ref_name;
    document.getElementById('zone-ref').classList.add('file-ok');
  }
  if (d.scan_running) {
    document.getElementById('btn-run').disabled   = true;
    document.getElementById('btn-pause').disabled = false;
    document.getElementById('btn-stop').disabled  = false;
    document.getElementById('btn-save').disabled  = false;
  }
  if (d.log) d.log.forEach(l => appendLog(l.ts, l.msg, l.tag));
  updateStats(d.stat || {});
  refreshDownloads();
});
</script>
</body>
</html>"""

# ══════════════════════════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/upload", methods=["POST"])
def upload():
    which = request.form.get("which")
    f     = request.files.get("file")
    if not f or not which:
        return jsonify(ok=False, error="Missing file or type")
    try:
        data = f.read()
        if which == "links":
            df = pd.read_excel(io.BytesIO(data), nrows=0)
            state["links_bytes"] = data
            state["links_name"]  = f.filename
            state["links_cols"]  = df.columns.tolist()
            return jsonify(ok=True, filename=f.filename, columns=df.columns.tolist())
        else:
            state["ref_bytes"] = data
            state["ref_name"]  = f.filename
            return jsonify(ok=True, filename=f.filename)
    except Exception as e:
        return jsonify(ok=False, error=str(e))


@app.route("/start", methods=["POST"])
def start():
    if state["scan_running"]:
        return jsonify(ok=False, error="Scan already running")
    if not state["links_bytes"]:
        return jsonify(ok=False, error="No links file uploaded")

    cfg = request.json
    keywords     = [k.strip() for k in cfg.get("kw_text","").splitlines()  if k.strip()]
    mil_keywords = [k.strip() for k in cfg.get("mil_text","").splitlines() if k.strip()]
    if not keywords:
        return jsonify(ok=False, error="Enter at least one keyword")

    url_col  = cfg.get("url_col","offlineURL").strip().lower()
    part_col = cfg.get("part_col","PartNumber").strip().lower()

    try:
        df = pd.read_excel(io.BytesIO(state["links_bytes"]), dtype={part_col: str})
    except Exception as e:
        return jsonify(ok=False, error=f"Cannot open links file: {e}")

    df_work = df.copy()
    df_work.columns = [c.strip().lower() for c in df_work.columns]

    if url_col not in df_work.columns:
        return jsonify(ok=False, error=f"Column '{url_col}' not found. Available: {list(df_work.columns)}")
    if part_col not in df_work.columns:
        df_work[part_col] = ""

    df_work[url_col]  = df_work[url_col].apply(_safe_str)
    df_work[part_col] = df_work[part_col].apply(_safe_str)

    orig_ztemp   = df_work.get("ztemperaturegrade",         pd.Series("", index=df_work.index))
    orig_feature = df_work.get("featurevalue",              pd.Series("", index=df_work.index))
    orig_qual    = df_work.get("qualificationrangemapping", pd.Series("", index=df_work.index))

    ref_map = _build_ref_map(state["ref_bytes"]) if state["ref_bytes"] else {}

    # Reset state
    state["log"]           = []
    state["result_bytes"]  = None
    state["failed_bytes"]  = None
    state["partial_bytes"] = None
    state["status"]        = "running"
    state["scan_paused"]   = False
    state["stat"]          = {"total_rows":0,"total_jobs":0,"done":0,"failed":0,"chunk":"—","cb_errors":0,"pct":0}

    stop_event  = threading.Event()
    pause_event = threading.Event()
    state["stop_event"]  = stop_event
    state["pause_event"] = pause_event
    state["scan_running"] = True

    t = threading.Thread(
        target=_run_scan,
        args=(df, df_work, url_col, part_col, orig_qual, orig_ztemp, orig_feature,
              keywords, mil_keywords, ref_map,
              int(cfg.get("chunk_size",500)), int(cfg.get("n_workers",4)),
              int(cfg.get("timeout",15)), int(cfg.get("rpm",30)),
              float(cfg.get("delay_min",0.5)), float(cfg.get("delay_max",2.0)),
              int(cfg.get("cb_errors",5)), int(cfg.get("cb_pause",60)),
              stop_event, pause_event),
        daemon=True,
    )
    t.start()
    return jsonify(ok=True)


@app.route("/control", methods=["POST"])
def control():
    action = request.json.get("action")
    if action == "pause" and state["scan_running"]:
        if state["pause_event"]: state["pause_event"].set()
        state["scan_paused"] = True
        state["status"] = "paused"
        _log("⏸ Paused by user.", "warn"); _push_stats()
        return jsonify(ok=True)
    if action == "resume" and state["scan_running"]:
        if state["pause_event"]: state["pause_event"].clear()
        state["scan_paused"] = False
        state["status"] = "running"
        _log("▶ Resumed.", "ok"); _push_stats()
        return jsonify(ok=True)
    if action == "stop" and state["scan_running"]:
        if state["stop_event"]:  state["stop_event"].set()
        if state["pause_event"]: state["pause_event"].clear()
        _log("⏹ Stop requested.", "warn")
        return jsonify(ok=True)
    if action == "save":
        return jsonify(ok=True, has_partial=state["partial_bytes"] is not None)
    return jsonify(ok=False, error="No action taken")


@app.route("/state")
def get_state():
    return jsonify(
        links_name  = state["links_name"],
        links_cols  = state["links_cols"],
        ref_name    = state["ref_name"],
        scan_running= state["scan_running"],
        log         = state["log"][-100:],
        stat        = {**state["stat"], "status": state["status"]},
    )


@app.route("/download_status")
def download_status():
    return jsonify(
        result  = state["result_bytes"]  is not None,
        partial = state["partial_bytes"] is not None,
        failed  = state["failed_bytes"]  is not None,
    )


@app.route("/download/<which>")
def download(which):
    mapping = {
        "result":  (state["result_bytes"],  "scan_results.xlsx"),
        "partial": (state["partial_bytes"], "scan_partial.xlsx"),
        "failed":  (state["failed_bytes"],  "scan_failed.xlsx"),
    }
    data, name = mapping.get(which, (None, None))
    if not data:
        return "Not available", 404
    return send_file(io.BytesIO(data), as_attachment=True,
                     download_name=name,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.route("/sse")
def sse():
    """Server-Sent Events stream — one per browser tab."""
    q = []
    with state["sse_lock"]:
        state["sse_queues"].append(q)

    def stream():
        yield "retry: 2000\n\n"   # tell browser to reconnect after 2s if lost
        try:
            while True:
                if q:
                    yield q.pop(0)
                else:
                    time.sleep(0.1)
        except GeneratorExit:
            pass
        finally:
            with state["sse_lock"]:
                if q in state["sse_queues"]:
                    state["sse_queues"].remove(q)

    return Response(stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    print("=" * 55)
    print("  Keyword & Part Scanner — Flask Edition")
    print("  Open your browser at: http://localhost:5000")
    print("=" * 55)
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
