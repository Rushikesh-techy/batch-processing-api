"""
Batch API Caller - Web UI
=========================
A small local web app to trigger one endpoint multiple times, substituting
query params from a list. The browser talks to this Flask server (same origin,
so no CORS); Flask makes the real API call server-side, so it works against any
endpoint including internal ones.

Run:
    pip install flask requests
    python app.py
Then open http://127.0.0.1:5000 in your browser.
"""

import base64
import json
import os
import shlex
import socket
import sys
import threading
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode, urlparse, parse_qs, quote

import requests
from flask import Flask, request, jsonify, render_template_string, Response

app = Flask(__name__)


# ---------------------------------------------------------------------------
# Core batch logic
# ---------------------------------------------------------------------------

def normalize_params(item, single_param_name):
    if single_param_name and not isinstance(item, dict):
        return {single_param_name: item}
    if isinstance(item, dict):
        return item
    raise ValueError(
        f"List item {item!r} is not an object. Set a param name to use plain "
        f"values, or make each item an object of query params."
    )


def build_url(base_url, call_params):
    # 1. Substitute {key} or ${key} placeholders found in the URL (e.g. a path
    #    segment like /BOM/Reprocess/{id}). Path values are URL-encoded.
    consumed = set()
    url = base_url
    for key, value in call_params.items():
        for token in ("${" + key + "}", "{" + key + "}"):
            if token in url:
                url = url.replace(token, quote(str(value), safe=""))
                consumed.add(key)

    # 2. Any params NOT used as placeholders become query-string params, merged
    #    with whatever query string the base URL already had.
    leftover = {k: v for k, v in call_params.items() if k not in consumed}
    if not leftover:
        return url

    parsed = urlparse(url)
    base_params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
    base_params.update({k: str(v) for k, v in leftover.items()})
    return parsed._replace(query=urlencode(base_params)).geturl()


def build_body(body_template, call_params):
    if not body_template:
        return None
    raw = json.dumps(body_template)
    for key, value in call_params.items():
        raw = raw.replace("{" + key + "}", str(value))
    return json.loads(raw)


def call_once(index, item, cfg):
    call_params = normalize_params(item, cfg["single_param_name"])
    url = build_url(cfg["base_url"], call_params)
    body = build_body(cfg["body_template"], call_params)

    attempt = 0
    last_error = None
    started = time.perf_counter()

    while attempt <= cfg["retries"]:
        attempt += 1
        try:
            response = requests.request(
                method=cfg["method"],
                url=url,
                headers=cfg["headers"],
                json=body,
                timeout=cfg["timeout"],
            )
            elapsed_ms = round((time.perf_counter() - started) * 1000)

            if response.status_code >= 500 and attempt <= cfg["retries"]:
                last_error = f"HTTP {response.status_code}"
                time.sleep(cfg["retry_delay"])
                continue

            try:
                payload = response.json()
            except ValueError:
                payload = response.text

            return {
                "index": index,
                "params": call_params,
                "url": url,
                "status_code": response.status_code,
                "ok": response.ok,
                "elapsed_ms": elapsed_ms,
                "attempts": attempt,
                "response": payload,
            }

        except requests.RequestException as exc:
            last_error = str(exc)
            if attempt <= cfg["retries"]:
                time.sleep(cfg["retry_delay"])

    elapsed_ms = round((time.perf_counter() - started) * 1000)
    return {
        "index": index,
        "params": call_params,
        "url": url,
        "status_code": None,
        "ok": False,
        "elapsed_ms": elapsed_ms,
        "attempts": attempt - 1,
        "error": last_error,
    }


RUNS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "runs")


def stream_batch(cfg):
    """Run the batch, yielding NDJSON progress lines and writing each result to
    disk as it completes (crash-safe). Yields:
        {"type":"start", ...}   once, with the output file path
        {"type":"result", ...}  per completed call
        {"type":"summary", ...} once at the end
    """
    items = cfg["param_list"]
    total = len(items)

    os.makedirs(RUNS_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_file = os.path.join(RUNS_DIR, f"run_{stamp}.ndjson")

    f = open(run_file, "w", encoding="utf-8")
    lock = threading.Lock()
    stop = threading.Event()
    pool = ThreadPoolExecutor(max_workers=cfg["workers"])

    def worker(index, item):
        if stop.is_set():
            return None
        res = call_once(index, item, cfg)
        try:
            with lock:
                if not f.closed:
                    f.write(json.dumps(res, ensure_ascii=False) + "\n")
                    f.flush()
        except Exception:
            pass
        return res

    yield json.dumps({"type": "start", "total": total, "file": run_file}) + "\n"

    futures = [pool.submit(worker, i, item) for i, item in enumerate(items)]
    done = ok = 0
    try:
        for fut in as_completed(futures):
            res = fut.result()
            if res is None:
                continue
            done += 1
            if res.get("ok"):
                ok += 1
            yield json.dumps({
                "type": "result", "done": done, "total": total,
                "ok": ok, "result": res,
            }) + "\n"
        yield json.dumps({
            "type": "summary", "total": total, "succeeded": ok,
            "failed": done - ok, "file": run_file,
        }) + "\n"
    except GeneratorExit:
        # Client disconnected (tab closed / Stop pressed): stop scheduling more.
        stop.set()
        raise
    finally:
        stop.set()
        pool.shutdown(wait=False)
        with lock:
            f.close()


# ---------------------------------------------------------------------------
# cURL import
# ---------------------------------------------------------------------------

# Flags that consume the following token as their value.
_VALUE_FLAGS = {
    "-X", "--request", "-H", "--header", "-d", "--data", "--data-raw",
    "--data-binary", "--data-ascii", "--data-urlencode", "-b", "--cookie",
    "-u", "--user", "--url", "-A", "--user-agent", "-e", "--referer",
    "-m", "--max-time", "--connect-timeout", "-o", "--output", "-T",
    "--upload-file", "--retry", "-x", "--proxy", "-E", "--cert",
}
_DATA_FLAGS = {
    "-d", "--data", "--data-raw", "--data-binary", "--data-ascii", "--data-urlencode",
}


def parse_curl(text):
    """Parse a curl command into {method, url, headers, body}."""
    text = text.strip()
    # Join line continuations from bash (\), cmd (^) and PowerShell (`).
    for cont in ("\\\n", "^\n", "`\n"):
        text = text.replace(cont, " ")
    text = text.replace("\r", " ").replace("\n", " ")

    try:
        tokens = shlex.split(text, posix=True)
    except ValueError:
        tokens = text.split()
    if tokens and tokens[0].lower() == "curl":
        tokens = tokens[1:]

    method = None
    url = None
    headers = []
    data_parts = []
    use_get = False

    i, n = 0, len(tokens)
    while i < n:
        t = tokens[i]
        if t.startswith("--") and "=" in t:
            flag, inline_val = t.split("=", 1)
        else:
            flag, inline_val = t, None

        takes_value = flag in _VALUE_FLAGS
        val = inline_val if inline_val is not None else (
            tokens[i + 1] if (takes_value and i + 1 < n) else None
        )

        if flag in ("-X", "--request"):
            if val:
                method = val.upper()
        elif flag in ("-H", "--header"):
            if val and ":" in val:
                k, v = val.split(":", 1)
                headers.append({"key": k.strip(), "value": v.strip()})
        elif flag in _DATA_FLAGS:
            if val is not None:
                data_parts.append(val)
        elif flag in ("-b", "--cookie"):
            if val:
                headers.append({"key": "Cookie", "value": val})
        elif flag in ("-u", "--user"):
            if val:
                encoded = base64.b64encode(val.encode()).decode()
                headers.append({"key": "Authorization", "value": "Basic " + encoded})
        elif flag == "--url":
            if val:
                url = val
        elif flag in ("-G", "--get"):
            use_get = True
        elif takes_value or flag.startswith("-"):
            pass  # known-but-unused value flag, or a boolean flag we ignore
        else:
            if url is None:
                url = t

        i += 2 if (takes_value and inline_val is None) else 1

    body = "&".join(data_parts) if data_parts else ""

    # With -G, data is appended to the query string instead of sent as a body.
    if use_get and body:
        sep = "&" if "?" in (url or "") else "?"
        url = (url or "") + sep + body
        body = ""

    if method is None:
        method = "POST" if body else "GET"

    return {"method": method, "url": url or "", "headers": headers, "body": body}


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template_string(PAGE)


@app.route("/parse-curl", methods=["POST"])
def parse_curl_route():
    data = request.get_json(force=True, silent=True) or {}
    text = (data.get("curl") or "").strip()
    if not text:
        return jsonify({"error": "Paste a curl command first."}), 400
    try:
        parsed = parse_curl(text)
    except Exception as exc:  # noqa: BLE001 - report any parse failure cleanly
        return jsonify({"error": f"Couldn't parse that command: {exc}"}), 400
    if not parsed["url"]:
        return jsonify({"error": "No URL found in the command."}), 400
    return jsonify(parsed)


@app.route("/run", methods=["POST"])
def run():
    data = request.get_json(force=True, silent=True) or {}

    single = (data.get("single_param_name") or "").strip()

    # Guard: the "Param name" field holds a single identifier (e.g. "id"), not
    # the list of values. Catch the common mix-up of pasting values there.
    if single and (any(c in single for c in '[]{}",') or " " in single):
        return jsonify({"error": "The 'Param name' field takes just the parameter's "
                                 "name (e.g. id) - it looks like the values were pasted "
                                 "there. Put the name here and the values in the items box."}), 400

    # Parse the items list (sent as raw text so we can give good errors).
    raw_list = (data.get("param_list") or "").strip()
    if not raw_list:
        if single:
            return jsonify({"error": f"Add the values for '{single}' in the items box "
                                     f"- one per line, comma-separated, or a JSON array."}), 400
        return jsonify({"error": "The items box is empty. Add a JSON array of objects, "
                                 "one per call."}), 400

    # Plain-values mode: when a param name is set AND the text isn't already a
    # JSON array, accept values typed naturally (one per line or comma-separated).
    if single and not raw_list.startswith("["):
        param_list = [
            v.strip()
            for line in raw_list.splitlines()
            for v in line.split(",")
            if v.strip()
        ]
        if not param_list:
            return jsonify({"error": f"No values found for '{single}'."}), 400
    else:
        try:
            param_list = json.loads(raw_list)
        except json.JSONDecodeError as exc:
            return jsonify({"error": f"Items aren't valid JSON: {exc}"}), 400
        if not isinstance(param_list, list):
            return jsonify({"error": "Items must be a JSON array (e.g. [\"101\",\"102\"] "
                                     "or [{...},{...}])."}), 400

    # Optional JSON body template.
    body_template = None
    raw_body = (data.get("body_template") or "").strip()
    if raw_body:
        try:
            body_template = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            return jsonify({"error": f"Body template isn't valid JSON: {exc}"}), 400

    # Headers come in as a list of {key, value} rows.
    headers = {}
    for row in data.get("headers", []):
        key = (row.get("key") or "").strip()
        if key:
            headers[key] = row.get("value", "")

    cfg = {
        "base_url": (data.get("base_url") or "").strip(),
        "method": (data.get("method") or "GET").upper(),
        "headers": headers,
        "param_list": param_list,
        "single_param_name": single,
        "body_template": body_template,
        "workers": max(1, int(data.get("workers") or 1)),
        "timeout": float(data.get("timeout") or 30),
        "retries": max(0, int(data.get("retries") or 0)),
        "retry_delay": 2,
    }

    if not cfg["base_url"]:
        return jsonify({"error": "Enter a URL to call."}), 400

    # Validate every item up front so item-shape errors return a clean 400
    # instead of surfacing mid-stream.
    try:
        for item in cfg["param_list"]:
            normalize_params(item, cfg["single_param_name"])
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    return Response(
        stream_batch(cfg),
        mimetype="application/x-ndjson",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------------------------------------------------------------------------
# Frontend (served inline)
# ---------------------------------------------------------------------------

PAGE = r"""
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Relay — batch API caller</title>
<style>
  :root {
    --ink: #11181C;
    --panel: #18222899;
    --panel-solid: #182228;
    --line: #2A3940;
    --line-soft: #212E34;
    --text: #DAE4E3;
    --muted: #74878A;
    --amber: #E8A33D;
    --amber-dim: #b6802f;
    --ok: #4FB286;
    --fail: #E5634D;
    --mono: ui-monospace, "SF Mono", "JetBrains Mono", Menlo, Consolas, monospace;
    --sans: ui-sans-serif, system-ui, "Segoe UI", Roboto, sans-serif;
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; height: 100%; }
  body {
    background:
      radial-gradient(900px 500px at 85% -10%, #1c2c33 0%, transparent 60%),
      var(--ink);
    color: var(--text);
    font-family: var(--sans);
    font-size: 14px;
    line-height: 1.5;
    -webkit-font-smoothing: antialiased;
  }
  .wrap { max-width: 1080px; margin: 0 auto; padding: 28px 24px 80px; }

  header { display: flex; align-items: baseline; gap: 14px; margin-bottom: 26px; }
  .logo {
    font-family: var(--mono); font-weight: 600; font-size: 19px;
    letter-spacing: -0.5px; color: var(--text);
  }
  .logo b { color: var(--amber); }
  .tag {
    font-size: 11px; letter-spacing: 2px; text-transform: uppercase;
    color: var(--muted);
  }

  .grid { display: grid; grid-template-columns: 1fr; gap: 18px; }
  @media (min-width: 880px) { .grid { grid-template-columns: 1.15fr 1fr; } }

  .card {
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: 10px;
    padding: 18px;
    backdrop-filter: blur(6px);
  }
  .card + .card { margin-top: 18px; }

  .eyebrow {
    font-size: 10px; letter-spacing: 2.5px; text-transform: uppercase;
    color: var(--muted); margin: 0 0 14px;
  }

  label { display: block; font-size: 11px; color: var(--muted); margin: 0 0 5px; letter-spacing: .3px; }
  .field { margin-bottom: 14px; }

  input, select, textarea {
    width: 100%; background: #0E1417; color: var(--text);
    border: 1px solid var(--line); border-radius: 7px;
    padding: 9px 11px; font-family: var(--mono); font-size: 13px;
  }
  textarea { resize: vertical; min-height: 92px; line-height: 1.45; }
  input:focus, select:focus, textarea:focus {
    outline: none; border-color: var(--amber-dim);
    box-shadow: 0 0 0 3px rgba(232,163,61,.12);
  }
  select { cursor: pointer; }

  .row { display: flex; gap: 10px; }
  .row > * { flex: 1; }
  .method-url { display: grid; grid-template-columns: 110px 1fr; gap: 10px; }

  .hdr-row { display: grid; grid-template-columns: 1fr 1.3fr 32px; gap: 8px; margin-bottom: 8px; }
  .icon-btn {
    background: transparent; border: 1px solid var(--line); color: var(--muted);
    border-radius: 7px; cursor: pointer; font-size: 16px; line-height: 1;
  }
  .icon-btn:hover { border-color: var(--fail); color: var(--fail); }
  .add-link {
    background: none; border: none; color: var(--amber); cursor: pointer;
    font-family: var(--mono); font-size: 12px; padding: 2px 0;
  }
  .add-link:hover { text-decoration: underline; }

  .curl-import { margin-bottom: 16px; }
  .import-btn {
    margin-top: 8px; padding: 8px 14px; background: transparent;
    border: 1px solid var(--amber-dim); color: var(--amber); border-radius: 7px;
    font-family: var(--mono); font-size: 12px; cursor: pointer;
  }
  .import-btn:hover { background: rgba(232,163,61,.10); }

  .btn-row { display: flex; gap: 10px; }
  .btn-row .run { flex: 1; }
  .stop {
    margin-top: 4px; padding: 13px 18px; background: transparent;
    border: 1px solid var(--fail); color: var(--fail); border-radius: 8px;
    font-family: var(--mono); font-weight: 600; font-size: 14px; cursor: pointer;
  }
  .stop:hover { background: rgba(229,99,77,.12); }
  .runfile {
    font-family: var(--mono); font-size: 11px; color: var(--muted);
    margin-bottom: 10px; word-break: break-all;
  }
  .rerun {
    width: 100%; margin-bottom: 12px; padding: 10px; background: transparent;
    border: 1px solid var(--amber-dim); color: var(--amber); border-radius: 8px;
    font-family: var(--mono); font-size: 13px; cursor: pointer;
  }
  .rerun:hover { background: rgba(232,163,61,.10); }

  .pager {
    display: flex; align-items: center; justify-content: space-between;
    gap: 10px; margin-bottom: 10px; flex-wrap: wrap;
    font-family: var(--mono); font-size: 12px;
  }
  .filters { display: flex; gap: 6px; flex-wrap: wrap; }
  .chip {
    padding: 4px 10px; border: 1px solid var(--line); border-radius: 6px;
    color: var(--muted); background: #0E1417; cursor: pointer;
    font-family: var(--mono); font-size: 12px;
  }
  .chip:hover { border-color: var(--amber-dim); }
  .chip.active { border-color: var(--amber-dim); color: var(--amber); }
  .pagenav { display: flex; align-items: center; gap: 6px; }
  .pagenav button {
    padding: 4px 9px; border: 1px solid var(--line); border-radius: 6px;
    background: transparent; color: var(--text); cursor: pointer;
    font-family: var(--mono); font-size: 13px; min-width: 28px;
  }
  .pagenav button:hover:not(:disabled) { border-color: var(--amber-dim); }
  .pagenav button:disabled { color: var(--muted); opacity: .4; cursor: not-allowed; }
  .pageinfo { color: var(--muted); min-width: 96px; text-align: center; }

  .hint { font-size: 11px; color: var(--muted); margin-top: 5px; font-family: var(--mono); }

  details summary {
    cursor: pointer; color: var(--muted); font-size: 11px;
    letter-spacing: 2px; text-transform: uppercase; list-style: none;
    margin-bottom: 12px;
  }
  details summary::-webkit-details-marker { display: none; }
  details summary::before { content: "+ "; color: var(--amber); }
  details[open] summary::before { content: "– "; }
  .triple { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 10px; }

  .run {
    width: 100%; margin-top: 4px; padding: 13px;
    background: var(--amber); color: #1a1206; border: none; border-radius: 8px;
    font-family: var(--mono); font-weight: 600; font-size: 14px; letter-spacing: .5px;
    cursor: pointer; transition: transform .08s ease, background .15s ease;
  }
  .run:hover { background: #f1b052; }
  .run:active { transform: translateY(1px); }
  .run:disabled { background: var(--line); color: var(--muted); cursor: not-allowed; }

  /* Results */
  .summary {
    display: flex; align-items: center; gap: 16px; margin-bottom: 14px;
    font-family: var(--mono); font-size: 13px;
  }
  .meter { flex: 1; height: 6px; background: var(--line-soft); border-radius: 4px; overflow: hidden; }
  .meter > span { display: block; height: 100%; width: 0; background: var(--ok); transition: width .3s ease; }

  .result {
    border: 1px solid var(--line); border-radius: 8px; margin-bottom: 8px;
    overflow: hidden; animation: fade .25s ease both;
  }
  @keyframes fade { from { opacity: 0; transform: translateY(4px); } to { opacity: 1; transform: none; } }
  .result > .head {
    display: grid; grid-template-columns: 26px 64px 1fr 70px; gap: 10px;
    align-items: center; padding: 10px 12px; cursor: pointer;
    font-family: var(--mono); font-size: 12px;
  }
  .result > .head:hover { background: #0E1417; }
  .idx { color: var(--muted); }
  .code {
    font-weight: 600; padding: 2px 0; text-align: center; border-radius: 5px; font-size: 12px;
  }
  .code.ok { color: var(--ok); }
  .code.fail { color: var(--fail); }
  .params { color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .ms { color: var(--muted); text-align: right; }
  .body {
    display: none; border-top: 1px solid var(--line-soft);
    background: #0C1114; padding: 12px; margin: 0;
    font-family: var(--mono); font-size: 12px; color: #b9c6c5;
    white-space: pre-wrap; word-break: break-word; max-height: 320px; overflow: auto;
  }
  .result.open .body { display: block; }

  .err { color: var(--fail); font-family: var(--mono); font-size: 13px; margin-top: 6px; }
  .placeholder { color: var(--muted); font-family: var(--mono); font-size: 12px; text-align: center; padding: 30px 0; }

  @media (prefers-reduced-motion: reduce) { * { animation: none !important; transition: none !important; } }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <span class="logo"><b>relay</b>/batch</span>
    <span class="tag">fire one endpoint over a list</span>
  </header>

  <div class="grid">
    <!-- Request panel -->
    <div>
      <div class="card">
        <p class="eyebrow">Request</p>
        <details id="curlDetails" class="curl-import">
          <summary>Import from cURL</summary>
          <textarea id="curlInput" placeholder="Paste a curl command — from Postman or browser DevTools 'Copy as cURL'" spellcheck="false" style="min-height:80px"></textarea>
          <button class="import-btn" onclick="importCurl()">Import</button>
        </details>
        <div class="field method-url">
          <div>
            <label>Method</label>
            <select id="method">
              <option>GET</option><option>POST</option><option>PUT</option>
              <option>PATCH</option><option>DELETE</option>
            </select>
          </div>
          <div>
            <label>URL</label>
            <input id="url" placeholder="https://host/api/v1/BOM/Reprocess/{id}" spellcheck="false">
            <p class="hint">Use {name} in the path for path params, or list params get added to the query string.</p>
          </div>
        </div>

        <div class="field">
          <label>Headers</label>
          <div id="headers"></div>
          <button class="add-link" onclick="addHeader()">+ add header</button>
        </div>
      </div>

      <div class="card">
        <p class="eyebrow">The list — one call per item</p>
        <div class="field">
          <label>Param name <span style="text-transform:none;letter-spacing:0">(the name, e.g. id — not the values)</span></label>
          <input id="paramName" placeholder="id" spellcheck="false">
          <p class="hint">Enter the parameter's name here (like id); the values go in the items box below. Leave blank to pass full objects.</p>
        </div>
        <div class="field">
          <label>Items (JSON array)</label>
          <textarea id="paramList" spellcheck="false">[
  {"id": "101", "region": "EU"},
  {"id": "102", "region": "US"},
  {"id": "103", "region": "APAC"}
]</textarea>
        </div>
        <div class="field">
          <label>Body template <span style="text-transform:none;letter-spacing:0">(POST/PUT/PATCH — optional)</span></label>
          <textarea id="bodyTemplate" placeholder='{"productId": "{id}", "market": "{region}"}' spellcheck="false"></textarea>
          <p class="hint">Use {paramName} placeholders to inject values per call.</p>
        </div>
      </div>
    </div>

    <!-- Run + results panel -->
    <div>
      <div class="card">
        <details>
          <summary>Options</summary>
          <div class="triple">
            <div><label>Workers</label><input id="workers" type="number" value="5" min="1"></div>
            <div><label>Timeout (s)</label><input id="timeout" type="number" value="30" min="1"></div>
            <div><label>Retries</label><input id="retries" type="number" value="1" min="0"></div>
          </div>
        </details>
        <div class="btn-row">
          <button class="run" id="runBtn" onclick="runBatch()">Run batch</button>
          <button class="stop" id="stopBtn" onclick="stopBatch()" style="display:none">Stop</button>
        </div>
        <div id="error"></div>
      </div>

      <div class="card">
        <p class="eyebrow">Results</p>
        <div id="summary"></div>
        <div id="runfile" class="runfile"></div>
        <button class="rerun" id="rerunBtn" onclick="rerunFailed()" style="display:none"></button>
        <div id="pager" class="pager"></div>
        <div id="results"><div class="placeholder">No calls yet. Configure a request and run.</div></div>
      </div>
    </div>
  </div>
</div>

<script>
  function addHeader(key, value) {
    const wrap = document.getElementById('headers');
    const row = document.createElement('div');
    row.className = 'hdr-row';
    const k = document.createElement('input');
    k.placeholder = 'Header'; k.spellcheck = false; k.value = key || '';
    const v = document.createElement('input');
    v.placeholder = 'Value'; v.spellcheck = false; v.value = value || '';
    const del = document.createElement('button');
    del.className = 'icon-btn'; del.title = 'remove'; del.innerHTML = '&times;';
    del.onclick = () => row.remove();
    row.append(k, v, del);
    wrap.appendChild(row);
  }

  function setMethod(m) {
    const sel = document.getElementById('method');
    m = (m || 'GET').toUpperCase();
    if (![...sel.options].some(o => o.value === m)) sel.add(new Option(m, m));
    sel.value = m;
  }

  async function importCurl() {
    const text = document.getElementById('curlInput').value.trim();
    if (!text) { showError('Paste a curl command first.'); return; }
    try {
      const res = await fetch('/parse-curl', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ curl: text }),
      });
      const data = await res.json();
      if (!res.ok) { showError(data.error || 'Could not parse that command.'); return; }
      setMethod(data.method);
      document.getElementById('url').value = data.url || '';
      const wrap = document.getElementById('headers');
      wrap.innerHTML = '';
      (data.headers || []).forEach(h => addHeader(h.key, h.value));
      if (!wrap.children.length) addHeader('', '');
      if (data.body) document.getElementById('bodyTemplate').value = data.body;
      showError('');
      document.getElementById('curlDetails').open = false;
    } catch (e) {
      showError('Could not parse: ' + e.message);
    }
  }
  // Seed a couple of common headers.
  addHeader('Authorization', 'Bearer ');
  addHeader('Content-Type', 'application/json');

  function collectHeaders() {
    return [...document.querySelectorAll('#headers .hdr-row')].map(r => {
      const [k, v] = r.querySelectorAll('input');
      return { key: k.value, value: v.value };
    });
  }

  function showError(msg) {
    document.getElementById('error').innerHTML = msg ? `<div class="err">${msg}</div>` : '';
  }

  let abortCtrl = null;
  let lastFailedParams = [];

  // Results are kept in memory; only one page is ever in the DOM.
  let allResults = [];
  let expanded = new Set();      // result indexes the user has expanded
  let currentPage = 0;
  let pageSize = 100;
  let filterMode = 'all';        // all | ok | failed
  let following = true;          // tail the newest page during a live run
  let dirty = false;             // new results waiting to render
  let renderTimer = null;

  function basePayload() {
    return {
      base_url: document.getElementById('url').value,
      method: document.getElementById('method').value,
      headers: collectHeaders(),
      single_param_name: document.getElementById('paramName').value,
      param_list: document.getElementById('paramList').value,
      body_template: document.getElementById('bodyTemplate').value,
      workers: document.getElementById('workers').value,
      timeout: document.getElementById('timeout').value,
      retries: document.getElementById('retries').value,
    };
  }

  function stopBatch() { if (abortCtrl) abortCtrl.abort(); }
  function rerunFailed() { if (lastFailedParams.length) runBatch(lastFailedParams.slice()); }

  async function runBatch(overrideItems) {
    showError('');
    const runBtn = document.getElementById('runBtn');
    const stopBtn = document.getElementById('stopBtn');
    const rerun = document.getElementById('rerunBtn');

    const payload = basePayload();
    if (overrideItems) {
      payload.single_param_name = '';
      payload.param_list = JSON.stringify(overrideItems);
    }

    // Reset state.
    rerun.style.display = 'none';
    lastFailedParams = [];
    allResults = [];
    expanded = new Set();
    currentPage = 0;
    filterMode = 'all';
    following = true;
    renderResults(true);
    document.getElementById('summary').innerHTML = '';
    document.getElementById('runfile').textContent = '';
    runBtn.disabled = true; runBtn.textContent = 'Running…';
    stopBtn.style.display = 'inline-block';
    abortCtrl = new AbortController();
    renderTimer = setInterval(liveRefresh, 250);

    let total = 0, done = 0, ok = 0;

    try {
      const res = await fetch('/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        signal: abortCtrl.signal,
      });

      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        showError(data.error || 'Request failed.');
        return;
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      while (true) {
        const { done: streamDone, value } = await reader.read();
        if (streamDone) break;
        buffer += decoder.decode(value, { stream: true });
        let nl;
        while ((nl = buffer.indexOf('\n')) >= 0) {
          const line = buffer.slice(0, nl).trim();
          buffer = buffer.slice(nl + 1);
          if (!line) continue;
          const ev = JSON.parse(line);
          if (ev.type === 'start') {
            total = ev.total;
            document.getElementById('runfile').textContent = 'Saving to: ' + ev.file;
            updateSummary(0, 0, total, false);
          } else if (ev.type === 'result') {
            done = ev.done; ok = ev.ok;
            pushResult(ev.result);
            updateSummary(ok, done - ok, total, false);
          } else if (ev.type === 'summary') {
            updateSummary(ev.succeeded, ev.failed, ev.total, true);
          }
        }
      }
    } catch (e) {
      if (e.name === 'AbortError') {
        showError(`Stopped — ${done} of ${total} calls finished and are saved to disk.`);
      } else {
        showError('Connection lost: ' + e.message + ' — completed calls are saved to disk.');
      }
    } finally {
      clearInterval(renderTimer); renderTimer = null;
      dirty = false;
      renderResults(false);   // flush any remaining results
      runBtn.disabled = false; runBtn.textContent = 'Run batch';
      stopBtn.style.display = 'none';
      abortCtrl = null;
      if (lastFailedParams.length) {
        rerun.style.display = 'block';
        rerun.textContent = `Re-run failed (${lastFailedParams.length})`;
      }
    }
  }

  function updateSummary(ok, failed, total, finished) {
    const done = ok + failed;
    const pct = total ? Math.round(done / total * 100) : 0;
    const label = finished ? 'done' : 'running…';
    document.getElementById('summary').innerHTML = `
      <div class="summary">
        <span style="color:var(--ok)">${ok} ok</span>
        <span style="color:var(--fail)">${failed} failed</span>
        <div class="meter"><span style="width:${pct}%"></span></div>
        <span style="color:var(--muted)">${done}/${total} ${label}</span>
      </div>`;
  }

  function pushResult(r) {
    if (!r.ok) lastFailedParams.push(r.params);
    allResults.push(r);
    dirty = true;   // rendered by the throttled liveRefresh
  }

  function liveRefresh() {
    if (!dirty) return;
    dirty = false;
    if (following) {
      const n = filteredResults().length;
      currentPage = Math.max(0, Math.ceil(n / pageSize) - 1);
    }
    renderResults(false);
  }

  function filteredResults() {
    if (filterMode === 'ok') return allResults.filter(r => r.ok);
    if (filterMode === 'failed') return allResults.filter(r => !r.ok);
    return allResults;
  }

  function setFilter(mode) { filterMode = mode; currentPage = 0; following = false; renderResults(true); }
  function setPageSize(v) { pageSize = +v; currentPage = 0; renderResults(true); }

  function gotoPage(p) {
    const pages = Math.max(1, Math.ceil(filteredResults().length / pageSize));
    currentPage = Math.min(Math.max(0, p), pages - 1);
    following = (currentPage === pages - 1);   // reaching the last page re-enables tailing
    renderResults(true);
  }

  function makeRow(r) {
    const codeTxt = r.status_code != null ? r.status_code : 'ERR';
    const bodyContent = r.error
      ? r.error
      : (typeof r.response === 'string' ? r.response : JSON.stringify(r.response, null, 2));
    const el = document.createElement('div');
    el.className = 'result' + (expanded.has(r.index) ? ' open' : '');
    el.innerHTML = `
      <div class="head">
        <span class="idx">${r.index}</span>
        <span class="code ${r.ok ? 'ok' : 'fail'}">${codeTxt}</span>
        <span class="params">${escapeHtml(JSON.stringify(r.params))}</span>
        <span class="ms">${r.elapsed_ms}ms</span>
      </div>
      <pre class="body">${escapeHtml(bodyContent)}</pre>`;
    el.querySelector('.head').onclick = () => {
      if (expanded.has(r.index)) { expanded.delete(r.index); el.classList.remove('open'); }
      else { expanded.add(r.index); el.classList.add('open'); }
    };
    return el;
  }

  function renderResults(resetScroll) {
    const list = document.getElementById('results');
    const pager = document.getElementById('pager');

    if (allResults.length === 0) {
      pager.innerHTML = '';
      list.innerHTML = '<div class="placeholder">No calls yet. Configure a request and run.</div>';
      return;
    }

    const data = filteredResults();
    const pages = Math.max(1, Math.ceil(data.length / pageSize));
    if (currentPage >= pages) currentPage = pages - 1;
    if (currentPage < 0) currentPage = 0;

    const okCount = allResults.filter(r => r.ok).length;
    const failCount = allResults.length - okCount;

    pager.innerHTML = `
      <div class="filters">
        <button class="chip ${filterMode === 'all' ? 'active' : ''}" onclick="setFilter('all')">All ${allResults.length}</button>
        <button class="chip ${filterMode === 'ok' ? 'active' : ''}" onclick="setFilter('ok')">OK ${okCount}</button>
        <button class="chip ${filterMode === 'failed' ? 'active' : ''}" onclick="setFilter('failed')">Failed ${failCount}</button>
        <select class="chip" onchange="setPageSize(this.value)" title="rows per page">
          <option value="50" ${pageSize === 50 ? 'selected' : ''}>50/pg</option>
          <option value="100" ${pageSize === 100 ? 'selected' : ''}>100/pg</option>
          <option value="250" ${pageSize === 250 ? 'selected' : ''}>250/pg</option>
          <option value="500" ${pageSize === 500 ? 'selected' : ''}>500/pg</option>
        </select>
      </div>
      <div class="pagenav">
        <button ${currentPage === 0 ? 'disabled' : ''} onclick="gotoPage(0)">&laquo;</button>
        <button ${currentPage === 0 ? 'disabled' : ''} onclick="gotoPage(${currentPage - 1})">&lsaquo;</button>
        <span class="pageinfo">Page ${currentPage + 1}/${pages}</span>
        <button ${currentPage >= pages - 1 ? 'disabled' : ''} onclick="gotoPage(${currentPage + 1})">&rsaquo;</button>
        <button ${currentPage >= pages - 1 ? 'disabled' : ''} onclick="gotoPage(${pages - 1})">&raquo;</button>
      </div>`;

    const start = currentPage * pageSize;
    const slice = data.slice(start, start + pageSize);
    if (slice.length === 0) {
      list.innerHTML = '<div class="placeholder">Nothing matches this filter.</div>';
      return;
    }
    const frag = document.createDocumentFragment();
    slice.forEach(r => frag.appendChild(makeRow(r)));
    list.innerHTML = '';
    list.appendChild(frag);
    if (resetScroll) list.scrollTop = 0;
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
  }
</script>
</body>
</html>
"""


def find_open_port(preferred):
    """Return the first port we can actually bind on localhost.

    On Windows, ports can be blocked by reserved/excluded ranges (Hyper-V,
    WSL, WinNAT) which raises WinError 10013. We just skip to the next one.
    """
    candidates = [preferred, 5050, 8000, 8080, 8765, 3333, 0]  # 0 = let OS pick
    for port in candidates:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("127.0.0.1", port))
            chosen = s.getsockname()[1]
            s.close()
            return chosen
        except OSError:
            continue
    return None


if __name__ == "__main__":
    # Port can be set via:  python app.py 5050   or   PORT=5050 python app.py
    requested = int(os.environ.get("PORT") or (sys.argv[1] if len(sys.argv) > 1 else 5000))
    port = find_open_port(requested)

    if port is None:
        print("Couldn't bind any port on 127.0.0.1. Try a specific one: python app.py 8123")
        sys.exit(1)
    if port != requested:
        print(f"Port {requested} is unavailable (likely reserved by Windows). Using {port} instead.")

    print(f"\n  Relay is running -> http://127.0.0.1:{port}\n  Press Ctrl+C to stop.\n")
    app.run(host="127.0.0.1", port=port, debug=False)
