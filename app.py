"""
ytdl-b4a â€” yt-dlp download service for Back4App (or any Docker host).

Downloads YouTube audio/video with yt-dlp (cookies from remote URLs, rotated),
uploads the result to a Hugging Face Storage Bucket and returns a direct link.
Optionally streams the file back too.

  GET  /health
  POST /get-info   {url}                                  -> metadata
  POST /convert    {url, type:"audio"|"video"}            -> {url, filename, video_id, size, cached}
                   (202 {status:"processing"} if not done within SYNC_WAIT_SEC â€” call again)
  GET  /logs, /jobs  diagnostics (protected by RELAY_SECRET)
  POST /download   {url, type}                            -> streams the file
  GET  /file/<video_id>.<mp3|mp4>                         -> redirect to bucket (or stream if private)

Env:
  HF_TOKEN            write token for the bucket                         (required for upload)
  HF_BUCKET           e.g. hansaka01/songs                               (required for upload)
  HF_BUCKET_PUBLIC    "1" if bucket is public (return CDN links)         default "0"
  COOKIES_URLS        comma/newline separated URLs of Netscape cookie files (rotated)
  COOKIES_REFRESH_MIN minutes between re-fetching cookie files           default 60
  RELAY_SECRET        if set, callers must send  x-relay-key: <secret>
  MAX_HEIGHT          video cap                                          default 720
  MAX_DURATION_SEC    refuse longer videos                               default 3600
  CONCURRENCY         parallel yt-dlp jobs                               default 1
  PORT                default 8080
"""
import os, re, sys, json, time, shutil, threading, subprocess, tempfile, hashlib, logging, urllib.parse
from pathlib import Path
from flask import Flask, request, jsonify, Response, redirect, send_file, abort
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger("ytdl")
from collections import deque
_ring = deque(maxlen=400)
class _Ring(logging.Handler):
    def emit(self, r):
        try: _ring.append(self.format(r))
        except Exception: pass
_rh = _Ring(); _rh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s")); logging.getLogger().addHandler(_rh)

HF_TOKEN = os.getenv("HF_TOKEN", "").strip()
# player clients tried in order per cookie file ("" = yt-dlp default chain)
YT_CLIENTS = [c.strip() for c in os.getenv("YT_CLIENTS", "mweb,web_safari,android,default").split(",") if c.strip()]
RELOAD_RE = re.compile(r"needs to be reloaded|Requested format is not available|Sign in to confirm|not a bot|LOGIN_REQUIRED|Please sign in", re.I)
HF_BUCKET = os.getenv("HF_BUCKET", "").strip()
HF_BUCKET_PUBLIC = os.getenv("HF_BUCKET_PUBLIC", "0") == "1"
COOKIES_URLS = [u.strip() for u in re.split(r"[,\n]", os.getenv("COOKIES_URLS", "")) if u.strip()]
COOKIES_REFRESH = int(os.getenv("COOKIES_REFRESH_MIN", "60")) * 60
RELAY_SECRET = os.getenv("RELAY_SECRET", "").strip()
MAX_HEIGHT = int(os.getenv("MAX_HEIGHT", "720"))
MAX_DURATION = int(os.getenv("MAX_DURATION_SEC", "3600"))
CONCURRENCY = max(1, int(os.getenv("CONCURRENCY", "1")))
WORK = Path(os.getenv("WORK_DIR", "/tmp/ytdl"))
COOKIE_DIR = WORK / "cookies"
WORK.mkdir(parents=True, exist_ok=True); COOKIE_DIR.mkdir(parents=True, exist_ok=True)

YT_ID = re.compile(r"(?:youtube(?:-nocookie)?\.com/(?:shorts/|live/|embed/|v/|watch\?.*v=)|youtu\.be/)([-_0-9A-Za-z]{11})")
def video_id(url):
    m = YT_ID.search(str(url or ""))
    return m.group(1) if m else None

app = Flask(__name__)
sem = threading.Semaphore(CONCURRENCY)
inflight = {}          # key -> threading.Event  (dedupe concurrent identical requests)
inflight_lock = threading.Lock()

# ----------------------------------------------------------------- cookies
class CookieJar:
    """Fetches N cookie files from URLs, rotates through them, refreshes periodically,
    and benches a file for a while when it produced a bot-check / login error."""
    def __init__(self, urls):
        self.urls = urls
        self.files = []            # list of Path
        self.bad_until = {}        # Path -> epoch
        self.i = 0
        self.last = 0
        self.lock = threading.Lock()

    def refresh(self, force=False):
        if not self.urls: return
        if not force and time.time() - self.last < COOKIES_REFRESH: return
        files = []
        for n, u in enumerate(self.urls):
            try:
                r = requests.get(u, timeout=15)
                r.raise_for_status()
                txt = r.text
                if "youtube.com" not in txt:
                    log.warning("cookie url %d has no youtube cookies, skipping", n); continue
                p = COOKIE_DIR / f"c{n}_{hashlib.md5(u.encode()).hexdigest()[:8]}.txt"
                p.write_text(txt)
                files.append(p)
            except Exception as e:
                log.warning("cookie url %d fetch failed: %s", n, e)
        with self.lock:
            if files: self.files = files
            self.last = time.time()
        log.info("cookies loaded: %d file(s)", len(self.files))

    def pick(self):
        """Return (path or None). Round-robin over non-benched files."""
        self.refresh()
        with self.lock:
            if not self.files: return None
            now = time.time()
            for _ in range(len(self.files)):
                p = self.files[self.i % len(self.files)]; self.i += 1
                if self.bad_until.get(p, 0) < now: return p
            return self.files[self.i % len(self.files)]  # all benched: try anyway

    def bench(self, p, minutes=30):
        if p:
            with self.lock: self.bad_until[p] = time.time() + minutes * 60
            log.warning("benching cookie file %s for %d min", p.name, minutes)

    def all_paths(self):
        with self.lock: return list(self.files)

cookies = CookieJar(COOKIES_URLS)
cookies.refresh(force=True)

# ----------------------------------------------------------------- yt-dlp
BOT_RE = re.compile(r"sign in to confirm|not a bot|login_required|HTTP Error 429|HTTP Error 403|account.*(?:terminated|suspended)", re.I)

def ytdlp_base(cookie_path, client=None):
    a = ["yt-dlp", "--no-warnings", "--no-playlist", "--no-progress", "--no-cache-dir",
         "--retries", "3", "--fragment-retries", "3", "--socket-timeout", "20",
         "--js-runtimes", "deno", "--ffmpeg-location", shutil.which("ffmpeg") or "ffmpeg",
         "--concurrent-fragments", "1", "--postprocessor-args", "ffmpeg:-threads 1"]
    if cookie_path: a += ["--cookies", str(cookie_path)]
    ea = "youtube:formats=missing_pot"
    if client and client != "default": ea += f";player_client={client}"
    a += ["--extractor-args", ea]
    return a

def run(args, timeout):
    p = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout, p.stderr

def probe(vid):
    """title, duration(sec), channel via yt-dlp (with cookies) â€” falls back to oEmbed."""
    c = cookies.pick()
    err = ""
    for client in YT_CLIENTS[:2]:
        rc, out, err = run(ytdlp_base(c, client) + ["--skip-download", "--print", "%(title)s\t%(duration)s\t%(channel,uploader)s", f"https://youtu.be/{vid}"], 60)
        if rc == 0 and out.strip():
            t, d, ch = (out.strip().split("\n")[-1].split("\t") + ["", "", ""])[:3]
            return t, int(float(d or 0) or 0), ch
    if BOT_RE.search(err): cookies.bench(c)
    try:
        j = requests.get(f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={vid}&format=json", timeout=8).json()
        return j.get("title", vid), 0, j.get("author_name", "YouTube")
    except Exception:
        return vid, 0, "YouTube"

def download(vid, is_video):
    """Run yt-dlp with cookie rotation. Returns (path, title)."""
    ext = "mp4" if is_video else "mp3"
    tmpdir = Path(tempfile.mkdtemp(prefix=f"{vid}_", dir=WORK))
    out = str(tmpdir / f"{vid}.%(ext)s")
    # keep it simple: take whatever is available, ffmpeg converts afterwards
    fmt = (["-f", f"bv*[height<={MAX_HEIGHT}]+ba/b[height<={MAX_HEIGHT}]/bv*+ba/b", "--merge-output-format", "mp4",
            "--recode-video", "mp4"]
           if is_video else ["-f", "ba/b", "-x", "--audio-format", "mp3", "--audio-quality", "128K"])
    tries = len(cookies.all_paths()) or 1          # each cookie file once (no-cookie pass only if none configured)
    last = ""
    for n in range(tries):
        c = cookies.pick()
        hard = False
        for client in YT_CLIENTS:
            args = ytdlp_base(c, client) + ["--print", "after_move:title", "-o", out] + fmt + [f"https://youtu.be/{vid}"]
            if MAX_DURATION: args += ["--match-filter", f"duration<={MAX_DURATION}"]
            log.info("yt-dlp %s %s cookies=%s client=%s", vid, ext, c.name if c else "none", client)
            try:
                rc, so, se = run(args, 900 if is_video else 400)
            except subprocess.TimeoutExpired:
                last = "yt-dlp timeout"; continue
            files = sorted(tmpdir.glob(f"{vid}.{ext}"))
            if rc == 0 and files:
                title = (so.strip().split("\n") or [vid])[-1].strip() or vid
                return files[0], title
            last = (se.strip().split("\n") or ["unknown"])[-1]
            log.warning("yt-dlp failed (%s/%s): %s", c.name if c else "no-cookies", client, last[:200])
            for f in tmpdir.glob("*"): f.unlink(missing_ok=True)
            if RELOAD_RE.search(last): continue           # try next client
            if "match-filter" in last or "does not pass filter" in last: hard = True
            hard = True; break                             # private/geo/etc â€“ no point rotating
        if hard: break
        if BOT_RE.search(last): cookies.bench(c)
    shutil.rmtree(tmpdir, ignore_errors=True)
    raise RuntimeError(last)

# ----------------------------------------------------------------- HF bucket
_hf = None
def hf():
    global _hf
    if _hf is None:
        from huggingface_hub import HfApi
        _hf = HfApi(token=HF_TOKEN)
    return _hf

def bucket_path(vid, ext): return f"{ext}/{vid}.{ext}"
def bucket_url(path): return f"https://huggingface.co/buckets/{HF_BUCKET}/resolve/{urllib.parse.quote(path, safe='')}"

def bucket_exists(path):
    try:
        m = hf().get_bucket_file_metadata(HF_BUCKET, path)
        return getattr(m, "size", None) or True
    except Exception:
        return None

def bucket_upload(local, path):
    hf().batch_bucket_files(HF_BUCKET, add=[(str(local), path)])
    return bucket_url(path)

def public_link(vid, ext, filename):
    """Link the caller can hand to users."""
    if HF_BUCKET_PUBLIC:
        return bucket_url(bucket_path(vid, ext))
    return request.host_url.rstrip("/") + f"/file/{vid}.{ext}?name={urllib.parse.quote(filename)}"

# ----------------------------------------------------------------- core
def safe_name(s): return re.sub(r'[\\/:*?"<>|\r\n]+', "_", s or "").strip()[:150]

def ensure(vid, is_video):
    """Make sure the file exists in the bucket; returns dict."""
    ext = "mp4" if is_video else "mp3"
    key = f"{vid}:{ext}"
    if HF_BUCKET and HF_TOKEN:
        size = bucket_exists(bucket_path(vid, ext))
        if size:
            title, _, _ = probe(vid)
            return {"video_id": vid, "filename": f"{safe_name(title)}.{ext}", "size": size if isinstance(size, int) else None, "cached": True, "bucket_path": bucket_path(vid, ext)}
    # dedupe concurrent identical requests
    with inflight_lock:
        ev = inflight.get(key)
        if ev is None:
            ev = inflight[key] = threading.Event(); owner = True
        else: owner = False
    if not owner:
        ev.wait(1200)
        return ensure(vid, is_video)
    try:
        with sem:
            local, title = download(vid, is_video)
        size = local.stat().st_size
        result = {"video_id": vid, "filename": f"{safe_name(title)}.{ext}", "size": size, "cached": False, "local": str(local)}
        if HF_BUCKET and HF_TOKEN:
            for attempt in range(3):
                try:
                    bucket_upload(local, bucket_path(vid, ext)); result["bucket_path"] = bucket_path(vid, ext); break
                except Exception as e:
                    log.warning("bucket upload attempt %d failed: %s", attempt + 1, e); time.sleep(3)
            else:
                log.error("bucket upload failed; serving from local temp")
        return result
    finally:
        with inflight_lock: inflight.pop(key, None)
        ev.set()

# ----------------------------------------------------------------- async jobs (Back4App proxy kills requests >30 s)
jobs = {}                      # key -> {"ev": Event, "result": dict|None, "error": str|None, "started": float}
jobs_lock = threading.Lock()
SYNC_WAIT = float(os.getenv("SYNC_WAIT_SEC", "20"))

def _job_run(key, vid, is_video):
    j = jobs[key]
    try:
        j["result"] = ensure(vid, is_video)
        log.info("job %s done: %s", key, j["result"].get("size"))
    except Exception as e:
        j["error"] = str(e); log.error("job %s failed: %s", key, e)
    finally:
        j["ev"].set()

def submit(vid, is_video, wait=SYNC_WAIT):
    """Start (or join) the job for this video; wait up to `wait` s. Returns (result, error, pending)."""
    ext = "mp4" if is_video else "mp3"; key = f"{vid}:{ext}"
    with jobs_lock:
        j = jobs.get(key)
        if j is None:
            j = jobs[key] = {"ev": threading.Event(), "result": None, "error": None, "started": time.time()}
            threading.Thread(target=_job_run, args=(key, vid, is_video), daemon=True).start()
    j["ev"].wait(wait)
    if not j["ev"].is_set(): return None, None, True
    r, e = j["result"], j["error"]
    # finished: forget the job (error -> next call retries; success in bucket -> bucket cache serves it)
    if e or (r and not r.get("local")):
        with jobs_lock:
            if jobs.get(key) is j: jobs.pop(key, None)
    return r, e, False

def cleanup_local(result):
    p = result.get("local")
    if p and result.get("bucket_path"):
        shutil.rmtree(Path(p).parent, ignore_errors=True)

# ----------------------------------------------------------------- routes
@app.before_request
def auth():
    if request.path == "/health" or request.method == "OPTIONS": return
    if RELAY_SECRET and request.headers.get("x-relay-key") != RELAY_SECRET:
        return jsonify(error="unauthorized"), 401

@app.after_request
def cors(r):
    r.headers["Access-Control-Allow-Origin"] = "*"; r.headers["Access-Control-Allow-Headers"] = "*"
    return r

@app.get("/health")
def health():
    info = []
    for f in cookies.all_paths():
        try:
            names = {l.split("\t")[5] for l in f.read_text().splitlines() if not l.startswith("#") and l.count("\t") >= 6}
        except Exception:
            names = set()
        info.append({"file": f.name, "cookies": len(names),
                     "logged_in": all(n in names for n in ("SID", "HSID", "SSID", "APISID", "SAPISID", "LOGIN_INFO")),
                     "benched": cookies.bad_until.get(f, 0) > time.time()})
    return jsonify(ok=True, cookies=len(cookies.all_paths()), cookie_files=info, clients=YT_CLIENTS,
                   bucket=HF_BUCKET or None, public=HF_BUCKET_PUBLIC,
                   ytdlp=subprocess.run(["yt-dlp", "--version"], capture_output=True, text=True).stdout.strip())

@app.post("/get-info")
def get_info():
    vid = video_id((request.get_json(silent=True) or {}).get("url"))
    if not vid: return jsonify(error="Invalid YouTube URL"), 400
    title, dur, ch = probe(vid)
    h, m, s = dur // 3600, (dur % 3600) // 60, dur % 60
    return jsonify(status="success", title=title, video_id=vid, channel=ch,
                   duration=(f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"),
                   thumbnail=f"https://i.ytimg.com/vi/{vid}/maxresdefault.jpg")

@app.post("/convert")
def convert():
    b = request.get_json(silent=True) or {}
    vid = video_id(b.get("url"))
    if not vid: return jsonify(error="Invalid YouTube URL"), 400
    is_video = b.get("type") == "video"; ext = "mp4" if is_video else "mp3"
    r, err, pending = submit(vid, is_video, wait=float(b.get("wait", SYNC_WAIT)))
    if pending:
        return jsonify(status="processing", video_id=vid, type=ext, retry_after=5,
                       message="still downloading â€” POST /convert again with the same body"), 202
    if err: return jsonify(error=err), 502
    out = {k: r.get(k) for k in ("video_id", "filename", "size", "cached")}
    out["url"] = public_link(vid, ext, r["filename"]) if r.get("bucket_path") else request.host_url.rstrip("/") + f"/file/{vid}.{ext}"
    if r.get("bucket_path"): out["bucket_url"] = bucket_url(r["bucket_path"])
    cleanup_local(r)
    return jsonify(out)

def stream_bucket(path, filename, mime):
    rr = requests.get(bucket_url(path), headers={"Authorization": f"Bearer {HF_TOKEN}"} if HF_TOKEN else {}, stream=True, timeout=60)
    if rr.status_code != 200: abort(502)
    hdr = {"Content-Type": mime,
           "Content-Disposition": f"attachment; filename=\"{filename.encode('ascii', 'replace').decode()}\"; filename*=UTF-8''{urllib.parse.quote(filename)}"}
    if rr.headers.get("Content-Length"): hdr["Content-Length"] = rr.headers["Content-Length"]
    return Response(rr.iter_content(1 << 16), headers=hdr)

@app.post("/download")
def download_route():
    b = request.get_json(silent=True) or {}
    vid = video_id(b.get("url"))
    if not vid: return jsonify(error="Invalid YouTube URL"), 400
    is_video = b.get("type") == "video"; ext = "mp4" if is_video else "mp3"
    mime = "video/mp4" if is_video else "audio/mpeg"
    r, err, pending = submit(vid, is_video, wait=25)
    if pending:
        return jsonify(status="processing", video_id=vid, type=ext, retry_after=5), 202
    if err: return jsonify(error=err), 502
    if r.get("local") and Path(r["local"]).exists():
        resp = send_file(r["local"], mimetype=mime, as_attachment=True, download_name=r["filename"])
        @resp.call_on_close
        def _c(): shutil.rmtree(Path(r["local"]).parent, ignore_errors=True)
        return resp
    return stream_bucket(r["bucket_path"], r["filename"], mime)

@app.get("/logs")
def logs_route():
    n = min(int(request.args.get("n", 200)), 400)
    return Response("\n".join(list(_ring)[-n:]), mimetype="text/plain")

@app.get("/jobs")
def jobs_route():
    with jobs_lock:
        return jsonify({k: {"done": v["ev"].is_set(), "error": v["error"], "age": round(time.time() - v["started"])} for k, v in jobs.items()})

@app.get("/file/<name>")
def file_route(name):
    m = re.fullmatch(r"([-_0-9A-Za-z]{11})\.(mp3|mp4)", name)
    if not m: abort(404)
    vid, ext = m.groups()
    path = bucket_path(vid, ext)
    if not bucket_exists(path): abort(404)
    fname = request.args.get("name") or f"{vid}.{ext}"
    if HF_BUCKET_PUBLIC: return redirect(bucket_url(path), 302)
    return stream_bucket(path, fname, "video/mp4" if ext == "mp4" else "audio/mpeg")

if __name__ == "__main__":
    log.info("cookies=%d bucket=%s public=%s", len(cookies.all_paths()), HF_BUCKET, HF_BUCKET_PUBLIC)
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), threaded=True)