import json
import os
import queue
import re
import tempfile
import threading
import uuid
from pathlib import Path
from typing import Dict, List

from flask import Flask, Response, render_template, request, send_file, jsonify
import yt_dlp
  
import os

import shutil

_COOKIE_SRC = "/etc/secrets/cookies.txt"

if os.path.exists(_COOKIE_SRC):
    _COOKIE_COPY = os.path.join(tempfile.gettempdir(), "cookies.txt")
    shutil.copy2(_COOKIE_SRC, _COOKIE_COPY)
    cookie_opts = {"cookiefile": _COOKIE_COPY}
else:
    cookie_opts = {}


app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-change-me")

DOWNLOAD_DIR = Path(tempfile.gettempdir()) / "ytdlp_downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)

URL_RE = re.compile(r"^https?://", re.IGNORECASE)

_job_results: Dict[str, dict] = {}
_job_lock = threading.Lock()


# ── helpers ───────────────────────────────────────────────────────────────────



 

def _sse(data: dict) -> str:
    return "data: {}\n\n".format(json.dumps(data))


def _pick_formats(formats: list) -> dict:
    """
    Build structured option groups from raw yt-dlp formats.
    Returns {"video": [...], "audio": [...]}
    """
    video_opts: Dict[int, dict] = {}   # height -> best format
    audio_opts: Dict[int, dict] = {}   # abr    -> best format

    for f in formats:
        vcodec = f.get("vcodec", "none") or "none"
        acodec = f.get("acodec", "none") or "none"
        ext    = f.get("ext", "")
        height = f.get("height") or 0
        abr    = int(f.get("abr") or 0)
        fps    = f.get("fps") or 0
        tbr    = float(f.get("tbr") or 0)

        # Pure video stream – pair with best audio at download time
        if vcodec != "none" and acodec == "none" and height:
            existing = video_opts.get(height)
            if not existing or tbr > float(existing.get("tbr") or 0):
                video_opts[height] = {
                    "format_id": f["format_id"],
                    "height": height,
                    "fps": fps,
                    "ext": ext,
                    "tbr": tbr,
                    "label": "{}p{}".format(height, int(fps) if fps else ""),
                    "type": "video",
                }

        # Mixed stream (video+audio in one container) – fallback option
        if vcodec != "none" and acodec != "none" and height:
            if height not in video_opts:
                video_opts[height] = {
                    "format_id": f["format_id"],
                    "height": height,
                    "fps": fps,
                    "ext": ext,
                    "tbr": tbr,
                    "label": "{}p{}".format(height, int(fps) if fps else ""),
                    "type": "video_combined",
                }

        # Pure audio stream
        if acodec != "none" and vcodec == "none" and abr:
            existing = audio_opts.get(abr)
            if not existing or tbr > float(existing.get("tbr") or 0):
                audio_opts[abr] = {
                    "format_id": f["format_id"],
                    "abr": abr,
                    "ext": ext,
                    "acodec": acodec.split(".")[0],
                    "label": "{} kbps · {}".format(abr, acodec.split(".")[0].upper()),
                    "type": "audio",
                }

    video_list = sorted(video_opts.values(), key=lambda x: x["height"], reverse=True)
    audio_list = sorted(audio_opts.values(), key=lambda x: x["abr"], reverse=True)
    return {"video": video_list, "audio": audio_list}


def _run_download(job_id: str, url: str, format_spec: str, is_audio: bool,
                  q: "queue.Queue"):
    out_template = str(DOWNLOAD_DIR / "{}.%(ext)s".format(job_id))

    def hook(d):
        status = d.get("status")
        if status == "downloading":
            total      = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            downloaded = d.get("downloaded_bytes", 0)
            speed      = d.get("speed") or 0
            eta        = d.get("eta") or 0
            pct        = round(downloaded / total * 100, 1) if total else 0
            q.put({"type": "progress", "pct": pct, "downloaded": downloaded,
                   "total": total, "speed": speed, "eta": eta})
        elif status == "finished":
            q.put({"type": "processing"})

    import shutil
    print("[grab] job={} format_spec={!r} is_audio={}".format(job_id, format_spec, is_audio), flush=True)

    ydl_opts = {
        "outtmpl": out_template,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "progress_hooks": [hook],
        # Same extractor client in both /info and here = consistent formats
        "extractor_args": {"youtube": {"player_client": ["android", "web"]}},
    }

    if is_audio:
        ydl_opts.update({
            "format": format_spec,
            "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3"}],
        })
    else:
        ydl_opts.update({
            "format": format_spec,
            "merge_output_format": "mp4",
            "postprocessor_args": {
                "merger": ["-c:v", "copy", "-c:a", "aac", "-b:a", "192k"],
            },
        })

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info     = ydl.extract_info(url, download=True)

            formats = info.get("formats") or []
            print("[DOWNLOAD] total formats: {}".format(len(formats)))
            for f in formats:
                print("[DOWNLOAD] fmt: id={} height={} abr={} vcodec={} acodec={} ext={}".format(
                    f.get("format_id"), f.get("height"), f.get("abr"),
                    f.get("vcodec"), f.get("acodec"), f.get("ext")
                ))


            filename = ydl.prepare_filename(info)
            base, _  = os.path.splitext(filename)
            expected = base + (".mp3" if is_audio else ".mp4")

        if os.path.exists(expected):
            filename = expected
        else:
            all_files = list(DOWNLOAD_DIR.glob("{}.*".format(job_id)))
            if all_files:
                filename = str(all_files[0])
            else:
                raise FileNotFoundError("Output file not found after download.")

        raw_title = info.get("title") or job_id
        safe_title = re.sub(r'[\\/:*?"<>|]', "_", raw_title).strip()[:180]
        ext = ".mp3" if is_audio else ".mp4"
        download_name = safe_title + ext

        with _job_lock:
            _job_results[job_id] = {"file": filename, "download_name": download_name}
        q.put({"type": "done", "job_id": job_id})

    except Exception as exc:
        print("[grab] EXCEPTION: {}".format(exc), flush=True)
        with _job_lock:
            _job_results[job_id] = {"error": str(exc)}
        q.put({"type": "error", "message": str(exc)})

# ── routes ────────────────────────────────────────────────────────────────────

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")



@app.route("/version")
def version():
    import yt_dlp
    return jsonify({"yt_dlp": yt_dlp.version.__version__})


@app.route("/info", methods=["GET"])
def info():
    url = request.args.get("url", "").strip()
    if not url or not URL_RE.match(url):
        return jsonify({"error": "Invalid URL"}), 400

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "skip_download": True,
        **cookie_opts,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

        formats = info.get("formats") or []
        print("[INFO] request user-agent: {}".format(request.headers.get("User-Agent")))
        print("[INFO] total formats: {}".format(len(formats)))
        for f in formats:
            print("[INFO] fmt: id={} height={} abr={} vcodec={} acodec={} ext={}".format(
                f.get("format_id"), f.get("height"), f.get("abr"),
                f.get("vcodec"), f.get("acodec"), f.get("ext")
            ))


    except Exception as exc:
        return jsonify({"error": str(exc)}), 400

    formats = _pick_formats(info.get("formats") or [])

    dur = info.get("duration") or 0
    h, rem = divmod(int(dur), 3600)
    m, s   = divmod(rem, 60)
    duration_str = ("{}:{:02d}:{:02d}".format(h, m, s) if h else "{}:{:02d}".format(m, s)) if dur else None

    return jsonify({
        "title":       info.get("title"),
        "channel":     info.get("uploader") or info.get("channel"),
        "thumbnail":   info.get("thumbnail"),
        "duration":    duration_str,
        "view_count":  info.get("view_count"),
        "upload_date": info.get("upload_date"),
        "formats":     formats,
    })



@app.route("/stream", methods=["GET"])
def stream():
    url       = request.args.get("url", "").strip()
    height    = request.args.get("height", "").strip()    # e.g. "1080", "720"
    abr       = request.args.get("abr", "").strip()       # e.g. "128", "256"
    is_audio  = request.args.get("is_audio", "0") == "1"

    if not url or not URL_RE.match(url):
        def bad():
            yield _sse({"type": "error", "message": "Enter a valid URL starting with http:// or https://"})
        return Response(bad(), mimetype="text/event-stream")

    # Build format spec from quality params, not format_id
    if is_audio:
        if abr:
            # best audio at or below requested bitrate
            format_spec = "bestaudio[abr<={}]/bestaudio/best".format(abr)
        else:
            format_spec = "bestaudio/best"
    else:
        if height:
            # best video at or below requested height, merged with best audio
            format_spec = (
                "bestvideo[height<={}]+bestaudio/"
                "bestvideo+bestaudio/best".format(height)
            )
        else:
            format_spec = "bestvideo+bestaudio/best"

    job_id = uuid.uuid4().hex
    q = queue.Queue()

    t = threading.Thread(
        target=_run_download,
        args=(job_id, url, format_spec, is_audio, q),
        daemon=True,
    )
    t.start()

    def generate():
        yield _sse({"type": "start", "job_id": job_id})
        while True:
            try:
                msg = q.get(timeout=60)
            except queue.Empty:
                yield _sse({"type": "error", "message": "Download timed out."})
                break
            yield _sse(msg)
            if msg["type"] in ("done", "error"):
                break

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/file/<job_id>")
def serve_file(job_id: str):
    if not re.fullmatch(r"[0-9a-f]{32}", job_id):
        return "Invalid job ID", 400

    with _job_lock:
        result = _job_results.pop(job_id, None)

    if result is None:
        return "File not found or already downloaded.", 404
    if "error" in result:
        return result["error"], 500

    filename = result["file"]
    if not os.path.exists(filename):
        return "File missing on disk.", 404

    download_name = result.get("download_name") or os.path.basename(filename)
    response = send_file(filename, as_attachment=True, download_name=download_name)

    @response.call_on_close
    def cleanup():
        try:
            os.remove(filename)
        except OSError:
            pass

    return response








@app.route("/healthz")
def healthz():
    resp = jsonify({"status": "ok"})
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp 







@app.route("/debug-formats", methods=["GET"])
def debug_formats():
    url = request.args.get("url", "").strip()
    height = request.args.get("height", "").strip()
    abr = request.args.get("abr", "").strip()
    is_audio = request.args.get("is_audio", "0") == "1"

    if is_audio:
        format_spec = "bestaudio[abr<={}]/bestaudio/best".format(abr) if abr else "bestaudio/best"
    else:
        format_spec = "bestvideo[height<={}]+bestaudio/bestvideo+bestaudio/best".format(height) if height else "bestvideo+bestaudio/best"

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "skip_download": True,
        **cookie_opts,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            # Try to resolve which format yt-dlp would actually pick
            ydl2_opts = {**ydl_opts, "format": format_spec}
        with yt_dlp.YoutubeDL(ydl2_opts) as ydl2:
            info2 = ydl2.extract_info(url, download=False)
            chosen = info2.get("format_id") or info2.get("format")
    except Exception as exc:
        return jsonify({"error": str(exc), "format_spec": format_spec})

    return jsonify({
        "format_spec_built": format_spec,
        "format_yt_dlp_chose": chosen,
        "all_available": [
            {"id": f.get("format_id"), "height": f.get("height"),
             "abr": f.get("abr"), "vcodec": f.get("vcodec"), "acodec": f.get("acodec")}
            for f in (info.get("formats") or [])
        ]
    })

  

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=True)