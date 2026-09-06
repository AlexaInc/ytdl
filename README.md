# ytdl-b4a — yt-dlp API for Back4App (Docker) + Hugging Face Storage Bucket

Downloads YouTube audio/video with **yt-dlp + rotating cookie files**, stores results in an
**HF Storage Bucket** (`hansaka01/songs`) and returns a link. Second request for the same
video is served straight from the bucket (no re-download).

## Endpoints
| Method | Path | Body / Query | Returns |
|---|---|---|---|
| GET | `/health` | – | `{ok, ytdlp, cookies, bucket, public}` |
| POST | `/get-info` | `{url}` | title, channel, duration, thumbnail, video_id |
| POST | `/convert` | `{url, type:"audio"\|"video"}` | `{url, filename, video_id, size, cached}` |
| POST | `/download` | `{url, type}` | the file itself (attachment) |
| GET | `/file/<id>.mp3` / `.mp4` | `?name=` | streams from the bucket (works when bucket is private) |

If `RELAY_SECRET` is set, every request must send header `x-relay-key: <secret>`.

## Deploy on Back4App (Containers)
1. Push this folder to a GitHub repo (Dockerfile at repo root).
2. Back4App → **Containers → New App → Import GitHub repo**.
3. Set **Environment variables** (see `.env.example`):
   ```
   HF_TOKEN=hf_xxx                 # write token (Settings → Access Tokens → Write)
   HF_BUCKET=hansaka01/songs
   HF_BUCKET_PUBLIC=1              # set 0 once you make the bucket private
   COOKIES_URLS=https://gist.githubusercontent.com/.../cookies1.txt,https://.../cookies2.txt
   COOKIES_REFRESH_MIN=60
   RELAY_SECRET=some-long-secret   # optional
   MAX_HEIGHT=720
   MAX_DURATION_SEC=1800
   CONCURRENCY=1                   # keep 1 on the free 256 MB plan
   ```
   Port: **8080** (Back4App sets `PORT` automatically; the app reads it).
4. Deploy. Check `https://<app>.b4a.run/health`.

Notes
- Free plan: 256 MB RAM, sleeps when idle (first request after sleep is slow). yt-dlp + ffmpeg
  for a 5-minute song peaks ~150 MB, so `CONCURRENCY=1` on free tier; raise on paid plans.
- yt-dlp self-updates (`pip install -U yt-dlp`) on every container start.
- Cookies: each request picks a random healthy cookie file; a file that hits a bot-check /
  login error is benched for 30 min and the next one is tried. Files are re-fetched every
  `COOKIES_REFRESH_MIN` minutes, so updating the gist rotates cookies with no redeploy.
- Private bucket: set `HF_BUCKET_PUBLIC=0`; `/convert` then returns a `/file/...` link on your
  app which streams from the bucket with the token. Public bucket: `/convert` returns the direct
  `https://huggingface.co/buckets/hansaka01/songs/resolve/mp3/<id>.mp3` CDN link.

## Use from the bot
In `ytdlp.js` / `y2mate.js` add the Back4App URL to `YTDL_RELAYS` and set `YTDL_RELAY_KEY`
to `RELAY_SECRET`. `/convert` and `/download` accept the same `{url, type}` body.

## Local test
```
pip install -r requirements.txt
COOKIES_URLS=<url> HF_BUCKET=hansaka01/songs HF_BUCKET_PUBLIC=1 PORT=8080 python app.py
curl -X POST localhost:8080/convert -H 'content-type: application/json' -d '{"url":"https://youtu.be/9HBCQqpw-nw","type":"audio"}'
```
