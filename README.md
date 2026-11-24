# Video Split Pro V11 — "Pipeline Edition"

A Telegram bot that splits, trims, re-frames and subtitles videos. Built on Pyrogram, ffmpeg, optional Groq AI, and a restart-safe job store.
Render ready (`python bot.py`, or use the included Dockerfile).

## What's new in V11

### ⚡ Faster
| Change | Effect |
|---|---|
| **Cut ⇄ upload pipeline** | Part N+1 is cut (and its thumbnail prepared) *while part N uploads*. Disk use stays bounded (`PIPELINE_PARTS_AHEAD`+1 part files) and falls back to sequential automatically when the disk is tight. |
| **Concurrent transmissions** | Default 2, up to 4 (`MAX_CONCURRENT_TRANSMISSIONS`). |
| **Parallel Whisper** | Long videos transcribe `WHISPER_PARALLEL` chunks at once (order preserved, one retry on rate limits). |
| **Pooled Postgres** | One small connection pool instead of a new TCP+TLS connection per query; stale Neon connections are discarded and retried. Use Neon's `-pooler` host. |
| **Startup cleanup after recovery** | Source files that unfinished jobs still need are no longer deleted at import time. |

### 🧠 More advanced
| Feature | How |
|---|---|
| **Smart cut** | Lossless cuts are *keyframe-aligned*: neighbouring parts share the exact same boundary keyframe, so video packets partition exactly (no overlap, no gap, no frozen first frames). Short stand-alone clips (highlights, `/trim`) are re-encoded automatically when snapping would move their edges (`/settings → Cut`: Auto / Fast / Precise). |
| **Global job queue** | At most `MAX_PARALLEL_JOBS` heavy jobs run; everyone else sees a live "#N in queue" message with a Cancel button. |
| **Access control + quota** | `PRIVATE_MODE`, `ALLOWED_IDS`, `/allow` `/deny` `/ban` `/unban`, and `DAILY_QUOTA_MIN` (video-minutes per user per UTC day). |
| **`/subs`** | Whisper → `.srt` file; `/subs burn` burns captions into the video. |
| **`/reels`** | 9:16 export with blurred background. `/reels 0:30 1:20 subs` adds captions. |
| **`/splitat 1:20 3:45`** | Cut at your own timestamps. |
| **`/silence`** | Removes dead air (`/silence -30 0.5` = threshold dB, min gap seconds). |
| **`/settings`, `/lang`** | Per-user: cut mode, caption style, thumbnails, AI caption, burned captions & 9:16 for highlight clips, Hinglish / English. |
| **🎞 Timeline Editor** | Telegram Mini App: drag cut points on a filmstrip with live preview. Needs a public HTTPS URL (see below). |

### ✨ Better looking
- **One live dashboard card per job** (steps, overall bar, per-part grid, speed, ETA) with a **❌ Cancel** button — instead of a new status message per part.
- **Rich captions**: `⏱ 03:20 → 04:45 · 📦 18.2 MB`, AI reason on highlights.
- **Real thumbnails + width/height/duration** on every upload, so Telegram shows the right aspect ratio.
- **Info card + dynamic buttons**: `✂️ 3 × 04:11`, `⏱ 5 min × 8` computed from the real duration.
- **Result buttons** (split again · highlights · reels · subtitles · clear) — the source stays for `SOURCE_TTL_MIN` minutes; after that it is re-downloaded from Telegram automatically when you tap a button.

### 🐛 Fixed along the way
- Saved source records were written as `active` but looked up as `ready`, so the source was never found after a restart (v10). Existing rows are migrated at boot.
- `/cancel`led jobs are now really marked `cancelled` (v10 left them `active`, so they restarted on the next boot).
- Failed jobs stop being retried at every boot after `RECOVERY_MAX_ATTEMPTS`.
- Thumbnails respect Telegram's 320×320 limit (portrait phone videos used to violate it).
- ffmpeg is killed when a task is cancelled mid-run; the old runner could leave it running.

## Required
- `API_ID`, `API_HASH`, `BOT_TOKEN`

## Recommended
- `DATABASE_URL` — PostgreSQL/Neon for restart-safe recovery (SQLite fallback is *not* durable on Render Free).
- `ADMIN_IDS` — comma-separated Telegram user ids.
- `PUBLIC_URL` — your service's public https URL. Render sets `RENDER_EXTERNAL_URL` automatically, which is picked up; set `ENABLE_MINIAPP=0` to disable the Mini App.

## AI (optional)
`GROQ_API_KEY`, `GROQ_MODEL`, `GROQ_VISION_MODEL`, `GROQ_WHISPER_MODEL`

## V11 tuning (all optional)
| Variable | Default | Meaning |
|---|---|---|
| `MAX_PARALLEL_JOBS` | 2 | Global heavy jobs at once |
| `PIPELINE_PARTS_AHEAD` | 1 | Parts cut ahead of the uploader (0 = old sequential behaviour) |
| `MAX_CONCURRENT_TRANSMISSIONS` | 2 | Pyrogram parallel transfers (1-4) |
| `WHISPER_PARALLEL` | 3 | Parallel transcription chunks |
| `DB_POOL_MAX` | 4 | Postgres pool size |
| `SOURCE_TTL_MIN` | 15 | Keep the source this long after last use (0 = delete after a split, like v10) |
| `PRIVATE_MODE` | 0 | 1 = only admins, `ALLOWED_IDS` and `/allow`ed users |
| `ALLOWED_IDS` | — | Comma-separated ids allowed in private mode |
| `DAILY_QUOTA_MIN` | 0 | Video-minutes per user per UTC day (0 = unlimited; admins exempt) |
| `ENC_PRESET` / `ENC_CRF` | veryfast / 21 | Re-encode quality (precise cuts, reels, burn-in, silence) |
| `AUTO_ENC_MAX_SEG` / `AUTO_SHIFT_TOL` | 120 / 1.0 | In Auto mode, clips up to this many seconds are re-encoded if keyframe snapping would move their edges by more than this many seconds |
| `FASTSTART_MAX_MB` | 700 | Move the MP4 index to the front (streaming) for parts up to this size |
| `REELS_W` / `REELS_H` / `REELS_MAX_SEC` | 720 / 1280 / 180 | 9:16 export size and length limit without a range |
| `BURN_MAX_SEC` | 600 | Longest video that may be burned with captions |
| `SILENCE_DB` / `SILENCE_MIN` / `SILENCE_PAD` / `SILENCE_MAX_SEC` | -35 / 0.7 / 0.12 / 1800 | Silence remover |
| `RECOVERY_MAX_ATTEMPTS` | 3 | Boot-time recovery attempts per job |
| `DASH_MIN_INTERVAL` | 1.6 | Seconds between dashboard edits |
| `DEFAULT_THUMBS` | 1 | Default for the per-user "thumbs" setting |

Free-tier note: re-encoding (precise cuts, `/reels`, `/subs burn`, `/silence`) is CPU-heavy; on Render Free keep `MAX_PARALLEL_JOBS=1` and prefer the lossless modes.

## Commands
```
/split N · /splitmin N · /splitsize MB · /splitat 1:20 3:45 · /splitscene
/highlights · /subs [burn] · /describe · /aistatus
/trim 1:00 3:30 · /reels [start end] [subs] · /silence [dB] [sec] · /compress low|medium|high · /extractaudio
/mergestart → /mergedone · /mergecancel
/settings · /lang · /queue · /quota · /info · /status · /cancel · /clear · /retry (/resume)
admin: /stats · /broadcast text · /allow id · /deny id · /ban id · /unban id
```

## Recovery behavior
- Original Telegram chat/message ID is saved (as a `ready` source record).
- Split parts checkpoint after every successful upload, together with their cut options (reels / burned captions / highlight reasons).
- Scene scan stores detected cuts before splitting; the highlight pipeline stores transcript/candidates/vision results by stage.
- On restart, unfinished jobs resume automatically (through the global queue). If the temporary source is gone, the original is downloaded again from Telegram.
- `/retry` and `/resume` resume the latest unfinished (or failed) job. Cancelled jobs are never auto-restarted.

## Telegram Mini App setup
1. Deploy somewhere with a public HTTPS URL (Render does this for you).
2. Make sure `PUBLIC_URL` (or Render's `RENDER_EXTERNAL_URL`) is available to the bot.
3. Send a video → tap **🎞 Timeline Editor**. Tap/drag the filmstrip to place cuts, then press the Telegram main button.

## Render start
`python bot.py`
