# Ultra Bot v11.0 — "Pipeline Edition"
#   FAST      cut⇄upload overlap (producer/consumer), up to 4 concurrent transmissions, parallel Whisper,
#             pooled Postgres connections, startup cleanup that respects recovery.
#   ADVANCED  global job queue, access control + daily quota, per-user /settings + /lang, keyframe-aligned
#             lossless cuts with optional precise re-encode, /subs (SRT + burn-in), /reels (9:16),
#             /splitat, /silence, Telegram Mini App timeline editor.
#   LOOKS     one live dashboard card per job (with Cancel button), rich captions, real thumbnails and
#             video dimensions, info card with dynamic buttons, result buttons.
# (v10 history is kept below as the FIX #NN comments.)
import os
import sys
import re
import json
import glob
import time
import math
import base64
import asyncio
import logging
import traceback
import shutil
import random
import sqlite3
import uuid
import threading
import hmac
import hashlib
import contextlib
from collections import deque
from urllib.parse import parse_qsl
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
try:
    from pyrogram.types import WebAppInfo          # Telegram Mini App button (newer Pyrogram builds)
except ImportError:
    WebAppInfo = None
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.errors import RPCError as ServerError  # FIX #24 — ServerError removed in latest pyrogram

try:
    from groq import AsyncGroq
except ImportError:
    AsyncGroq = None

# ══════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("UltraBot")


# ══════════════════════════════════════════════════════════════
#  FIX #12 + #19 — ENV VALIDATION
#  - Clear error if missing (not cryptic TypeError)
#  - ValueError if API_ID is not a number
# ══════════════════════════════════════════════════════════════
def _require_env(key: str) -> str:
    val = os.getenv(key, "").strip()
    if not val:
        log.critical(f"❌ Environment variable '{key}' is not set. Bot cannot start.")
        sys.exit(1)
    return val

def _require_int_env(key: str) -> int:
    raw = _require_env(key)
    try:
        return int(raw)
    except ValueError:
        log.critical(f"❌ '{key}' must be an integer, got: '{raw}'")
        sys.exit(1)

API_ID    = _require_int_env("API_ID")
API_HASH  = _require_env("API_HASH")
BOT_TOKEN = _require_env("BOT_TOKEN")

# ══════════════════════════════════════════════════════════════
#  OPTIONAL — AI FEATURES (Groq)
#  Not required to run the bot. If GROQ_API_KEY isn't set, or the
#  groq package isn't installed, all AI features are silently
#  skipped and the bot behaves exactly as before.
#
#  FIX #26 — llama-3.3-70b-versatile is deprecated by Groq
#  (shutdown 2026-08-16). Default updated to openai/gpt-oss-120b,
#  Groq's current recommended replacement. Vision default updated
#  to qwen/qwen3.6-27b — the only vision-capable model Groq
#  currently hosts (meta-llama/llama-4-scout, used in older docs/
#  examples, was shut down 2026-07-17). Override either via env
#  var if Groq's lineup changes again — check console.groq.com/docs/models.
# ══════════════════════════════════════════════════════════════
GROQ_API_KEY      = os.getenv("GROQ_API_KEY", "").strip()
GROQ_MODEL        = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b").strip()
GROQ_VISION_MODEL = os.getenv("GROQ_VISION_MODEL", "qwen/qwen3.6-27b").strip()
WHISPER_MODEL     = os.getenv("GROQ_WHISPER_MODEL", "whisper-large-v3-turbo").strip()
AI_ENABLED        = bool(GROQ_API_KEY and AsyncGroq is not None)
_groq_client       = AsyncGroq(api_key=GROQ_API_KEY) if AI_ENABLED else None
AI_TIMEOUT         = 8   # seconds — captions never stall the upload pipeline
AI_VISION_TIMEOUT  = 20  # seconds — vision calls are slower

if GROQ_API_KEY and AsyncGroq is None:
    log.warning("⚠️ GROQ_API_KEY set but 'groq' package not installed — AI features disabled. "
                "Add `groq` to requirements.txt to enable.")

# ══════════════════════════════════════════════════════════════
#  OPTIONAL — ADMIN FEATURES
#  ADMIN_IDS: comma-separated Telegram user IDs, e.g. "123456,987654"
#  Leave unset to disable /stats and /broadcast for everyone.
# ══════════════════════════════════════════════════════════════
ADMIN_IDS: set[int] = set()
for _part in os.getenv("ADMIN_IDS", "").split(","):
    _part = _part.strip()
    if _part.isdigit():
        ADMIN_IDS.add(int(_part))

def _is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


# ══════════════════════════════════════════════════════════════
#  V11 — CONFIG  (every variable below is optional)
# ══════════════════════════════════════════════════════════════
def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.getenv(key)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}

def _env_int(key: str, default: int, lo: int | None = None, hi: int | None = None) -> int:
    try:
        v = int(float(os.getenv(key, str(default)).strip()))
    except ValueError:
        v = default
    if lo is not None: v = max(lo, v)
    if hi is not None: v = min(hi, v)
    return v

def _env_float(key: str, default: float, lo: float | None = None, hi: float | None = None) -> float:
    try:
        v = float(os.getenv(key, str(default)).strip())
    except ValueError:
        v = default
    if lo is not None: v = max(lo, v)
    if hi is not None: v = min(hi, v)
    return v

# access control / fair-use
PRIVATE_MODE     = _env_bool("PRIVATE_MODE", False)     # True → only ADMIN_IDS + ALLOWED_IDS + /allow'ed users
ALLOWED_IDS: set[int] = set()
for _part in os.getenv("ALLOWED_IDS", "").split(","):
    if _part.strip().isdigit():
        ALLOWED_IDS.add(int(_part.strip()))
DAILY_QUOTA_MIN  = _env_float("DAILY_QUOTA_MIN", 0.0, 0.0)   # video-minutes per user per UTC day, 0 = unlimited

# speed / scheduling
MAX_PARALLEL_JOBS = _env_int("MAX_PARALLEL_JOBS", 2, 1, 8)   # heavy jobs running at the same time (global)
PIPELINE_AHEAD    = _env_int("PIPELINE_PARTS_AHEAD", 1, 0, 3)  # parts cut in advance while one uploads (0 = sequential)
WHISPER_PARALLEL  = _env_int("WHISPER_PARALLEL", 3, 1, 6)
DB_POOL_MAX       = _env_int("DB_POOL_MAX", 4, 1, 10)
SOURCE_TTL_MIN    = _env_float("SOURCE_TTL_MIN", 15.0, 0.0)   # keep the source file this long after a job (0 = delete at once)
RECOVERY_MAX_ATTEMPTS = _env_int("RECOVERY_MAX_ATTEMPTS", 3, 1, 20)
DASH_MIN_INTERVAL = _env_float("DASH_MIN_INTERVAL", 1.6, 0.8, 10.0)

# cutting
ENC_PRESET       = (os.getenv("ENC_PRESET", "veryfast").strip() or "veryfast")
ENC_CRF          = _env_int("ENC_CRF", 21, 14, 35)
AUTO_ENC_MAX_SEG = _env_float("AUTO_ENC_MAX_SEG", 120.0, 5.0)   # 'auto' mode re-encodes clips up to this long if keyframes are far
AUTO_SHIFT_TOL   = _env_float("AUTO_SHIFT_TOL", 1.0, 0.0)       # ...and the keyframe snap would move the edges by more than this
FASTSTART_MAX_MB = _env_int("FASTSTART_MAX_MB", 700, 0)         # moov-at-front rewrite only for parts below this size

# reels / subtitles / silence
REELS_W          = _env_int("REELS_W", 720, 360, 1080)
REELS_H          = _env_int("REELS_H", 1280, 640, 1920)
REELS_MAX_SEC    = _env_float("REELS_MAX_SEC", 180.0, 10.0)
BURN_MAX_SEC     = _env_float("BURN_MAX_SEC", 600.0, 10.0)
SILENCE_DB       = _env_float("SILENCE_DB", -35.0, -90.0, -5.0)
SILENCE_MIN      = _env_float("SILENCE_MIN", 0.7, 0.2, 10.0)
SILENCE_PAD      = _env_float("SILENCE_PAD", 0.12, 0.0, 1.0)
SILENCE_MAX_SEC  = _env_float("SILENCE_MAX_SEC", 1800.0, 30.0)

# public https URL of this service — needed for the Telegram Mini App (Render sets RENDER_EXTERNAL_URL itself)
PUBLIC_URL       = (os.getenv("PUBLIC_URL") or os.getenv("RENDER_EXTERNAL_URL") or "").strip().rstrip("/")


# ══════════════════════════════════════════════════════════════
#  V10/V11 — PERSISTENT STORAGE  (jobs, user settings, ACL, usage)
#  Uses Postgres/Neon when DATABASE_URL is configured, SQLite otherwise.
#  V11: Postgres goes through a small thread-safe connection POOL (the old
#  code opened a brand-new TCP+TLS connection for every single query, which
#  is slow on Neon). Stale pooled connections (Neon suspends idle computes)
#  are discarded and the statement is retried transparently.
#  Tip: use Neon's "-pooler" hostname in DATABASE_URL for best results.
# ══════════════════════════════════════════════════════════════
RECOVERY_DB_URL = (os.getenv("DATABASE_URL", "") or os.getenv("NEON_DATABASE_URL", "")).strip()
RECOVERY_DB_PATH = os.getenv("RECOVERY_DB_PATH", "recovery_jobs.sqlite3")

try:
    import psycopg2
    import psycopg2.pool
    _HAS_PG = True
except ImportError:
    psycopg2 = None
    _HAS_PG = False

_USE_PG = bool(RECOVERY_DB_URL and _HAS_PG)
if RECOVERY_DB_URL and not _HAS_PG:
    log.warning("⚠️ DATABASE_URL set but 'psycopg2' is not installed — using local SQLite (NOT restart-durable on Render).")

_PG_POOL = None
_PG_POOL_LOCK = threading.Lock()
_PG_SLOTS = threading.BoundedSemaphore(DB_POOL_MAX)   # never ask the pool for more than it owns

def _pg_pool():
    global _PG_POOL
    with _PG_POOL_LOCK:
        if _PG_POOL is None:
            _PG_POOL = psycopg2.pool.ThreadedConnectionPool(
                1, DB_POOL_MAX, RECOVERY_DB_URL, connect_timeout=10,
                keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3,
            )
        return _PG_POOL

def _db_conn():
    """SQLite connection (Postgres goes through the pool in _db_exec_sync)."""
    conn = sqlite3.connect(RECOVERY_DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn

def _db_exec_sync(sql, params=(), fetch=False, many=False):
    if _USE_PG:
        with _PG_SLOTS:
            pool = _pg_pool()
            last_err = None
            for _ in range(DB_POOL_MAX + 1):
                conn = pool.getconn()
                bad = False
                try:
                    cur = conn.cursor()
                    try:
                        if many: cur.executemany(sql, params)
                        else:    cur.execute(sql, params)
                        rows = cur.fetchall() if fetch else None
                    finally:
                        cur.close()
                    conn.commit()
                    return rows
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    bad, last_err = True, e          # stale/dropped connection → discard + retry
                except Exception:
                    try: conn.rollback()
                    except Exception: bad = True
                    raise
                finally:
                    try: pool.putconn(conn, close=bad or bool(conn.closed))
                    except Exception: pass
            raise last_err
    conn = _db_conn()
    try:
        cur = conn.cursor()
        sql = sql.replace("%s", "?")
        if many: cur.executemany(sql, params)
        else:    cur.execute(sql, params)
        rows = cur.fetchall() if fetch else None
        conn.commit()
        return rows
    finally:
        conn.close()

def _ddl_statements() -> list[str]:
    big  = "BIGINT" if _USE_PG else "INTEGER"
    real = "DOUBLE PRECISION" if _USE_PG else "REAL"
    return [
        f"""CREATE TABLE IF NOT EXISTS video_jobs (
            job_id TEXT PRIMARY KEY, uid {big} NOT NULL, chat_id {big} NOT NULL,
            source_message_id {big} NOT NULL, kind TEXT NOT NULL, state TEXT NOT NULL,
            payload TEXT NOT NULL, source_path TEXT, attempts INTEGER NOT NULL DEFAULT 0,
            created_at {real} NOT NULL, updated_at {real} NOT NULL)""",
        "CREATE INDEX IF NOT EXISTS idx_video_jobs_state ON video_jobs(state)",
        "CREATE INDEX IF NOT EXISTS idx_video_jobs_uid ON video_jobs(uid, kind, state)",
        f"""CREATE TABLE IF NOT EXISTS user_settings (
            uid {big} PRIMARY KEY, data TEXT NOT NULL, updated_at {real} NOT NULL)""",
        f"""CREATE TABLE IF NOT EXISTS user_acl (
            uid {big} PRIMARY KEY, status TEXT NOT NULL, updated_at {real} NOT NULL)""",
        f"""CREATE TABLE IF NOT EXISTS usage_daily (
            uid {big} NOT NULL, day TEXT NOT NULL,
            seconds {real} NOT NULL DEFAULT 0, jobs INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (uid, day))""",
    ]

def _db_init_sync():
    for stmt in _ddl_statements():
        _db_exec_sync(stmt)

async def _db_init():
    await asyncio.to_thread(_db_init_sync)

async def _db_maintenance():
    """Drop finished job rows after a week, old source records after 2 weeks and usage rows after 45 days.
    Also migrates v10 source records: they were saved as 'active' but looked up as 'ready', so the
    saved source was never found again after a restart."""
    try:
        await asyncio.to_thread(_db_exec_sync,
            "UPDATE video_jobs SET state='ready' WHERE kind='source' AND state IN ('active','recovering','retry')")
        await asyncio.to_thread(_db_exec_sync,
            "DELETE FROM video_jobs WHERE kind='source' AND updated_at < %s", (time.time() - 14 * 86400,))
        await asyncio.to_thread(_db_exec_sync,
            "DELETE FROM video_jobs WHERE state IN ('done','cancelled','failed') AND updated_at < %s",
            (time.time() - 7 * 86400,))
        await asyncio.to_thread(_db_exec_sync,
            "DELETE FROM usage_daily WHERE day < %s",
            (time.strftime("%Y-%m-%d", time.gmtime(time.time() - 45 * 86400)),))
    except Exception as e:
        log.warning("DB maintenance skipped: %s", e)

# ── jobs ─────────────────────────────────────────────────────
async def _job_create(uid, chat_id, source_message_id, kind, payload, source_path=None, job_id=None, state="active"):
    job_id = job_id or uuid.uuid4().hex[:16]
    now=time.time()
    await asyncio.to_thread(_db_exec_sync,
        "INSERT INTO video_jobs(job_id,uid,chat_id,source_message_id,kind,state,payload,source_path,attempts,created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (job_id,uid,chat_id,source_message_id,kind,state,json.dumps(payload),source_path,0,now,now))
    return job_id

async def _job_update(job_id, state=None, payload=None, source_path=None, attempts_inc=False):
    fields=[]; vals=[]
    if state is not None: fields += ["state=%s"]; vals += [state]
    if payload is not None: fields += ["payload=%s"]; vals += [json.dumps(payload)]
    if source_path is not None: fields += ["source_path=%s"]; vals += [source_path]
    if attempts_inc: fields += ["attempts=attempts+1"]
    fields += ["updated_at=%s"]; vals += [time.time(), job_id]
    await asyncio.to_thread(_db_exec_sync, "UPDATE video_jobs SET "+", ".join(fields)+" WHERE job_id=%s", tuple(vals))

async def _job_finish(job_id, state="done"):
    await _job_update(job_id, state=state)

async def _job_get_active(include_failed: bool = False):
    states = "'active','recovering','retry'" + (",'failed'" if include_failed else "")
    rows=await asyncio.to_thread(_db_exec_sync,
        "SELECT job_id,uid,chat_id,source_message_id,kind,state,payload,source_path,attempts FROM video_jobs WHERE state IN (" + states + ") ORDER BY created_at", (), True)
    out=[]
    for r in rows:
        try: payload = json.loads(r[6] or "{}")
        except Exception: payload = {}
        out.append({"job_id":r[0],"uid":int(r[1]),"chat_id":int(r[2]),"source_message_id":int(r[3]),"kind":r[4],"state":r[5],"payload":payload,"source_path":r[7],"attempts":int(r[8] or 0)})
    return out

async def _job_delete(job_id):
    await asyncio.to_thread(_db_exec_sync, "DELETE FROM video_jobs WHERE job_id=%s", (job_id,))

async def _source_for(uid):
    rows=await asyncio.to_thread(_db_exec_sync,
        "SELECT job_id,chat_id,source_message_id,source_path FROM video_jobs WHERE uid=%s AND kind='source' AND state='ready' ORDER BY updated_at DESC LIMIT 1", (uid,), True)
    if not rows: return None
    r=rows[0]
    return {"job_id":r[0],"chat_id":int(r[1]),"source_message_id":int(r[2]),"source_path":r[3]}

async def _protected_paths() -> set[str]:
    """Source files that unfinished jobs / the saved source record still point to —
    startup cleanup must NOT delete these (V10 claimed this; V11 actually does it)."""
    try:
        rows = await asyncio.to_thread(_db_exec_sync,
            "SELECT source_path FROM video_jobs WHERE source_path IS NOT NULL AND state IN ('active','recovering','retry','ready')",
            (), True)
    except Exception as e:
        log.warning("protected-path lookup failed: %s", e)
        return set()
    return {os.path.abspath(r[0]) for r in rows if r and r[0]}

# ── ACL (allow / ban) ────────────────────────────────────────
async def _acl_get(uid: int) -> str | None:
    rows = await asyncio.to_thread(_db_exec_sync, "SELECT status FROM user_acl WHERE uid=%s", (uid,), True)
    return rows[0][0] if rows else None

async def _acl_set(uid: int, status: str | None) -> None:
    if status is None:
        await asyncio.to_thread(_db_exec_sync, "DELETE FROM user_acl WHERE uid=%s", (uid,))
        return
    await asyncio.to_thread(_db_exec_sync,
        "INSERT INTO user_acl(uid,status,updated_at) VALUES (%s,%s,%s) "
        "ON CONFLICT(uid) DO UPDATE SET status=excluded.status, updated_at=excluded.updated_at",
        (uid, status, time.time()))

# ── daily usage (quota) ──────────────────────────────────────
def _today() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())

async def _usage_get(uid: int) -> tuple[float, int]:
    rows = await asyncio.to_thread(_db_exec_sync,
        "SELECT seconds,jobs FROM usage_daily WHERE uid=%s AND day=%s", (uid, _today()), True)
    return (float(rows[0][0]), int(rows[0][1])) if rows else (0.0, 0)

async def _usage_add(uid: int, seconds: float) -> None:
    await asyncio.to_thread(_db_exec_sync,
        "INSERT INTO usage_daily(uid,day,seconds,jobs) VALUES (%s,%s,%s,1) "
        "ON CONFLICT(uid,day) DO UPDATE SET seconds=usage_daily.seconds+excluded.seconds, jobs=usage_daily.jobs+1",
        (uid, _today(), float(max(0.0, seconds))))

# ── per-user settings ────────────────────────────────────────
async def _settings_load(uid: int) -> dict:
    rows = await asyncio.to_thread(_db_exec_sync, "SELECT data FROM user_settings WHERE uid=%s", (uid,), True)
    if not rows: return {}
    try:
        d = json.loads(rows[0][0] or "{}")
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}

async def _settings_save(uid: int, data: dict) -> None:
    await asyncio.to_thread(_db_exec_sync,
        "INSERT INTO user_settings(uid,data,updated_at) VALUES (%s,%s,%s) "
        "ON CONFLICT(uid) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at",
        (uid, json.dumps(data), time.time()))


# ══════════════════════════════════════════════════════════════
#  CONSTANTS & DIRS
# ══════════════════════════════════════════════════════════════
DOWNLOAD_DIR        = "downloads"
THUMB_DIR           = "thumbs"
MAX_FILE_WARN       = 1.8 * 1024 ** 3  # 1.8 GB — warn near TG limit
FFMPEG_CUT_TIMEOUT  = 600              # 10 min per segment
FFPROBE_TIMEOUT     = 30              # 30 s for duration probe
THUMB_TIMEOUT       = 15              # 15 s for thumbnail
MIN_PART_BYTES      = 1024            # FIX #21 — reject parts smaller than 1 KB
# NOTE: download idle/total timeouts removed per user request —
# downloads now run for as long as they take, with no timeout of any kind.
FFMPEG_MERGE_TIMEOUT    = 900          # 15 min for merge
FFMPEG_COMPRESS_TIMEOUT = 1800         # 30 min for re-encode/compress
FFMPEG_AUDIO_TIMEOUT    = 300          # 5 min for audio extraction
SCENE_DETECT_TIMEOUT    = 600          # hard upper bound; scene scan is downscaled/low-FPS
SCENE_MIN_GAP_SEC       = float(os.getenv("SCENE_MIN_GAP_SEC", "4.5"))
SCENE_MAX_COUNT         = int(os.getenv("SCENE_MAX_COUNT", "40"))
SCENE_SCAN_FPS          = float(os.getenv("SCENE_SCAN_FPS", "5"))
SCENE_SCAN_WIDTH        = int(os.getenv("SCENE_SCAN_WIDTH", "480"))
MAX_MERGE_VIDEOS        = 20           # cap merge queue size
COMPRESS_PRESETS        = {"low": 28, "medium": 23, "high": 18}  # CRF values (lower = better quality)
WHISPER_MAX_BYTES       = 24 * 1024 * 1024   # stay under Groq's 25MB free-tier cap, with margin
WHISPER_CHUNK_SECONDS   = 1200                # 20 min per transcription chunk if audio is too big
TRANSCRIBE_TIMEOUT      = 120                 # per-chunk Whisper API timeout
HIGHLIGHT_MIN_SEC       = 20                  # floor — LLM picks are clamped/filtered around this
HIGHLIGHT_MAX_SEC       = 70                  # ceiling
HIGHLIGHT_MIN_COUNT     = 5
HIGHLIGHT_MAX_COUNT     = 10
HIGHLIGHT_CANDIDATE_MAX  = int(os.getenv("HIGHLIGHT_CANDIDATE_MAX", "24"))
HIGHLIGHT_VISION_CHECKS  = int(os.getenv("HIGHLIGHT_VISION_CHECKS", "6"))
HIGHLIGHT_USE_VISION     = os.getenv("HIGHLIGHT_USE_VISION", "1").lower() not in {"0", "false", "no"}

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(THUMB_DIR,    exist_ok=True)


# ══════════════════════════════════════════════════════════════
#  FIX #13 + #22 — Stale file cleanup on startup
#  Added .wmv and .3gp patterns that were missing in v2
# ══════════════════════════════════════════════════════════════
def _cleanup_stale_files(protected: set[str] | None = None) -> None:
    """V11: runs AFTER the DB is up (see _boot) so files that unfinished jobs still
    need (their saved source) are skipped instead of being deleted at import time."""
    keep = {os.path.abspath(p) for p in (protected or set())}
    patterns = [
        f"{DOWNLOAD_DIR}/video_*.mp4",  f"{DOWNLOAD_DIR}/video_*.mkv",
        f"{DOWNLOAD_DIR}/video_*.avi",  f"{DOWNLOAD_DIR}/video_*.mov",
        f"{DOWNLOAD_DIR}/video_*.webm", f"{DOWNLOAD_DIR}/video_*.wmv",
        f"{DOWNLOAD_DIR}/video_*.3gp",  f"{DOWNLOAD_DIR}/part_*.mp4",
        f"{DOWNLOAD_DIR}/merge_*.*",    f"{DOWNLOAD_DIR}/merged_*.mp4",
        f"{DOWNLOAD_DIR}/compressed_*.mp4", f"{DOWNLOAD_DIR}/trim_*.mp4",
        f"{DOWNLOAD_DIR}/audio_*.mp3",  f"{DOWNLOAD_DIR}/scene_*.mp4",
        f"{DOWNLOAD_DIR}/concat_*.txt", f"{DOWNLOAD_DIR}/subs_*.*",
        f"{DOWNLOAD_DIR}/burn_*.mp4",   f"{DOWNLOAD_DIR}/silence_*.mp4",
        f"{DOWNLOAD_DIR}/reels_*.mp4",  f"{DOWNLOAD_DIR}/speech_*.mp3",
        f"{THUMB_DIR}/thumb_*.jpg",     f"{THUMB_DIR}/describe_*.jpg",
        f"{THUMB_DIR}/ma_*.jpg",
    ]
    removed = skipped = 0
    for pat in patterns:
        for f in glob.glob(pat):
            if os.path.abspath(f) in keep:
                skipped += 1
                continue
            try:
                os.remove(f)
                removed += 1
            except Exception:
                pass
    if removed or skipped:
        log.info(f"🧹 Startup cleanup: removed {removed} stale file(s), kept {skipped} protected.")

# ══════════════════════════════════════════════════════════════
#  CLIENT
# ══════════════════════════════════════════════════════════════
app = Client(
    "ultra-bot",
    api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
    in_memory=True,
    sleep_threshold=60,
    ipv6=False,
    max_concurrent_transmissions=max(1, min(4, int(os.getenv("MAX_CONCURRENT_TRANSMISSIONS", "2")))),
)


# ══════════════════════════════════════════════════════════════
#  PER-USER STATE
# ══════════════════════════════════════════════════════════════
user_files:  dict[int, str]           = {}
user_sources: dict[int, dict]         = {}
user_locks:  dict[int, asyncio.Lock]  = {}
user_cancel: dict[int, asyncio.Event] = {}
user_status: dict[int, dict]          = {}
user_merge_queue: dict[int, list[str]] = {}   # uid -> list of queued file paths for /merge
user_settings: dict[int, dict]        = {}   # V11 — per-user prefs (persisted in DB)
user_transcripts: dict[int, dict]     = {}   # V11 — {"path":..., "segments":[...]} cache for /subs & burn-in
_file_touch: dict[int, float]         = {}   # V11 — last time a user's source file was used (TTL janitor)
_acl_cache: dict[int, str | None]     = {}   # V11 — 'allow' | 'ban' | None

# ══════════════════════════════════════════════════════════════
#  STATS (in-memory — resets on restart, same as all other state here)
# ══════════════════════════════════════════════════════════════
_stats_users_seen: set[int] = set()
_stats_videos_processed     = 0
_stats_parts_created        = 0
_stats_start_time           = time.time()

def _track_video_processed() -> None:
    global _stats_videos_processed
    _stats_videos_processed += 1

def _track_part_created() -> None:
    global _stats_parts_created
    _stats_parts_created += 1

# FIX #23 — Prune throttled: track last prune time, don't prune on every call
_MAX_USERS        = 2000
_last_prune_time  = 0.0
_PRUNE_INTERVAL   = 300  # prune at most once every 5 minutes

def _prune_state() -> None:
    """
    Drop oldest inactive users when state grows too large.
    FIX #17: Never prune a user whose lock is currently held (task running).
    FIX #23: Throttled — runs at most once per 5 minutes.
    """
    global _last_prune_time
    now = time.time()
    if len(user_locks) < _MAX_USERS:
        return
    if now - _last_prune_time < _PRUNE_INTERVAL:
        return
    _last_prune_time = now

    # FIX #17 — exclude users with active locks from pruning
    active = {uid for uid, lk in user_locks.items() if lk.locked()}
    inactive = [uid for uid in list(user_locks.keys()) if uid not in active]
    # Drop oldest half of inactive users
    to_remove = set(inactive[:len(inactive) // 2])

    for d in (user_locks, user_cancel, user_status, user_settings, user_transcripts, _file_touch, _acl_cache):
        for k in list(to_remove):
            d.pop(k, None)
    for k in list(to_remove):
        path = user_files.pop(k, None)
        if path:
            try: os.remove(path)
            except: pass
        queue = user_merge_queue.pop(k, None)
        if queue:
            for p in queue:
                try:
                    if os.path.exists(p): os.remove(p)
                except: pass

    log.info(f"🧹 Pruned {len(to_remove)} inactive user states.")


def _get_lock(uid: int) -> asyncio.Lock:
    _prune_state()
    if uid not in user_locks:
        user_locks[uid] = asyncio.Lock()
    return user_locks[uid]

def _get_cancel(uid: int) -> asyncio.Event:
    if uid not in user_cancel:
        user_cancel[uid] = asyncio.Event()
    return user_cancel[uid]

def _set_status(uid: int, task: str, detail: str = "") -> None:
    user_status[uid] = {"task": task, "detail": detail, "since": time.time()}

def _clear_status(uid: int) -> None:
    user_status.pop(uid, None)

def _uid(message) -> int | None:
    """Return user id, or None for channel posts / anonymous senders."""
    uid = message.from_user.id if message.from_user else None
    if uid is not None:
        _stats_users_seen.add(uid)
    return uid


# ══════════════════════════════════════════════════════════════
#  DEDUP  (FIX #16 — correct eviction order)
#
#  v2 BUG:  eviction checked _seen_deque[0] AFTER append.
#           When deque (maxlen=500) is full, append auto-pops
#           the OLDEST item from the deque. So _seen_deque[0]
#           after append is the 2nd-oldest — wrong key discarded
#           from _seen_set → set could grow unbounded.
#
#  v3 FIX:  check len BEFORE append. If deque is full, grab
#           the item that WILL be evicted (index 0), remove it
#           from the set, THEN append the new key.
# ══════════════════════════════════════════════════════════════
_seen_set:       set               = set()
_seen_deque:     deque             = deque(maxlen=500)
_seen_lock_obj:  asyncio.Lock | None = None

def _get_seen_lock() -> asyncio.Lock:
    global _seen_lock_obj
    if _seen_lock_obj is None:
        _seen_lock_obj = asyncio.Lock()
    return _seen_lock_obj

async def _dedup(message) -> bool:
    """Returns True if this exact message was already processed."""
    key = (message.chat.id, message.id)
    async with _get_seen_lock():
        if key in _seen_set:
            return True
        # FIX #16 — evict the item that WILL be auto-popped BEFORE appending
        if len(_seen_deque) == _seen_deque.maxlen:
            _seen_set.discard(_seen_deque[0])  # this is the one about to leave
        _seen_set.add(key)
        _seen_deque.append(key)
        return False


# ══════════════════════════════════════════════════════════════
#  PROGRESS ENGINE
#  FIX #34 — /cancel didn't actually stop an in-flight download/upload.
#  Root cause: progress() only checked the cancel Event to skip editing
#  the status message; the underlying message.download()/reply_video()
#  call from Pyrogram kept running regardless, since nothing ever told
#  it to stop. Result: user taps /cancel -> "Stopping at next
#  checkpoint..." -> nothing happens (no checkpoint exists inside a
#  single download) -> lock stays held -> /clear also refuses ("finish
#  /cancel first") -> user is stuck for up to the 30 min hard timeout.
#  FIX: raise _UserCancelled from inside progress(); Pyrogram calls this
#  callback synchronously from within its own chunk-read loop, so a
#  raised exception propagates straight out of .download()/.reply_video()
#  and aborts the transfer immediately instead of only skipping the UI.
# ══════════════════════════════════════════════════════════════
class _UserCancelled(Exception):
    pass

THROTTLE  = 1.2
EMA_ALPHA = 0.35

FAST_UPLOAD_MODE = os.getenv("FAST_UPLOAD_MODE", "1").strip().lower() not in {"0", "false", "no"}
ENABLE_UPLOAD_THUMB = os.getenv("ENABLE_UPLOAD_THUMB", "0").strip().lower() in {"1", "true", "yes"}
MAX_INPUT_BYTES = int(os.getenv("MAX_INPUT_MB", "2000")) * 1024 * 1024
DISK_SAFETY_BYTES = 256 * 1024 * 1024
SPINNER   = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]

_last_edit: dict[int, float] = {}
_ema_speed: dict[int, float] = {}
_spin_idx:  dict[int, int]   = {}
_shown_pct: dict[int, float] = {}

def _reset(uid: int) -> None:
    for d in (_last_edit, _ema_speed, _spin_idx, _shown_pct):
        d.pop(uid, None)

def _sz(b: float) -> str:
    b = max(0.0, b)  # guard negative
    for u in ("B","KB","MB","GB"):
        if b < 1024: return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} TB"

def _eta(s: float) -> str:
    s = max(0.0, s)
    if s >= 3600: return f"{int(s//3600)}h {int(s%3600//60)}m"
    if s >= 60:   return f"{int(s//60)}m {int(s%60)}s"
    return f"{int(s)}s"

def _bar(pct: float, w: int = 20) -> str:
    pct = max(0.0, min(100.0, pct))
    n = int(pct / 100 * w)
    return "[" + "█" * n + "░" * (w - n) + "]"

def _parse_time(s: str) -> float | None:
    """Parse 'HH:MM:SS', 'MM:SS', or plain seconds into float seconds. None if invalid."""
    s = s.strip()
    try:
        if ":" in s:
            pieces = [float(p) for p in s.split(":")]
            if len(pieces) == 3:
                h, m, sec = pieces
                return h * 3600 + m * 60 + sec
            if len(pieces) == 2:
                m, sec = pieces
                return m * 60 + sec
            return None
        return float(s)
    except (ValueError, TypeError):
        return None

def _badge(pct: float) -> str:
    return ("🏁" if pct>=100 else "🔥" if pct>=80 else
            "⚡" if pct>=60 else "🚀" if pct>=40 else
            "💫" if pct>=20 else "🌀")

def _count_up(uid: int, real: float, step: float = 1.8) -> float:
    prev  = _shown_pct.get(uid, 0.0)
    shown = min(real, prev + step) if real > prev else real
    _shown_pct[uid] = shown
    return shown

async def _safe_edit(msg, text: str, reply_markup=None) -> None:
    try:
        await msg.edit(text, reply_markup=reply_markup)
    except FloodWait as e:
        await asyncio.sleep(e.value + 0.5)
        try: await msg.edit(text, reply_markup=reply_markup)
        except: pass
    except MessageNotModified:
        pass
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════
#  V11 — SETTINGS, LANGUAGE, ACCESS CONTROL, QUOTA
# ══════════════════════════════════════════════════════════════
DEFAULT_SETTINGS = {
    "lang": "hinglish",    # hinglish | english
    "cut": "auto",         # auto | fast | precise
    "caption": "rich",     # rich | simple | off
    "thumbs": 1 if _env_bool("DEFAULT_THUMBS", True) else 0,   # real thumbnails + width/height/duration on uploads
    "aicap": 0,            # AI caption per part (needs GROQ_API_KEY)
    "burn": 0,             # burn captions into highlight clips (needs GROQ_API_KEY)
    "reels": 0,            # export highlight clips as 9:16
}
_SETTING_CYCLE = {
    "lang": ["hinglish", "english"],
    "cut": ["auto", "fast", "precise"],
    "caption": ["rich", "simple", "off"],
    "thumbs": [1, 0], "aicap": [0, 1], "burn": [0, 1], "reels": [0, 1],
}

TXT = {
"hinglish": {
    "no_video": "❌ Pehle video bhejo!",
    "file_missing": "❌ File mil nahi rahi. Dobara video bhejo!",
    "busy": "⏳ Already processing.\n👉 /status · /cancel",
    "redownload": "♻️ **Source video dobara download ho raha hai…**",
    "redownload_ok": "✅ Source wapas aa gaya.",
    "redownload_fail": "❌ Original video wapas nahi mil paaya. Dobara bhejo.",
    "queue_wait": "⏳ **Queue mein ho** — number **#{pos}**\n  Aage: `{ahead}` job · chal rahe: `{running}/{slots}`\n  ❌ Cancel button ya /cancel",
    "queue_left": "🚫 Queue se hata diya.",
    "quota_hit": "⛔ **Daily limit poori ho gayi**\n  Aaj use: `{used}` / `{limit}` min\n  Is video ko chahiye: `{need}` min\n  00:00 UTC pe reset hoga.",
    "quota_line": "📊 Aaj: `{used}` / `{limit}` min · jobs: `{jobs}`",
    "quota_free": "📊 Aaj: `{used}` min · jobs: `{jobs}` · limit: **unlimited**",
    "denied_private": "🔒 Ye private bot hai. Access ke liye admin se baat karo.\n🆔 Tumhara ID: `{uid}`",
    "denied_ban": "🚫 Tumhe is bot se block kar diya gaya hai.",
    "d_part": "Part {n}/{t}",
    "d_cutting": "✂️ Part {n} cut ho raha hai…",
    "d_uploading": "⬆️ Part {n} upload",
    "d_speed": "🚄 `{speed}/s` · ETA `{eta}`",
    "d_elapsed": "⏳ `{el}` ho gaye",
    "d_eta": " · baaki ≈ `{eta}`",
    "d_hint": "❌ Cancel button ya /cancel",
    "d_lossless": "⚡ Lossless cut · keyframe-aligned",
    "d_precise": "🎯 Precise re-encode",
    "d_mixed": "🎯 {n} clip(s) exact edges ke liye re-encode",
    "done_all": "🏁 **Done! {n} part(s) bhej diye**",
    "done_stats": "  ⏱ Total `{total}` · 🚄 avg `{avg}`/part · 📦 `{size}`",
    "cancelled": "🚫 **Cancel ho gaya** — {done}/{n} part(s) pehle bhej diye the.",
    "stopped": "⚠️ **Ruk gaya** — {done}/{n} part(s) bheje.\n  Wajah: `{why}`\n  👉 /retry se wahin se resume hoga.",
    "dl_done": "✅ **Download complete!**",
    "info_title": "📋 **Video Info**",
    "info_tip": "💡 {txt}",
    "info_tap": "👇 Tap karo, ya `/split N` · `/splitat 1:20 3:45` likho:",
    "sug_parts": "~{each} ke {n} parts ban sakte hain",
    "sug_minutes": "{m} min ke chunks → {n} parts",
    "set_title": "⚙️ **Settings**",
    "set_body": "Button tap karke badlo 👇\n\n✂️ **Cut** — Auto: lossless, par chhote clips ho to exact edges · Fast: hamesha lossless (keyframe) · Precise: frame-accurate re-encode (slow)\n📝 **Caption** — Rich: time + size · Simple · Off\n🖼 **Thumbs** — real thumbnail + sahi width/height\n🤖 **AI caption** — har part ka AI caption (Groq)\n💬 **Burn subs** — highlight clips mein captions chhapo\n📱 **Reels** — highlight clips 9:16 mein",
    "lang_set": "🌐 Language: **Hinglish** ✅",
    "lang_ask": "🌐 Language chuno:",
    "splitat_usage": "❌ Usage: `/splitat 1:20 3:45 9:10`\n  Har time pe ek cut. Format `MM:SS`, `HH:MM:SS` ya seconds.",
    "splitat_bad": "❌ Koi valid cut point nahi mila (0 se zyada aur video ki length se kam hona chahiye).",
    "splitat_label": "{n} cut point(s) → {p} parts…",
    "subs_need_ai": "🤖 Subtitles ke liye AI chahiye.\n👉 `/aistatus` check karo.",
    "subs_working": "🎙 Subtitles ban rahe hain…",
    "subs_none": "❌ Speech transcript nahi mila — audio na ho ya bahut lambi ho sakti hai.",
    "subs_caption": "💬 **Subtitles (.srt)** · {n} lines",
    "subs_burn_limit": "ℹ️ Burn-in sirf {lim} tak ke video pe chalta hai (CPU bachane ke liye). SRT file upar hai.",
    "subs_burn_cap": "💬 **Burned-in subtitles**",
    "reels_long": "❌ Video {dur} ka hai. Reels ke liye range do: `/reels 0:30 1:20`\n  (bina range ke max {lim})",
    "reels_label": "Reels 9:16 export…",
    "reels_cap": "📱 **Reels cut** · 9:16",
    "silence_none": "✅ Is video mein khaas silence nahi mili — kuch cut karne layak nahi.",
    "silence_long": "❌ Silence remover ke liye video bahut lamba hai ({dur}). Limit: {lim}.",
    "silence_label": "Silence remove ho rahi hai…",
    "silence_done": "🔇 **Silence removed**\n  ✂️ `{cut}` hata diya ({pct}%) · 🎞 `{old}` → `{new}`",
    "trim_label": "Trimming {a} → {b}…",
    "trim_cap": "✂️ **Trimmed Clip**",
    "btn_again": "🔁 Dobara split",
    "btn_hl": "🔥 Highlights",
    "btn_reels": "📱 Reels",
    "btn_subs": "💬 Subtitles",
    "btn_clear": "🧹 Clear",
    "btn_menu": "🏠 Menu",
    "btn_cancel": "❌ Cancel",
    "btn_timeline": "🎞 Timeline Editor",
    "queue_info": "📋 **Queue**\n  ▶️ Chal rahe: `{running}/{slots}`\n  ⏳ Wait mein: `{waiting}`",
    "queue_you": "\n  👤 Tumhara number: **#{pos}**",
    "acl_ok": "✅ Done: `{uid}` → **{status}**",
    "acl_usage": "❌ Usage: `/{cmd} 123456789`",
    "admin_only": "🚫 Admin-only command.",
    "miniapp_busy": "⏳ Pehle chal raha task khatam hone do.",
    "miniapp_label": "Timeline split · {n} parts…",
},
"english": {
    "no_video": "❌ Send a video first!",
    "file_missing": "❌ File not found. Please send the video again.",
    "busy": "⏳ Already processing.\n👉 /status · /cancel",
    "redownload": "♻️ **Re-downloading your source video…**",
    "redownload_ok": "✅ Source restored.",
    "redownload_fail": "❌ Couldn't fetch the original video again. Please resend it.",
    "queue_wait": "⏳ **You're in the queue** — position **#{pos}**\n  Ahead: `{ahead}` job(s) · running: `{running}/{slots}`\n  ❌ Cancel button or /cancel",
    "queue_left": "🚫 Removed from the queue.",
    "quota_hit": "⛔ **Daily limit reached**\n  Used today: `{used}` / `{limit}` min\n  This video needs: `{need}` min\n  Resets at 00:00 UTC.",
    "quota_line": "📊 Today: `{used}` / `{limit}` min · jobs: `{jobs}`",
    "quota_free": "📊 Today: `{used}` min · jobs: `{jobs}` · limit: **unlimited**",
    "denied_private": "🔒 This is a private bot. Ask the admin for access.\n🆔 Your ID: `{uid}`",
    "denied_ban": "🚫 You have been blocked from this bot.",
    "d_part": "Part {n}/{t}",
    "d_cutting": "✂️ Cutting part {n}…",
    "d_uploading": "⬆️ Uploading part {n}",
    "d_speed": "🚄 `{speed}/s` · ETA `{eta}`",
    "d_elapsed": "⏳ `{el}` elapsed",
    "d_eta": " · ≈ `{eta}` left",
    "d_hint": "❌ Cancel button or /cancel",
    "d_lossless": "⚡ Lossless cut · keyframe-aligned",
    "d_precise": "🎯 Precise re-encode",
    "d_mixed": "🎯 {n} clip(s) re-encoded for exact edges",
    "done_all": "🏁 **Done! {n} part(s) delivered**",
    "done_stats": "  ⏱ Total `{total}` · 🚄 avg `{avg}`/part · 📦 `{size}`",
    "cancelled": "🚫 **Cancelled** — {done}/{n} part(s) had already been sent.",
    "stopped": "⚠️ **Stopped** — {done}/{n} part(s) sent.\n  Reason: `{why}`\n  👉 /retry resumes from there.",
    "dl_done": "✅ **Download complete!**",
    "info_title": "📋 **Video Info**",
    "info_tip": "💡 {txt}",
    "info_tap": "👇 Tap a button, or type `/split N` · `/splitat 1:20 3:45`:",
    "sug_parts": "about {n} parts of ~{each} each",
    "sug_minutes": "{m}-min chunks → {n} parts",
    "set_title": "⚙️ **Settings**",
    "set_body": "Tap a button to change 👇\n\n✂️ **Cut** — Auto: lossless, but short clips get exact edges · Fast: always lossless (keyframe) · Precise: frame-accurate re-encode (slow)\n📝 **Caption** — Rich: time + size · Simple · Off\n🖼 **Thumbs** — real thumbnail + correct width/height\n🤖 **AI caption** — AI caption per part (Groq)\n💬 **Burn subs** — burn captions into highlight clips\n📱 **Reels** — export highlight clips as 9:16",
    "lang_set": "🌐 Language: **English** ✅",
    "lang_ask": "🌐 Choose a language:",
    "splitat_usage": "❌ Usage: `/splitat 1:20 3:45 9:10`\n  One cut per time. Format `MM:SS`, `HH:MM:SS` or seconds.",
    "splitat_bad": "❌ No valid cut point found (must be after 0 and before the end of the video).",
    "splitat_label": "{n} cut point(s) → {p} parts…",
    "subs_need_ai": "🤖 Subtitles need AI.\n👉 Check `/aistatus`.",
    "subs_working": "🎙 Building subtitles…",
    "subs_none": "❌ No speech transcript found — the audio may be missing or too long.",
    "subs_caption": "💬 **Subtitles (.srt)** · {n} lines",
    "subs_burn_limit": "ℹ️ Burn-in only runs on videos up to {lim} (to protect the CPU). The SRT file is above.",
    "subs_burn_cap": "💬 **Burned-in subtitles**",
    "reels_long": "❌ The video is {dur} long. Give a range for Reels: `/reels 0:30 1:20`\n  (max {lim} without a range)",
    "reels_label": "Exporting Reels 9:16…",
    "reels_cap": "📱 **Reels cut** · 9:16",
    "silence_none": "✅ No significant silence found in this video — nothing worth cutting.",
    "silence_long": "❌ The video is too long for the silence remover ({dur}). Limit: {lim}.",
    "silence_label": "Removing silence…",
    "silence_done": "🔇 **Silence removed**\n  ✂️ Cut `{cut}` ({pct}%) · 🎞 `{old}` → `{new}`",
    "trim_label": "Trimming {a} → {b}…",
    "trim_cap": "✂️ **Trimmed Clip**",
    "btn_again": "🔁 Split again",
    "btn_hl": "🔥 Highlights",
    "btn_reels": "📱 Reels",
    "btn_subs": "💬 Subtitles",
    "btn_clear": "🧹 Clear",
    "btn_menu": "🏠 Menu",
    "btn_cancel": "❌ Cancel",
    "btn_timeline": "🎞 Timeline Editor",
    "queue_info": "📋 **Queue**\n  ▶️ Running: `{running}/{slots}`\n  ⏳ Waiting: `{waiting}`",
    "queue_you": "\n  👤 Your position: **#{pos}**",
    "acl_ok": "✅ Done: `{uid}` → **{status}**",
    "acl_usage": "❌ Usage: `/{cmd} 123456789`",
    "admin_only": "🚫 Admin-only command.",
    "miniapp_busy": "⏳ Let the running task finish first.",
    "miniapp_label": "Timeline split · {n} parts…",
},
}

def _sanitize_settings(d: dict) -> dict:
    out = {}
    for k, allowed in _SETTING_CYCLE.items():
        if k in d and d[k] in allowed:
            out[k] = d[k]
    return out

def _S(uid: int | None) -> dict:
    """Effective settings for a user (defaults + overrides)."""
    d = dict(DEFAULT_SETTINGS)
    if uid is not None:
        d.update(user_settings.get(uid) or {})
    return d

def _lang(uid: int | None) -> str:
    return _S(uid).get("lang", "hinglish")

def _t(uid: int | None, key: str, /, **kw) -> str:
    table = TXT.get(_lang(uid), TXT["hinglish"])
    s = table.get(key) or TXT["english"].get(key) or key
    try:
        return s.format(**kw) if kw else s
    except (KeyError, IndexError, ValueError):
        return s

async def _load_user(uid: int) -> None:
    """Warm the per-user caches (settings + ACL) once; DB trouble never blocks the user."""
    if uid not in user_settings:
        try:
            user_settings[uid] = _sanitize_settings(await _settings_load(uid))
        except Exception as e:
            log.warning("settings load failed for %s: %s", uid, e)
            user_settings[uid] = {}
    if uid not in _acl_cache:
        try:
            _acl_cache[uid] = await _acl_get(uid)
        except Exception as e:
            log.warning("ACL load failed for %s: %s", uid, e)
            _acl_cache[uid] = None

async def _set_setting(uid: int, key: str, value) -> None:
    if key not in _SETTING_CYCLE or value not in _SETTING_CYCLE[key]:
        return                                   # ignore unknown keys / invalid values, keep what the user had
    cur = dict(user_settings.get(uid) or {})
    cur[key] = value
    user_settings[uid] = _sanitize_settings(cur)
    try:
        await _settings_save(uid, user_settings[uid])
    except Exception as e:
        log.warning("settings save failed for %s: %s", uid, e)

def _access_ok(uid: int) -> tuple[bool, str]:
    if _is_admin(uid):
        return True, ""
    st = _acl_cache.get(uid)
    if st == "ban":
        return False, _t(uid, "denied_ban")
    if PRIVATE_MODE and uid not in ALLOWED_IDS and st != "allow":
        return False, _t(uid, "denied_private", uid=uid)
    return True, ""

_deny_notice: dict[int, float] = {}

@app.on_message(filters.incoming, group=-10)
async def _gate_message(client, message):
    """Runs before every other handler: blocks banned users / non-allowed users in PRIVATE_MODE."""
    uid = message.from_user.id if message.from_user else None
    if uid is None:
        return
    await _load_user(uid)
    ok, why = _access_ok(uid)
    if ok:
        return
    now = time.time()
    if now - _deny_notice.get(uid, 0.0) > 60:
        _deny_notice[uid] = now
        try: await message.reply(why)
        except Exception: pass
    message.stop_propagation()

@app.on_callback_query(group=-10)
async def _gate_callback(client, callback_query):
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None:
        return
    await _load_user(uid)
    ok, why = _access_ok(uid)
    if ok:
        return
    try: await callback_query.answer(why.split("\n")[0][:190], show_alert=True)
    except Exception: pass
    callback_query.stop_propagation()

async def _quota_gate(message, uid: int, seconds: float) -> bool:
    """Check + record the daily video-minute quota. Admins and DAILY_QUOTA_MIN=0 are unlimited.
    Fails OPEN if the DB is unreachable (a broken DB should not lock everyone out)."""
    seconds = max(0.0, float(seconds or 0.0))
    if _is_admin(uid) or DAILY_QUOTA_MIN <= 0:
        try: await _usage_add(uid, seconds)
        except Exception: pass
        return True
    try:
        used, _jobs = await _usage_get(uid)
    except Exception as e:
        log.warning("quota lookup failed: %s", e)
        return True
    if (used + seconds) / 60.0 > DAILY_QUOTA_MIN + 1e-6:
        await message.reply(_t(uid, "quota_hit", used=f"{used/60:.1f}",
                               limit=f"{DAILY_QUOTA_MIN:.0f}", need=f"{seconds/60:.1f}"))
        return False
    try: await _usage_add(uid, seconds)
    except Exception: pass
    return True


# ══════════════════════════════════════════════════════════════
#  V11 — GLOBAL JOB QUEUE
#  At most MAX_PARALLEL_JOBS heavy jobs (ffmpeg / Whisper / upload) run at once;
#  everyone else gets a live "#N in queue" message instead of thrashing the CPU.
# ══════════════════════════════════════════════════════════════
class _SlotQueue:
    def __init__(self, slots: int):
        self.slots = slots
        self.active = 0
        self.waiting: deque = deque()        # items: (uid, future)

    def pos_of_user(self, uid: int) -> int | None:
        for i, (u, _f) in enumerate(self.waiting):
            if u == uid:
                return i + 1
        return None

    def _pos(self, item) -> int:
        try: return list(self.waiting).index(item) + 1
        except ValueError: return 0

    def _grant(self) -> None:
        while self.waiting and self.active < self.slots:
            _uid_, fut = self.waiting.popleft()
            if fut.done():
                continue
            self.active += 1
            fut.set_result(True)

    def release(self) -> None:
        self.active = max(0, self.active - 1)
        self._grant()

    def _drop(self, item) -> None:
        fut = item[1]
        if fut.done() and not fut.cancelled():
            self.release()                    # slot was granted a moment ago — give it back
            return
        try: self.waiting.remove(item)
        except ValueError: pass
        if not fut.done():
            fut.cancel()

    async def acquire(self, uid: int, on_wait=None, cancel_event=None) -> bool:
        if self.active < self.slots and not self.waiting:
            self.active += 1
            return True
        item = (uid, asyncio.get_running_loop().create_future())
        self.waiting.append(item)
        last_pos = None
        try:
            while True:
                if item[1].done():
                    return True
                if cancel_event is not None and cancel_event.is_set():
                    self._drop(item)
                    return False
                pos = self._pos(item)
                if on_wait is not None and pos != last_pos:
                    last_pos = pos
                    await on_wait(pos, len(self.waiting))
                try:
                    await asyncio.wait_for(asyncio.shield(item[1]), timeout=2)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            self._drop(item)
            raise

_jobq = _SlotQueue(MAX_PARALLEL_JOBS)
_slot_holders: set[int] = set()

@contextlib.asynccontextmanager
async def _job_slot(uid: int, message=None):
    """`async with _job_slot(uid, message) as got:` — yields False if the user cancelled while
    waiting. Re-entrant per user, so a handler can hold the slot and still call helpers that
    ask for one again."""
    if uid in _slot_holders:
        yield True
        return
    cancel = _get_cancel(uid)
    notice = {"msg": None}

    async def on_wait(pos: int, _total: int) -> None:
        if message is None:
            return
        text = _t(uid, "queue_wait", pos=pos, ahead=max(0, pos - 1), running=_jobq.active, slots=_jobq.slots)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(_t(uid, "btn_cancel"), callback_data=f"dcancel:{uid}")]])
        try:
            if notice["msg"] is None:
                notice["msg"] = await message.reply(text, reply_markup=kb)
            else:
                await _try_edit(notice["msg"], text, reply_markup=kb)
        except Exception:
            pass

    got = await _jobq.acquire(uid, on_wait, cancel)
    if got and cancel.is_set():               # cancelled in the same instant the slot was granted
        _jobq.release()
        got = False
    if notice["msg"] is not None:
        if got:
            try: await notice["msg"].delete()
            except Exception: pass
        else:
            await _safe_edit(notice["msg"], _t(uid, "queue_left"))
    elif not got and message is not None:
        try: await message.reply(_t(uid, "queue_left"))
        except Exception: pass
    if not got:
        yield False
        return
    _slot_holders.add(uid)
    try:
        yield True
    finally:
        _slot_holders.discard(uid)
        _jobq.release()


async def _try_edit(msg, text: str, reply_markup=None) -> float:
    """Edit a message WITHOUT ever sleeping. Returns 0.0 normally, or the epoch time until which
    Telegram asked us to back off (FloodWait). The legacy _safe_edit sleeps for the whole flood
    window, which would freeze an upload/cut pipeline if it were called from the dashboard."""
    try:
        await asyncio.wait_for(msg.edit(text, reply_markup=reply_markup), timeout=15)
    except FloodWait as e:
        return time.time() + float(getattr(e, "value", 5) or 5) + 1.0
    except (MessageNotModified, asyncio.TimeoutError):
        pass
    except Exception:
        pass
    return 0.0


# ══════════════════════════════════════════════════════════════
#  V11 — LIVE DASHBOARD CARD
#  One message per job, edited in place (never a new message per part), with a Cancel button.
# ══════════════════════════════════════════════════════════════
_RULE = "━━━━━━━━━━━━━━━━━━━━━━"

def _pbar(pct: float, w: int = 10) -> str:
    pct = max(0.0, min(100.0, pct))
    n = int(round(pct / 100 * w))
    return "▰" * n + "▱" * (w - n)

def _hms(sec: float) -> str:
    sec = int(max(0, sec))
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"

def _cancel_kb(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(_t(uid, "btn_cancel"), callback_data=f"dcancel:{uid}")]])

class Dashboard:
    STEP = {"todo": "⬜", "run": "⏳", "done": "✅", "fail": "❌", "skip": "➖"}
    PART = {"todo": "⬜", "cut": "✂️", "up": "⬆️", "done": "✅", "fail": "❌", "skip": "➖"}

    def __init__(self, message, uid: int, title: str, steps: list[str], total: int = 0, emoji: str = "✂️"):
        self.message, self.uid, self.title, self.emoji = message, uid, title, emoji
        self.steps = [[name, "todo"] for name in steps]
        self.parts = ["todo"] * total
        self.msg = None
        self.t0 = time.time()
        self.t_work = None            # when the first part started
        self.note = ""                # one-line free text (mode, retry reason, transcription chunk…)
        self.mode_line = ""
        self.cut_pct: float | None = None
        self.cut_idx: int | None = None
        self.up: dict | None = None   # {"i":, "pct":, "speed":, "eta":}
        self.finished = False
        self._last = 0.0
        self._blocked_until = 0.0     # FloodWait back-off (never sleeps — the pipeline keeps running)
        self._task = None             # edit currently in flight
        self.kb = _cancel_kb(uid)

    # ── state setters (cheap, no I/O) ──
    def step(self, idx: int, state: str) -> None:
        if 0 <= idx < len(self.steps):
            self.steps[idx][1] = state

    def part(self, i: int, state: str) -> None:
        if 0 <= i < len(self.parts):
            self.parts[i] = state
            if state in ("cut", "up") and self.t_work is None:
                self.t_work = time.time()
            if state == "cut":
                self.cut_idx, self.cut_pct = i, 0.0
            elif state == "up":
                self.up = {"i": i, "pct": 0.0, "speed": 0.0, "eta": None}
            elif self.cut_idx == i:
                self.cut_idx = self.cut_pct = None
            if state in ("done", "fail", "skip") and self.up and self.up.get("i") == i:
                self.up = None

    def set_cut(self, pct: float) -> None:
        if self.cut_idx is not None:
            self.cut_pct = pct

    def set_up(self, i: int, pct: float, speed: float, eta: float | None) -> None:
        self.up = {"i": i, "pct": pct, "speed": speed, "eta": eta}

    # ── rendering ──
    def _grid(self) -> str:
        n = len(self.parts)
        if n == 0:
            return ""
        if n > 40:
            cnt = {k: self.parts.count(k) for k in self.PART}
            return "  ".join(f"{self.PART[k]} {v}" for k, v in cnt.items() if v)
        rows = []
        for i in range(0, n, 10):
            rows.append("".join(self.PART[s] for s in self.parts[i:i + 10]))
        return "\n".join(rows)

    def _overall(self) -> tuple[float, float]:
        total = len(self.parts)
        if not total:
            return 0.0, 0.0
        done = sum(1 for s in self.parts if s in ("done", "skip"))
        frac = (self.up["pct"] / 100.0) if self.up else 0.0
        done_f = done + min(frac, 0.99)
        return done_f * 100.0 / total, done_f

    def render(self) -> str:
        u = self.uid
        total = len(self.parts)
        L = [f"{self.emoji} **{self.title}**", _RULE,
             "  ".join(f"{self.STEP[s]} {n}" for n, s in self.steps), _RULE]
        pct, done_f = self._overall()
        if total:
            n_done = sum(1 for s in self.parts if s in ("done", "skip"))
            L.append(f"{_pbar(pct)} **{pct:.0f}%** · {_t(u, 'd_part', n=min(n_done + (0 if self.finished else 1), total), t=total)}")
        if self.up:
            L.append(f"{_t(u, 'd_uploading', n=self.up['i'] + 1)} · {_pbar(self.up['pct'])} {self.up['pct']:.0f}%")
            if self.up.get("speed"):
                eta = _eta(self.up["eta"]) if self.up.get("eta") is not None else "…"
                L.append(_t(u, "d_speed", speed=_sz(self.up["speed"]), eta=eta))
        if self.cut_idx is not None:
            extra = f" · {self.cut_pct:.0f}%" if self.cut_pct else ""
            L.append(_t(u, "d_cutting", n=self.cut_idx + 1) + extra)
        grid = self._grid()
        if grid:
            L.append(grid)
        if self.mode_line:
            L.append(self.mode_line)
        if self.note:
            L.append(f"_{self.note}_")
        el = time.time() - self.t0
        foot = _t(u, "d_elapsed", el=_eta(el))
        if self.t_work and done_f >= 0.15 and total and not self.finished:
            spent = time.time() - self.t_work
            rem = spent / done_f * (total - done_f)
            if rem < 359999:
                foot += _t(u, "d_eta", eta=_eta(rem))
        L.append(foot)
        if not self.finished:
            L.append(_t(u, "d_hint"))
        return "\n".join(L)

    # ── I/O ──
    async def start(self) -> None:
        try:
            self.msg = await self.message.reply(self.render(), reply_markup=self.kb)
        except Exception as e:
            log.warning("dashboard could not be created: %s", e)
            self.msg = None

    async def _edit(self, text: str) -> None:
        until = await _try_edit(self.msg, text, self.kb)
        if until:
            self._blocked_until = max(self._blocked_until, until)

    async def push(self, force: bool = False) -> None:
        """Fire-and-forget (throttled) card refresh: the cut/upload pipeline never waits for Telegram,
        and a FloodWait only pauses the card — not the job."""
        if self.msg is None or self.finished:
            return
        now = time.time()
        if now < self._blocked_until:
            return
        if now - self._last < (0.8 if force else DASH_MIN_INTERVAL):
            return
        if self._task is not None and not self._task.done():
            return
        self._last = now
        self._task = asyncio.ensure_future(self._edit(self.render()))

    async def finish(self, text: str, kb=None) -> None:
        """Replace the card with a final summary (keeps the part grid)."""
        self.finished = True
        if self._task is not None and not self._task.done():
            try: await asyncio.wait_for(self._task, 10)
            except Exception: pass
        body = text
        grid = self._grid()
        if grid:
            body += f"\n{grid}"
        if self.mode_line:
            body += f"\n{self.mode_line}"
        if self.msg is not None:
            until = await _try_edit(self.msg, body, kb)
            if until:                                   # flood-limited: wait (bounded), then retry once
                await asyncio.sleep(min(25.0, max(0.0, until - time.time())))
                until = await _try_edit(self.msg, body, kb)
            if not until:
                return
        try:
            await self.message.reply(body, reply_markup=kb)
        except Exception:
            pass


class _DashNote:
    """Duck-types a Telegram message for helpers that call `.edit(text)` (transcription, scene scan…):
    the text lands in the dashboard's note line instead of a separate message."""
    def __init__(self, dash: Dashboard):
        self.dash = dash
    async def edit(self, text, reply_markup=None, **_kw):
        first = re.sub(r"[`*_]", "", str(text)).strip().split("\n")[0]
        self.dash.note = first[:90]
        await self.dash.push()
    async def reply(self, *a, **k):
        return self
    async def delete(self):
        return None


# ══════════════════════════════════════════════════════════════
#  V11 — SOURCE FILE LIFECYCLE
#  Source videos are kept for SOURCE_TTL_MIN after their last use so result buttons
#  (re-split / highlights / reels …) work instantly; after that the janitor frees the
#  disk and the bot transparently re-downloads from Telegram when needed again.
# ══════════════════════════════════════════════════════════════
def _touch(uid: int) -> None:
    _file_touch[uid] = time.time()

def _forget_source(uid: int) -> None:
    path = user_files.pop(uid, None)
    if path:
        try: os.remove(path)
        except Exception: pass
        _probe_cache.pop(path, None)
    user_transcripts.pop(uid, None)
    _file_touch.pop(uid, None)
    for f in glob.glob(f"{THUMB_DIR}/ma_{uid}_*.jpg"):
        try: os.remove(f)
        except Exception: pass

_EXT_MAP = {
    "video/x-matroska": "mkv", "video/mkv": "mkv", "video/avi": "avi", "video/x-msvideo": "avi",
    "video/webm": "webm", "video/quicktime": "mov", "video/x-ms-wmv": "wmv", "video/3gpp": "3gp",
}

def _ext_for(media) -> str:
    return _EXT_MAP.get(getattr(media, "mime_type", "") or "", "mp4")

async def _redownload_source(message, uid: int, src: dict) -> bool:
    """Fetch the user's original video from Telegram again (after TTL cleanup / restart)."""
    lock = _get_lock(uid)
    async with lock:
        if _is_ready(uid):
            return True
        status = None
        try:
            status = await message.reply(_t(uid, "redownload"))
            src_msg = await app.get_messages(src["chat_id"], src["source_message_id"])
            media = (src_msg.video or src_msg.document) if src_msg else None
            if not media:
                await _safe_edit(status, _t(uid, "redownload_fail"))
                return False
            path = f"{DOWNLOAD_DIR}/video_{uid}_{src['source_message_id']}.{_ext_for(media)}"
            _get_cancel(uid).clear()
            _reset(uid)
            path = await _await_with_watchdog(
                src_msg.download(file_name=path, progress=progress,
                                 progress_args=(status, time.time(), uid, "♻️ Source")), uid)
            if not path or not os.path.exists(path):
                await _safe_edit(status, _t(uid, "redownload_fail"))
                return False
        except _UserCancelled:
            if status: await _safe_edit(status, "🚫 Cancelled.")
            return False
        except Exception as e:
            log.warning("source re-download failed: %s", e)
            if status: await _safe_edit(status, _t(uid, "redownload_fail"))
            return False
        user_files[uid] = path
        user_sources[uid] = {"chat_id": src["chat_id"], "source_message_id": src["source_message_id"], "source_path": path}
        _touch(uid)
        _reset(uid)
        try:
            if src.get("job_id"):
                await _job_update(src["job_id"], source_path=path)
        except Exception:
            pass
        await _safe_edit(status, _t(uid, "redownload_ok"))
        return True

async def _require_ready(message, uid: int) -> bool:
    """True when the user's source video is on disk. If it was cleaned up (TTL / restart) but the
    original Telegram message is known, it is fetched again automatically."""
    if _is_ready(uid):
        _touch(uid)
        return True
    if not _get_lock(uid).locked():
        try:
            src = user_sources.get(uid) or await _source_for(uid)
        except Exception:
            src = None
        if src:
            if await _redownload_source(message, uid, src):
                return True
            return False
    await message.reply(_t(uid, "no_video"))
    return False

async def _janitor_loop() -> None:
    """Frees idle source files after SOURCE_TTL_MIN and prunes dead per-user state."""
    while True:
        try:
            await asyncio.sleep(60)
            if SOURCE_TTL_MIN > 0:
                now = time.time()
                for uid in list(user_files.keys()):
                    lk = user_locks.get(uid)
                    if (lk and lk.locked()) or uid in _slot_holders:
                        _file_touch[uid] = now
                        continue
                    last = _file_touch.setdefault(uid, now)
                    if now - last > SOURCE_TTL_MIN * 60:
                        log.info("🧹 TTL: freeing idle source of user %s", uid)
                        _forget_source(uid)
            _prune_state()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("janitor error: %s", e)



async def progress(current, total, msg, start, uid: int = 0,
                   mode: str = "📥 Download") -> None:
    """Keep the transmission callback lightweight; UI updates are throttled."""
    if _get_cancel(uid).is_set():
        try:
            app.stop_transmission()
        except Exception:
            pass
        raise _UserCancelled()

    if not isinstance(total, (int, float)) or total <= 0:
        return

    now = time.time()
    elapsed = max(now - start, 0.001)
    prev_edit = _last_edit.get(uid, 0.0)
    _last_edit[uid] = now  # liveness signal on every transferred chunk
    if now - prev_edit < THROTTLE and prev_edit != 0.0:
        return

    raw = current / elapsed
    ema = EMA_ALPHA * raw + (1 - EMA_ALPHA) * _ema_speed.get(uid, raw)
    _ema_speed[uid] = ema
    eta_s = (total - current) / ema if ema > 0 else 0
    pct = max(0.0, min(100.0, current * 100 / total))
    kb = ema / 1024
    tier = "🟢 Fast" if kb >= 1024 else ("🟡 Good" if kb >= 256 else "🔴 Slow")
    spin = SPINNER[_spin_idx.get(uid, 0) % len(SPINNER)]
    _spin_idx[uid] = _spin_idx.get(uid, 0) + 1

    await _safe_edit(msg,
        f"{spin} **{mode}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{_bar(pct)}\n"
        f"  {_badge(pct)} **{pct:.1f}%** · {_sz(current)} / {_sz(total)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  🚄 **Speed** : `{_sz(ema)}/s`  {tier}\n"
        f"  ⏱ **ETA** : `{_eta(eta_s)}`\n"
        f"  ⏳ **Elapsed** : `{_eta(elapsed)}`\n"
        f"  ❌ /cancel to stop"
    )

async def upload_progress(current, total, msg, start, uid: int = 0) -> None:
    await progress(current, total, msg, start, uid=uid, mode="⬆️ Upload")


# ══════════════════════════════════════════════════════════════
#  FIX #35 — IDLE WATCHDOG for downloads
#  The OLD 30-min asyncio.wait_for() only caught a stall once the WHOLE
#  operation had run 30 min — a connection that dies 2 min in still sat
#  there frozen for the remaining ~28 min with nothing watching it, which
#  is exactly the "same %, same elapsed time, for 50+ minutes" freeze
#  reported (and stale files/stuck locks piled up the same way).
#
#  UPDATE (user request) — both the idle-stall check and the absolute
#  30-min cap have been removed entirely. Downloads now run for as long
#  as they take, with no timeout of any kind. Note: a genuinely dead
#  connection will now hang forever (holding the user's lock) since
#  nothing aborts it anymore — this is the tradeoff of "unlimited time".
# ══════════════════════════════════════════════════════════════
class _DownloadStalled(Exception):
    """No longer raised anywhere — kept only so any stray references don't break."""
    pass

async def _await_with_watchdog(coro, uid: int):
    # No timeout logic — just run the download to completion, however long it takes.
    return await coro


# ══════════════════════════════════════════════════════════════
#  FFMPEG HELPERS
#  FIX #18 — proc.wait() after every proc.kill() to reap zombies
# ══════════════════════════════════════════════════════════════
async def _kill_proc(proc) -> None:
    """Kill a subprocess and wait for it to avoid zombie processes."""
    try:
        proc.kill()
    except ProcessLookupError:
        pass  # already dead
    try:
        await asyncio.wait_for(proc.wait(), timeout=5)
    except asyncio.TimeoutError:
        pass  # best effort

async def ffmpeg_cut(inp: str, out: str, ss: float, t: float) -> bool:
    """Cut a video segment. Returns True on success."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-ss", str(ss), "-i", inp,
            "-t", str(t), "-c", "copy", "-avoid_negative_ts", "make_zero", out,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr_data = await asyncio.wait_for(
                proc.communicate(), timeout=FFMPEG_CUT_TIMEOUT
            )
        except asyncio.TimeoutError:
            await _kill_proc(proc)  # FIX #18
            log.error(f"ffmpeg_cut timed out ({FFMPEG_CUT_TIMEOUT}s): {inp}")
            return False
        if proc.returncode != 0:
            log.error(f"ffmpeg_cut rc={proc.returncode}: {stderr_data.decode()[-400:]}")
        return proc.returncode == 0
    except Exception as e:
        log.error(f"ffmpeg_cut exception: {e}")
        return False

async def make_thumb(video: str, ss: float, out: str) -> str | None:
    """Generate thumbnail image. Returns path or None."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-ss", str(ss), "-i", video,
            "-vframes", "1", "-q:v", "2", "-vf", "scale=320:320:force_original_aspect_ratio=decrease", out,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            await asyncio.wait_for(proc.wait(), timeout=THUMB_TIMEOUT)
        except asyncio.TimeoutError:
            await _kill_proc(proc)  # FIX #18
            return None
        return out if os.path.exists(out) else None
    except Exception:
        return None

async def get_duration(file: str) -> float | None:
    """Get video duration in seconds. Returns None on failure."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", file,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            out, _ = await asyncio.wait_for(
                proc.communicate(), timeout=FFPROBE_TIMEOUT
            )
        except asyncio.TimeoutError:
            await _kill_proc(proc)  # FIX #18
            log.error(f"ffprobe timed out: {file}")
            return None
        raw = out.decode().strip()
        return float(raw) if raw and raw != "N/A" else None
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════
#  ADVANCED FFMPEG OPS — merge, compress, extract audio, scene detect
# ══════════════════════════════════════════════════════════════
async def _emit_ffmpeg_progress(uid: int, status_msg, label: str, snap: dict[str, str],
                                t0: float, total_duration: float) -> None:
    """Shared renderer for ffmpeg `-progress pipe:1` snapshots — same visual
    language (bar/badge/spinner/speed/ETA) as the download/upload `progress()`
    engine, so every long-running operation in the bot looks and feels the
    same. Used by compress, merge, and scene-detection.

    FIX #32 — reads `out_time_us` (ffmpeg's newer, unambiguously-named field)
    rather than the legacy `out_time_ms` — verified empirically that both
    carry the identical microsecond value, but `_us` doesn't need a comment
    explaining a naming quirk. Rate is computed from OUR OWN elapsed-time /
    video-seconds-processed ratio (EMA-smoothed like the download/upload
    speed) rather than trusting ffmpeg's own `speed=` field, which can read
    `N/A` or lag right after a seek/keyframe.
    """
    now = time.time()
    if now - _last_edit.get(uid, 0.0) < THROTTLE and _last_edit.get(uid, 0.0) != 0.0:
        return
    _last_edit[uid] = now
    elapsed = max(now - t0, 0.001)

    out_us = snap.get("out_time_us", "")
    try:
        out_sec = max(0.0, float(out_us) / 1_000_000) if out_us and out_us != "N/A" else 0.0
    except ValueError:
        out_sec = 0.0

    raw_rate = out_sec / elapsed
    ema = EMA_ALPHA * raw_rate + (1 - EMA_ALPHA) * _ema_speed.get(uid, raw_rate)
    _ema_speed[uid] = ema

    pct   = min(100.0, out_sec * 100 / total_duration) if total_duration > 0 else 0.0
    shown = _count_up(uid, pct)
    spin  = SPINNER[_spin_idx.get(uid, 0) % len(SPINNER)]
    _spin_idx[uid] = _spin_idx.get(uid, 0) + 1

    eta_s   = (total_duration - out_sec) / ema if ema > 0 else 0.0
    eta_txt = _eta(eta_s) if eta_s < 359999 else "calculating…"  # guard absurd values

    await _safe_edit(status_msg,
        f"{spin} **{label}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{_bar(shown)}\n"
        f"  {_badge(shown)} **{shown:.1f}%**  ·  `{_eta(out_sec)}` / `{_eta(total_duration)}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  🚄 **Speed** : `{ema:.2f}x realtime`\n"
        f"  ⏱ **ETA** : `{eta_txt}`  ·  ⏳ `{_eta(elapsed)}`\n"
        f"  ❌ /cancel to stop"
    )


async def _progress_to_callback(snap: dict, t0: float, total: float | None, cb) -> None:
    """V11 — feed ffmpeg `-progress` snapshots to a callback cb(pct, speed_x) (used by the dashboard)."""
    try:
        out_sec = max(0.0, float(snap.get("out_time_us", "0")) / 1_000_000)
    except ValueError:
        out_sec = 0.0
    pct = min(100.0, out_sec * 100 / total) if total and total > 0 else 0.0
    speed = out_sec / max(time.time() - t0, 0.001)
    try:
        await cb(pct, speed)
    except Exception:
        pass


async def _run_ffmpeg_polled(args: list[str], uid: int, status_msg, label: str,
                             timeout: int, total_duration: float | None = None,
                             on_progress=None) -> bool:
    """Run an ffmpeg command, updating status_msg live, honoring /cancel.
    Returns True iff ffmpeg exits with code 0.

    FIX #32 — when `total_duration` (seconds) is known, streams the full
    bar/%/speed/ETA UI via `-progress pipe:1` (see _emit_ffmpeg_progress),
    correctly batching the several key=value lines ffmpeg emits per snapshot
    and only rendering once the `progress=` marker line closes that batch
    (rather than reacting to each field the instant it's seen, which can
    pair a fresh position with stale size/speed fields from the previous
    snapshot). When duration isn't known, or if progress lines stop arriving
    for a few seconds (e.g. a build without -progress support, or a stalled
    pipe), falls back to the original elapsed-time + spinner display — same
    safety net as before, never blocks either way.

    FIX #28 — ffmpeg writes continuous stats to stderr by default; if nobody
    drains that pipe, the OS buffer (~64KB) fills up and ffmpeg blocks on
    write(), hanging forever even though we're "polling" via proc.wait().
    -loglevel error -nostats silences the routine spew, and a background
    task drains stderr as a belt-and-suspenders fix, keeping only the tail
    for error logging.
    """
    use_progress = bool(total_duration and total_duration > 0)
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-loglevel", "error", "-nostats",
            *(["-progress", "pipe:1"] if use_progress else []),
            *args,
            stdout=asyncio.subprocess.PIPE if use_progress else asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
    except Exception as e:
        log.error(f"{label} failed to start: {e}")
        return False

    stderr_chunks: list[bytes] = []

    async def _drain_stderr():
        try:
            while True:
                chunk = await proc.stderr.read(4096)
                if not chunk:
                    break
                stderr_chunks.append(chunk)
                if sum(len(c) for c in stderr_chunks) > 8192:
                    stderr_chunks[:] = [b"".join(stderr_chunks)[-4096:]]
        except Exception:
            pass

    _reset(uid)
    t0 = time.time()
    spin_i = 0
    last_progress_seen = t0
    drain_task = asyncio.create_task(_drain_stderr())
    progress_task = None

    if use_progress:
        async def _read_stdout_progress():
            nonlocal last_progress_seen
            buf = b""
            snap: dict[str, str] = {}
            try:
                while True:
                    chunk = await proc.stdout.read(4096)
                    if not chunk:
                        break
                    buf += chunk
                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        s = line.decode(errors="ignore").strip()
                        if "=" not in s:
                            continue
                        k, _, v = s.partition("=")
                        snap[k] = v
                        if k == "progress":
                            last_progress_seen = time.time()
                            if on_progress is not None:
                                await _progress_to_callback(snap, t0, total_duration, on_progress)
                            elif status_msg is not None:
                                await _emit_ffmpeg_progress(uid, status_msg, label, snap, t0, total_duration)
                            snap = {}
            except Exception:
                pass
        progress_task = asyncio.create_task(_read_stdout_progress())

    try:
        while True:
            if _get_cancel(uid).is_set():
                await _kill_proc(proc)
                return False
            if time.time() - t0 > timeout:
                await _kill_proc(proc)
                log.error(f"{label} timed out ({timeout}s)")
                return False
            try:
                await asyncio.wait_for(proc.wait(), timeout=2)
                break  # process exited
            except asyncio.TimeoutError:
                # Fallback spinner — only if real progress lines aren't
                # actually coming through (no duration, or a stale/stuck feed)
                if status_msg is not None and (not use_progress or (time.time() - last_progress_seen > 4)):
                    spin = SPINNER[spin_i % len(SPINNER)]
                    spin_i += 1
                    await _safe_edit(status_msg,
                        f"{spin} **{label}**\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"  ⏳ Elapsed: `{_eta(time.time()-t0)}`\n"
                        f"  ❌ /cancel to stop"
                    )
    finally:
        if proc.returncode is None:          # V11 — task cancelled mid-run: never leave ffmpeg running
            await _kill_proc(proc)
        drain_task.cancel()
        if progress_task:
            progress_task.cancel()
        for t in (drain_task, progress_task):
            if t is None:
                continue
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    if proc.returncode != 0:
        tail = b"".join(stderr_chunks).decode(errors="ignore")[-400:]
        log.error(f"{label} rc={proc.returncode}: {tail}")
    return proc.returncode == 0


async def ffmpeg_merge(file_list: list[str], out: str, uid: int, status_msg,
                       timeout: int = FFMPEG_MERGE_TIMEOUT) -> bool:
    """Merge videos in order using ffmpeg's concat demuxer. Falls back to
    a re-encoding concat if stream-copy fails (common with mismatched codecs).

    FIX #32 — total duration (summed across all inputs, probed in PARALLEL so
    one slow ffprobe can't serialize the rest) is computed internally and fed
    to _run_ffmpeg_polled, so both the stream-copy attempt and the re-encode
    fallback show a real %/speed/ETA bar instead of just an elapsed spinner.
    If any single probe fails, we fall back to the spinner entirely rather
    than show a percentage computed from incomplete data.
    """
    concat_path = f"{DOWNLOAD_DIR}/concat_{uid}.txt"
    try:
        durations = await asyncio.gather(*(get_duration(fp) for fp in file_list))
        total_duration = sum(durations) if all(durations) else None

        with open(concat_path, "w") as f:
            for fp in file_list:
                safe = os.path.abspath(fp).replace("'", "'\\''")
                f.write(f"file '{safe}'\n")

        ok = await _run_ffmpeg_polled(
            ["-f", "concat", "-safe", "0", "-i", concat_path, "-c", "copy", out],
            uid, status_msg, f"Merging {len(file_list)} videos…", timeout,
            total_duration=total_duration,
        )
        if not ok and not _get_cancel(uid).is_set():
            log.info("Stream-copy merge failed — retrying with re-encode fallback.")
            if os.path.exists(out):
                try: os.remove(out)
                except: pass
            ok = await _run_ffmpeg_polled(
                ["-f", "concat", "-safe", "0", "-i", concat_path,
                 "-c:v", "libx264", "-preset", "veryfast", "-c:a", "aac", out],
                uid, status_msg, f"Merging {len(file_list)} videos (re-encode)…", timeout,
                total_duration=total_duration,
            )
        return ok
    except Exception as e:
        log.error(f"ffmpeg_merge exception: {e}")
        return False
    finally:
        try: os.remove(concat_path)
        except: pass


async def ffmpeg_compress(inp: str, out: str, crf: int, uid: int, status_msg,
                          timeout: int = FFMPEG_COMPRESS_TIMEOUT) -> bool:
    """Re-encode at given CRF to shrink file size.

    FIX #32 — duration is probed internally to feed _run_ffmpeg_polled's live
    %/speed/ETA bar; if the probe fails for any reason this just falls back
    to the elapsed-time spinner, so a probe failure never blocks compression.
    """
    dur = await get_duration(inp)
    return await _run_ffmpeg_polled(
        ["-i", inp, "-c:v", "libx264", "-crf", str(crf), "-preset", "veryfast",
         "-c:a", "aac", "-b:a", "128k", out],
        uid, status_msg, "Compressing…", timeout, total_duration=dur,
    )


async def ffmpeg_extract_audio(inp: str, out: str,
                               timeout: int = FFMPEG_AUDIO_TIMEOUT) -> bool:
    """Extract audio track as MP3. No progress UI — usually fast."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-i", inp, "-vn", "-acodec", "libmp3lame", "-q:a", "2", out,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr_data = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            await _kill_proc(proc)
            log.error(f"ffmpeg_extract_audio timed out ({timeout}s): {inp}")
            return False
        if proc.returncode != 0:
            log.error(f"ffmpeg_extract_audio rc={proc.returncode}: {stderr_data.decode(errors='ignore')[-400:]}")
        return proc.returncode == 0
    except Exception as e:
        log.error(f"ffmpeg_extract_audio exception: {e}")
        return False


async def detect_scenes(file: str, uid: int, status_msg, total_duration: float,
                        threshold: float = 8.0, max_cuts: int | None = None) -> list[float]:
    """Fast, conservative scene detector.

    The old implementation decoded every source frame and then accepted *any*
    threshold hit. On fast-cut TV/video that turns tiny score spikes into
    dozens of 0.5-3 second clips (e.g. 85 scenes from a 3-minute video).

    This version:
      1. scans a downscaled 480px stream at 5fps (much lighter on Render Free),
      2. collects scdet scores without retaining a giant debug log,
      3. uses an adaptive threshold with a hard floor,
      4. performs non-maximum suppression so nearby spikes become one cut,
      5. enforces a minimum scene length, and
      6. caps the total number of scenes according to video duration.

    Returned values are GLOBAL timestamps of accepted cuts.
    """
    if not total_duration or total_duration <= 1:
        return []

    min_gap = max(2.5, SCENE_MIN_GAP_SEC)
    duration_cap = max(2, int(math.ceil(total_duration / min_gap)))
    if max_cuts is None:
        max_cuts = max(1, min(SCENE_MAX_COUNT - 1, duration_cap - 1))
    max_cuts = max(1, min(int(max_cuts), SCENE_MAX_COUNT - 1))

    fps = max(2.0, min(8.0, SCENE_SCAN_FPS))
    width = max(240, min(720, SCENE_SCAN_WIDTH))

    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-hide_banner", "-loglevel", "info", "-i", file,
            "-vf", f"fps={fps:g},scale={width}:-2:flags=fast_bilinear,scdet=threshold=0",
            "-an", "-f", "null", "-", "-progress", "pipe:1", "-nostats",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except Exception as e:
        log.error(f"detect_scenes failed to start: {e}")
        return []

    # Keep only the tiny amount of stderr needed for score parsing.  The old
    # code stored up to 24MB of debug output, which is unnecessary on a free
    # 512MB service and could itself become a memory pressure point.
    scores: list[tuple[float, float]] = []
    stderr_buf = b""
    progress_buf = b""
    last_ui = 0.0
    t0 = time.time()

    async def drain_stderr():
        nonlocal stderr_buf
        try:
            while True:
                chunk = await proc.stderr.read(65536)
                if not chunk:
                    break
                stderr_buf += chunk
                # Parse complete lines and immediately discard them.
                if len(stderr_buf) > 512 * 1024:
                    stderr_buf = stderr_buf[-256 * 1024:]
                lines = stderr_buf.split(b"\n")
                stderr_buf = lines.pop() if lines else b""
                for raw in lines:
                    line = raw.decode(errors="ignore")
                    m = re.search(r"lavfi\.scd\.score:\s*([0-9.]+),\s*lavfi\.scd\.time:\s*([0-9.]+)", line)
                    if m:
                        try:
                            scores.append((float(m.group(1)), float(m.group(2))))
                        except ValueError:
                            pass
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    async def drain_progress():
        nonlocal progress_buf, last_ui
        try:
            while True:
                chunk = await proc.stdout.read(4096)
                if not chunk:
                    break
                progress_buf += chunk
                while b"\n" in progress_buf:
                    line, progress_buf = progress_buf.split(b"\n", 1)
                    m = re.search(rb"out_time_us=(\d+)", line)
                    if not m:
                        continue
                    now = time.time()
                    if now - last_ui < 1.2:
                        continue
                    last_ui = now
                    done_sec = min(total_duration, int(m.group(1)) / 1_000_000)
                    pct = int(done_sec * 100 / total_duration)
                    await _safe_edit(status_msg,
                        f"🔍 **Scene scan**\n{_bar(pct, 16)} **{pct}%**\n"
                        f"  Scanning at `{fps:g}fps / {width}px`\n"
                        f"  ❌ /cancel to stop")
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    drain_task = asyncio.create_task(drain_stderr())
    progress_task = asyncio.create_task(drain_progress())
    try:
        while True:
            if _get_cancel(uid).is_set():
                await _kill_proc(proc)
                return []
            if time.time() - t0 > SCENE_DETECT_TIMEOUT:
                await _kill_proc(proc)
                log.error("detect_scenes timed out")
                return []
            try:
                await asyncio.wait_for(proc.wait(), timeout=1)
                break
            except asyncio.TimeoutError:
                continue
    finally:
        try:
            await asyncio.wait_for(drain_task, timeout=2)
        except Exception:
            drain_task.cancel()
        progress_task.cancel()
        try:
            await progress_task
        except BaseException:
            pass

    if proc.returncode != 0:
        log.error(f"detect_scenes ffmpeg rc={proc.returncode}")
        return []

    # Flush any final complete stderr line.
    tail = stderr_buf.decode(errors="ignore")
    for m in re.finditer(r"lavfi\.scd\.score:\s*([0-9.]+),\s*lavfi\.scd\.time:\s*([0-9.]+)", tail):
        try:
            scores.append((float(m.group(1)), float(m.group(2))))
        except ValueError:
            pass

    if not scores:
        log.warning("detect_scenes: no scdet scores found")
        return []

    # Ignore the initial frame and extremely late/end timestamps.
    scores = [(s, t) for s, t in scores if 0.15 < t < total_duration - 0.15 and math.isfinite(s)]
    if not scores:
        return []

    values = sorted(s for s, _ in scores)
    p90 = values[int(0.90 * (len(values) - 1))]
    adaptive = max(float(threshold), min(14.0, p90 * 1.5))
    # Hard cuts in scdet are commonly much larger than ordinary motion. Keep
    # a floor so a low-contrast source cannot explode into hundreds of cuts.
    adaptive = max(6.0, adaptive)

    raw = [(s, t) for s, t in scores if s >= adaptive]
    if not raw:
        raw = [(s, t) for s, t in scores if s >= max(4.0, adaptive * 0.65)]
    if not raw:
        return []

    # Non-maximum suppression: for each minimum-gap bucket keep the strongest
    # actual cut instead of accepting every nearby spike.
    raw.sort(key=lambda x: x[1])
    candidates: list[tuple[float, float]] = []
    cluster: list[tuple[float, float]] = []
    for item in raw:
        if not cluster or item[1] - cluster[-1][1] <= min_gap:
            cluster.append(item)
        else:
            candidates.append(max(cluster, key=lambda x: x[0]))
            cluster = [item]
    if cluster:
        candidates.append(max(cluster, key=lambda x: x[0]))

    # Enforce a true minimum distance between accepted boundaries.
    accepted: list[tuple[float, float]] = []
    for score, ts in sorted(candidates, key=lambda x: x[1]):
        if not accepted or ts - accepted[-1][1] >= min_gap:
            accepted.append((score, ts))
        elif score > accepted[-1][0]:
            accepted[-1] = (score, ts)

    # If still too many, retain the strongest cuts while preserving timeline order.
    if len(accepted) > max_cuts:
        strongest = sorted(accepted, key=lambda x: x[0], reverse=True)[:max_cuts]
        accepted = sorted(strongest, key=lambda x: x[1])

    cuts = [round(ts, 2) for _, ts in accepted]
    log.info("Scene detector: %d scores -> %d cuts (threshold=%.2f, min_gap=%.2fs)",
             len(scores), len(cuts), adaptive, min_gap)
    return cuts


# ══════════════════════════════════════════════════════════════
#  AI HIGHLIGHT EXTRACTION — /highlights
#  Content-aware alternative to /splitscene. Instead of cutting at every
#  visual shot change (which can produce dozens of tiny, meaningless
#  clips on heavily-edited content like TV/movies), this transcribes the
#  dialogue and asks the LLM to pick a handful of genuinely "best" /
#  shareable moments — the way a person skimming a transcript would.
# ══════════════════════════════════════════════════════════════
async def _cut_audio(inp: str, out: str, start: float, dur: float,
                     timeout: int = FFMPEG_CUT_TIMEOUT) -> bool:
    """Stream-copy cut of an audio-only file (used for chunking before
    transcription) — deliberately separate from ffmpeg_cut (video-oriented)
    to avoid any cross-contamination between the two code paths."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-ss", str(start), "-i", inp, "-t", str(dur), "-c", "copy", out,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            await _kill_proc(proc)
            return False
        return proc.returncode == 0
    except Exception as e:
        log.error(f"_cut_audio exception: {e}")
        return False


async def _extract_speech_audio(inp: str, out: str,
                                timeout: int = FFMPEG_AUDIO_TIMEOUT) -> bool:
    """Extract a small, speech-optimized mono track (16kHz, low bitrate) —
    this is for transcription accuracy/size, not listening quality, so it
    stays well under Groq's per-file size limit for as long as possible."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-i", inp, "-vn", "-ar", "16000", "-ac", "1",
            "-c:a", "libmp3lame", "-b:a", "48k", out,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr_data = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            await _kill_proc(proc)
            return False
        if proc.returncode != 0:
            log.error(f"_extract_speech_audio rc={proc.returncode}: {stderr_data.decode(errors='ignore')[-300:]}")
        return proc.returncode == 0
    except Exception as e:
        log.error(f"_extract_speech_audio exception: {e}")
        return False


async def _transcribe_chunk(path: str) -> list[dict]:
    """Transcribe one audio file (must already be under Groq's size limit)
    via Whisper. Returns [{start, end, text}, ...] with LOCAL timestamps
    (relative to the start of this specific file) — [] on any failure."""
    if not AI_ENABLED:
        return []
    try:
        with open(path, "rb") as f:
            data = f.read()
        resp = await asyncio.wait_for(
            _groq_client.audio.transcriptions.create(
                file=(os.path.basename(path), data),
                model=WHISPER_MODEL,
                response_format="verbose_json",
                timestamp_granularities=["segment"],
            ),
            timeout=TRANSCRIBE_TIMEOUT,
        )
        segments = getattr(resp, "segments", None)
        if segments is None and isinstance(resp, dict):
            segments = resp.get("segments", [])
        out = []
        for seg in (segments or []):
            try:
                if isinstance(seg, dict):
                    out.append({"start": float(seg["start"]), "end": float(seg["end"]),
                               "text": str(seg["text"])})
                else:
                    out.append({"start": float(seg.start), "end": float(seg.end),
                               "text": str(seg.text)})
            except (KeyError, AttributeError, ValueError, TypeError):
                continue
        return out
    except asyncio.TimeoutError:
        log.warning(f"Whisper transcription timed out: {path}")
        return []
    except Exception as e:
        log.warning(f"Whisper transcription failed: {e}")
        return []


async def _transcribe_chunk_retry(path: str) -> list[dict]:
    """One retry (after a short pause) — parallel chunks can hit Groq's rate limit."""
    segs = await _transcribe_chunk(path)
    if not segs:
        await asyncio.sleep(3)
        segs = await _transcribe_chunk(path)
    return segs


async def _transcribe_video(file: str, dur: float, uid: int, status_msg) -> list[dict]:
    """Extract speech audio, chunk it if it's too big for a single Whisper
    call, transcribe the chunks IN PARALLEL (V11: WHISPER_PARALLEL at a time,
    ordering preserved) and merge into one list of {start, end, text}
    segments with GLOBAL timestamps (chunk-offset corrected)."""
    audio_path = f"{DOWNLOAD_DIR}/speech_{uid}.mp3"
    ok = await _extract_speech_audio(file, audio_path)
    if not ok or not os.path.exists(audio_path):
        return []

    chunk_paths: list[str] = []
    try:
        size = os.path.getsize(audio_path)
        if size <= WHISPER_MAX_BYTES:
            return await _transcribe_chunk(audio_path)

        # 1) cut every chunk first (stream copy → cheap)
        n_chunks = math.ceil(dur / WHISPER_CHUNK_SECONDS)
        chunks: list[tuple[int, float, str]] = []
        for i in range(n_chunks):
            if _get_cancel(uid).is_set():
                break
            start = i * WHISPER_CHUNK_SECONDS
            seg_len = min(WHISPER_CHUNK_SECONDS, dur - start)
            if seg_len <= 0:
                break
            cp = f"{DOWNLOAD_DIR}/speech_{uid}_c{i}.mp3"
            if await _cut_audio(audio_path, cp, start, seg_len) and os.path.exists(cp):
                chunk_paths.append(cp)
                chunks.append((i, start, cp))

        # 2) transcribe them concurrently
        sem = asyncio.Semaphore(WHISPER_PARALLEL)
        finished = 0

        async def work(i: int, start: float, cp: str):
            nonlocal finished
            async with sem:
                if _get_cancel(uid).is_set():
                    return i, start, []
                segs = await _transcribe_chunk_retry(cp)
            finished += 1
            await _safe_edit(status_msg, f"🎙 **Transcribing…** {finished}/{len(chunks)} chunks done")
            return i, start, segs

        results = await asyncio.gather(*(work(*c) for c in chunks))
        all_segments: list[dict] = []
        for _i, start, segs in sorted(results, key=lambda r: r[0]):
            for s in segs:
                all_segments.append({"start": s["start"] + start, "end": s["end"] + start, "text": s["text"]})
        return all_segments
    finally:
        for p in chunk_paths + [audio_path]:
            try: os.remove(p)
            except Exception: pass


async def _select_highlights(segments: list[dict], duration: float) -> list[dict]:
    """Multi-stage text-AI highlight selector.

    Stage 1: Whisper supplies timestamped speech.
    Stage 2: the LLM ranks self-contained moments instead of blindly asking
             for arbitrary timestamps.
    Stage 3 (in _vision_rank_highlights): a vision model checks the selected
             frames and re-ranks them before the final cuts are made.
    """
    if not AI_ENABLED or not segments:
        return []

    # Compact transcript keeps API payload reasonable on long videos while
    # retaining timestamps and enough surrounding dialogue for setup/punchline.
    lines = []
    for s in segments:
        text = re.sub(r"\s+", " ", str(s.get("text", "")).strip())
        if not text:
            continue
        m, sec = divmod(int(max(0, s["start"])), 60)
        lines.append(f"[{m:02d}:{sec:02d}] {text[:500]}")
    transcript = "\n".join(lines)
    if len(transcript) > 50000:
        transcript = transcript[:50000] + "\n...[transcript truncated]"

    candidate_target = min(HIGHLIGHT_CANDIDATE_MAX, max(HIGHLIGHT_MAX_COUNT * 2, 12))
    prompt = (
        f"You are the first AI judge for a {duration/60:.1f}-minute video.\n"
        f"Find up to {candidate_target} candidate highlight moments from this timestamped transcript.\n\n"
        f"Rules:\n"
        f"- A moment should be self-contained and understandable.\n"
        f"- Prefer comedy, emotion, surprise, conflict, useful information, a punchline, reveal, or strong reaction.\n"
        f"- Include enough setup before the payoff.\n"
        f"- Target {HIGHLIGHT_MIN_SEC}-{HIGHLIGHT_MAX_SEC}s per final clip.\n"
        f"- Never invent timestamps outside the transcript/video.\n"
        f"- Candidates may overlap at this stage, but final clips must not overlap.\n"
        f"- score is 0-100 for highlight quality, not video quality.\n\n"
        f"TRANSCRIPT:\n{transcript}\n\n"
        f'Respond ONLY JSON: {{"highlights":[{{"start":123.4,"end":165.0,"score":92,"reason":"short reason"}}]}}'
    )
    try:
        resp = await asyncio.wait_for(
            _groq_client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1800,
                temperature=0.2,
                response_format={"type": "json_object"},
            ),
            timeout=90,
        )
        data = json.loads(resp.choices[0].message.content)
        items = data.get("highlights", []) if isinstance(data, dict) else []
    except asyncio.TimeoutError:
        log.warning("_select_highlights timed out")
        return []
    except Exception as e:
        log.warning(f"_select_highlights failed: {e}")
        return []

    picked: list[dict] = []
    for it in items:
        try:
            start = max(0.0, float(it["start"]))
            end = min(duration, float(it["end"]))
            score = max(0.0, min(100.0, float(it.get("score", 50))))
            reason = re.sub(r"\s+", " ", str(it.get("reason", "Highlight moment"))).strip()[:90]
        except (KeyError, TypeError, ValueError):
            continue

        # Expand very short AI picks to preserve setup + payoff, then clamp.
        if end - start < HIGHLIGHT_MIN_SEC:
            center = (start + end) / 2
            half = HIGHLIGHT_MIN_SEC / 2
            start = max(0.0, center - half)
            end = min(duration, start + HIGHLIGHT_MIN_SEC)
            if end - start < HIGHLIGHT_MIN_SEC:
                start = max(0.0, end - HIGHLIGHT_MIN_SEC)
        if end - start > HIGHLIGHT_MAX_SEC:
            end = start + HIGHLIGHT_MAX_SEC
        if end <= start or end - start < HIGHLIGHT_MIN_SEC * 0.75:
            continue
        picked.append({"start": start, "end": min(end, duration),
                       "score": score, "reason": reason or "Highlight moment"})

    # Deduplicate heavily overlapping model picks, retaining the stronger one.
    picked.sort(key=lambda x: x["score"], reverse=True)
    selected: list[dict] = []
    for item in picked:
        overlap = False
        for kept in selected:
            inter = max(0.0, min(item["end"], kept["end"]) - max(item["start"], kept["start"]))
            shorter = min(item["end"] - item["start"], kept["end"] - kept["start"])
            if shorter > 0 and inter / shorter >= 0.35:
                overlap = True
                break
        if not overlap:
            selected.append(item)
        if len(selected) >= HIGHLIGHT_MAX_COUNT:
            break

    return sorted(selected, key=lambda x: x["start"])


async def _vision_rank_highlights(video: str, uid: int, highlights: list[dict]) -> list[dict]:
    """Second AI judge: inspect representative frames and re-rank picks.

    Vision is intentionally limited to a handful of candidates so Render Free
    is not buried under dozens of FFmpeg/API calls. If vision fails, the text-AI
    ranking is retained unchanged.
    """
    if not AI_ENABLED or not HIGHLIGHT_USE_VISION or not highlights:
        return highlights

    ranked = list(highlights)
    checks = min(HIGHLIGHT_VISION_CHECKS, len(ranked))
    # Check strongest candidates first; the rest keep their text-AI score.
    ranked.sort(key=lambda h: h.get("score", 0), reverse=True)

    async def score_one(index: int, item: dict):
        thumb_path = f"{THUMB_DIR}/hv_{uid}_{index}.jpg"
        try:
            mid = (item["start"] + item["end"]) / 2
            thumb = await make_thumb(video, mid, thumb_path)
            if not thumb:
                return
            with open(thumb, "rb") as f:
                img_b64 = base64.b64encode(f.read()).decode()
            prompt = (
                "You are the second AI judge for a short-video highlight. "
                "Judge ONLY the visible frame: does it look like a meaningful, "
                "engaging moment rather than a blank/transition/boring frame? "
                "Return JSON only: {\"score\":0-100,\"reason\":\"max 8 words\"}."
            )
            resp = await asyncio.wait_for(
                _groq_client.chat.completions.create(
                    model=GROQ_VISION_MODEL,
                    messages=[{"role": "user", "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}},
                    ]}],
                    max_tokens=80,
                    temperature=0.1,
                    response_format={"type": "json_object"},
                ),
                timeout=AI_VISION_TIMEOUT,
            )
            data = json.loads(resp.choices[0].message.content)
            vscore = max(0.0, min(100.0, float(data.get("score", 50))))
            # Text AI remains the primary judge; vision breaks ties / removes
            # visually weak frames instead of overpowering transcript quality.
            item["score"] = item.get("score", 50) * 0.70 + vscore * 0.30
            vr = str(data.get("reason", "")).strip()
            if vr:
                item["vision_reason"] = vr[:60]
        except Exception as e:
            log.warning(f"highlight vision check failed: {e}")
        finally:
            try:
                if os.path.exists(thumb_path):
                    os.remove(thumb_path)
            except Exception:
                pass

    await asyncio.gather(*(score_one(i, ranked[i]) for i in range(checks)), return_exceptions=True)

    # Re-rank and remove overlaps again after the second AI score.
    ranked.sort(key=lambda h: h.get("score", 0), reverse=True)
    final: list[dict] = []
    for item in ranked:
        if any(
            max(0.0, min(item["end"], x["end"]) - max(item["start"], x["start"]))
            / max(0.1, min(item["end"] - item["start"], x["end"] - x["start"])) >= 0.35
            for x in final
        ):
            continue
        final.append(item)
        if len(final) >= HIGHLIGHT_MAX_COUNT:
            break
    return sorted(final, key=lambda h: h["start"])


# ══════════════════════════════════════════════════════════════
#  AI CAPTION + DESCRIPTION (optional, Groq) — never blocks/crashes
# ══════════════════════════════════════════════════════════════
async def _ai_caption(orig_filename: str, num: int, total: int) -> str | None:
    """Return a short punchy caption for this part, or None on any failure/timeout."""
    if not AI_ENABLED:
        return None
    try:
        prompt = (
            f"Write ONE short, punchy Telegram caption (max 12 words, 1 emoji max) "
            f"for part {num} of {total} of a video file named '{orig_filename}'. "
            f"No hashtags, no quotes, just the caption text."
        )
        resp = await asyncio.wait_for(
            _groq_client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=40,
                temperature=0.8,
            ),
            timeout=AI_TIMEOUT,
        )
        text = resp.choices[0].message.content.strip().strip('"')
        return text if text else None
    except asyncio.TimeoutError:
        log.warning(f"AI caption timed out (part {num}/{total}) — using fallback.")
        return None
    except Exception as e:
        log.warning(f"AI caption failed (part {num}/{total}): {e} — using fallback.")
        return None


async def _ai_describe(thumb_path: str) -> str | None:
    """Generate a short AI description of a video from one representative frame.
    Returns None on any failure — caller should show a graceful fallback message."""
    if not AI_ENABLED:
        return None
    try:
        with open(thumb_path, "rb") as f:
            img_b64 = base64.b64encode(f.read()).decode()
        resp = await asyncio.wait_for(
            _groq_client.chat.completions.create(
                model=GROQ_VISION_MODEL,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": (
                            "Describe what's likely happening in this video in one "
                            "short sentence (max 15 words), based on this frame."
                        )},
                        {"type": "image_url",
                         "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}},
                    ],
                }],
                max_tokens=60,
            ),
            timeout=AI_VISION_TIMEOUT,
        )
        text = resp.choices[0].message.content.strip()
        return text if text else None
    except asyncio.TimeoutError:
        log.warning("AI describe timed out.")
        return None
    except Exception as e:
        log.warning(f"AI describe failed: {e} — vision model may be unavailable; "
                    f"check GROQ_VISION_MODEL against console.groq.com/docs/vision")
        return None


async def _ai_caption_from_frame(thumb_path: str, num: int, total: int) -> str | None:
    """FIX #30 — per-part captions now analyze that part's OWN thumbnail frame
    via the vision model, instead of only guessing from the filename. Gives a
    genuinely content-aware caption per part (e.g. reacts to what's actually
    on screen at that timestamp) rather than a generic "Part N" filler line.
    Returns None on any failure/timeout — caller falls back to the older,
    filename-only _ai_caption(), and if that also fails, to a plain caption."""
    if not AI_ENABLED:
        return None
    try:
        with open(thumb_path, "rb") as f:
            img_b64 = base64.b64encode(f.read()).decode()
        resp = await asyncio.wait_for(
            _groq_client.chat.completions.create(
                model=GROQ_VISION_MODEL,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": (
                            f"This is a frame from part {num} of {total} of a video. "
                            f"Write ONE short, punchy Telegram caption (max 12 words, "
                            f"1 emoji max) based on what's actually shown in this frame. "
                            f"No hashtags, no quotes, just the caption text."
                        )},
                        {"type": "image_url",
                         "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}},
                    ],
                }],
                max_tokens=40,
                temperature=0.8,
            ),
            timeout=AI_VISION_TIMEOUT,
        )
        text = resp.choices[0].message.content.strip().strip('"')
        return text if text else None
    except asyncio.TimeoutError:
        log.warning(f"AI frame-caption timed out (part {num}/{total}).")
        return None
    except Exception as e:
        log.warning(f"AI frame-caption failed (part {num}/{total}): {e} — falling back.")
        return None


# ══════════════════════════════════════════════════════════════
#  V11 — MEDIA TOOLBOX
#  probe · keyframe-aligned "smart cut" planner · clip cutter · SRT · reels filter · silence
# ══════════════════════════════════════════════════════════════
_probe_cache: dict[str, tuple[tuple[int, int], dict]] = {}

async def _run_capture(cmd: list[str], timeout: float, want_stderr: bool = False):
    """Run a subprocess, return (returncode|None-on-timeout, stdout, stderr).
    Always reaps the child — also when the awaiting task is cancelled."""
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE if want_stderr else asyncio.subprocess.DEVNULL)
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        await _kill_proc(proc)
        return None, b"", b""
    except asyncio.CancelledError:
        await _kill_proc(proc)
        raise
    return proc.returncode, out or b"", err or b""


def _parse_fps(raw: str | None) -> float | None:
    try:
        if raw and "/" in raw:
            a, b = raw.split("/", 1)
            return float(a) / float(b) if float(b) else None
        return float(raw) if raw else None
    except (ValueError, TypeError):
        return None


async def _probe_video(path: str) -> dict:
    """Duration, display size (rotation-aware), fps, codecs, bitrate, size. Cached per file version."""
    try:
        st = os.stat(path)
    except OSError:
        return {}
    key = (st.st_mtime_ns, st.st_size)
    hit = _probe_cache.get(path)
    if hit and hit[0] == key:
        return hit[1]
    info: dict = {"size": st.st_size, "duration": None}
    try:
        rc, out, _ = await _run_capture(
            ["ffprobe", "-v", "error", "-show_format", "-show_streams", "-of", "json", path], FFPROBE_TIMEOUT)
        data = json.loads(out.decode(errors="ignore") or "{}") if rc == 0 else {}
    except Exception as e:
        log.warning("probe failed: %s", e)
        data = {}
    fmt = data.get("format") or {}
    try: info["duration"] = float(fmt.get("duration"))
    except (TypeError, ValueError): pass
    try: info["bitrate"] = int(fmt.get("bit_rate"))
    except (TypeError, ValueError): pass
    try: info["start_time"] = float(fmt.get("start_time") or 0.0)
    except (TypeError, ValueError): info["start_time"] = 0.0
    for s in data.get("streams") or []:
        ctype = s.get("codec_type")
        if ctype == "video" and "vcodec" not in info and not (s.get("disposition") or {}).get("attached_pic"):
            info["vcodec"] = s.get("codec_name")
            w, h = s.get("width"), s.get("height")
            rot = 0
            try: rot = int(float((s.get("tags") or {}).get("rotate", 0)))
            except (TypeError, ValueError): pass
            for sd in s.get("side_data_list") or []:
                if "rotation" in sd:
                    try: rot = int(float(sd["rotation"]))
                    except (TypeError, ValueError): pass
            if w and h and abs(rot) % 180 == 90:
                w, h = h, w
            info["width"], info["height"] = w, h
            info["fps"] = _parse_fps(s.get("avg_frame_rate")) or _parse_fps(s.get("r_frame_rate"))
            if info["duration"] is None:
                try: info["duration"] = float(s.get("duration"))
                except (TypeError, ValueError): pass
        elif ctype == "audio" and "acodec" not in info:
            info["acodec"] = s.get("codec_name")
    _probe_cache[path] = (key, info)
    if len(_probe_cache) > 64:
        _probe_cache.pop(next(iter(_probe_cache)), None)
    return info


# ── keyframes ────────────────────────────────────────────────
# ffmpeg's `-ss/-t` are relative to the container's start_time while ffprobe prints absolute
# timestamps, so every helper below takes `base` (= start_time) and returns RELATIVE times.
# For streams with B-frames ffmpeg ends a stream-copy cut by packet DTS (not PTS): the next
# keyframe's DTS is therefore what marks the exact end of a GOP-aligned cut → `_KF_DTS`.
_KF_EPS = 0.0015
_KF_DTS: dict[tuple[str, int], float] = {}

def _kf_key(file: str, pts: float) -> tuple[str, int]:
    return (file, int(round(pts * 1000)))

async def _kf_scan(file: str, lo: float, hi: float, base: float = 0.0) -> list[float]:
    """Keyframe PTS (relative) inside [lo, hi] — packet headers only, no decoding. Also fills _KF_DTS."""
    lo = max(0.0, lo)
    span = max(0.1, hi - lo)
    rc, out, _ = await _run_capture(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", "packet=pts_time,dts_time,flags", "-of", "csv=p=0",
         "-read_intervals", f"{lo + base:.3f}%+{span:.3f}", file], FFPROBE_TIMEOUT)
    if rc != 0:
        return []
    kfs: set[float] = set()
    for line in out.decode(errors="ignore").splitlines():
        parts = line.strip().split(",")
        if len(parts) < 3 or "K" not in parts[2]:
            continue
        try:
            pts = round(float(parts[0]) - base, 6)
        except ValueError:
            continue
        kfs.add(pts)
        try:
            _KF_DTS[_kf_key(file, pts)] = float(parts[1]) - base
        except ValueError:
            pass
    if len(_KF_DTS) > 20000:
        _KF_DTS.clear()
    return sorted(kfs)

async def _floor_kf(file: str, t: float, base: float = 0.0) -> float | None:
    """Latest keyframe at or before t."""
    if t <= 0.05:
        return 0.0
    for back in (20.0, 120.0, 900.0):
        lo = max(0.0, t - back)
        cand = [k for k in await _kf_scan(file, lo, t + 1.0, base) if k <= t + _KF_EPS]
        if cand:
            return max(0.0, max(cand))
        if lo <= 0:
            break
    return None

async def _ceil_kf(file: str, t: float, base: float = 0.0) -> float | None:
    """Earliest keyframe at or after t."""
    for fwd in (20.0, 120.0, 900.0):
        cand = [k for k in await _kf_scan(file, max(0.0, t - 1.0), t + fwd, base) if k >= t - _KF_EPS]
        if cand:
            return min(cand)
    return None


# ── smart-cut planner ────────────────────────────────────────
async def _plan_cuts(file: str, segments: list, mode: str, opts: dict, dur: float,
                     base: float = 0.0) -> list[dict]:
    """One plan per segment: {"ss","dur"(None = until EOF),"enc"(re-encode?),"shift","orig"}.

    • fast    — stream copy, edges snapped to keyframes (lossless, never overlaps its neighbour:
                the boundary between two touching segments is the SAME keyframe for both).
    • precise — every segment re-encoded → frame-accurate.
    • auto    — like fast, but short stand-alone clips (highlights/trim) whose edges would move
                by more than AUTO_SHIFT_TOL seconds are re-encoded for exact edges.
    Segments that cannot be snapped sanely (no keyframe found, collapsed length) fall back to
    re-encode instead of being dropped, so part numbering/resume indices never change."""
    EOF_TOL = 0.08
    items = []
    for ss, d in segments:
        ss = max(0.0, float(ss))
        end = dur if d is None else min(dur, ss + float(d))
        items.append((ss, max(ss, end)))
    n = len(items)
    force = mode == "precise" or bool(opts.get("vf") or opts.get("srt_fn") or opts.get("force_encode"))
    plans: list[dict] = []
    if force:
        for ss, end in items:
            at_eof = end >= dur - EOF_TOL
            plans.append({"ss": ss, "dur": None if at_eof else end - ss, "enc": True,
                          "shift": 0.0, "orig": (ss, end - ss)})
        return plans

    contig_next = [i + 1 < n and abs(items[i][1] - items[i + 1][0]) < 0.05 for i in range(n)]
    need: set[tuple[str, float]] = set()
    for i, (ss, end) in enumerate(items):
        need.add(("f", round(ss, 3)))
        if end < dur - EOF_TOL:
            need.add(("f" if contig_next[i] else "c", round(end, 3)))
    sem = asyncio.Semaphore(4)

    async def look(key):
        kind, t = key
        async with sem:
            return key, await (_floor_kf(file, t, base) if kind == "f" else _ceil_kf(file, t, base))

    res = dict(await asyncio.gather(*(look(k) for k in sorted(need))))

    for i, (ss, end) in enumerate(items):
        at_eof = end >= dur - EOF_TOL
        contig_prev = i > 0 and contig_next[i - 1]
        sk = res.get(("f", round(ss, 3)))
        ek = None if at_eof else res.get(("f" if contig_next[i] else "c", round(end, 3)))
        enc = False
        if sk is None or (not at_eof and ek is None):
            enc = True
        else:
            e_eff = dur if at_eof else ek
            if e_eff <= sk + 0.25:
                enc = True
        shift = 0.0
        if not enc:
            shift = abs(ss - sk) + (0.0 if at_eof else abs(end - ek))
            if (mode == "auto" and not contig_prev and not contig_next[i]
                    and (end - ss) <= AUTO_ENC_MAX_SEG and shift > AUTO_SHIFT_TOL):
                enc = True
        if enc:
            plans.append({"ss": ss, "dur": None if at_eof else end - ss, "enc": True,
                          "shift": shift, "orig": (ss, end - ss)})
        else:
            plans.append({"ss": sk, "dur": None if at_eof else ek - sk, "enc": False,
                          "end_dts": None if at_eof else _KF_DTS.get(_kf_key(file, ek)),
                          "shift": shift, "orig": (ss, end - ss)})
    return plans


# ── clip cutter ──────────────────────────────────────────────
def _ff_path(p: str) -> str:
    """Escape a file path for use inside an ffmpeg filter argument."""
    return p.replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")

def _sub_style(portrait: bool) -> str:
    fs, mv = (11, 22) if portrait else (14, 14)
    return (f"FontName=Noto Sans,FontSize={fs},Bold=1,PrimaryColour=&H00FFFFFF&,OutlineColour=&H00000000&,"
            f"BorderStyle=1,Outline=2,Shadow=0,Alignment=2,MarginV={mv}")

async def _copy_clip(file: str, out: str, plan: dict, faststart: bool) -> bool:
    ss, d = plan["ss"], plan["dur"]
    seek = ss + 0.002 if ss > 0 else 0.0        # +2 ms: land on THIS keyframe even if its printed pts rounded down
    args = ["ffmpeg", "-y", "-v", "error"]
    if seek > 0:
        args += ["-ss", f"{seek:.3f}"]
    args += ["-i", file]
    if d is not None:
        end_dts = plan.get("end_dts")
        if end_dts is not None:
            t = end_dts - seek - 0.001          # stop right before the NEXT keyframe packet (decode order)
        else:
            t = d - 0.012 - 0.12                # no DTS known → be conservative (≤ 3 frames short, never overlapping)
        args += ["-t", f"{max(0.05, t):.3f}"]
    args += ["-map", "0:v:0", "-map", "0:a:0?", "-sn", "-dn", "-c", "copy",
             "-avoid_negative_ts", "make_zero"]
    if faststart:
        args += ["-movflags", "+faststart"]
    args.append(out)
    rc, _o, err = await _run_capture(args, FFMPEG_CUT_TIMEOUT, want_stderr=True)
    if rc != 0:
        log.error("copy-cut rc=%s: %s", rc, err.decode(errors="ignore")[-300:])
        return False
    return os.path.exists(out) and os.path.getsize(out) >= MIN_PART_BYTES

async def _encode_clip(file: str, out: str, plan: dict, opts: dict, uid: int, idx: int,
                       total_dur: float, on_progress=None) -> bool:
    ss, d = plan["ss"], plan["dur"]
    clip_dur = d if d is not None else max(0.1, total_dur - ss)
    vf: list[str] = []
    if opts.get("vf"):
        vf.append(opts["vf"])
    if opts.get("srt_fn"):
        srt = opts["srt_fn"](idx, ss, clip_dur)
        if srt:
            vf.append(f"subtitles=filename='{_ff_path(srt)}':force_style='{_sub_style(bool(opts.get('reels')))}'")
    args = ["-ss", f"{ss:.3f}", "-i", file]
    if d is not None:
        args += ["-t", f"{d:.3f}"]
    args += ["-map", "0:v:0", "-map", "0:a:0?", "-sn", "-dn"]
    if vf:
        args += ["-vf", ",".join(vf)]
    args += ["-c:v", "libx264", "-preset", ENC_PRESET, "-crf", str(ENC_CRF), "-pix_fmt", "yuv420p",
             "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", out]
    ok = await _run_ffmpeg_polled(args, uid, None, "Encode", FFMPEG_COMPRESS_TIMEOUT,
                                  total_duration=clip_dur, on_progress=on_progress)
    return bool(ok) and os.path.exists(out) and os.path.getsize(out) >= MIN_PART_BYTES

async def _cut_part(file: str, out: str, plan: dict, opts: dict, uid: int, idx: int,
                    total_dur: float, est_bytes: float = 0.0, on_progress=None) -> bool:
    try:
        if os.path.exists(out): os.remove(out)
    except OSError:
        pass
    if plan["enc"]:
        return await _encode_clip(file, out, plan, opts, uid, idx, total_dur, on_progress)
    faststart = FASTSTART_MAX_MB > 0 and (est_bytes <= FASTSTART_MAX_MB * 1048576 or est_bytes <= 0)
    return await _copy_clip(file, out, plan, faststart)


# ── subtitles (SRT) ──────────────────────────────────────────
def _srt_ts(t: float) -> str:
    ms = int(round(max(0.0, t) * 1000))
    h, r = divmod(ms, 3_600_000)
    m, r = divmod(r, 60_000)
    s, ms = divmod(r, 1000)
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"

def _wrap_cue(text: str, width: int = 38) -> str:
    lines, cur = [], ""
    for w in text.split():
        if cur and len(cur) + 1 + len(w) > width:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}".strip()
    if cur:
        lines.append(cur)
    return "\n".join(lines)

def _srt_cues(segments: list[dict], clip_start: float = 0.0, clip_end: float | None = None,
              max_chars: int = 76) -> list[tuple[float, float, str]]:
    """Whisper segments → readable cues, clipped to [clip_start, clip_end] and shifted so
    clip_start becomes 0. Long segments are split on word boundaries, proportionally in time."""
    cues: list[tuple[float, float, str]] = []
    for sg in segments:
        try:
            a, b = float(sg["start"]), float(sg["end"])
        except (KeyError, TypeError, ValueError):
            continue
        text = re.sub(r"\s+", " ", str(sg.get("text", "")).strip())
        if not text or b <= a:
            continue
        if clip_end is not None and (b <= clip_start or a >= clip_end):
            continue
        if clip_end is None and b <= clip_start:
            continue
        words = text.split()
        k = max(1, math.ceil(len(text) / max_chars))
        per = max(1, math.ceil(len(words) / k))
        groups = [words[i:i + per] for i in range(0, len(words), per)]
        total_chars = sum(len(" ".join(g)) for g in groups) or 1
        t = a
        for g in groups:
            seg_txt = " ".join(g)
            t_end = t + (b - a) * len(seg_txt) / total_chars
            ca, cb = max(t, clip_start), t_end if clip_end is None else min(t_end, clip_end)
            if cb - ca >= 0.15:
                cues.append((ca - clip_start, cb - clip_start, seg_txt))
            t = t_end
    return cues

def _cues_to_srt(cues: list[tuple[float, float, str]]) -> str:
    out = []
    for i, (a, b, txt) in enumerate(cues, 1):
        out.append(f"{i}\n{_srt_ts(a)} --> {_srt_ts(b)}\n{_wrap_cue(txt)}\n")
    return "\n".join(out)

def _write_srt(path: str, cues: list[tuple[float, float, str]]) -> str | None:
    if not cues:
        return None
    with open(path, "w", encoding="utf-8") as f:
        f.write(_cues_to_srt(cues))
    return path

def _make_srt_fn(uid: int, transcript: list[dict]):
    """srt_fn(idx, clip_start, clip_dur) → path of a per-clip SRT (timestamps relative to the clip)."""
    def fn(idx: int, ss: float, clip_dur: float):
        cues = _srt_cues(transcript, clip_start=ss, clip_end=ss + clip_dur)
        return _write_srt(f"{DOWNLOAD_DIR}/subs_{uid}_{idx + 1}.srt", cues)
    return fn


# ── reels (9:16) ─────────────────────────────────────────────
def _reels_vf(w: int | None, h: int | None) -> str:
    tw, th = REELS_W, REELS_H
    if w and h and h / w >= (th / tw) * 0.95:            # already (almost) portrait → just fit
        return (f"scale={tw}:{th}:force_original_aspect_ratio=decrease,"
                f"pad={tw}:{th}:(ow-iw)/2:(oh-ih)/2:black,setsar=1")
    bw, bh = max(2, tw // 4), max(2, th // 4)             # blur a tiny copy, then upscale: far cheaper
    return (f"split=2[bg][fg];"
            f"[bg]scale={bw}:{bh}:force_original_aspect_ratio=increase,crop={bw}:{bh},boxblur=8:2,scale={tw}:{th}[bgb];"
            f"[fg]scale={tw}:-2[fgs];"
            f"[bgb][fgs]overlay=(W-w)/2:(H-h)/2,setsar=1")


# ── silence removal ──────────────────────────────────────────
async def _detect_silences(file: str, noise_db: float, min_dur: float, dur: float,
                           timeout: float = 900) -> list[tuple[float, float]] | None:
    rc, _o, err = await _run_capture(
        ["ffmpeg", "-hide_banner", "-nostats", "-i", file, "-vn",
         "-af", f"silencedetect=noise={noise_db}dB:d={min_dur}", "-f", "null", "-"],
        timeout, want_stderr=True)
    if rc != 0:
        return None
    sil: list[tuple[float, float]] = []
    cur = None
    for line in err.decode(errors="ignore").splitlines():
        m = re.search(r"silence_start:\s*(-?[\d.]+)", line)
        if m:
            cur = max(0.0, float(m.group(1)))
            continue
        m = re.search(r"silence_end:\s*(-?[\d.]+)", line)
        if m and cur is not None:
            sil.append((cur, min(dur, float(m.group(1)))))
            cur = None
    if cur is not None:
        sil.append((cur, dur))
    return sil

def _keep_intervals(silences: list[tuple[float, float]], dur: float, pad: float = SILENCE_PAD,
                    min_keep: float = 0.25) -> list[tuple[float, float]]:
    keep, prev = [], 0.0
    for s, e in sorted(silences):
        if s - prev > 0.05:
            keep.append((prev, s))
        prev = max(prev, e)
    if dur - prev > 0.05:
        keep.append((prev, dur))
    padded = [(max(0.0, a - pad), min(dur, b + pad)) for a, b in keep]
    merged: list[list[float]] = []
    for a, b in padded:
        if merged and a <= merged[-1][1] + 0.02:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    return [(a, b) for a, b in merged if b - a >= min_keep]

def _silence_filters(keep: list[tuple[float, float]], fps: float | None) -> tuple[str, str]:
    expr = "+".join(f"between(t,{a:.3f},{b:.3f})" for a, b in keep)
    f = min(60.0, max(10.0, fps or 30.0))
    vf = f"fps={f:.3f},select='{expr}',setpts=N/FRAME_RATE/TB"
    af = f"aselect='{expr}',asetpts=N/SR/TB"
    return vf, af


# ── captions ─────────────────────────────────────────────────
def _build_caption(uid: int, base: str | None, num: int, total: int, start: float, dur: float | None,
                   size: int, ai: str | None = None) -> str:
    style = _S(uid).get("caption", "rich")
    if style == "off":
        return ""
    base = base or f"🎬 **Part {num} / {total}**"
    if style == "simple":
        return base
    bits = []
    if dur:
        bits.append(f"⏱ `{_hms(start)} → {_hms(start + dur)}`")
    if size:
        bits.append(f"📦 {_sz(size)}")
    cap = base + ("\n" + "  ·  ".join(bits) if bits else "")
    if ai:
        cap += f"\n_{ai}_"
    return cap[:1000]


async def _prep_part_meta(out: str, plan: dict, info: dict, opts: dict, uid: int, num: int, total: int) -> dict:
    """Everything the uploader needs, prepared while the PREVIOUS part is still uploading:
    real duration, width/height, thumbnail, optional AI caption."""
    st = _S(uid)
    meta: dict = {"size": os.path.getsize(out) if os.path.exists(out) else 0, "thumb": None, "ai": None}
    meta["duration"] = await get_duration(out)
    if opts.get("reels"):
        meta["width"], meta["height"] = REELS_W, REELS_H
    else:
        meta["width"], meta["height"] = info.get("width"), info.get("height")
    if st.get("thumbs"):
        tp = f"{THUMB_DIR}/thumb_{uid}_{num}.jpg"
        meta["thumb"] = await make_thumb(out, (meta["duration"] or 2.0) / 2, tp)
    if st.get("aicap") and AI_ENABLED:
        try:
            if meta["thumb"]:
                meta["ai"] = await _ai_caption_from_frame(meta["thumb"], num, total)
            if not meta["ai"]:
                meta["ai"] = await _ai_caption(os.path.basename(out), num, total)
        except Exception as e:
            log.debug("ai caption skipped: %s", e)
    return meta


# ══════════════════════════════════════════════════════════════
#  INLINE KEYBOARDS — button-based UX (tap instead of type)
#  V11: dynamic split buttons (labels computed from the real duration), result buttons,
#  settings keyboard, Mini App button.
# ══════════════════════════════════════════════════════════════
MINIAPP_ENABLED = bool(PUBLIC_URL) and WebAppInfo is not None and _env_bool("ENABLE_MINIAPP", True)

def _main_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✂️ Split & Scene", callback_data="menu:split"),
         InlineKeyboardButton("🤖 AI Highlights", callback_data="menu:ai")],
        [InlineKeyboardButton("🎬 Video Tools", callback_data="menu:tools"),
         InlineKeyboardButton("🔗 Merge", callback_data="menu:merge")],
        [InlineKeyboardButton("📊 Status / Info", callback_data="menu:status"),
         InlineKeyboardButton("📖 All Commands", callback_data="menu:help")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="set:open"),
         InlineKeyboardButton("⚙️ How it works", callback_data="menu:how")],
    ])

_CHUNK_CHOICES = [1, 2, 3, 5, 10, 15, 20, 30, 45, 60]

def _suggest_parts(dur: float | None) -> list[int]:
    if not dur or dur <= 0: return [2, 3, 5]
    if dur <= 180:   return [2, 3, 4]
    if dur <= 900:   return [2, 3, 5]
    if dur <= 3600:  return [3, 5, 8]
    return [4, 6, 10]

def _suggest_minutes(dur: float | None) -> list[tuple[int, int]]:
    """[(minutes_per_chunk, resulting_parts)] — up to two nice round chunk lengths."""
    if not dur or dur < 240:
        return []
    mins = dur / 60.0
    out: list[tuple[int, int]] = []
    for target in (4, 8):
        best = min(_CHUNK_CHOICES, key=lambda m: abs(mins / m - target))
        n = math.ceil(dur / (best * 60))
        if 2 <= n <= 40 and all(best != b for b, _ in out):
            out.append((best, n))
    return out

def _suggest_text(uid: int | None, dur: float | None) -> str:
    if not dur or dur < 20:
        return ""
    n = _suggest_parts(dur)[1]
    return _t(uid, "info_tip", txt=_t(uid, "sug_parts", n=n, each=_hms(dur / n)))

def _quick_split_kb(dur: float | None = None, uid: int | None = None) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for n in _suggest_parts(dur):
        label = f"✂️ {n} × {_hms(dur / n)}" if dur else f"✂️ {n} Parts"
        row.append(InlineKeyboardButton(label, callback_data=f"qs:{n}"))
    rows.append(row)
    mins = _suggest_minutes(dur)
    if mins:
        rows.append([InlineKeyboardButton(f"⏱ {m} min × {n}", callback_data=f"qm:{m}") for m, n in mins])
    rows.append([InlineKeyboardButton("🎬 Smart Scene", callback_data="qscene"),
                 InlineKeyboardButton("🔥 AI Highlights", callback_data="qhighlights")])
    rows.append([InlineKeyboardButton("📱 Reels 9:16", callback_data="qreels"),
                 InlineKeyboardButton("💬 Subtitles", callback_data="qsubs")])
    if MINIAPP_ENABLED:
        try:
            rows.append([InlineKeyboardButton(_t(uid, "btn_timeline"), web_app=WebAppInfo(url=f"{PUBLIC_URL}/ma"))])
        except TypeError:
            pass          # this Pyrogram build has no web_app support on buttons
    rows.append([InlineKeyboardButton("🛠 Tools", callback_data="menu:tools"),
                 InlineKeyboardButton("🤖 Describe", callback_data="qdesc")])
    rows.append([InlineKeyboardButton("🔗 Merge", callback_data="menu:merge"),
                 InlineKeyboardButton("⚙️ Settings", callback_data="set:open")])
    rows.append([InlineKeyboardButton("📊 Status", callback_data="menu:status"),
                 InlineKeyboardButton("📖 Help", callback_data="menu:help")])
    return InlineKeyboardMarkup(rows)

def _merge_kb(count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"✅ Done — Merge {count} Videos", callback_data="mdone")],
        [InlineKeyboardButton("❌ Cancel Merge", callback_data="mcancel")],
    ])

def _result_kb(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(_t(uid, "btn_again"), callback_data="qagain"),
         InlineKeyboardButton(_t(uid, "btn_hl"), callback_data="qhighlights")],
        [InlineKeyboardButton(_t(uid, "btn_reels"), callback_data="qreels"),
         InlineKeyboardButton(_t(uid, "btn_subs"), callback_data="qsubs")],
        [InlineKeyboardButton(_t(uid, "btn_clear"), callback_data="qclear"),
         InlineKeyboardButton(_t(uid, "btn_menu"), callback_data="menu:home")],
    ])

_SETTING_LABELS = {
    "cut": {"auto": "Auto", "fast": "⚡ Fast", "precise": "🎯 Precise"},
    "caption": {"rich": "Rich", "simple": "Simple", "off": "Off"},
    "lang": {"hinglish": "Hinglish", "english": "English"},
}

def _setting_text(key: str, val) -> str:
    if key in _SETTING_LABELS:
        return _SETTING_LABELS[key].get(val, str(val))
    return "ON" if val else "OFF"

def _settings_kb(uid: int) -> InlineKeyboardMarkup:
    s = _S(uid)
    def b(emoji, name, key):
        return InlineKeyboardButton(f"{emoji} {name}: {_setting_text(key, s[key])}", callback_data=f"set:{key}")
    return InlineKeyboardMarkup([
        [b("✂️", "Cut", "cut"), b("📝", "Caption", "caption")],
        [b("🖼", "Thumbs", "thumbs"), b("🤖", "AI caption", "aicap")],
        [b("💬", "Burn subs", "burn"), b("📱", "Reels", "reels")],
        [b("🌐", "Language", "lang")],
        [InlineKeyboardButton(_t(uid, "btn_menu"), callback_data="menu:home")],
    ])

def _lang_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🇮🇳 Hinglish", callback_data="lang:hinglish"),
                                  InlineKeyboardButton("🇬🇧 English", callback_data="lang:english")]])


# ══════════════════════════════════════════════════════════════
#  V11 — INFO CARD
# ══════════════════════════════════════════════════════════════
_VCODEC_NAMES = {"h264": "H.264", "hevc": "H.265", "vp9": "VP9", "av1": "AV1", "mpeg4": "MPEG-4"}
_ACODEC_NAMES = {"aac": "AAC", "mp3": "MP3", "opus": "Opus", "ac3": "AC3", "vorbis": "Vorbis"}

def _info_card(uid: int, path: str, info: dict, header: str = "info_title", elapsed: float | None = None) -> str:
    dur = info.get("duration")
    size = info.get("size") or (os.path.getsize(path) if os.path.exists(path) else 0)
    L = [_t(uid, header), _RULE, f"  📁 `{os.path.basename(path)}`"]
    vbits = []
    if info.get("width") and info.get("height"):
        vbits.append(f"{info['width']}×{info['height']}")
    if info.get("fps"):
        vbits.append(f"{info['fps']:.0f} fps")
    if info.get("vcodec"):
        vbits.append(_VCODEC_NAMES.get(info["vcodec"], str(info["vcodec"]).upper()))
    if vbits:
        L.append("  🎞 " + " · ".join(vbits) + (f" · 🔊 {_ACODEC_NAMES.get(info.get('acodec'), str(info.get('acodec')).upper())}" if info.get("acodec") else ""))
    tail = [f"⏱ {_hms(dur)}" if dur else "⏱ ?", f"📦 {_sz(size)}"]
    if info.get("bitrate"):
        tail.append(f"🎚 {info['bitrate'] / 1e6:.1f} Mbps")
    if elapsed is not None:
        tail.append(f"⬇️ {_eta(elapsed)}")
    L.append("  " + " · ".join(tail))
    tip = _suggest_text(uid, dur)
    if tip:
        L += [_RULE, tip]
    return "\n".join(L)


# ══════════════════════════════════════════════════════════════
#  UPLOAD PART
#  V11: dashboard-aware (no extra status message per part), real thumbnail + width/height/
#  duration so Telegram shows the right aspect ratio, speed/ETA from an EMA of chunk deltas.
# ══════════════════════════════════════════════════════════════
def _make_upload_cb(uid: int, dash, idx: int, status, t0: float):
    st = {"t": t0, "c": 0, "ema": None}

    async def cb(current, total_bytes):
        if _get_cancel(uid).is_set():
            try: app.stop_transmission()
            except Exception: pass
            raise _UserCancelled()
        if not total_bytes:
            return
        if dash is None:
            await upload_progress(current, total_bytes, status, t0, uid)
            return
        now = time.time()
        dt = now - st["t"]
        if dt >= 0.5:
            inst = (current - st["c"]) / dt
            st["ema"] = inst if st["ema"] is None else 0.35 * inst + 0.65 * st["ema"]
            st["t"], st["c"] = now, current
        speed = st["ema"] or 0.0
        eta = (total_bytes - current) / speed if speed > 0 else None
        dash.set_up(idx, current * 100.0 / total_bytes, speed, eta)
        await dash.push()
    return cb


def _rm(path: str | None) -> None:
    if path:
        try: os.remove(path)
        except OSError: pass


async def _upload_part(message, path: str, num: int, total: int, uid: int, caption: str | None,
                       dash=None, idx: int | None = None, meta: dict | None = None,
                       as_document: bool = False) -> bool:
    if _get_cancel(uid).is_set(): return False

    # FIX #21 — reject 0-byte or tiny part files before even trying to upload
    part_size = os.path.getsize(path) if os.path.exists(path) else 0
    if part_size < MIN_PART_BYTES:
        log.error(f"Part {num} is too small ({part_size} bytes) — skipping upload.")
        return False

    meta = meta or {}
    idx = (num - 1) if idx is None else idx
    status = None
    if dash is None:
        try:
            status = await message.reply(
                f"⬆️ **Upload** — part {num}/{total}\n━━━━━━━━━━━━━━━━━━━━━━\n[░░░░░░░░░░░░░░░░░░░░]\n  🌀 **0%** ·  starting…")
        except Exception as e:
            log.error(f"Could not send upload status for part {num}: {e}")
            return False

    _reset(uid)
    t0 = time.time()
    cb = _make_upload_cb(uid, dash, idx, status, t0)
    thumb = meta.get("thumb")
    if thumb and not os.path.exists(thumb):
        thumb = None

    async def note(text: str) -> None:
        if dash is not None:
            dash.note = text
            await dash.push(True)
        elif status is not None:
            await _safe_edit(status, text)

    # FIX #25 — distinguish "definitely sent" vs "safe to retry" network errors: retry a bounded
    # number of times on transient errors, give up at once on errors that mean TG already has it.
    MAX_UPLOAD_ATTEMPTS = 4
    NO_RETRY_MARKERS = ("FILE_PARTS_INVALID", "MEDIA_EMPTY", "FILE_ID_INVALID")
    uploaded = False
    for attempt in range(MAX_UPLOAD_ATTEMPTS):
        if _get_cancel(uid).is_set():
            break
        try:
            if as_document:
                sent = await message.reply_document(path, caption=caption or "", progress=cb)
            else:
                sent = await message.reply_video(
                    path, caption=caption or "", thumb=thumb, supports_streaming=True,
                    duration=int(meta.get("duration") or 0),
                    width=int(meta.get("width") or 0), height=int(meta.get("height") or 0),
                    progress=cb)
            if sent is None:
                if _get_cancel(uid).is_set():
                    break
                raise RuntimeError("Telegram transmission returned no message")
            uploaded = True
            _track_part_created()
            break

        except _UserCancelled:
            break

        except FloodWait as e:
            wait = e.value + 2            # safe to retry: Telegram rejected it before storing
            await note(f"⏳ Flood wait — part {num}/{total}, resuming in {wait}s")
            await asyncio.sleep(wait)

        except Exception as e:
            err_str = str(e)
            is_final = attempt == MAX_UPLOAD_ATTEMPTS - 1
            looks_fatal = any(marker in err_str for marker in NO_RETRY_MARKERS)
            if looks_fatal or is_final:
                log.error(f"Upload part {num} FAILED (attempt {attempt+1}/{MAX_UPLOAD_ATTEMPTS}): {e}")
                await note(f"❌ Upload failed part {num}: {err_str[:80]}")
                break
            backoff = min(20, (2 ** attempt) + random.uniform(0.25, 1.25))
            log.error(f"Upload part {num} attempt {attempt+1} failed, retrying in {backoff:.1f}s: {e}")
            await note(f"⚠️ Retry part {num}/{total} (attempt {attempt+2}/{MAX_UPLOAD_ATTEMPTS}): {err_str[:60]}")
            if dash is not None:
                dash.set_up(idx, 0.0, 0.0, None)
            await asyncio.sleep(backoff)

    if dash is not None and uploaded:
        dash.note = ""
    _reset(uid)
    _rm(thumb)
    if status is not None:
        try: await status.delete()
        except Exception: pass
    return uploaded


# ══════════════════════════════════════════════════════════════
#  CORE SPLIT ENGINE — V11 PIPELINE
#  A producer cuts part N+1 (and prepares its thumbnail / metadata / AI caption) WHILE part N is
#  uploading. Disk use is bounded: at most PIPELINE_AHEAD+1 part files exist at any time, and the
#  pipeline falls back to strictly sequential when the disk is too tight for look-ahead.
#  The same engine serves /split*, scenes, highlights, /trim, /reels, /splitat and the Mini App.
# ══════════════════════════════════════════════════════════════
_CANCELLED = object()

async def _q_get_or_cancel(q: asyncio.Queue, cancel: asyncio.Event):
    getter = asyncio.ensure_future(q.get())
    waiter = asyncio.ensure_future(cancel.wait())
    try:
        await asyncio.wait({getter, waiter}, return_when=asyncio.FIRST_COMPLETED)
        if getter.done():
            return getter.result()
        return _CANCELLED
    finally:
        for t in (getter, waiter):
            if not t.done():
                t.cancel()
        await asyncio.gather(getter, waiter, return_exceptions=True)


def _build_cut_opts(uid: int, kind: str, info: dict, transcript: list[dict] | None = None,
                    reels: bool | None = None, burn: bool | None = None, mode: str | None = None) -> dict:
    """Per-job cut options. For highlight clips the user's /settings (reels / burn) apply unless
    overridden; /reels passes reels=True explicitly."""
    st = _S(uid)
    opts: dict = {}
    if mode:
        opts["mode"] = mode
    if reels is None:
        reels = bool(st.get("reels")) and kind == "highlights"
    if burn is None:
        burn = bool(st.get("burn")) and kind == "highlights"
    if reels:
        opts["reels"] = True
        opts["vf"] = _reels_vf(info.get("width"), info.get("height"))
    if burn and transcript:
        opts["srt_fn"] = _make_srt_fn(uid, transcript)
        opts["burn"] = True
    return opts


async def _run_split_segments(message, uid: int, segments: list, label: str, caption_fn=None,
                              job_id: str | None = None, start_index: int = 0,
                              cut_opts: dict | None = None, extra_payload: dict | None = None,
                              keep_source: bool = False, kind: str = "split", dash=None) -> bool:
    """
    segments: list of (start_seconds, duration_seconds | None) — one per output part.
    caption_fn(part_num, total) -> str | None — custom caption per part, or None for the default.
    Returns True when every part was delivered.
    The caller clears the cancel flag when the JOB starts (so a /cancel during queueing sticks).
    """
    file = user_files.get(uid)
    if not file or not os.path.exists(file):
        await message.reply(_t(uid, "file_missing"))
        return False
    async with _job_slot(uid, message) as got:
        if not got:
            if job_id:
                await _job_finish(job_id, "cancelled")
            return False
        return await _split_pipeline(message, uid, file, segments, label, caption_fn, job_id,
                                     start_index, dict(cut_opts or {}), dict(extra_payload or {}),
                                     keep_source, kind, dash)


async def _split_pipeline(message, uid, file, segments, label, caption_fn, job_id, start_index,
                          opts, extra, keep_source, kind, dash=None) -> bool:
    cancel = _get_cancel(uid)
    st = _S(uid)
    parts = len(segments)
    start_index = max(0, min(start_index, parts))
    emoji = {"split": "✂️", "scene": "🎬", "highlights": "🔥", "trim": "✂️",
             "reels": "📱", "silence": "🔇"}.get(kind, "✂️")
    own_dash = dash is None
    if own_dash:
        dash = Dashboard(message, uid, label, ["Download", "Analyze", "Cut & Upload", "Done"],
                         total=parts, emoji=emoji)
        dash.step(0, "done")
        dash.step(1, "run")
    else:                                   # caller (e.g. /highlights) already runs a card with earlier stages
        dash.parts = ["todo"] * parts
    i_cut, i_done = len(dash.steps) - 2, len(dash.steps) - 1
    for i in range(start_index):
        dash.parts[i] = "done"
    if own_dash:
        await dash.start()
    t_start = time.time()
    _set_status(uid, "Splitting", label[:40])

    base_payload = {"stage": "split", "segments": [list(s) for s in segments], "label": label, "kind": kind}
    base_payload.update(extra)

    def ck(next_index: int) -> dict:
        d = dict(base_payload)
        d["next_index"] = next_index
        return d

    uploaded_count = start_index
    sent_bytes = 0
    cancelled = False
    fail_reason: str | None = None
    prod = None
    q: asyncio.Queue = asyncio.Queue()
    stop = asyncio.Event()
    try:
        info = await _probe_video(file)
        dur = info.get("duration") or await get_duration(file) or 0.0
        if dur <= 0:
            dur = max((ss + (d or 0.0)) for ss, d in segments) or 1.0
        mode = opts.get("mode") or st.get("cut", "auto")
        plans = await _plan_cuts(file, segments, mode, opts, dur, base=float(info.get("start_time") or 0.0))
        n_enc = sum(1 for p in plans if p["enc"])
        key = "d_precise" if n_enc == parts else ("d_lossless" if n_enc == 0 else "d_mixed")
        dash.mode_line = _t(uid, key, n=n_enc)
        if own_dash:
            dash.step(1, "done")
        dash.step(i_cut, "run")
        await dash.push(True)

        src_size = info.get("size") or os.path.getsize(file)

        def est_bytes(p: dict) -> float:
            d = p["dur"] if p["dur"] is not None else max(0.0, dur - p["ss"])
            return src_size * d / dur if dur else float(src_size)

        biggest = max((est_bytes(p) for p in plans[start_index:]), default=0.0)
        ahead = PIPELINE_AHEAD
        try:
            free = shutil.disk_usage(DOWNLOAD_DIR).free
        except Exception:
            free = None
        if free is not None and free < biggest * (ahead + 1) + DISK_SAFETY_BYTES:
            ahead = 0                                  # not enough disk for look-ahead → sequential
        slots = asyncio.Semaphore(ahead + 1)

        async def producer():
            i = start_index
            try:
                for i in range(start_index, parts):
                    await slots.acquire()
                    if stop.is_set() or cancel.is_set():
                        slots.release()
                        return
                    plan = plans[i]
                    out = f"{DOWNLOAD_DIR}/part_{uid}_{i+1}.mp4"
                    dash.part(i, "cut")
                    await dash.push()

                    async def on_prog(pct, _speed, _i=i):
                        dash.set_cut(pct)
                        await dash.push()

                    ok = await _cut_part(file, out, plan, opts, uid, i, dur, est_bytes(plan), on_prog)
                    if not ok:
                        slots.release()
                        q.put_nowait(("err", i, out, f"ffmpeg failed on part {i+1}"))
                        return
                    meta = await _prep_part_meta(out, plan, info, opts, uid, i + 1, parts)
                    q.put_nowait(("part", i, out, meta))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error("pipeline producer error: %s", traceback.format_exc())
                q.put_nowait(("err", i, None, str(e)[:100]))
            finally:
                q.put_nowait(None)

        prod = asyncio.ensure_future(producer())
        while True:
            item = await _q_get_or_cancel(q, cancel)
            if item is _CANCELLED:
                cancelled = True
                break
            if item is None:
                break
            tag, i, out, meta = item
            if tag == "err":
                fail_reason = str(meta)
                dash.part(i, "fail")
                break
            if cancel.is_set():
                cancelled = True
                _rm(out)
                _rm(meta.get("thumb"))
                slots.release()
                break
            dash.part(i, "up")
            await dash.push(True)
            _set_status(uid, "Uploading", f"part {i+1}/{parts}")
            plan = plans[i]
            base = caption_fn(i + 1, parts) if caption_fn else None
            caption = _build_caption(uid, base, i + 1, parts, plan["ss"], meta.get("duration"),
                                     meta.get("size", 0), meta.get("ai"))
            ok = await _upload_part(message, out, i + 1, parts, uid, caption, dash=dash, idx=i, meta=meta)
            _rm(out)
            slots.release()
            if not ok:
                if cancel.is_set():
                    cancelled = True
                    dash.part(i, "skip")
                else:
                    fail_reason = "upload failed"
                    dash.part(i, "fail")
                break
            uploaded_count += 1
            sent_bytes += meta.get("size", 0)
            dash.part(i, "done")
            if job_id:
                try: await _job_update(job_id, payload=ck(i + 1))
                except Exception as e: log.warning("checkpoint failed: %s", e)
            await dash.push(True)
    except Exception as e:
        log.error(f"_split_pipeline error uid={uid}: {traceback.format_exc()}")
        fail_reason = str(e)[:100] or e.__class__.__name__
    finally:
        stop.set()
        if prod is not None:
            prod.cancel()
            await asyncio.gather(prod, return_exceptions=True)
        while not q.empty():                           # leftovers the consumer never took
            it = q.get_nowait()
            if it and it[0] == "part":
                _rm(it[2]); _rm((it[3] or {}).get("thumb"))
        for f in glob.glob(f"{DOWNLOAD_DIR}/part_{uid}_*.mp4") + glob.glob(f"{THUMB_DIR}/thumb_{uid}_*.jpg"):
            _rm(f)
        for f in glob.glob(f"{DOWNLOAD_DIR}/subs_{uid}_*.srt"):
            _rm(f)
        _clear_status(uid)

    # ── outcome ──────────────────────────────────────────────
    for j in range(parts):
        if dash.parts[j] in ("todo", "cut", "up"):
            dash.parts[j] = "skip" if (cancelled or fail_reason) else dash.parts[j]
    dash.cut_idx = dash.cut_pct = None
    dash.up = None
    dash.note = ""
    if (cancelled or fail_reason) and dash.steps[i_cut][1] == "run":
        dash.step(i_cut, "fail")
    if cancelled:
        if job_id: await _job_finish(job_id, "cancelled")
        await dash.finish(_t(uid, "cancelled", done=uploaded_count, n=parts), kb=_result_kb(uid))
        _touch(uid)
        return False
    if fail_reason:
        if job_id:
            try: await _job_update(job_id, state="retry", payload=ck(uploaded_count))
            except Exception: pass
        await dash.finish(_t(uid, "stopped", done=uploaded_count, n=parts, why=fail_reason))
        return False

    elapsed = time.time() - t_start
    delivered = max(1, uploaded_count - start_index)
    dash.step(i_cut, "done")
    dash.step(i_done, "done")
    await dash.finish(
        _t(uid, "done_all", n=parts) + "\n" +
        _t(uid, "done_stats", total=_eta(elapsed), avg=_eta(elapsed / delivered), size=_sz(sent_bytes)),
        kb=_result_kb(uid))
    if job_id:
        await _job_finish(job_id, "done")
    if keep_source or SOURCE_TTL_MIN > 0:
        _touch(uid)
    else:
        _forget_source(uid)
    log.info(f"Split done uid={uid} kind={kind} parts={parts} in {elapsed:.0f}s")
    return True


async def _do_split(message, uid: int, parts: int, seg_override: float | None = None,
                    label: str = "", segments: list | None = None, kind: str = "split",
                    cut_opts: dict | None = None, extra_payload: dict | None = None,
                    caption_fn=None, keep_source: bool = False, quota: bool = True) -> bool:
    """
    seg_override: if given, use as segment length (splitmin/splitsize).
                  If None, compute as dur/parts (split N).
    segments:     explicit [(start, duration)] list (used by /splitat and the Mini App).
    """
    # Re-validate + re-fetch duration INSIDE the lock (FIX #20)
    file = user_files.get(uid)
    if not file or not os.path.exists(file):
        await message.reply(_t(uid, "file_missing"))
        return False
    dur = await get_duration(file)
    if not dur:
        await message.reply("❌ Video duration nahi mila (inside lock).")
        return False
    if segments is None:
        seg = seg_override if seg_override is not None else dur / parts
        segments = [(i * seg, seg) for i in range(parts)]
    if quota:
        bill = sum(max(0.0, (d if d is not None else dur - ss)) for ss, d in segments)
        if not await _quota_gate(message, uid, min(dur, bill) if bill else dur):
            return False
    _get_cancel(uid).clear()
    _track_video_processed()
    src = user_sources.get(uid) or await _source_for(uid)
    job_id = None
    if src:
        payload = {"stage": "split", "segments": [list(s) for s in segments], "next_index": 0,
                   "label": label, "kind": kind}
        payload.update(extra_payload or {})
        job_id = await _job_create(uid, message.chat.id, src["source_message_id"], kind, payload, file)
    return await _run_split_segments(message, uid, segments, label, caption_fn=caption_fn, job_id=job_id,
                                     cut_opts=cut_opts, extra_payload=extra_payload,
                                     keep_source=keep_source, kind=kind)



# ══════════════════════════════════════════════════════════════
#  COMMAND LIST — defined before receive handler (FIX #3)
# ══════════════════════════════════════════════════════════════
COMMAND_LIST = [
    "start", "help", "split", "splitmin", "splitsize",
    "info",  "status", "cancel", "clear", "aistatus",
    "trim", "extractaudio", "compress", "splitscene", "highlights", "describe",
    "mergestart", "mergedone", "mergecancel",
    "stats", "broadcast", "retry", "resume",
    "splitat", "silence", "subs", "reels", "settings", "lang", "queue", "quota",
    "allow", "deny", "ban", "unban",
]


# ══════════════════════════════════════════════════════════════
#  V10/V11 RECOVERY COMMANDS
#  V11: re-runs every segment-based job kind (split / scene / highlights / trim / reels / subs) with its
#  saved cut options + captions, runs recoveries through the global queue, and gives up after
#  RECOVERY_MAX_ATTEMPTS boots instead of retrying a poisoned job forever.
# ══════════════════════════════════════════════════════════════
_RESUMABLE_KINDS = {"split", "scene", "highlights", "trim", "reels", "subs"}

async def _resume_job_record(job, message=None):
    uid = job["uid"]
    kind = job["kind"]
    if kind not in _RESUMABLE_KINDS:
        return False, "unsupported"
    lock = _get_lock(uid)
    if lock.locked(): return False, "Already processing"
    async with lock:
        _get_cancel(uid).clear()
        try:
            src_msg = await app.get_messages(job["chat_id"], job["source_message_id"])
        except Exception as e:
            log.warning("recovery: get_messages failed: %s", e)
            src_msg = None
        if not src_msg or not (src_msg.video or src_msg.document):
            await _job_update(job["job_id"], state="retry")
            return False, "Original Telegram message is unavailable"
        path = job.get("source_path")
        if not path or not os.path.exists(path):
            media = src_msg.video or src_msg.document
            path = f"{DOWNLOAD_DIR}/recovery_{uid}_{job['job_id']}.{_ext_for(media)}"
            st = await (message or src_msg).reply("♻️ **Recovering original video from Telegram…**")
            try:
                _reset(uid)
                path = await _await_with_watchdog(
                    src_msg.download(file_name=path, progress=progress,
                                     progress_args=(st, time.time(), uid, "♻️ Recover")), uid)
                await _safe_edit(st, "✅ Original recovered. Resuming job…")
            except Exception as e:
                await _job_update(job["job_id"], state="retry", attempts_inc=True)
                await _safe_edit(st, f"⚠️ Recovery retry needed: `{str(e)[:120]}`")
                return False, str(e)
            await _job_update(job["job_id"], source_path=path)
        user_files[uid] = path
        user_sources[uid] = {"chat_id": job["chat_id"], "source_message_id": job["source_message_id"], "source_path": path}
        _touch(uid)
        payload = job["payload"]
        stage = payload.get("stage")
        async with _job_slot(uid, src_msg) as got:
            if not got:
                await _job_update(job["job_id"], state="retry")
                return False, "cancelled while queued"
            segs = [tuple(x) for x in payload.get("segments", [])]
            if kind == "scene" and stage == "scan":
                dur = await get_duration(path)
                cuts = await detect_scenes(path, uid, await src_msg.reply("🔍 **Resuming scene scan…**"), dur)
                segs = _scene_segments_from_cuts(cuts, dur)
                payload.update({"stage": "split", "cuts": cuts, "segments": [list(s) for s in segs], "next_index": 0})
                await _job_update(job["job_id"], payload=payload)
            if kind == "highlights" and stage in {"transcribe", "select", "vision"}:
                dur = await get_duration(path)
                st = await src_msg.reply("♻️ **Resuming AI highlight analysis…**")
                if stage == "transcribe" or not payload.get("segments"):
                    tx = await _transcribe_video(path, dur, uid, st)
                    payload["segments"] = tx
                    payload["stage"] = stage = "select"
                    await _job_update(job["job_id"], payload=payload)
                if stage == "select":
                    hs = await _select_highlights(payload.get("segments", []), dur)
                    payload["highlights"] = hs
                    payload["stage"] = stage = "vision" if HIGHLIGHT_USE_VISION else "split"
                    await _job_update(job["job_id"], payload=payload)
                if stage == "vision" and HIGHLIGHT_USE_VISION:
                    hs = await _vision_rank_highlights(path, uid, payload.get("highlights", []))
                    payload["highlights"] = hs
                    payload["stage"] = "split"
                    await _job_update(job["job_id"], payload=payload)
                segs = [(h["start"], h["end"] - h["start"]) for h in payload.get("highlights", [])]
                payload["segments"] = [list(s) for s in segs]
                payload["next_index"] = 0
                await _job_update(job["job_id"], payload=payload)
            if not segs:
                await _job_finish(job["job_id"], "failed")
                return False, "nothing left to cut"
            label = payload.get("label", f"Recovered {kind} job…")
            info = await _probe_video(path)
            o = payload.get("opts") or {}
            opts = _build_cut_opts(uid, kind, info, transcript=payload.get("tx"),
                                   reels=bool(o.get("reels")), burn=bool(o.get("burn")), mode=o.get("mode"))
            extra = {k: payload[k] for k in ("cuts", "highlights", "tx", "opts") if k in payload}
            await _run_split_segments(src_msg, uid, segs, label, caption_fn=_caption_fn_for(uid, kind, payload),
                                      job_id=job["job_id"], start_index=int(payload.get("next_index", 0)),
                                      cut_opts=opts, extra_payload=extra, kind=kind,
                                      keep_source=kind in {"trim", "reels", "subs"})
            return True, "resumed"

@app.on_message(filters.command("retry"), group=1)
async def cmd_retry(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    jobs = [j for j in await _job_get_active(include_failed=True) if j["uid"] == uid and j["kind"] in _RESUMABLE_KINDS]
    if not jobs: return await message.reply("ℹ️ No recoverable job found.")
    ok, why = await _resume_job_record(jobs[-1], message)
    if not ok: await message.reply(f"⚠️ Recovery could not start: `{why}`")

@app.on_message(filters.command("resume"), group=1)
async def cmd_resume(client, message):
    return await cmd_retry(client, message)

# ══════════════════════════════════════════════════════════════
#  COMMANDS
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("start"), group=1)
async def cmd_start(client, message):
    if await _dedup(message): return
    name = getattr(message.from_user, "first_name", "User") or "User"
    await message.reply(
        f"⚡ **VIDEO SPLIT PRO v11**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👋 Welcome, **{name}**!\n\n"
        f"🎥 **Step 1 — Send your video**\n"
        f"📥 Wait for the info card\n"
        f"🎛 **Step 2 — Tap an action**\n\n"
        f"✨ Lossless smart cuts • AI highlights • Reels 9:16 • Subtitles\n"
        f"🚄 Cut ⇄ upload pipeline • Live dashboard • `/cancel` anytime\n\n"
        f"👇 **Choose what you want to do:**",
        reply_markup=_main_menu_kb(),
    )

@app.on_message(filters.command("help"), group=1)
async def cmd_help(client, message):
    if await _dedup(message): return
    await message.reply(
        "📖 **VIDEO SPLIT PRO v11 — COMMAND CENTER**\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "🎥 **1. Start**\n"
        "Send a video → wait for the info card → tap a button.\n\n"
        "✂️ **2. Split**\n"
        "`/split 3` → 3 equal parts\n"
        "`/splitmin 5` → every 5 minutes\n"
        "`/splitsize 500` → ~500 MB chunks\n"
        "`/splitat 1:20 3:45` → cut at your timestamps\n"
        "`/splitscene` → smart scene-based clips\n\n"
        "🤖 **3. AI**\n"
        "`/highlights` → transcript + AI ranking + vision check\n"
        "`/subs` → SRT subtitles (`/subs burn` = burned-in)\n"
        "`/describe` · `/aistatus`\n\n"
        "🎬 **4. Video Tools**\n"
        "`/trim 1:00 3:30` → cut a time range\n"
        "`/reels` · `/reels 0:30 1:20 subs` → 9:16 export\n"
        "`/silence` → remove dead air\n"
        "`/compress medium` · `/extractaudio`\n\n"
        "🔗 **5. Merge**\n"
        "`/mergestart` → `/mergedone` · `/mergecancel`\n\n"
        "⚙️ **6. Control**\n"
        "`/settings` · `/lang` · `/queue` · `/quota`\n"
        "`/info` · `/status` · `/cancel` · `/clear` · `/retry`\n\n"
        "📊 **Admin**\n"
        "`/stats` · `/broadcast text`\n"
        "`/allow` `/deny` `/ban` `/unban` + user id\n\n"
        "💡 Buttons are the easiest way — tap **📖 All Commands** or send `/start`.",
        reply_markup=_main_menu_kb(),
    )

@app.on_message(filters.command("info"), group=1)
async def cmd_info(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    if not await _require_ready(message, uid):
        return
    path = user_files[uid]
    info = await _probe_video(path)
    await message.reply(_info_card(uid, path, info) + "\n\n" + _t(uid, "info_tap"),
                        reply_markup=_quick_split_kb(info.get("duration"), uid))

@app.on_message(filters.command("status"), group=1)
async def cmd_status(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    info = user_status.get(uid)
    if not _get_lock(uid).locked() or not info:
        path = user_files.get(uid)
        if path and os.path.exists(path):
            return await message.reply(
                f"💤 **Idle — Ready**\n"
                f"  📁 `{os.path.basename(path)}`\n"
                f"  📦 `{_sz(os.path.getsize(path))}`\n"
                f"  👉 `/split N` · `/splitmin N`"
            )
        return await message.reply("💤 **Idle** — send a video!")
    elapsed = _eta(time.time() - info["since"])
    await message.reply(
        f"⚙️ **Running…**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  📌 **Task** : `{info['task']}`\n"
        f"  📝 **Detail** : `{info['detail']}`\n"
        f"  ⏳ **Time** : `{elapsed}`\n\n"
        f"  ❌ /cancel to stop"
    )

@app.on_message(filters.command("aistatus"), group=1)
async def cmd_aistatus(client, message):
    if await _dedup(message): return
    if AI_ENABLED:
        await message.reply(
            f"🤖 **AI Captions: ON**\n"
            f"  Model: `{GROQ_MODEL}`\n"
            f"  Each part's thumbnail is analyzed → real content-aware caption.\n"
            f"  Falls back silently if AI is slow/unavailable."
        )
    else:
        reason = "GROQ_API_KEY not set" if not GROQ_API_KEY else "`groq` package not installed"
        await message.reply(
            f"🤖 **AI Captions: OFF**\n"
            f"  Reason: {reason}\n"
            f"  Set `GROQ_API_KEY` env var (and add `groq` to requirements.txt) to enable."
        )

@app.on_message(filters.command("cancel"), group=1)
async def cmd_cancel(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    if not _get_lock(uid).locked():
        return await message.reply("💤 Koi task nahi chal raha.")
    _get_cancel(uid).set()
    await message.reply("🚫 **Cancel requested!**\nStopping at next checkpoint…")

@app.on_message(filters.command("clear"), group=1)
async def cmd_clear(client, message):
    """Reset stuck user state without bot restart."""
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    if _get_lock(uid).locked():
        return await message.reply(
            "⚠️ Task abhi chal raha hai.\n"
            "Pehle `/cancel` karo, phir `/clear`."
        )
    await _clear_user(uid)
    await message.reply(
        "🗑️ **State cleared!**\n"
        "Sab kuch reset ho gaya.\n"
        "Naya video bhejo 👇"
    )


async def _clear_user(uid: int) -> None:
    _forget_source(uid)
    queue = user_merge_queue.pop(uid, None)
    if queue:
        for p in queue:
            _rm(p)
    _get_cancel(uid).clear()
    _clear_status(uid)
    user_locks.pop(uid, None)


# ══════════════════════════════════════════════════════════════
#  LARGE-FILE PREFLIGHT
# ══════════════════════════════════════════════════════════════
def _preflight_large_file(file_size: int) -> tuple[bool, str]:
    """Fail early instead of letting a nearly-full filesystem look stuck."""
    if file_size and file_size > MAX_INPUT_BYTES:
        return False, f"File too large: `{_sz(file_size)}`. Limit is about `{_sz(MAX_INPUT_BYTES)}`."
    try:
        free = shutil.disk_usage(DOWNLOAD_DIR).free
    except Exception:
        return True, ""
    if file_size and free < file_size + DISK_SAFETY_BYTES:
        return False, (
            f"Server storage low. Need about `{_sz(file_size + DISK_SAFETY_BYTES)}` free, "
            f"but only `{_sz(free)}` is available."
        )
    return True, ""


# ══════════════════════════════════════════════════════════════
#  RECEIVE VIDEO
#  group=-1 ensures this runs BEFORE command handlers (group=1)
#  ~filters.command(COMMAND_LIST) prevents overlap with commands
# ══════════════════════════════════════════════════════════════
@app.on_message(
    filters.incoming & (filters.video | filters.document)
    & ~filters.command(COMMAND_LIST),
    group=-1,
)
async def receive(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return

    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply("⏳ Task running.\n👉 /status · /cancel")

    media = message.document or message.video
    if not media: return

    mime      = getattr(media, "mime_type", "") or ""
    file_size = getattr(media, "file_size", 0) or 0

    ok_preflight, preflight_reason = _preflight_large_file(file_size)
    if not ok_preflight:
        return await message.reply(
            f"❌ **Cannot start safely**\n{preflight_reason}\n\n"
            "Try a smaller file or free server storage first."
        )

    # Filter: only accept video-like MIME types
    if mime and not (mime.startswith("video/") or mime == "application/octet-stream"):
        return

    ext_map = {
        "video/x-matroska": "mkv", "video/mkv": "mkv",
        "video/avi":         "avi", "video/x-msvideo": "avi",
        "video/webm":       "webm", "video/quicktime": "mov",
        "video/x-ms-wmv":   "wmv", "video/3gpp": "3gp",
    }
    ext    = ext_map.get(mime, "mp4")
    sz_str = _sz(file_size) if file_size else "?"

    # ── MERGE-MODE branch: user ran /mergestart — collect instead of replacing ──
    if uid in user_merge_queue:
        if lock.locked():
            return  # race guard, same pattern as the normal flow below
        if len(user_merge_queue[uid]) >= MAX_MERGE_VIDEOS:
            return await message.reply(
                f"⚠️ Merge queue full ({MAX_MERGE_VIDEOS} max).\n👉 `/mergedone` ya `/mergecancel`"
            )
        async with lock:
            idx = len(user_merge_queue[uid]) + 1
            fname_m = f"{DOWNLOAD_DIR}/merge_{uid}_{idx}_{message.id}.{ext}"
            status = await message.reply(f"📥 **Downloading video #{idx} for merge…** {sz_str}")
            _reset(uid)  # fresh progress baseline — don't inherit stale speed/timing from a prior op
            t0 = time.time()
            try:
                path = await _await_with_watchdog(
                    message.download(
                        file_name=fname_m,
                        progress=progress,
                        progress_args=(status, t0, uid, f"📥 Merge #{idx}"),
                    ),
                    uid,
                )
            except _UserCancelled:
                try:
                    if os.path.exists(fname_m): os.remove(fname_m)
                except: pass
                await _safe_edit(status, "🚫 **Download cancelled.**")
                return
            except Exception as e:
                try:
                    if os.path.exists(fname_m): os.remove(fname_m)
                except: pass
                await _safe_edit(status, f"❌ Download failed: `{e}`")
                return
            if not path or not os.path.exists(path):
                await _safe_edit(status, "❌ File not saved — try again.")
                return
            user_merge_queue[uid].append(path)
            _track_video_processed()
            listing = "\n".join(
                f"  ✅ Video {i+1} — {_sz(os.path.getsize(p))}"
                for i, p in enumerate(user_merge_queue[uid]) if os.path.exists(p)
            )
            await _safe_edit(status,
                f"🔗 **Merge Queue ({idx} video{'s' if idx != 1 else ''})**\n"
                f"{listing}\n\n"
                f"👉 Aur bhejo, ya neeche tap karo:",
                reply_markup=_merge_kb(idx),
            )
        return

    fname  = f"{DOWNLOAD_DIR}/video_{uid}_{message.id}.{ext}"

    size_warn = ""
    if file_size and file_size > MAX_FILE_WARN:
        size_warn = f"\n  ⚠️ File `{sz_str}` — near Telegram 2 GB limit!"

    # FIX double message: acquire lock BEFORE sending status reply
    # so only one task ever starts the download for this user
    if lock.locked():
        return  # silently drop — first task already handling it
    async with lock:
        status = await message.reply(
            f"📥 **Downloading…** `{ext.upper()}` · {sz_str}{size_warn}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏳ Starting…"
        )

        _forget_source(uid)

        _get_cancel(uid).clear()
        _reset(uid)
        _set_status(uid, "Downloading", sz_str)
        t0 = time.time()

        try:
            path = await _await_with_watchdog(
                message.download(
                    file_name=fname,
                    progress=progress,
                    progress_args=(status, t0, uid, "📥 Download"),
                ),
                uid,
            )
        except _UserCancelled:
            _clear_status(uid)
            try:
                if os.path.exists(fname): os.remove(fname)
            except: pass
            await _safe_edit(status, "🚫 **Download cancelled.**")
            return
        except Exception as e:
            _clear_status(uid)
            await _safe_edit(status, f"❌ Download failed: `{e}`")
            return

        if not path or not os.path.exists(path):
            _clear_status(uid)
            await _safe_edit(status, "❌ File not saved — try again.")
            return

        _reset(uid)
        _clear_status(uid)
        user_files[uid] = path
        user_sources[uid] = {"chat_id": message.chat.id, "source_message_id": message.id, "source_path": path}
        # Persistent source record lets recovery re-download the original from Telegram after a Render restart.
        try:
            old = await _source_for(uid)
            if old: await _job_delete(old["job_id"])
            await _job_create(uid, message.chat.id, message.id, "source", {"stage":"ready"}, path, job_id=uuid.uuid4().hex[:16], state="ready")
        except Exception as e:
            log.warning("Could not persist source checkpoint: %s", e)
        _track_video_processed()
        _touch(uid)
        info = await _probe_video(path)
        await _safe_edit(status,
            _info_card(uid, path, info, header="dl_done", elapsed=time.time() - t0) + "\n\n" + _t(uid, "info_tap"),
            reply_markup=_quick_split_kb(info.get("duration"), uid),
        )


# ══════════════════════════════════════════════════════════════
#  SPLIT COMMANDS
#  FIX #20 — duration re-fetched inside _do_split (inside lock)
#            so the seg value is always consistent with the file
#  V11 — every entry point goes through _begin() (busy check + automatic source re-download),
#        quota gate, global job slot, and the live dashboard.
# ══════════════════════════════════════════════════════════════
def _is_ready(uid: int) -> bool:
    path = user_files.get(uid)
    return bool(path and os.path.exists(path))


async def _begin(message, uid: int) -> bool:
    """Common preamble: user not busy + source video available (re-downloaded if it was cleaned up)."""
    if _get_lock(uid).locked():
        await message.reply(_t(uid, "busy"))
        return False
    return await _require_ready(message, uid)


def _caption_fn_for(uid: int, kind: str, payload: dict | None = None):
    """Caption factory shared by fresh runs and recovered jobs."""
    payload = payload or {}
    if kind == "scene":
        return lambda i, t: f"🎬 **Scene {i} / {t}**"
    if kind == "highlights":
        reasons = [str(h.get("reason", "")).strip() for h in payload.get("highlights", [])]
        def fn(i, t):
            base = f"🔥 **Highlight {i} / {t}**"
            r = reasons[i - 1] if 0 < i <= len(reasons) else ""
            return f"{base}\n_{r}_" if r else base
        return fn
    if kind == "trim":
        return lambda i, t: _t(uid, "trim_cap")
    if kind == "reels":
        return lambda i, t: _t(uid, "reels_cap")
    if kind == "subs":
        return lambda i, t: _t(uid, "subs_burn_cap")
    return None


def _relevant_tx(tx: list[dict], segs: list) -> list[dict]:
    """Transcript pieces that overlap at least one clip (keeps the job payload small)."""
    out = []
    for sg in tx:
        a, b = float(sg["start"]), float(sg["end"])
        if any(b > s and a < s + d for s, d in segs):
            out.append({"start": round(a, 2), "end": round(b, 2), "text": sg["text"]})
    return out


def _cached_transcript(uid: int, file: str) -> list[dict] | None:
    c = user_transcripts.get(uid)
    return c["segments"] if c and c.get("path") == file and c.get("segments") else None


async def _quick_split(message, uid: int, parts: int) -> None:
    if not await _begin(message, uid):
        return
    async with _get_lock(uid):
        if not _is_ready(uid):
            await message.reply(_t(uid, "file_missing"))
            return
        await _do_split(message, uid, parts, seg_override=None, label=f"Splitting into {parts} equal parts…")


async def _quick_splitmin(message, uid: int, mins: int) -> None:
    if not await _begin(message, uid):
        return
    # Quick pre-check (outside lock) — full check happens inside _do_split
    dur_pre = await get_duration(user_files[uid])
    if not dur_pre:
        await message.reply("❌ Video duration nahi mila.")
        return
    seg = mins * 60
    parts = math.ceil(dur_pre / seg)
    if parts > 100:
        await message.reply(f"❌ Too many parts ({parts}). Bada chunk lo.")
        return
    if parts < 2:
        await message.reply(f"❌ Video {mins} min se chhota hai!")
        return
    async with _get_lock(uid):
        if not _is_ready(uid):
            await message.reply(_t(uid, "file_missing"))
            return
        await _do_split(message, uid, parts, seg_override=seg, label=f"{mins} min chunks → {parts} parts…")


def _scene_segments_from_cuts(cuts: list[float], duration: float) -> list[tuple[float, float]]:
    """Turn scene boundaries into sane segments and eliminate edge fragments."""
    if duration <= 0:
        return []
    min_len = max(2.5, SCENE_MIN_GAP_SEC)
    clean = sorted({round(float(c), 2) for c in cuts if 0.0 < float(c) < duration})

    # A cut very close to the beginning/end creates a useless tiny clip.
    clean = [c for c in clean if c >= min_len * 0.75 and duration - c >= min_len * 0.75]

    # Re-run spacing after edge cleanup.
    spaced: list[float] = []
    for c in clean:
        if not spaced or c - spaced[-1] >= min_len:
            spaced.append(c)

    bounds = [0.0] + spaced + [duration]
    segments: list[tuple[float, float]] = []
    for a, b in zip(bounds, bounds[1:]):
        d = b - a
        if d < min_len * 0.75 and segments:
            # Merge a tiny tail into the previous scene.
            prev_start, prev_dur = segments[-1]
            segments[-1] = (prev_start, b - prev_start)
        elif d >= min_len * 0.75:
            segments.append((a, d))

    # Safety fallback: never return an empty result for a valid video.
    if not segments and duration > 0:
        return [(0.0, duration)]
    return segments


async def _quick_splitscene(message, uid: int) -> None:
    if not await _begin(message, uid):
        return
    async with _get_lock(uid):
        if not _is_ready(uid):
            await message.reply(_t(uid, "file_missing"))
            return
        file = user_files[uid]
        dur = await get_duration(file)
        if not dur:
            await message.reply("❌ Video duration nahi mila.")
            return
        if not await _quota_gate(message, uid, dur):
            return
        _get_cancel(uid).clear()
        async with _job_slot(uid, message) as got:
            if not got:
                return
            src = user_sources.get(uid) or await _source_for(uid)
            job_id = await _job_create(uid, message.chat.id, src["source_message_id"], "scene",
                                       {"stage": "scan", "cuts": [], "segments": [], "next_index": 0, "kind": "scene"}, file) if src else None
            scan_msg = await message.reply("🔍 **Scanning for scene changes…**")
            cuts = await detect_scenes(file, uid, scan_msg, dur)
            if job_id:
                await _job_update(job_id, payload={"stage": "segments", "cuts": cuts, "segments": [], "next_index": 0, "kind": "scene"})
            if _get_cancel(uid).is_set():
                await _safe_edit(scan_msg, "🚫 Cancelled.")
                if job_id: await _job_finish(job_id, "cancelled")
                return
            if not cuts:
                await _safe_edit(scan_msg, "❌ Koi clear scene change nahi mila.\n👉 `/split N` try karo instead.")
                if job_id: await _job_finish(job_id, "failed")
                return
            segments = _scene_segments_from_cuts(cuts, dur)
            if len(segments) < 2:
                await _safe_edit(scan_msg, "❌ Sirf 1 scene mila — poora video ek jaisa hai.\n👉 `/split N` try karo.")
                if job_id: await _job_finish(job_id, "failed")
                return
            # Absolute safety cap; normal duration-aware detection stays well below this.
            if len(segments) > SCENE_MAX_COUNT:
                segments = segments[:SCENE_MAX_COUNT]
            label = f"{len(segments)} scenes — smart scene split…"
            await _safe_edit(scan_msg, f"🎬 **{len(segments)} scenes mile!** Splitting shuru ho raha hai…")
            _track_video_processed()
            extra = {"cuts": cuts}
            if job_id:
                await _job_update(job_id, payload={"stage": "split", "cuts": cuts, "segments": [list(s) for s in segments],
                                                   "next_index": 0, "label": label, "kind": "scene"})
            ok = await _run_split_segments(message, uid, segments, label,
                                           caption_fn=_caption_fn_for(uid, "scene"), job_id=job_id,
                                           extra_payload=extra, kind="scene")
            if ok:
                try: await scan_msg.delete()
                except Exception: pass


async def _quick_highlights(message, uid: int) -> None:
    """/highlights — content-aware alternative to /splitscene. Transcribes the dialogue (in parallel
    chunks), asks the LLM to pick 5-10 shareable moments, double-checks them with a vision model,
    then cuts them. One live dashboard card covers every stage."""
    if not await _begin(message, uid):
        return
    if not AI_ENABLED:
        await message.reply("🤖 AI abhi off hai.\n👉 `/aistatus` check karo.")
        return
    async with _get_lock(uid):
        if not _is_ready(uid):
            await message.reply(_t(uid, "file_missing"))
            return
        file = user_files[uid]
        dur = await get_duration(file)
        if not dur:
            await message.reply("❌ Video duration nahi mila.")
            return
        if not await _quota_gate(message, uid, dur):
            return
        cancel = _get_cancel(uid)
        cancel.clear()
        async with _job_slot(uid, message) as got:
            if not got:
                return
            src = user_sources.get(uid) or await _source_for(uid)
            job_id = await _job_create(uid, message.chat.id, src["source_message_id"], "highlights",
                                       {"stage": "transcribe", "segments": [], "highlights": [], "next_index": 0, "kind": "highlights"}, file) if src else None
            dash = Dashboard(message, uid, "AI Highlights", ["Transcribe", "Pick", "Vision", "Cut & Upload", "Done"],
                             total=0, emoji="🔥")
            dash.note = "Audio nikal rahe hain aur samajh rahe hain…"
            dash.step(0, "run")
            await dash.start()

            async def abort(text: str, state: str) -> None:
                for i, (_n, s) in enumerate(dash.steps):
                    if s == "run":
                        dash.step(i, "fail")
                dash.note = ""
                await dash.finish(text, kb=_result_kb(uid))
                if job_id:
                    await _job_finish(job_id, state)

            segments = await _transcribe_video(file, dur, uid, _DashNote(dash))
            if job_id:
                await _job_update(job_id, payload={"stage": "select", "segments": segments, "highlights": [], "next_index": 0, "kind": "highlights"})
            if cancel.is_set():
                return await abort("🚫 Cancelled.", "cancelled")
            if not segments:
                return await abort("❌ Transcription fail ho gayi — audio na ho ya bahut lambi ho sakti hai.\n👉 `/splitscene` try karo instead.", "failed")
            user_transcripts[uid] = {"path": file, "segments": segments}
            dash.step(0, "done")
            dash.step(1, "run")
            dash.note = f"{len(segments)} dialogue segments mile — best clips choose ho rahe hain…"
            await dash.push(True)
            highlights = await _select_highlights(segments, dur)
            dash.step(1, "done")
            if job_id:
                await _job_update(job_id, payload={"stage": "vision" if HIGHLIGHT_USE_VISION else "split", "segments": segments,
                                                   "highlights": highlights, "next_index": 0, "kind": "highlights"})
            if highlights and HIGHLIGHT_USE_VISION and not cancel.is_set():
                dash.step(2, "run")
                dash.note = f"Vision check: {min(HIGHLIGHT_VISION_CHECKS, len(highlights))} candidates"
                await dash.push(True)
                highlights = await _vision_rank_highlights(file, uid, highlights)
                dash.step(2, "done")
            else:
                dash.step(2, "skip")
            if cancel.is_set():
                return await abort("🚫 Cancelled.", "cancelled")
            if not highlights:
                return await abort("❌ Koi strong highlight nahi mila.\n👉 `/splitscene` ya `/split N` try karo.", "failed")

            segs = [(h["start"], h["end"] - h["start"]) for h in highlights]
            info = await _probe_video(file)
            opts = _build_cut_opts(uid, "highlights", info, transcript=segments)
            extra = {"highlights": highlights, "opts": {"reels": bool(opts.get("reels")), "burn": bool(opts.get("burn"))}}
            if opts.get("burn"):
                extra["tx"] = _relevant_tx(segments, segs)
            label = f"{len(highlights)} AI-picked highlights…"
            dash.title = label
            dash.note = ""
            _track_video_processed()
            if job_id:
                await _job_update(job_id, payload={"stage": "split", "segments": [list(s) for s in segs], "next_index": 0,
                                                   "label": label, "kind": "highlights", **extra})
            await _run_split_segments(message, uid, segs, label,
                                      caption_fn=_caption_fn_for(uid, "highlights", {"highlights": highlights}),
                                      job_id=job_id, cut_opts=opts, extra_payload=extra, kind="highlights", dash=dash)


async def _quick_describe(message, uid: int) -> None:
    if not await _begin(message, uid):
        return
    if not AI_ENABLED:
        await message.reply("🤖 AI abhi off hai.\n👉 `/aistatus` check karo.")
        return
    async with _get_lock(uid):
        if not _is_ready(uid):
            await message.reply(_t(uid, "file_missing"))
            return
        file = user_files[uid]
        status = await message.reply("🔍 **Analyzing video frame…**")
        dur = await get_duration(file) or 0
        mid_point = dur / 2 if dur else 1.0
        thumb_path = f"{THUMB_DIR}/describe_{uid}.jpg"
        thumb = await make_thumb(file, mid_point, thumb_path)
        if not thumb:
            await _safe_edit(status, "❌ Thumbnail nahi ban paya.")
            return
        desc = await _ai_describe(thumb_path)
        _rm(thumb_path)
        if not desc:
            await _safe_edit(status,
                "❌ AI description generate nahi ho paayi.\n"
                "  Vision model unavailable ho sakta hai — `GROQ_VISION_MODEL` env var check karo.")
            return
        await _safe_edit(status, f"🤖 **AI Description:**\n_{desc}_")


@app.on_message(filters.command("split"), group=1)
async def cmd_split(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    try:
        parts = int(message.command[1])
        assert 2 <= parts <= 100
    except Exception:
        return await message.reply("❌ Usage: `/split 3`\n  Min 2, max 100.")
    await _quick_split(message, uid, parts)

@app.on_message(filters.command("splitmin"), group=1)
async def cmd_splitmin(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    try:
        mins = int(message.command[1])
        assert 1 <= mins <= 120
    except Exception:
        return await message.reply("❌ Usage: `/splitmin 2`\n  Minutes 1-120.")
    await _quick_splitmin(message, uid, mins)

@app.on_message(filters.command("splitsize"), group=1)
async def cmd_splitsize(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    try:
        mb = int(message.command[1])
        assert 10 <= mb <= 2000
    except Exception:
        return await message.reply("❌ Usage: `/splitsize 500`\n  MB 10-2000.")
    if not await _begin(message, uid):
        return
    file     = user_files[uid]
    total_mb = os.path.getsize(file) / 1048576
    dur_pre  = await get_duration(file)
    if not dur_pre:
        return await message.reply("❌ Video duration nahi mila.")
    parts = math.ceil(total_mb / mb)
    if parts > 100:
        return await message.reply(f"❌ Too many parts ({parts}).")
    if parts < 2:
        return await message.reply(f"❌ File already ≤{mb}MB. Split ki zaroorat nahi.")
    seg = dur_pre / parts
    async with _get_lock(uid):
        if not _is_ready(uid):
            return await message.reply(_t(uid, "file_missing"))
        await _do_split(message, uid, parts, seg_override=seg, label=f"{mb}MB chunks → {parts} parts…")

@app.on_message(filters.command("splitat"), group=1)
async def cmd_splitat(client, message):
    """/splitat 1:20 3:45 9:10 — one cut per timestamp."""
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    args = message.command[1:]
    pts = [_parse_time(a) for a in args]
    if not args or any(p is None for p in pts):
        return await message.reply(_t(uid, "splitat_usage"))
    if not await _begin(message, uid):
        return
    async with _get_lock(uid):
        if not _is_ready(uid):
            return await message.reply(_t(uid, "file_missing"))
        dur = await get_duration(user_files[uid])
        if not dur:
            return await message.reply("❌ Video duration nahi mila.")
        cuts = sorted({round(float(p), 2) for p in pts if 0.5 < float(p) < dur - 0.5})[:99]
        if not cuts:
            return await message.reply(_t(uid, "splitat_bad"))
        bounds = [0.0] + cuts + [dur]
        segments = [(a, b - a) for a, b in zip(bounds, bounds[1:])]
        await _do_split(message, uid, len(segments), segments=segments,
                        label=_t(uid, "splitat_label", n=len(cuts), p=len(segments)),
                        extra_payload={"cuts": cuts})


# ══════════════════════════════════════════════════════════════
#  AI SMART SPLIT — scene-change detection
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("splitscene"), group=1)
async def cmd_splitscene(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    await _quick_splitscene(message, uid)

@app.on_message(filters.command("highlights"), group=1)
async def cmd_highlights(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    await _quick_highlights(message, uid)


# ══════════════════════════════════════════════════════════════
#  V11 — REELS (9:16), SUBTITLES, SILENCE REMOVER
# ══════════════════════════════════════════════════════════════
async def _quick_reels(message, uid: int, start_s: float | None = None, end_s: float | None = None,
                       want_subs: bool = False) -> None:
    """9:16 export with a blurred background (or a plain fit for already-vertical sources)."""
    if not await _begin(message, uid):
        return
    async with _get_lock(uid):
        if not _is_ready(uid):
            await message.reply(_t(uid, "file_missing"))
            return
        file = user_files[uid]
        info = await _probe_video(file)
        dur = info.get("duration") or await get_duration(file)
        if not dur:
            await message.reply("❌ Video duration nahi mila.")
            return
        has_range = start_s is not None
        a = max(0.0, start_s or 0.0)
        b = min(dur, end_s if end_s is not None else dur)
        if b - a < 1.0 or a >= dur:
            await message.reply(_t(uid, "splitat_bad"))
            return
        if (b - a) > REELS_MAX_SEC and not _is_admin(uid):
            await message.reply(_t(uid, "reels_long", dur=_hms(b - a), lim=_hms(REELS_MAX_SEC)))
            return
        tx = None
        if want_subs:
            if not AI_ENABLED:
                await message.reply(_t(uid, "subs_need_ai"))
                return
            tx = _cached_transcript(uid, file)
            if tx is None:
                if dur > BURN_MAX_SEC and not _is_admin(uid):
                    await message.reply("ℹ️ Pehle `/subs` chalao (transcript ban jayega), phir `/reels … subs`.")
                    return
                note = await message.reply(_t(uid, "subs_working"))
                async with _job_slot(uid, message) as got:
                    if not got:
                        return
                    _get_cancel(uid).clear()
                    tx = await _transcribe_video(file, dur, uid, note)
                if tx:
                    user_transcripts[uid] = {"path": file, "segments": tx}
                    await _safe_edit(note, "✅ Transcript ready.")
        opts = _build_cut_opts(uid, "reels", info, transcript=tx, reels=True, burn=bool(tx))
        extra = {"opts": {"reels": True, "burn": bool(tx)}}
        if tx:
            extra["tx"] = _relevant_tx(tx, [(a, b - a)])
        await _do_split(message, uid, 1, segments=[(a, b - a)], label=_t(uid, "reels_label"), kind="reels",
                        caption_fn=_caption_fn_for(uid, "reels"), cut_opts=opts, extra_payload=extra,
                        keep_source=True)


async def _quick_subs(message, uid: int, burn: bool = False) -> None:
    """Whisper transcript → .srt document; optionally burn the captions into the video."""
    if not await _begin(message, uid):
        return
    if not AI_ENABLED:
        await message.reply(_t(uid, "subs_need_ai"))
        return
    async with _get_lock(uid):
        if not _is_ready(uid):
            await message.reply(_t(uid, "file_missing"))
            return
        file = user_files[uid]
        dur = await get_duration(file)
        if not dur:
            await message.reply("❌ Video duration nahi mila.")
            return
        tx = _cached_transcript(uid, file)
        cancel = _get_cancel(uid)
        cancel.clear()
        if tx is None:
            if not await _quota_gate(message, uid, dur):
                return
            async with _job_slot(uid, message) as got:
                if not got:
                    return
                dash = Dashboard(message, uid, _t(uid, "subs_working"), ["Download", "Transcribe", "SRT", "Upload"],
                                 total=0, emoji="💬")
                dash.step(0, "done")
                dash.step(1, "run")
                await dash.start()
                tx = await _transcribe_video(file, dur, uid, _DashNote(dash))
                if cancel.is_set():
                    dash.step(1, "fail")
                    await dash.finish("🚫 Cancelled.", kb=_result_kb(uid))
                    return
                cues = _srt_cues(tx or [])
                if not cues:
                    dash.step(1, "fail")
                    await dash.finish(_t(uid, "subs_none"), kb=_result_kb(uid))
                    return
                user_transcripts[uid] = {"path": file, "segments": tx}
                dash.step(1, "done")
                dash.step(2, "done")
                dash.step(3, "run")
                dash.note = ""
                await dash.push(True)
                srt_path = _write_srt(f"{DOWNLOAD_DIR}/subs_{uid}_all.srt", cues)
                try:
                    await message.reply_document(srt_path, caption=_t(uid, "subs_caption", n=len(cues)),
                                                 file_name="subtitles.srt")
                except Exception as e:
                    dash.step(3, "fail")
                    await dash.finish(f"❌ SRT upload failed: `{str(e)[:90]}`")
                    _rm(srt_path)
                    return
                _rm(srt_path)
                dash.step(3, "done")
                can_burn = dur <= BURN_MAX_SEC or _is_admin(uid)
                kb = InlineKeyboardMarkup([
                    ([InlineKeyboardButton("🔥 Burn-in video", callback_data="qsubsburn")] if can_burn and not burn else []) +
                    [InlineKeyboardButton(_t(uid, "btn_menu"), callback_data="menu:home")]])
                await dash.finish(f"✅ **{_t(uid, 'subs_caption', n=len(cues))}**", kb=kb)
        if burn:
            if dur > BURN_MAX_SEC and not _is_admin(uid):
                await message.reply(_t(uid, "subs_burn_limit", lim=_hms(BURN_MAX_SEC)))
                return
            tx = tx or _cached_transcript(uid, file)
            if not tx:
                return
            await _do_split(message, uid, 1, segments=[(0.0, dur)], label="Burning subtitles…", kind="subs",
                            caption_fn=_caption_fn_for(uid, "subs"), cut_opts={"srt_fn": _make_srt_fn(uid, tx), "burn": True},
                            extra_payload={"opts": {"burn": True}, "tx": tx}, keep_source=True, quota=False)


async def _quick_silence(message, uid: int, noise_db: float | None = None, min_dur: float | None = None) -> None:
    """Cut dead air: detect silences with ffmpeg, keep the rest (re-encoded), send one video."""
    if not await _begin(message, uid):
        return
    async with _get_lock(uid):
        if not _is_ready(uid):
            await message.reply(_t(uid, "file_missing"))
            return
        file = user_files[uid]
        info = await _probe_video(file)
        dur = info.get("duration") or await get_duration(file)
        if not dur:
            await message.reply("❌ Video duration nahi mila.")
            return
        if dur > SILENCE_MAX_SEC and not _is_admin(uid):
            await message.reply(_t(uid, "silence_long", dur=_hms(dur), lim=_hms(SILENCE_MAX_SEC)))
            return
        if not await _quota_gate(message, uid, dur):
            return
        cancel = _get_cancel(uid)
        cancel.clear()
        db = SILENCE_DB if noise_db is None else noise_db
        md = SILENCE_MIN if min_dur is None else min_dur
        async with _job_slot(uid, message) as got:
            if not got:
                return
            dash = Dashboard(message, uid, _t(uid, "silence_label"), ["Download", "Detect", "Encode", "Upload"],
                             total=1, emoji="🔇")
            dash.step(0, "done")
            dash.step(1, "run")
            dash.note = f"threshold {db:.0f} dB · min {md:.1f}s"
            await dash.start()
            out = f"{DOWNLOAD_DIR}/silence_{uid}_{message.id}.mp4"
            try:
                sil = await _detect_silences(file, db, md, dur)
                if cancel.is_set():
                    await dash.finish("🚫 Cancelled.", kb=_result_kb(uid)); return
                if sil is None:
                    dash.step(1, "fail")
                    await dash.finish("❌ Silence detect fail ho gaya (audio track hai?).", kb=_result_kb(uid)); return
                keep = _keep_intervals(sil, dur)
                if len(keep) > 1500:                      # absurdly fragmented → ignore the tiny islands
                    keep = _keep_intervals(sil, dur, min_keep=1.0)
                kept = sum(b - a for a, b in keep)
                removed = dur - kept
                if not keep or removed < max(1.5, dur * 0.03) or len(keep) > 1500:
                    dash.step(1, "done")
                    await dash.finish(_t(uid, "silence_none"), kb=_result_kb(uid)); return
                dash.step(1, "done")
                dash.step(2, "run")
                dash.note = f"{len(sil)} silent gaps · {_hms(removed)} hatega"
                dash.part(0, "cut")
                await dash.push(True)
                vf, af = _silence_filters(keep, info.get("fps"))
                args = ["-i", file, "-map", "0:v:0", "-map", "0:a:0?", "-sn", "-dn", "-vf", vf, "-af", af,
                        "-c:v", "libx264", "-preset", ENC_PRESET, "-crf", str(ENC_CRF), "-pix_fmt", "yuv420p",
                        "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", out]

                async def on_prog(pct, _speed):
                    dash.set_cut(pct)
                    await dash.push()

                ok = await _run_ffmpeg_polled(args, uid, None, "Silence", FFMPEG_COMPRESS_TIMEOUT,
                                              total_duration=kept, on_progress=on_prog)
                if cancel.is_set():
                    await dash.finish("🚫 Cancelled.", kb=_result_kb(uid)); return
                if not ok or not os.path.exists(out):
                    dash.step(2, "fail"); dash.part(0, "fail")
                    await dash.finish("❌ Encode fail ho gaya. Logs check karo.", kb=_result_kb(uid)); return
                dash.step(2, "done")
                dash.step(3, "run")
                dash.part(0, "up")
                dash.note = ""
                await dash.push(True)
                meta = await _prep_part_meta(out, {"ss": 0.0, "dur": None}, info, {}, uid, 1, 1)
                new_dur = meta.get("duration") or kept
                caption = _t(uid, "silence_done", cut=_hms(removed), pct=f"{removed * 100 / dur:.0f}",
                             old=_hms(dur), new=_hms(new_dur))
                up_ok = await _upload_part(message, out, 1, 1, uid, caption, dash=dash, idx=0, meta=meta)
                if up_ok:
                    dash.part(0, "done"); dash.step(3, "done")
                    _track_video_processed()
                    await dash.finish(caption, kb=_result_kb(uid))
                else:
                    dash.part(0, "fail"); dash.step(3, "fail")
                    await dash.finish("❌ Upload fail ho gaya.", kb=_result_kb(uid))
            finally:
                _rm(out)
                _touch(uid)


@app.on_message(filters.command("reels"), group=1)
async def cmd_reels(client, message):
    """/reels            → whole video (up to REELS_MAX_SEC)
       /reels 0:30 1:20  → just that range, 9:16
       add the word `subs` to burn captions in."""
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    args = [a for a in message.command[1:]]
    want_subs = any(a.lower() in {"subs", "sub", "captions", "cc"} for a in args)
    nums = [a for a in args if a.lower() not in {"subs", "sub", "captions", "cc"}]
    if len(nums) == 0:
        return await _quick_reels(message, uid, want_subs=want_subs)
    if len(nums) == 2:
        a, b = _parse_time(nums[0]), _parse_time(nums[1])
        if a is not None and b is not None and b > a:
            return await _quick_reels(message, uid, a, b, want_subs=want_subs)
    await message.reply("❌ Usage: `/reels` · `/reels 0:30 1:20` · `/reels 0:30 1:20 subs`")

@app.on_message(filters.command("subs"), group=1)
async def cmd_subs(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    burn = any(a.lower() in {"burn", "hard", "burnin"} for a in message.command[1:])
    await _quick_subs(message, uid, burn=burn)

@app.on_message(filters.command("silence"), group=1)
async def cmd_silence(client, message):
    """/silence [threshold_dB] [min_seconds]  e.g. /silence -30 0.5"""
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    db = md = None
    try:
        if len(message.command) > 1:
            db = float(message.command[1])
            if not -90 <= db <= -5: raise ValueError
        if len(message.command) > 2:
            md = float(message.command[2])
            if not 0.2 <= md <= 10: raise ValueError
    except ValueError:
        return await message.reply("❌ Usage: `/silence` · `/silence -30 0.5`\n  threshold −90…−5 dB, min gap 0.2…10 s")
    await _quick_silence(message, uid, db, md)


# ══════════════════════════════════════════════════════════════
#  VIDEO TOOLS — trim, compress, extract audio
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("trim"), group=1)
async def cmd_trim(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    if len(message.command) < 3:
        return await message.reply("❌ Usage: `/trim 1:00 3:30`\n  Ya seconds mein: `/trim 60 210`")
    start_s = _parse_time(message.command[1])
    end_s   = _parse_time(message.command[2])
    if start_s is None or end_s is None or end_s <= start_s:
        return await message.reply("❌ Invalid time range.\n  Format: `HH:MM:SS`, `MM:SS`, ya seconds.")
    if not await _begin(message, uid):
        return
    async with _get_lock(uid):
        if not _is_ready(uid):
            return await message.reply(_t(uid, "file_missing"))
        file = user_files[uid]
        dur = await get_duration(file)
        if not dur:
            return await message.reply("❌ Video duration nahi mila.")
        if start_s >= dur:
            return await message.reply(f"❌ Start time video duration (`{_eta(dur)}`) se zyada/barabar hai.")
        end_s = min(end_s, dur)
        # V11: trim goes through the same pipeline as every other cut (dashboard, exact edges for short
        # clips in 'auto' mode, thumbnail + real dimensions, queue, quota, cancel).
        await _do_split(message, uid, 1, segments=[(start_s, end_s - start_s)], kind="trim",
                        label=_t(uid, "trim_label", a=_hms(start_s), b=_hms(end_s)),
                        caption_fn=_caption_fn_for(uid, "trim"), keep_source=True)


@app.on_message(filters.command("compress"), group=1)
async def cmd_compress(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    level = message.command[1].lower() if len(message.command) > 1 else "medium"
    if level not in COMPRESS_PRESETS:
        return await message.reply("❌ Usage: `/compress low` · `/compress medium` · `/compress high`")
    crf = COMPRESS_PRESETS[level]
    if not await _begin(message, uid):
        return
    async with _get_lock(uid):
        if not _is_ready(uid):
            return await message.reply(_t(uid, "file_missing"))
        file = user_files[uid]
        orig_size = os.path.getsize(file)
        dur = await get_duration(file) or 0.0
        if dur and not await _quota_gate(message, uid, dur):
            return
        _get_cancel(uid).clear()
        async with _job_slot(uid, message) as got:
            if not got:
                return
            _set_status(uid, "Compressing", level)
            msg = await message.reply(f"🗜 **Compressing** (`{level}`)…")
            t0 = time.time()
            out = f"{DOWNLOAD_DIR}/compressed_{uid}_{message.id}.mp4"
            ok = await ffmpeg_compress(file, out, crf, uid, msg)
            elapsed = time.time() - t0
            if _get_cancel(uid).is_set():
                _clear_status(uid)
                _rm(out)
                return await _safe_edit(msg, "🚫 Compression cancelled.")
            if not ok or not os.path.exists(out):
                _clear_status(uid)
                _rm(out)
                return await _safe_edit(msg, "❌ Compression failed. Check logs.")
            new_size = os.path.getsize(out)
            _rm(file)
            _probe_cache.pop(file, None)
            user_files[uid] = out
            user_transcripts.pop(uid, None)
            _touch(uid)
            _clear_status(uid)
            saved_pct = (1 - new_size / orig_size) * 100 if orig_size else 0
            reaction = ("🤯 **Massive shrink!**" if saved_pct >= 60 else
                        "🔥 **Great compression!**" if saved_pct >= 35 else
                        "👍 **Nicely trimmed down!**" if saved_pct >= 10 else
                        "✅ **Done!** _(already fairly optimized)_")
            await _safe_edit(msg,
                f"{reaction}\n"
                f"  📦 `{_sz(orig_size)}` → `{_sz(new_size)}`  ({saved_pct:.0f}% smaller)\n"
                f"  ⏱ Took `{_eta(elapsed)}`\n"
                f"  👉 `/split N` ab is compressed file pe chalega.",
                reply_markup=_result_kb(uid))


@app.on_message(filters.command("extractaudio"), group=1)
async def cmd_extract_audio(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    if not await _begin(message, uid):
        return
    async with _get_lock(uid):
        if not _is_ready(uid):
            return await message.reply(_t(uid, "file_missing"))
        file = user_files[uid]
        _get_cancel(uid).clear()
        async with _job_slot(uid, message) as got:
            if not got:
                return
            _set_status(uid, "Extracting audio")
            msg = await message.reply("🎵 **Extracting audio…**")
            out = f"{DOWNLOAD_DIR}/audio_{uid}_{message.id}.mp3"
            ok = await ffmpeg_extract_audio(file, out)
            if not ok or not os.path.exists(out):
                _clear_status(uid)
                return await _safe_edit(msg, "❌ Audio extraction failed. Check logs.")
            try:
                await message.reply_audio(out, caption="🎵 **Extracted Audio**")
            except Exception as e:
                _clear_status(uid)
                _rm(out)
                return await _safe_edit(msg, f"❌ Upload failed: `{e}`")
            _rm(out)
            _touch(uid)
            _clear_status(uid)
            await _safe_edit(msg, "✅ **Audio extracted & sent!**")


# ══════════════════════════════════════════════════════════════
#  V11 — SETTINGS, LANGUAGE, QUEUE, QUOTA, ACCESS (admin)
# ══════════════════════════════════════════════════════════════
def _settings_text(uid: int) -> str:
    return f"{_t(uid, 'set_title')}\n{_RULE}\n{_t(uid, 'set_body')}"

@app.on_message(filters.command("settings"), group=1)
async def cmd_settings(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    await message.reply(_settings_text(uid), reply_markup=_settings_kb(uid))

@app.on_message(filters.command("lang"), group=1)
async def cmd_lang(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    await message.reply(_t(uid, "lang_ask"), reply_markup=_lang_kb())

@app.on_message(filters.command("queue"), group=1)
async def cmd_queue(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    text = _t(uid, "queue_info", running=_jobq.active, slots=_jobq.slots, waiting=len(_jobq.waiting))
    pos = _jobq.pos_of_user(uid)
    if pos:
        text += _t(uid, "queue_you", pos=pos)
    await message.reply(text)

@app.on_message(filters.command("quota"), group=1)
async def cmd_quota(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    target = uid
    if len(message.command) > 1 and _is_admin(uid) and message.command[1].lstrip("-").isdigit():
        target = int(message.command[1])
    try:
        used, jobs = await _usage_get(target)
    except Exception as e:
        return await message.reply(f"⚠️ Usage lookup failed: `{str(e)[:80]}`")
    if DAILY_QUOTA_MIN > 0 and not _is_admin(target):
        text = _t(uid, "quota_line", used=f"{used/60:.1f}", limit=f"{DAILY_QUOTA_MIN:.0f}", jobs=jobs)
    else:
        text = _t(uid, "quota_free", used=f"{used/60:.1f}", jobs=jobs)
    if target != uid:
        text = f"👤 `{target}`\n{text}"
    await message.reply(text)

async def _acl_command(message, cmd: str, status: str | None) -> None:
    uid = _uid(message)
    if uid is None: return
    if not _is_admin(uid):
        return await message.reply(_t(uid, "admin_only"))
    if len(message.command) < 2 or not message.command[1].lstrip("-").isdigit():
        return await message.reply(_t(uid, "acl_usage", cmd=cmd))
    target = int(message.command[1])
    if target in ADMIN_IDS and status == "ban":
        return await message.reply("❌ Admin ko ban nahi kar sakte.")
    try:
        await _acl_set(target, status)
    except Exception as e:
        return await message.reply(f"⚠️ DB error: `{str(e)[:80]}`")
    _acl_cache[target] = status
    await message.reply(_t(uid, "acl_ok", uid=target, status=status or "default"))

@app.on_message(filters.command("allow"), group=1)
async def cmd_allow(client, message):
    if await _dedup(message): return
    await _acl_command(message, "allow", "allow")

@app.on_message(filters.command("deny"), group=1)
async def cmd_deny(client, message):
    if await _dedup(message): return
    await _acl_command(message, "deny", None)

@app.on_message(filters.command("ban"), group=1)
async def cmd_ban(client, message):
    if await _dedup(message): return
    await _acl_command(message, "ban", "ban")

@app.on_message(filters.command("unban"), group=1)
async def cmd_unban(client, message):
    if await _dedup(message): return
    await _acl_command(message, "unban", None)


# ══════════════════════════════════════════════════════════════
#  MERGE — collect multiple videos, join into one
#  FIX #31 — core logic extracted into _merge_finish/_merge_abort so
#  the inline "✅ Done" / "❌ Cancel" buttons can trigger the exact
#  same path as /mergedone and /mergecancel, not a re-implementation.
# ══════════════════════════════════════════════════════════════
async def _merge_abort(uid: int, reply_target) -> bool:
    """Returns True if a queue was actually found & cancelled."""
    queue = user_merge_queue.pop(uid, None)
    if queue is None:
        await reply_target.reply("💤 Koi merge queue active nahi hai.")
        return False
    for p in queue:
        try:
            if os.path.exists(p): os.remove(p)
        except: pass
    await reply_target.reply(f"🚫 Merge queue cancelled. {len(queue)} video(s) discarded.")
    return True


async def _merge_finish(uid: int, reply_target) -> None:
    lock = _get_lock(uid)
    if lock.locked():
        await reply_target.reply("⏳ Already processing.\n👉 /status · /cancel")
        return
    queue = user_merge_queue.get(uid)
    if not queue:
        await reply_target.reply("❌ Merge queue khaali hai.\n👉 Pehle `/mergestart` karo, phir videos bhejo.")
        return
    if len(queue) < 2:
        await reply_target.reply(f"❌ Kam se kam 2 videos chahiye (abhi {len(queue)}).\n👉 Aur bhejo ya `/mergecancel`.")
        return

    async with lock:
        files = user_merge_queue.pop(uid, [])
        if len(files) < 2:
            for p in files:  # race guard — queue changed before lock acquired
                try:
                    if os.path.exists(p): os.remove(p)
                except: pass
            await reply_target.reply("❌ Merge queue khaali ho gaya. Dobara `/mergestart` karo.")
            return

        _get_cancel(uid).clear()
        _set_status(uid, "Merging", f"{len(files)} videos")
        msg = await reply_target.reply(f"🔗 **Merging {len(files)} videos…**")

        # Combined source duration, just for the celebration message below —
        # ffmpeg_merge now probes durations itself (in parallel) for its OWN
        # live progress bar, so this is a separate, cheap, parallel lookup.
        durations = await asyncio.gather(*(get_duration(p) for p in files))
        combined_dur = sum(durations) if all(durations) else 0.0
        if combined_dur and not await _quota_gate(reply_target, uid, combined_dur):
            for p in files:
                _rm(p)
            return

        t0 = time.time()
        out = f"{DOWNLOAD_DIR}/merged_{uid}_{int(time.time()*1000)}.mp4"
        async with _job_slot(uid, reply_target) as got:
            ok = await ffmpeg_merge(files, out, uid, msg) if got else False
        elapsed = time.time() - t0

        for p in files:
            try:
                if os.path.exists(p): os.remove(p)
            except: pass

        if _get_cancel(uid).is_set():
            _clear_status(uid)
            try:
                if os.path.exists(out): os.remove(out)
            except: pass
            await _safe_edit(msg, "🚫 Merge cancelled.")
            return

        if not ok or not os.path.exists(out):
            _clear_status(uid)
            try:
                if os.path.exists(out): os.remove(out)
            except: pass
            await _safe_edit(msg, "❌ Merge failed. Videos ka codec/format bahut alag ho sakta hai.")
            return

        old = user_files.pop(uid, None)
        if old and os.path.exists(old):
            try: os.remove(old)
            except: pass
        user_files[uid] = out
        user_transcripts.pop(uid, None)
        _touch(uid)
        _clear_status(uid)
        _track_video_processed()
        await _safe_edit(msg,
            f"🎉 **{len(files)} videos merged into 1!**\n"
            f"  📦 `{_sz(os.path.getsize(out))}`"
            f"{f'  ·  🎞 `{_eta(combined_dur)}` total' if combined_dur else ''}\n"
            f"  ⏱ Took `{_eta(elapsed)}`\n"
            f"  👉 Neeche se quick split karo, ya `/split N` type karo:",
            reply_markup=_quick_split_kb(combined_dur or None, uid),
        )


@app.on_message(filters.command("mergestart"), group=1)
async def cmd_merge_start(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply("⏳ Already processing.\n👉 /status · /cancel")
    if uid in user_merge_queue:
        return await message.reply(
            f"⚠️ Merge queue already active ({len(user_merge_queue[uid])} video(s)).\n"
            f"👉 `/mergedone` ya `/mergecancel`"
        )
    user_merge_queue[uid] = []
    await message.reply(
        "🔗 **Merge mode ON!**\n"
        f"Ab jitni videos merge karni hain bhejo (max {MAX_MERGE_VIDEOS}, order maintain hoga).\n"
        "Jab bhej chuko, neeche button se ✅ Done tap karo (ya `/mergedone` type karo)."
    )

@app.on_message(filters.command("mergecancel"), group=1)
async def cmd_merge_cancel(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    await _merge_abort(uid, message)

@app.on_message(filters.command("mergedone"), group=1)
async def cmd_merge_done(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    await _merge_finish(uid, message)


# ══════════════════════════════════════════════════════════════
#  AI DESCRIBE
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("describe"), group=1)
async def cmd_describe(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    await _quick_describe(message, uid)


# ══════════════════════════════════════════════════════════════
#  ADMIN — stats & broadcast (only for IDs in ADMIN_IDS env var)
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("stats"), group=1)
async def cmd_stats(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    if not _is_admin(uid):
        return await message.reply("🚫 Admin-only command.")

    uptime = _eta(time.time() - _stats_start_time)
    active_tasks  = sum(1 for lk in user_locks.values() if lk.locked())
    active_merges = sum(1 for q in user_merge_queue.values() if q)
    await message.reply(
        f"📊 **Bot Stats**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  👥 Users seen: `{len(_stats_users_seen)}`\n"
        f"  📹 Videos processed: `{_stats_videos_processed}`\n"
        f"  ✂️ Parts created: `{_stats_parts_created}`\n"
        f"  ⚙️ Active tasks: `{active_tasks}`\n"
        f"  📋 Queue: `{_jobq.active}/{_jobq.slots}` running · `{len(_jobq.waiting)}` waiting\n"
        f"  🗄 DB: `{'Postgres (pooled)' if _USE_PG else 'SQLite'}`\n"
        f"  🔒 Mode: `{'PRIVATE' if PRIVATE_MODE else 'open'}` · quota `{DAILY_QUOTA_MIN:g}` min/day\n"
        f"  🔗 Active merge queues: `{active_merges}`\n"
        f"  🤖 AI: `{'ON — ' + GROQ_MODEL if AI_ENABLED else 'OFF'}`\n"
        f"  ⏳ Uptime: `{uptime}`"
    )

@app.on_message(filters.command("broadcast"), group=1)
async def cmd_broadcast(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    if not _is_admin(uid):
        return await message.reply("🚫 Admin-only command.")
    if len(message.command) < 2:
        return await message.reply("❌ Usage: `/broadcast Your message here`")

    text = message.text.split(None, 1)[1]
    targets = list(_stats_users_seen)
    sent = failed = 0
    status = await message.reply(f"📢 Broadcasting to {len(targets)} user(s)…")

    for target_uid in targets:
        try:
            await client.send_message(target_uid, f"📢 **Announcement**\n\n{text}")
            sent += 1
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
            try:
                await client.send_message(target_uid, f"📢 **Announcement**\n\n{text}")
                sent += 1
            except Exception:
                failed += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)  # gentle throttle

    await _safe_edit(status, f"📢 **Broadcast done!**\n  ✅ Sent: {sent}  ❌ Failed: {failed}")


# ══════════════════════════════════════════════════════════════
#  CALLBACK QUERIES — inline button taps
#  Every handler answers the callback immediately (required by
#  Telegram, dismisses the button's loading spinner) before doing
#  any real work, then routes to the exact same shared logic the
#  text commands use — no separate/duplicated code paths.
# ══════════════════════════════════════════════════════════════
async def _menu_text(kind: str):
    pages = {
        "split": ("✂️ **SPLIT & SCENE**", "`/split N` — equal parts\n`/splitmin N` — minute chunks\n`/splitsize N` — size chunks\n`/splitat 1:20 3:45` — your own cut points\n`/splitscene` — smart scenes\n🎞 **Timeline Editor** — drag cut points visually (Mini App)\n\n⚡ Cuts are lossless and keyframe-aligned; short clips get exact edges automatically (see /settings)."),
        "ai": ("🤖 **AI CENTER**", "`/highlights` — multi-stage highlight selection\n`/subs` — SRT subtitles · `/subs burn` — burned-in\n`/describe` — describe video\n`/aistatus` — AI status\n\n🧠 Highlights use transcript analysis first, then a second vision check when enabled. In /settings you can burn captions into highlight clips and export them as 9:16."),
        "tools": ("🎬 **VIDEO TOOLS**", "`/trim 1:00 3:30` — trim\n`/reels` · `/reels 0:30 1:20 subs` — 9:16 export\n`/silence` · `/silence -30 0.5` — remove dead air\n`/compress medium` — compress\n`/extractaudio` — MP3\n\n⚡ One live card shows progress. Use the ❌ button or `/cancel` during long jobs."),
        "merge": ("🔗 **MERGE CENTER**", "1️⃣ `/mergestart`\n2️⃣ Send videos in order\n3️⃣ Tap **Done** or use `/mergedone`\n4️⃣ Use `/mergecancel` to discard\n\nMaximum queue: 20 videos."),
        "status": ("📊 **STATUS & CONTROL**", "`/info` — current video\n`/status` — current task\n`/queue` — job queue\n`/quota` — today's usage\n`/cancel` — stop safely\n`/clear` — reset idle state\n`/retry` — resume an interrupted job\n\nIf something looks stuck: `/status` → `/cancel` → `/clear`."),
        "how": ("⚙️ **HOW IT WORKS**", "📥 Download → 🔎 analyze keyframes → ✂️ cut part N+1 while part N uploads → 📤 Telegram\n\n🚄 Cutting and uploading overlap, so long splits finish much sooner.\n🎯 Lossless by default; precise re-encode when it matters.\n🧹 Temporary files are removed right after delivery; your source stays for a while so buttons keep working.\n\n⚠️ Render Free CPU/RAM/network limits can still affect speed."),
        "help": ("📖 **ALL COMMANDS**", "`/start` `/help` `/settings` `/lang`\n`/split` `/splitmin` `/splitsize` `/splitat` `/splitscene`\n`/highlights` `/subs` `/describe` `/aistatus`\n`/trim` `/reels` `/silence` `/compress` `/extractaudio`\n`/mergestart` `/mergedone` `/mergecancel`\n`/info` `/status` `/queue` `/quota` `/cancel` `/clear`\n`/retry` `/resume` — recovery\n`/stats` `/broadcast` `/allow` `/deny` `/ban` `/unban` (admin)"),
    }
    return pages.get(kind, pages["help"])


def _menu_kb(kind: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✂️ Split", callback_data="menu:split"), InlineKeyboardButton("🤖 AI", callback_data="menu:ai")],
        [InlineKeyboardButton("🎬 Tools", callback_data="menu:tools"), InlineKeyboardButton("🔗 Merge", callback_data="menu:merge")],
        [InlineKeyboardButton("📊 Status", callback_data="menu:status"), InlineKeyboardButton("📖 Commands", callback_data="menu:help")],
        [InlineKeyboardButton("🏠 Main Menu", callback_data="menu:home")],
    ])


@app.on_callback_query(filters.regex(r"^menu:(home|split|ai|tools|merge|status|how|help)$"))
async def cb_menu(client, callback_query):
    await callback_query.answer()
    kind = callback_query.data.split(":", 1)[1]
    if kind == "home":
        text = "⚡ **VIDEO SPLIT PRO v11**\n━━━━━━━━━━━━━━━━━━━━━━\n🎥 Send a video → choose an action.\n\n👇 Tap a category below:"
        kb = _main_menu_kb()
    else:
        title, body = await _menu_text(kind)
        text = f"{title}\n━━━━━━━━━━━━━━━━━━━━━━\n{body}"
        kb = _menu_kb(kind)
    try:
        await callback_query.message.edit_text(text, reply_markup=kb)
    except Exception as e:
        log.debug("menu edit failed: %s", e)


@app.on_callback_query(filters.regex(r"^qs:\d+$"))
async def cb_quick_split(client, callback_query):
    await callback_query.answer()
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None: return
    _stats_users_seen.add(uid)
    parts = int(callback_query.data.split(":")[1])
    await _quick_split(callback_query.message, uid, parts)

@app.on_callback_query(filters.regex(r"^qscene$"))
async def cb_quick_scene(client, callback_query):
    await callback_query.answer()
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None: return
    _stats_users_seen.add(uid)
    await _quick_splitscene(callback_query.message, uid)

@app.on_callback_query(filters.regex(r"^qhighlights$"))
async def cb_quick_highlights(client, callback_query):
    await callback_query.answer()
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None: return
    _stats_users_seen.add(uid)
    await _quick_highlights(callback_query.message, uid)

@app.on_callback_query(filters.regex(r"^qdesc$"))
async def cb_quick_describe(client, callback_query):
    await callback_query.answer()
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None: return
    _stats_users_seen.add(uid)
    await _quick_describe(callback_query.message, uid)

@app.on_callback_query(filters.regex(r"^mdone$"))
async def cb_merge_done(client, callback_query):
    await callback_query.answer()
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None: return
    _stats_users_seen.add(uid)
    await _merge_finish(uid, callback_query.message)

@app.on_callback_query(filters.regex(r"^mcancel$"))
async def cb_merge_cancel(client, callback_query):
    await callback_query.answer()
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None: return
    await _merge_abort(uid, callback_query.message)


@app.on_callback_query(filters.regex(r"^qm:\d+$"))
async def cb_quick_minutes(client, callback_query):
    await callback_query.answer()
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None: return
    _stats_users_seen.add(uid)
    await _quick_splitmin(callback_query.message, uid, int(callback_query.data.split(":")[1]))

@app.on_callback_query(filters.regex(r"^qreels$"))
async def cb_quick_reels(client, callback_query):
    await callback_query.answer()
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None: return
    _stats_users_seen.add(uid)
    await _quick_reels(callback_query.message, uid)

@app.on_callback_query(filters.regex(r"^qsubs$"))
async def cb_quick_subs(client, callback_query):
    await callback_query.answer()
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None: return
    _stats_users_seen.add(uid)
    await _quick_subs(callback_query.message, uid, burn=False)

@app.on_callback_query(filters.regex(r"^qsubsburn$"))
async def cb_quick_subs_burn(client, callback_query):
    await callback_query.answer()
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None: return
    _stats_users_seen.add(uid)
    await _quick_subs(callback_query.message, uid, burn=True)

@app.on_callback_query(filters.regex(r"^qagain$"))
async def cb_quick_again(client, callback_query):
    """Result-card button: show the split choices again (re-downloads the source if it was cleaned up)."""
    await callback_query.answer()
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None: return
    _stats_users_seen.add(uid)
    if not await _require_ready(callback_query.message, uid):
        return
    path = user_files[uid]
    info = await _probe_video(path)
    try:
        await callback_query.message.edit_text(
            _info_card(uid, path, info) + "\n\n" + _t(uid, "info_tap"),
            reply_markup=_quick_split_kb(info.get("duration"), uid))
    except Exception as e:
        log.debug("qagain edit failed: %s", e)

@app.on_callback_query(filters.regex(r"^qclear$"))
async def cb_quick_clear(client, callback_query):
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None:
        return await callback_query.answer()
    if _get_lock(uid).locked():
        return await callback_query.answer("⚠️ Task chal raha hai — pehle cancel karo.", show_alert=True)
    await _clear_user(uid)
    await callback_query.answer("🧹 Cleared!")

@app.on_callback_query(filters.regex(r"^dcancel:\d+$"))
async def cb_dash_cancel(client, callback_query):
    """❌ button on the live dashboard / queue notice."""
    uid = callback_query.from_user.id if callback_query.from_user else None
    target = int(callback_query.data.split(":")[1])
    if uid is None or (uid != target and not _is_admin(uid)):
        return await callback_query.answer("Ye tumhara job nahi hai.")
    if not _get_lock(target).locked():
        return await callback_query.answer("💤 Koi task nahi chal raha.")
    _get_cancel(target).set()
    await callback_query.answer("🚫 Cancel requested…")

@app.on_callback_query(filters.regex(r"^set:(open|cut|caption|thumbs|aicap|burn|reels|lang)$"))
async def cb_settings(client, callback_query):
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None:
        return await callback_query.answer()
    key = callback_query.data.split(":")[1]
    toast = None
    if key != "open":
        cyc = _SETTING_CYCLE[key]
        cur = _S(uid)[key]
        nxt = cyc[(cyc.index(cur) + 1) % len(cyc)] if cur in cyc else cyc[0]
        await _set_setting(uid, key, nxt)
        if key in {"aicap", "burn"} and nxt and not AI_ENABLED:
            toast = "⚠️ AI off hai — GROQ_API_KEY set karo, tab ye kaam karega."
    await callback_query.answer(toast)
    try:
        await callback_query.message.edit_text(_settings_text(uid), reply_markup=_settings_kb(uid))
    except Exception as e:
        log.debug("settings edit failed: %s", e)

@app.on_callback_query(filters.regex(r"^lang:(hinglish|english)$"))
async def cb_lang(client, callback_query):
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None:
        return await callback_query.answer()
    await _set_setting(uid, "lang", callback_query.data.split(":")[1])
    await callback_query.answer("✅")
    try:
        await callback_query.message.edit_text(_t(uid, "lang_set"))
    except Exception as e:
        log.debug("lang edit failed: %s", e)


# ══════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════
#  V11 — WEB SERVER + TELEGRAM MINI APP ("🎞 Timeline Editor")
#  Render needs an open port, so a small Flask server always runs. When PUBLIC_URL /
#  RENDER_EXTERNAL_URL is set it also serves the Mini App: the user drags cut points on a
#  filmstrip of their video, then the cuts go through the normal pipeline.
#  Security: every API call carries Telegram's signed `initData` (HMAC-SHA256 with the bot
#  token); media URLs carry a short-lived HMAC token bound to the user id. Nothing the client
#  sends is ever used as a file path — only the user's own current source file is served.
# ══════════════════════════════════════════════════════════════
MAIN_LOOP: asyncio.AbstractEventLoop | None = None

_MA_SECRET = hashlib.sha256(("ma:" + BOT_TOKEN).encode()).digest()
_MA_TTL = 3600
_MA_THUMBS = 12
_MA_THUMB_SEM = threading.Semaphore(2)

def _ma_token(uid: int) -> str:
    exp = int(time.time()) + _MA_TTL
    sig = hmac.new(_MA_SECRET, f"{uid}.{exp}".encode(), hashlib.sha256).hexdigest()[:32]
    return f"{uid}.{exp}.{sig}"

def _ma_verify(token: str) -> int | None:
    try:
        uid_s, exp_s, sig = token.split(".")
        good = hmac.new(_MA_SECRET, f"{uid_s}.{exp_s}".encode(), hashlib.sha256).hexdigest()[:32]
        if hmac.compare_digest(sig, good) and int(exp_s) >= time.time():
            return int(uid_s)
    except Exception:
        pass
    return None

def _tg_validate_init_data(init_data: str, max_age: int = 86400) -> dict | None:
    """Validate Telegram WebApp initData (https://core.telegram.org/bots/webapps#validating-data-received-via-the-mini-app)."""
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True))
        got = pairs.pop("hash", None)
        if not got:
            return None
        check = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
        secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        calc = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calc, got):
            return None
        if time.time() - int(pairs.get("auth_date", "0")) > max_age:
            return None
        user = json.loads(pairs.get("user", "{}"))
        return user if isinstance(user, dict) and user.get("id") else None
    except Exception:
        return None

def _run_on_loop(coro, timeout: float = 30.0):
    """Run a coroutine on the bot's event loop from a Flask worker thread."""
    if MAIN_LOOP is not None and MAIN_LOOP.is_running():
        return asyncio.run_coroutine_threadsafe(coro, MAIN_LOOP).result(timeout=timeout)
    return asyncio.run(coro)

async def _ma_launch(uid: int, cuts: list[float]) -> None:
    """Start the split the user composed in the Mini App (runs on the bot loop)."""
    lock = _get_lock(uid)
    if lock.locked():
        return
    target = None
    src = user_sources.get(uid) or await _source_for(uid)
    if src:
        try:
            target = await app.get_messages(src["chat_id"], src["source_message_id"])
        except Exception:
            target = None
    if target is None:
        try:
            target = await app.send_message(uid, "🎞 Timeline split…")
        except Exception as e:
            log.warning("mini app: cannot reach user %s: %s", uid, e)
            return
    async with lock:
        if not _is_ready(uid):
            await target.reply(_t(uid, "file_missing"))
            return
        dur = await get_duration(user_files[uid])
        if not dur:
            await target.reply("❌ Video duration nahi mila.")
            return
        cuts = sorted({c for c in cuts if 0.5 < c < dur - 0.5})
        if not cuts:
            await target.reply(_t(uid, "splitat_bad"))
            return
        bounds = [0.0] + cuts + [dur]
        segments = [(a, b - a) for a, b in zip(bounds, bounds[1:])]
        await _do_split(target, uid, len(segments), segments=segments,
                        label=_t(uid, "miniapp_label", n=len(segments)), extra_payload={"cuts": cuts})


MINIAPP_HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>Timeline Editor</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
:root{--bg:var(--tg-theme-bg-color,#fff);--fg:var(--tg-theme-text-color,#111);--hint:var(--tg-theme-hint-color,#8a8a8e);
--btn:var(--tg-theme-button-color,#2481cc);--btnfg:var(--tg-theme-button-text-color,#fff);--sec:var(--tg-theme-secondary-bg-color,#f0f0f3);--red:#e5484d}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
body{margin:0;padding:12px 12px 96px;background:var(--bg);color:var(--fg);font:15px/1.35 -apple-system,Roboto,"Segoe UI",sans-serif}
h1{font-size:17px;margin:2px 0 2px}
.sub{color:var(--hint);font-size:13px;margin-bottom:10px;word-break:break-all}
video{width:100%;max-height:34vh;background:#000;border-radius:12px;display:block}
.nov{display:none;padding:10px;background:var(--sec);border-radius:10px;color:var(--hint);font-size:13px}
.strip{position:relative;height:66px;margin:16px 8px 6px;border-radius:8px;background:var(--sec);touch-action:none;user-select:none}
.thumbs{position:absolute;inset:0;display:flex;border-radius:8px;overflow:hidden}
.thumbs img{flex:1 1 0;min-width:0;height:100%;object-fit:cover;pointer-events:none}
.ph{position:absolute;top:-6px;bottom:-6px;width:3px;margin-left:-1px;background:var(--btn);border-radius:2px;pointer-events:none;z-index:3}
.ph:before{content:"";position:absolute;top:-4px;left:-5px;width:13px;height:13px;border-radius:50%;background:var(--btn)}
.mk{position:absolute;top:-10px;bottom:-10px;width:28px;margin-left:-14px;z-index:2;cursor:ew-resize}
.mk:after{content:"";position:absolute;left:12px;top:6px;bottom:6px;width:4px;border-radius:2px;background:var(--red);box-shadow:0 0 0 1px #fff8}
.mk i{position:absolute;top:-14px;left:3px;font:11px/1 sans-serif;font-style:normal;background:var(--red);color:#fff;border-radius:8px;padding:2px 5px}
.time{display:flex;justify-content:space-between;color:var(--hint);font-size:12px;margin:0 8px 8px}
.row{display:flex;gap:8px;flex-wrap:wrap;margin:8px 0}
button{flex:1;min-width:84px;border:0;border-radius:10px;padding:11px 8px;font:600 14px sans-serif;background:var(--sec);color:var(--fg)}
button.pri{background:var(--btn);color:var(--btnfg)}
button:active{opacity:.7}
.parts{margin-top:10px;display:grid;gap:6px}
.part{display:flex;justify-content:space-between;background:var(--sec);border-radius:10px;padding:8px 11px;font-size:13px}
.part b{font-weight:600}
.err{color:var(--red);margin:10px 0;font-size:14px}
#fallback{display:none}
</style></head><body>
<h1>🎞 Timeline Editor</h1>
<div class="sub" id="name">Loading…</div>
<video id="v" controls playsinline preload="metadata"></video>
<div class="nov" id="nov">Is format ka preview browser mein nahi chal sakta — filmstrip pe tap karke cut points set karo.</div>
<div class="strip" id="strip"><div class="thumbs" id="thumbs"></div><div class="ph" id="ph"></div><div id="mks"></div></div>
<div class="time"><span>0:00</span><span id="cur">0:00</span><span id="tot">0:00</span></div>
<div class="row"><button class="pri" id="add">➕ Cut yahan</button><button id="del">🗑 Nearest</button><button id="undo">↩ Undo</button><button id="reset">🧹 Reset</button></div>
<div class="row"><span style="color:var(--hint);align-self:center;font-size:13px">Auto:</span>
<button data-n="2">2</button><button data-n="3">3</button><button data-n="4">4</button><button data-n="5">5</button></div>
<div class="err" id="err"></div>
<div class="parts" id="parts"></div>
<button class="pri" id="fallback">✂️ Split</button>
<script>
const tg=window.Telegram&&Telegram.WebApp;if(tg){tg.ready();tg.expand();}
const $=id=>document.getElementById(id);
let S=null,cuts=[],cur=0;
const MIN_GAP=1;
function fmt(t){t=Math.max(0,Math.round(t));const h=Math.floor(t/3600),m=Math.floor(t%3600/60),s=t%60;
  return (h?h+":"+String(m).padStart(2,"0"):m)+":"+String(s).padStart(2,"0");}
async function api(path,body){
  const r=await fetch(path,{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify(Object.assign({initData:tg?tg.initData:""},body||{}))});
  return r.json();
}
function showErr(m){$("err").textContent=m||"";}
function t2p(t){return (t/S.duration*100)+"%";}
function render(){
  $("ph").style.left=t2p(cur);$("cur").textContent=fmt(cur);
  const mk=$("mks");mk.innerHTML="";
  cuts.forEach((t,i)=>{const d=document.createElement("div");d.className="mk";d.dataset.i=i;d.style.left=t2p(t);
    d.innerHTML="<i>"+(i+1)+"</i>";mk.appendChild(d);});
  const b=[0].concat(cuts,[S.duration]),p=$("parts");p.innerHTML="";
  for(let i=0;i<b.length-1;i++){const d=document.createElement("div");d.className="part";
    d.innerHTML="<span>Part <b>"+(i+1)+"</b></span><span>"+fmt(b[i])+" → "+fmt(b[i+1])+" · "+fmt(b[i+1]-b[i])+"</span>";p.appendChild(d);}
  const label="✂️ Split — "+(cuts.length+1)+" parts";
  if(tg){ if(cuts.length){tg.MainButton.setText(label);tg.MainButton.show();}else tg.MainButton.hide(); }
  else{const f=$("fallback");f.style.display=cuts.length?"block":"none";f.textContent=label;}
}
function setCur(t,seek){cur=Math.min(S.duration,Math.max(0,t));if(seek){try{$("v").currentTime=cur;}catch(e){}}render();}
function addCut(){
  const t=Math.round(cur*10)/10;
  if(t<MIN_GAP||t>S.duration-MIN_GAP){showErr("Cut start/end ke bahut paas hai.");return;}
  if(cuts.some(c=>Math.abs(c-t)<MIN_GAP)){showErr("Is jagah ke paas cut pehle se hai.");return;}
  if(cuts.length>=99){showErr("Max 99 cuts.");return;}
  showErr("");cuts.push(t);cuts.sort((a,b)=>a-b);render();
}
function autoN(n){cuts=[];for(let i=1;i<n;i++)cuts.push(Math.round(S.duration*i/n*10)/10);showErr("");render();}
async function send(){
  if(!cuts.length)return;
  if(tg){tg.MainButton.showProgress&&tg.MainButton.showProgress();}
  let r;try{r=await api("/ma/api/split",{cuts:cuts});}catch(e){r={ok:false,error:"network"};}
  if(tg){tg.MainButton.hideProgress&&tg.MainButton.hideProgress();}
  if(r&&r.ok){ if(tg){tg.HapticFeedback&&tg.HapticFeedback.notificationOccurred("success");tg.close();}else alert("Started: "+r.parts+" parts"); }
  else showErr("Failed: "+((r&&r.error)||"unknown")+(r&&r.error==="busy"?" — pehle chal raha task khatam hone do.":""));
}
(function drag(){
  const strip=$("strip");let dragging=null,moved=false;
  function tAt(ev){const r=strip.getBoundingClientRect();return Math.min(1,Math.max(0,(ev.clientX-r.left)/r.width))*S.duration;}
  strip.addEventListener("pointerdown",ev=>{
    if(!S)return;strip.setPointerCapture(ev.pointerId);moved=false;
    const r=strip.getBoundingClientRect();let best=-1,bd=18;
    cuts.forEach((c,i)=>{const d=Math.abs(c/S.duration*r.width-(ev.clientX-r.left));if(d<bd){bd=d;best=i;}});
    dragging=best>=0?best:null;if(dragging===null)setCur(tAt(ev),true);
  });
  strip.addEventListener("pointermove",ev=>{
    if(!S||!(ev.buttons||ev.pressure))return;
    if(dragging===null){setCur(tAt(ev),true);return;}
    moved=true;let t=Math.round(tAt(ev)*10)/10;
    const lo=(dragging>0?cuts[dragging-1]:0)+MIN_GAP,hi=(dragging<cuts.length-1?cuts[dragging+1]:S.duration)-MIN_GAP;
    cuts[dragging]=Math.min(hi,Math.max(lo,t));setCur(cuts[dragging],true);
  });
  strip.addEventListener("pointerup",()=>{dragging=null;});
  strip.addEventListener("dblclick",ev=>{ // desktop: double-click a marker to delete it
    const m=ev.target.closest&&ev.target.closest(".mk");if(m){cuts.splice(+m.dataset.i,1);render();}
  });
})();
$("add").onclick=addCut;
$("undo").onclick=()=>{cuts.pop();showErr("");render();};
$("del").onclick=()=>{
  if(!cuts.length){showErr("Koi cut nahi hai.");return;}
  let bi=0;cuts.forEach((c,i)=>{if(Math.abs(c-cur)<Math.abs(cuts[bi]-cur))bi=i;});
  cuts.splice(bi,1);showErr("");render();
};
$("reset").onclick=()=>{cuts=[];showErr("");render();};
document.querySelectorAll("button[data-n]").forEach(b=>b.onclick=()=>autoN(+b.dataset.n));
$("fallback").onclick=send;
if(tg){tg.MainButton.onClick(send);}
(async function init(){
  let d;try{d=await api("/ma/api/session",{});}catch(e){d={ok:false,error:"network"};}
  if(!d||!d.ok){$("name").textContent="";showErr(d&&d.error==="no_source"?"Pehle bot ko video bhejo, phir Timeline Editor kholo.":"Open this page from Telegram ("+((d&&d.error)||"error")+").");return;}
  S=d;$("name").textContent=d.name;$("tot").textContent=fmt(d.duration);
  const th=$("thumbs");for(let i=0;i<d.thumbs;i++){const im=document.createElement("img");im.loading="lazy";im.src="/ma/thumb/"+d.token+"/"+i+".jpg";th.appendChild(im);}
  const v=$("v");v.src="/ma/video/"+d.token;
  v.addEventListener("timeupdate",()=>{if(!v.paused)setCur(v.currentTime,false);});
  v.addEventListener("error",()=>{v.style.display="none";$("nov").style.display="block";});
  render();
})();
</script></body></html>
"""


def _build_web_app():
    from flask import Flask, request, jsonify, send_file, Response
    web = Flask(__name__)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    @web.route("/")
    def _home():
        return "Bot is running!"

    @web.route("/healthz")
    def _health():
        return "ok"

    @web.route("/ma")
    def _ma_page():
        return Response(MINIAPP_HTML, mimetype="text/html", headers={"Cache-Control": "no-store"})

    def _auth(data) -> int | None:
        user = _tg_validate_init_data(str((data or {}).get("initData", "")))
        if not user:
            return None
        uid = int(user["id"])
        ok, _why = _access_ok(uid)
        return uid if ok else None

    def _source(uid: int):
        path = user_files.get(uid)
        if path and os.path.exists(path):
            return path
        return None

    @web.route("/ma/api/session", methods=["POST"])
    def _ma_session():
        data = request.get_json(silent=True) or {}
        uid = _auth(data)
        if uid is None:
            return jsonify(ok=False, error="auth"), 401
        path = _source(uid)
        if not path:
            return jsonify(ok=False, error="no_source")
        info = _run_on_loop(_probe_video(path))
        dur = info.get("duration") or 0
        if dur < 3:
            return jsonify(ok=False, error="too_short")
        _touch(uid)
        return jsonify(ok=True, name=os.path.basename(path), duration=float(dur), width=info.get("width"),
                       height=info.get("height"), thumbs=_MA_THUMBS, token=_ma_token(uid))

    @web.route("/ma/thumb/<token>/<int:i>.jpg")
    def _ma_thumb(token, i):
        uid = _ma_verify(token)
        path = _source(uid) if uid is not None else None
        if not path or not 0 <= i < _MA_THUMBS:
            return Response(status=404)
        info = _probe_cache.get(path)
        dur = (info[1].get("duration") if info else None) or _run_on_loop(_probe_video(path)).get("duration") or 0
        if dur <= 0:
            return Response(status=404)
        tag = hashlib.sha1(f"{path}{os.path.getmtime(path)}".encode()).hexdigest()[:8]
        out = f"{THUMB_DIR}/ma_{uid}_{tag}_{i}.jpg"
        if not os.path.exists(out):
            with _MA_THUMB_SEM:
                if not os.path.exists(out):
                    import subprocess
                    t = (i + 0.5) * dur / _MA_THUMBS
                    subprocess.run(["ffmpeg", "-y", "-v", "error", "-ss", f"{t:.2f}", "-i", path, "-frames:v", "1",
                                    "-vf", "scale=160:-2", "-q:v", "5", out],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=40)
        if not os.path.exists(out):
            return Response(status=404)
        return send_file(out, mimetype="image/jpeg", max_age=300)

    @web.route("/ma/video/<token>")
    def _ma_video(token):
        uid = _ma_verify(token)
        path = _source(uid) if uid is not None else None
        if not path:
            return Response(status=404)
        mime = "video/webm" if path.lower().endswith(".webm") else "video/mp4"
        return send_file(path, mimetype=mime, conditional=True, max_age=0)

    @web.route("/ma/api/split", methods=["POST"])
    def _ma_split():
        data = request.get_json(silent=True) or {}
        uid = _auth(data)
        if uid is None:
            return jsonify(ok=False, error="auth"), 401
        path = _source(uid)
        if not path:
            return jsonify(ok=False, error="no_source")
        try:
            raw = [float(c) for c in (data.get("cuts") or [])]
        except (TypeError, ValueError):
            return jsonify(ok=False, error="bad_cuts")
        info = _run_on_loop(_probe_video(path))
        dur = info.get("duration") or 0
        cuts: list[float] = []
        for c in sorted({round(c, 1) for c in raw}):
            if 0.9 < c < dur - 0.9 and (not cuts or c - cuts[-1] >= 0.9):
                cuts.append(c)
        if not cuts:
            return jsonify(ok=False, error="no_cuts")
        if len(cuts) > 99:
            return jsonify(ok=False, error="too_many")
        if _get_lock(uid).locked():
            return jsonify(ok=False, error="busy")
        if MAIN_LOOP is None or not MAIN_LOOP.is_running():
            return jsonify(ok=False, error="bot_not_ready")
        asyncio.run_coroutine_threadsafe(_ma_launch(uid, cuts), MAIN_LOOP)
        return jsonify(ok=True, parts=len(cuts) + 1)

    return web


def _start_web():
    """Render requires an open port; also hosts the Mini App when PUBLIC_URL is set."""
    port = int(os.environ.get("PORT", 10000))
    web = _build_web_app()
    threading.Thread(target=lambda: web.run(host="0.0.0.0", port=port, threaded=True), daemon=True).start()
    log.info(f"🌐 Web server on :{port}" + (f" · Mini App enabled at {PUBLIC_URL}/ma" if MINIAPP_ENABLED else ""))


async def _startup_recovery():
    try:
        jobs = [j for j in await _job_get_active() if j["kind"] in _RESUMABLE_KINDS]
        runnable = []
        for job in jobs:
            if job["attempts"] >= RECOVERY_MAX_ATTEMPTS:
                log.warning("Recovery: job %s failed %s times — marking failed (use /retry to force).",
                            job["job_id"], job["attempts"])
                await _job_update(job["job_id"], state="failed")
            else:
                runnable.append(job)
        if not runnable:
            return
        log.warning(f"♻️ Recovery: found {len(runnable)} unfinished job(s).")

        async def one(job):
            try:
                await _job_update(job["job_id"], state="recovering", attempts_inc=True)
                ok, why = await _resume_job_record(job)
                if not ok:
                    log.warning(f"Recovery skipped for {job['job_id']}: {why}")
            except Exception:
                log.exception("Recovery failed for %s", job["job_id"])
                try: await _job_update(job["job_id"], state="retry")
                except Exception: pass

        # every recovery waits for its turn in the global queue, so they never stampede the CPU
        await asyncio.gather(*(one(j) for j in runnable))
    except Exception:
        log.exception("Startup recovery failed")


async def _boot():
    """Runs once the event loop is live: housekeeping first, then recovery."""
    await asyncio.sleep(2)
    try:
        await _db_maintenance()
        protected = await _protected_paths()
        await asyncio.to_thread(_cleanup_stale_files, protected)
    except Exception:
        log.exception("boot housekeeping failed")
    asyncio.ensure_future(_janitor_loop())
    await asyncio.sleep(1)
    await _startup_recovery()


if __name__ == "__main__":
    log.info("⚡ Ultra Bot v11 starting…")
    log.info(f"   queue: {MAX_PARALLEL_JOBS} parallel job(s) · pipeline look-ahead: {PIPELINE_AHEAD} · "
             f"transmissions: {max(1, min(4, int(os.getenv('MAX_CONCURRENT_TRANSMISSIONS', '2'))))}")
    log.info(f"   DB: {'Postgres (pooled, max ' + str(DB_POOL_MAX) + ')' if _USE_PG else 'SQLite ' + RECOVERY_DB_PATH} · "
             f"access: {'PRIVATE' if PRIVATE_MODE else 'open'} · quota: {DAILY_QUOTA_MIN:g} min/day · source TTL: {SOURCE_TTL_MIN:g} min")
    loop = asyncio.get_event_loop()
    MAIN_LOOP = loop
    loop.run_until_complete(_db_init())
    _start_web()
    loop.create_task(_boot())
    app.run()
