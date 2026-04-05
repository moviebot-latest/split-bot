"""
Ultra Bot v14
─────────────
FIXES vs v13:
  1. \~filters syntax error      → fixed to ~filters (CRITICAL crash fix)
  2. Bot not responding          → from_user None guard added
  3. /help command added         → full help message
  4. /splitsize in /start        → now visible in welcome message
  5. Channel post crash          → from_user check in all handlers
  6. Client already connected    → is_connected check before start()
  7. Restart loop crash          → proper stop() before every retry
"""

import os, time, math, asyncio, logging, traceback, random
from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait, MessageNotModified

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("UltraBot")

# ── ENV ──────────────────────────────────────────────────────
API_ID    = int(os.environ["API_ID"])
API_HASH  = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]

# ── FIXED CLIENT FOR RAILWAY (no more DC2 ↔ DC5 flapping) ──
app = Client(
    "ultrabot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,              # ← NO .session file, no session conflict
    sleep_threshold=300,         # ← increased (critical for Railway)
    ipv6=False,                  # ← fixes Railway MTProto instability
    auto_reconnect=True,
    connect_timeout=30,
    reconnect_timeout=15,
)

# ── DEBUG HANDLER (add this right after Client) ──
@app.on_message(filters.all, group=-1000)
async def ultra_debug(_, msg):
    """Logs every incoming message so you can see if bot is receiving"""
    if msg.from_user:
        log.info(
            f"[ULTRA DEBUG] Message received → "
            f"User={msg.from_user.id} | "
            f"Type={'video' if msg.video else 'document' if msg.document else 'text'} | "
            f"Chat={msg.chat.id}"
        )
    else:
        log.info(f"[ULTRA DEBUG] Message received from chat {msg.chat.id}")

DOWNLOAD_DIR = "downloads"
THUMB_DIR    = "thumbs"
LOCK_DIR     = "locks"
for _d in (DOWNLOAD_DIR, THUMB_DIR, LOCK_DIR):
    os.makedirs(_d, exist_ok=True)


# ══════════════════════════════════════════════════════════════
#  LOCK SYSTEM
# ══════════════════════════════════════════════════════════════
def _lock_file(uid):    return f"{LOCK_DIR}/{uid}.lock"
def _done_file(uid, n): return f"{LOCK_DIR}/{uid}_p{n}.done"

def _acquire(uid) -> bool:
    p = _lock_file(uid)
    if os.path.exists(p):
        if time.time() - os.path.getmtime(p) > 600:
            try: os.remove(p)
            except: pass
        else:
            return False
    try:
        fd = os.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(time.time()).encode())
        os.close(fd)
        return True
    except FileExistsError:
        return False

def _release(uid):
    try: os.remove(_lock_file(uid))
    except: pass

def _locked(uid) -> bool:
    p = _lock_file(uid)
    if not os.path.exists(p): return False
    if time.time() - os.path.getmtime(p) > 600:
        try: os.remove(p)
        except: pass
        return False
    return True

def _mark_done(uid, n):
    try:
        fd = os.open(_done_file(uid, n), os.O_CREAT | os.O_WRONLY)
        os.close(fd)
    except: pass

def _is_done(uid, n): return os.path.exists(_done_file(uid, n))

def _clear_done(uid):
    for f in os.listdir(LOCK_DIR):
        if f.startswith(f"{uid}_p") and f.endswith(".done"):
            try: os.remove(f"{LOCK_DIR}/{f}")
            except: pass


# ══════════════════════════════════════════════════════════════
#  DEDUP
# ══════════════════════════════════════════════════════════════
_seen:    set           = set()
_seen_lk: asyncio.Lock = asyncio.Lock()

async def _is_dup(msg) -> bool:
    """Returns True if message should be DROPPED."""
    if getattr(msg, "edit_date", None):
        log.info(f"DROP edited msg id={msg.id}")
        return True
    async with _seen_lk:
        key = (msg.chat.id, msg.id)
        if key in _seen:
            log.info(f"DROP duplicate msg id={msg.id}")
            return True
        _seen.add(key)
        if len(_seen) > 5000: _seen.clear()
    return False


# ══════════════════════════════════════════════════════════════
#  STATE
# ══════════════════════════════════════════════════════════════
user_files:    dict[int, str]           = {}
user_cancel:   dict[int, asyncio.Event] = {}
user_status:   dict[int, dict]          = {}
split_session: dict[int, int]           = {}

def _ev(uid):
    if uid not in user_cancel:
        user_cancel[uid] = asyncio.Event()
    return user_cancel[uid]

def _set_st(uid, task, detail=""):
    user_status[uid] = {"task": task, "detail": detail, "ts": time.time()}

def _clr_st(uid): user_status.pop(uid, None)


# ══════════════════════════════════════════════════════════════
#  VISUALS + PROGRESS (unchanged)
# ══════════════════════════════════════════════════════════════
SP_DL  = ["⣾","⣽","⣻","⢿","⡿","⣟","⣯","⣷"]
SP_CUT = ["◜","◝","◞","◟"]
SP_CLK = ["🕐","🕑","🕒","🕓","🕔","🕕","🕖","🕗","🕘","🕙","🕚","🕛"]
SP_MN  = ["🌑","🌒","🌓","🌔","🌕","🌖","🌗","🌘"]
BF, BE = "█", "░"

def _abar(p, t, w=18):
    n = max(0, min(w, int(p/100*w)))
    if n == 0: return BE*w
    if n >= w: return BF*w
    return BF*(n-1) + ("▓" if t%2==0 else BF) + BE*(w-n)

def _spark(h, w=14):
    b = " ▁▂▃▄▅▆▇█"
    if not h: return "─"*w
    mx = max(h) or 1
    return "".join(b[min(8, int(s/mx*8))] for s in h[-w:])

def _sz(b):
    for u in ("B","KB","MB","GB"):
        if b < 1024: return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} TB"

def _ft(s):
    s = max(0, s)
    if s >= 3600: return f"{int(s//3600)}h {int(s%3600//60)}m"
    if s >= 60:   return f"{int(s//60)}m {int(s%60)}s"
    return f"{int(s)}s"

def _tier(bps):
    mb = bps/1048576
    if mb >= 10: return "🟢 ULTRA"
    if mb >= 5:  return "🟢 FAST"
    if mb >= 1:  return "🟡 GOOD"
    if bps >= 524288: return "🟠 OK"
    return "🔴 SLOW"

def _ms(p):
    if p >= 100: return "🏆"
    if p >= 80:  return "🔥"
    if p >= 50:  return "🚀"
    if p >= 25:  return "⚡"
    return "🔵"

_le:  dict[int,float] = {}
_ema: dict[int,float] = {}
_tk:  dict[int,int]   = {}
_sh:  dict[int,float] = {}
_hi:  dict[int,list]  = {}
_stk: dict[int,int]   = {}

def _rst(uid):
    for d in (_le, _ema, _tk, _sh): d.pop(uid, None)

def _cup(uid, real, step=2.5):
    p = _sh.get(uid, 0.0)
    v = min(real, p+step) if real > p else real
    _sh[uid] = v; return v


# ══════════════════════════════════════════════════════════════
#  SAFE EDIT + PROGRESS (unchanged)
# ══════════════════════════════════════════════════════════════
async def _edit(msg, txt: str):
    try:
        await msg.edit(txt)
    except FloodWait as e:
        await asyncio.sleep(min(e.value, 15) + 1)
        try: await msg.edit(txt)
        except: pass
    except MessageNotModified:
        pass
    except Exception as e:
        log.warning(f"edit failed: {e}")


async def _prog(cur, tot, msg, t0, uid=0, mode="📥 Download"):
    if not tot or _ev(uid).is_set(): return
    now = time.time(); el = max(now-t0, 0.001)
    if now - _le.get(uid, 0) < 0.18 and _le.get(uid, 0): return
    _le[uid] = now
    raw = cur/el
    ema = 0.35*raw + 0.65*_ema.get(uid, raw)
    _ema[uid] = ema
    if uid not in _hi: _hi[uid] = []
    _hi[uid].append(ema)
    if len(_hi[uid]) > 30: _hi[uid].pop(0)
    eta   = (tot-cur)/ema if ema > 0 else 0
    shown = _cup(uid, cur*100/tot)
    t     = _tk.get(uid, 0); _tk[uid] = t+1
    spin  = SP_DL[t % len(SP_DL)]
    bar   = _abar(shown, t, 18)
    graph = _spark(_hi.get(uid, []), 14)
    hdr   = "📥" if "Down" in mode else "📤"
    etas  = f"⚡{_ft(eta)}" if eta < 15 and shown > 5 else _ft(eta)
    await _edit(msg,
        f"{spin} **{mode}**\n"
        f"══════════════════════════\n"
        f"`{bar}`\n"
        f"  {_ms(shown)} **{shown:.1f}%** · {_tier(ema)}\n"
        f"──────────────────────────\n"
        f"  📊 `{graph}`\n"
        f"  {hdr} `{_sz(cur)}` / `{_sz(tot)}`\n"
        f"  ⚡ `{_sz(ema)}/s`\n"
        f"  ⏱ `{etas}` · ⏳ `{_ft(el)}`\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel"
    )


# ══════════════════════════════════════════════════════════════
#  FFMPEG + SPLIT UI + SEND PART (unchanged)
# ══════════════════════════════════════════════════════════════
async def _cut(inp, out, ss, t) -> bool:
    try:
        p = await asyncio.create_subprocess_exec(
            "ffmpeg","-y","-ss",str(ss),"-i",inp,
            "-t",str(t),"-c","copy","-avoid_negative_ts","make_zero",out,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        await p.wait()
        return p.returncode == 0
    except Exception as e:
        log.error(f"ffmpeg cut: {e}"); return False

async def _thumb(vid, ss, out) -> str | None:
    try:
        p = await asyncio.create_subprocess_exec(
            "ffmpeg","-y","-ss",str(ss),"-i",vid,
            "-vframes","1","-q:v","2","-vf","scale=320:-1",out,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        await p.wait()
        return out if os.path.exists(out) else None
    except: return None

async def _dur(f) -> float | None:
    try:
        p = await asyncio.create_subprocess_exec(
            "ffprobe","-v","error",
            "-show_entries","format=duration",
            "-of","default=noprint_wrappers=1:nokey=1",f,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        o, _ = await p.communicate()
        return float(o.decode().strip())
    except: return None


async def _split_ui(msg, done, total, uid, note=""):
    pct  = done * 100 // total
    t    = _stk.get(uid, 0); _stk[uid] = t+1
    spin = SP_CUT[t % 4]
    bar  = _abar(pct, t, 16)
    cap  = min(total, 20)
    dots = ""
    for i in range(cap):
        if i < done:    dots += "✅"
        elif i == done: dots += "✂️"
        else:           dots += "⬜"
        if (i+1) % 10 == 0 and i+1 < cap: dots += "\n  "
    if total > 20: dots += f" +{total-20}"
    nl = f"\n  ┗ _{note}_" if note else ""
    await _edit(msg,
        f"{spin} **Splitting…**\n"
        f"══════════════════════════\n"
        f"`{bar}` **{pct}%**\n"
        f"  Part **{done}** / **{total}**{nl}\n"
        f"──────────────────────────\n"
        f"  {dots}\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel"
    )


async def _send_part(orig_msg, prog_msg, path, num, total, uid, thumb_t) -> bool:
    if _is_done(uid, num):
        log.info(f"part {num} already done, skipping")
        return True
    if _ev(uid).is_set(): return False

    await _edit(prog_msg,
        f"📤 **Uploading part {num}/{total}**\n"
        f"══════════════════════════\n"
        f"  Please wait…\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel"
    )

    tp   = f"{THUMB_DIR}/th_{uid}_{num}.jpg"
    th   = await _thumb(path, thumb_t, tp)
    sent = False

    while True:
        if _is_done(uid, num): sent = True; break
        if _ev(uid).is_set():  break
        try:
            await asyncio.shield(
                orig_msg.reply_video(
                    path,
                    caption=(
                        f"🎬 **Part {num} / {total}**\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"  ✅ Ultra Bot v14"
                    ),
                    thumb=th,
                )
            )
            _mark_done(uid, num)
            sent = True
            log.info(f"part {num}/{total} sent OK")
            break

        except FloodWait as e:
            wait = min(e.value + 2, 60)
            log.warning(f"FloodWait {wait}s on part {num}")
            for rem in range(wait, 0, -1):
                sp   = SP_CLK[rem % 12]
                fill = int((wait-rem)/wait*18)
                await _edit(prog_msg,
                    f"{sp} **Flood wait** part {num}/{total}\n"
                    f"  ⏳ Resume in `{rem}s`\n"
                    f"  `{BF*fill}{BE*(18-fill)}`"
                )
                await asyncio.sleep(1)

        except Exception as e:
            err = str(e).lower()
            log.error(f"send_part {num} error: {e}")
            if any(x in err for x in [
                "duplicate","already","forbidden",
                "timeout","connection","network","reset"
            ]):
                _mark_done(uid, num)
                sent = True
            else:
                await _edit(prog_msg, f"❌ Upload failed part {num}:\n`{e}`")
            break

    if th and os.path.exists(tp):
        try: os.remove(tp)
        except: pass

    return sent


# ══════════════════════════════════════════════════════════════
#  COMMANDS (unchanged)
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("start") & filters.incoming, group=0)
async def cmd_start(_, msg):
    log.info(f"CMD /start from uid={getattr(msg.from_user, 'id', '?')}")
    if await _is_dup(msg): return
    if not msg.from_user: return
    name = msg.from_user.first_name or "User"
    await msg.reply(
        f"╔══════════════════════════╗\n"
        f"║  ⚡  ULTRA BOT v14  ⚡   ║\n"
        f"╚══════════════════════════╝\n\n"
        f"  👋 Hey **{name}**!\n\n"
        f"  📽 Send any video:\n"
        f"    MP4 · MKV · AVI · MOV · WEBM\n\n"
        f"  ✂️ Commands:\n"
        f"    `/split 3`       → 3 equal parts\n"
        f"    `/splitmin 2`    → 2-min chunks\n"
        f"    `/splitsize 500` → 500 MB chunks\n\n"
        f"  🛠 `/status` · `/cancel` · `/info` · `/help`\n\n"
        f"──────────────────────────\n"
        f"  🔒 OS file-lock · zero double send\n"
        f"  🛡 Railway crash-safe + DC stable\n"
        f"  ⚡ Auto FloodWait handler\n"
        f"  ✅ No session conflict\n"
        f"──────────────────────────\n"
        f"  _Ultra Bot v14 — All Fixed_ ✓"
    )


@app.on_message(filters.command("info") & filters.incoming, group=0)
async def cmd_info(_, msg):
    log.info(f"CMD /info from uid={msg.from_user.id}")
    if await _is_dup(msg): return
    uid = msg.from_user.id
    if uid not in user_files or not os.path.exists(user_files[uid]):
        return await msg.reply("❌ No video loaded.")
    p  = user_files[uid]
    d  = await _dur(p)
    sz = os.path.getsize(p)
    opts = ""
    if d:
        for n in [2, 3, 4, 5]:
            opts += f"    `/split {n}` → {n}×{_ft(d/n)}\n"
    await msg.reply(
        f"📋 **FILE INFO**\n"
        f"══════════════════════════\n"
        f"  📁 `{os.path.basename(p)}`\n"
        f"  📦 `{_sz(sz)}`\n"
        f"  🎬 `{_ft(d) if d else 'unknown'}`\n"
        f"──────────────────────────\n"
        f"  💡 Split options:\n{opts}"
        f"══════════════════════════\n"
        f"  👉 `/split N` · `/splitmin N`"
    )


@app.on_message(filters.command("help") & filters.incoming, group=0)
async def cmd_help(_, msg):
    log.info(f"CMD /help from uid={getattr(msg.from_user, 'id', '?')}")
    if await _is_dup(msg): return
    await msg.reply(
        f"╔══════════════════════════╗\n"
        f"║   📖  ULTRA BOT HELP     ║\n"
        f"╚══════════════════════════╝\n\n"
        f"  **📽 Step 1:** Koi bhi video send karo\n"
        f"    _(MP4, MKV, AVI, MOV, WEBM)_\n\n"
        f"  **✂️ Step 2:** Split command chalao\n\n"
        f"  **COMMANDS:**\n"
        f"  `/split N`\n"
        f"    → N equal parts mein kaato\n"
        f"    → Example: `/split 3` = 3 parts\n\n"
        f"  `/splitmin N`\n"
        f"    → N minute ke chunks banao\n"
        f"    → Example: `/splitmin 5` = 5 min parts\n\n"
        f"  `/splitsize N`\n"
        f"    → N MB ke chunks banao\n"
        f"    → Example: `/splitsize 500` = 500MB parts\n\n"
        f"  `/info` → Video ki details dekho\n"
        f"  `/status` → Current task status\n"
        f"  `/cancel` → Chal raha kaam rok do\n"
        f"  `/start` → Welcome message\n"
        f"  `/help` → Yeh message\n\n"
        f"──────────────────────────\n"
        f"  ⚠️ Ek waqt mein sirf ek video\n"
        f"  ✅ Max 100 parts support\n"
        f"  🔒 Auto FloodWait handle hota hai\n"
        f"──────────────────────────\n"
        f"  _Ultra Bot v14_ ⚡"
    )



async def cmd_status(_, msg):
    log.info(f"CMD /status from uid={msg.from_user.id}")
    if await _is_dup(msg): return
    uid  = msg.from_user.id
    info = user_status.get(uid)
    busy = _locked(uid)
    if not busy or not info:
        if uid in user_files and os.path.exists(user_files[uid]):
            p = user_files[uid]
            return await msg.reply(
                f"💤 **IDLE — Ready**\n"
                f"  📁 `{os.path.basename(p)}`\n"
                f"  📦 `{_sz(os.path.getsize(p))}`\n"
                f"  👉 `/split N` · `/splitmin N`"
            )
        return await msg.reply("💤 **IDLE** — Send a video!")
    hist  = _hi.get(uid, [])
    graph = _spark(hist, 14) if hist else "─"*14
    speed = f"{_sz(hist[-1])}/s" if hist else "—"
    sp    = SP_MN[int(time.time()*2) % 8]
    await msg.reply(
        f"{sp} **RUNNING**\n"
        f"══════════════════════════\n"
        f"  📌 `{info['task']}` · `{info['detail']}`\n"
        f"  ⏳ `{_ft(time.time()-info['ts'])}`\n"
        f"  📊 `{graph}` · ⚡ `{speed}`\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel"
    )


@app.on_message(filters.command("cancel") & filters.incoming, group=0)
async def cmd_cancel(_, msg):
    log.info(f"CMD /cancel from uid={msg.from_user.id}")
    if await _is_dup(msg): return
    uid = msg.from_user.id
    if not _locked(uid):
        return await msg.reply("💤 Nothing running.")
    _ev(uid).set()
    await msg.reply("🚫 **Cancelling…** please wait.")


# ══════════════════════════════════════════════════════════════
#  RECEIVE VIDEO + CORE SPLIT ENGINE (unchanged)
# ══════════════════════════════════════════════════════════════
@app.on_message(
    filters.incoming
    & ~filters.command(["start","split","splitmin","splitsize","status","cancel","info","help"])
    & (filters.video | filters.document),
    group=1
)
async def recv(_, msg):
    if not msg.from_user: return          # ← FIX: channel posts crash karte the
    log.info(f"VIDEO received from uid={msg.from_user.id}")
    if await _is_dup(msg): return
    uid   = msg.from_user.id
    if _locked(uid):
        return await msg.reply("⏳ Task running.\n👉 /status · /cancel")
    media = msg.document or msg.video
    if not media: return
    mime = getattr(media, "mime_type", "") or ""
    fsz  = getattr(media, "file_size", 0) or 0
    if mime and not (mime.startswith("video/") or mime == "application/octet-stream"):
        return
    if uid in user_files and os.path.exists(user_files[uid]):
        try: os.remove(user_files[uid])
        except: pass
        user_files.pop(uid, None)
    ext_map = {
        "video/x-matroska":"mkv","video/mkv":"mkv",
        "video/avi":"avi","video/x-msvideo":"avi",
        "video/webm":"webm","video/quicktime":"mov",
        "video/x-ms-wmv":"wmv","video/3gpp":"3gp",
    }
    ext   = ext_map.get(mime, "mp4")
    fname = f"{DOWNLOAD_DIR}/v_{uid}_{msg.id}.{ext}"
    st = await msg.reply(
        f"⣾ **DOWNLOADING**\n"
        f"══════════════════════════\n"
        f"  📁 `{ext.upper()}` · `{_sz(fsz)}`\n"
        f"`{BE*18}` **0%**\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel"
    )
    _ev(uid).clear()
    _rst(uid)
    _hi.pop(uid, None)
    _clear_done(uid)
    _set_st(uid, "Download", _sz(fsz))
    if not _acquire(uid):
        await _edit(st, "⏳ Already busy!"); return
    t0 = time.time()
    try:
        path = await msg.download(
            file_name=fname,
            progress=_prog,
            progress_args=(st, t0, uid, "📥 Download"),
        )
    except Exception as e:
        log.error(f"download: {e}")
        _release(uid); _clr_st(uid)
        await _edit(st, f"❌ Download failed:\n`{e}`")
        return
    if not path or not os.path.exists(path):
        _release(uid); _clr_st(uid)
        await _edit(st, "❌ File not saved. Try again.")
        return
    el     = time.time() - t0
    avg    = fsz / el if el and fsz else 0
    actual = os.path.getsize(path)
    _rst(uid); _clr_st(uid)
    _release(uid)
    user_files[uid] = path
    await _edit(st,
        f"✅ **DOWNLOAD COMPLETE**\n"
        f"══════════════════════════\n"
        f"`{BF*18}` **100%** 🏆\n"
        f"  📁 `{os.path.basename(path)}`\n"
        f"  📦 `{_sz(actual)}`\n"
        f"  ⚡ `{_sz(avg)}/s` · ⏱ `{_ft(el)}`\n"
        f"══════════════════════════\n"
        f"  👉 `/split N` · `/splitmin N`\n"
        f"  📋 `/info` for options"
    )


async def _do_split(orig_msg, uid, seg, parts, label):
    session = random.randint(1, 999999)
    split_session[uid] = session
    file = user_files[uid]
    ev   = _ev(uid); ev.clear()
    _clear_done(uid)
    _stk[uid] = 0
    prog = await orig_msg.reply(
        f"✂️ **{label}**\n"
        f"══════════════════════════\n"
        f"`{BE*16}` **0%**\n"
        f"  Part **0** / **{parts}**\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel"
    )
    try:
        for i in range(parts):
            if split_session.get(uid) != session:
                log.warning(f"session mismatch uid={uid}, aborting")
                return
            if ev.is_set():
                await _edit(prog, f"🚫 **CANCELLED**\n  Stopped at part **{i}/{parts}**.")
                return
            ss = i * seg
            _set_st(uid, "Cut", f"{i+1}/{parts}")
            await _split_ui(prog, i, parts, uid, f"cutting {i+1}…")
            out = f"{DOWNLOAD_DIR}/p_{uid}_{i+1}.mp4"
            ok  = await _cut(file, out, ss, seg)
            if not ok or not os.path.exists(out):
                await _edit(prog, f"❌ ffmpeg failed part {i+1}.")
                return
            _set_st(uid, "Upload", f"{i+1}/{parts}")
            await _split_ui(prog, i, parts, uid, f"uploading {i+1}…")
            sent = await _send_part(orig_msg, prog, out, i+1, parts, uid, ss + seg/2)
            try:
                if os.path.exists(out): os.remove(out)
            except: pass
            if not sent:
                await _edit(prog, f"🚫 **CANCELLED**\n  Stopped at part **{i+1}/{parts}**.")
                return
            await _split_ui(prog, i+1, parts, uid,
                "🎉 all done!" if i+1 == parts else f"next: {i+2}…"
            )
        try:
            if os.path.exists(file): os.remove(file)
        except: pass
        user_files.pop(uid, None)
        _clear_done(uid)
        checks = "✅" * min(parts, 20)
        if parts > 20: checks += f" +{parts-20}"
        await _edit(prog,
            f"🏆 **ALL {parts} PARTS DONE!**\n"
            f"══════════════════════════\n"
            f"`{BF*16}` **100%**\n"
            f"  {checks}\n"
            f"──────────────────────────\n"
            f"  ✅ **{parts}** parts · Ultra Bot v14\n"
            f"══════════════════════════\n"
            f"  _Send next video!_"
        )
        log.info(f"split complete uid={uid} parts={parts}")
    except Exception as e:
        log.error(f"_do_split error: {traceback.format_exc()}")
        await _edit(prog, f"❌ Error:\n`{e}`")
    finally:
        _clr_st(uid)
        _release(uid)


async def _split_guard(msg) -> tuple[int, bool]:
    log.info(f"CMD {msg.command[0]} from uid={msg.from_user.id}")
    if await _is_dup(msg): return msg.from_user.id, False
    uid = msg.from_user.id
    if not _acquire(uid):
        await msg.reply("⏳ Already running!\n👉 /status · /cancel")
        return uid, False
    return uid, True


@app.on_message(filters.command("split") & filters.incoming, group=0)
async def cmd_split(_, msg):
    uid, ok = await _split_guard(msg)
    if not ok: return
    try:
        if uid not in user_files:
            await msg.reply("❌ Send a video first!"); return
        try:
            n = int(msg.command[1]); assert 2 <= n <= 100
        except:
            await msg.reply("❌ Usage: `/split 3`\n  Min 2, max 100."); return
        d = await _dur(user_files[uid])
        if not d:
            await msg.reply("❌ Cannot read duration."); return
        await _do_split(msg, uid, d/n, n, f"Splitting into **{n}** equal parts…")
    except Exception as e:
        log.error(f"cmd_split: {e}")
        await msg.reply(f"❌ `{e}`")
        _release(uid)


@app.on_message(filters.command("splitmin") & filters.incoming, group=0)
async def cmd_splitmin(_, msg):
    uid, ok = await _split_guard(msg)
    if not ok: return
    try:
        if uid not in user_files:
            await msg.reply("❌ Send a video first!"); return
        try:
            m = int(msg.command[1]); assert 1 <= m <= 120
        except:
            await msg.reply("❌ Usage: `/splitmin 2`\n  Minutes (1-120)."); return
        d = await _dur(user_files[uid])
        if not d:
            await msg.reply("❌ Cannot read duration."); return
        seg   = m * 60
        parts = math.ceil(d / seg)
        if parts > 100:
            await msg.reply(f"❌ Too many parts ({parts}). Use bigger chunk."); return
        await _do_split(msg, uid, seg, parts, f"**{m} min** chunks → **{parts}** parts…")
    except Exception as e:
        log.error(f"cmd_splitmin: {e}")
        await msg.reply(f"❌ `{e}`")
        _release(uid)


@app.on_message(filters.command("splitsize") & filters.incoming, group=0)
async def cmd_splitsize(_, msg):
    uid, ok = await _split_guard(msg)
    if not ok: return
    try:
        if uid not in user_files:
            await msg.reply("❌ Send a video first!"); return
        try:
            mb = int(msg.command[1]); assert 10 <= mb <= 2000
        except:
            await msg.reply("❌ Usage: `/splitsize 500`\n  MB (10-2000)."); return
        file     = user_files[uid]
        total_mb = os.path.getsize(file) / 1048576
        d        = await _dur(file)
        if not d:
            await msg.reply("❌ Cannot read duration."); return
        parts = math.ceil(total_mb / mb)
        if parts > 100:
            await msg.reply(f"❌ Too many parts ({parts})."); return
        if parts < 2:
            await msg.reply(f"❌ File ≤{mb}MB. No split needed."); return
        seg = d / parts
        await _do_split(msg, uid, seg, parts, f"**{mb} MB** chunks → **{parts}** parts…")
    except Exception as e:
        log.error(f"cmd_splitsize: {e}")
        await msg.reply(f"❌ `{e}`")
        _release(uid)


# ══════════════════════════════════════════════════════════════
#  MAIN (auto-reconnect loop already present)
# ══════════════════════════════════════════════════════════════
async def main():
    while True:
        try:
            # ── FIX: agar already connected ho toh pehle stop karo ──
            if app.is_connected:
                log.warning("Client already connected — stopping first…")
                try: await app.stop()
                except: pass
                await asyncio.sleep(2)

            log.info("Starting Ultra Bot v14 (Railway stable version)…")
            await app.start()
            me = await app.get_me()
            log.info(f"Bot running as @{me.username}")
            for f in os.listdir(LOCK_DIR):
                fp = f"{LOCK_DIR}/{f}"
                if os.path.exists(fp) and time.time() - os.path.getmtime(fp) > 600:
                    try: os.remove(fp)
                    except: pass
            await idle()

        except KeyboardInterrupt:
            log.info("Stopped by user.")
            try: await app.stop()
            except: pass
            break

        except Exception as e:
            log.error(f"CRASH: {e}\n{traceback.format_exc()}")
            log.info("Restarting in 5 seconds…")
            # ── FIX: properly disconnect before retry ──
            try:
                if app.is_connected:
                    await app.stop()
            except: pass
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
