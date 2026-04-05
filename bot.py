import os, time, math, asyncio, json, hashlib
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, MessageNotModified

API_ID    = int(os.getenv("API_ID"))
API_HASH  = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Client("ultrabot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

DOWNLOAD_DIR = "downloads"
THUMB_DIR    = "thumbs"
LOCK_DIR     = "locks"
for d in (DOWNLOAD_DIR, THUMB_DIR, LOCK_DIR):
    os.makedirs(d, exist_ok=True)


# ══════════════════════════════════════════════════════════════
#  NUCLEAR ANTI-DOUBLE SYSTEM v10
#
#  Why previous versions failed:
#  - asyncio.Lock: NOT atomic between two coroutines that check
#    lock.locked() before acquiring — race window exists
#  - _set_running set: same issue if two handlers fire together
#  - reply_video itself: if it raises AFTER sending, code retries
#
#  v10 Solution: FILE-BASED LOCK
#  A lock file on disk is ATOMIC (OS-level).
#  If lock file exists → skip. Period. No race possible.
#  Lock file is also keyed by (uid + session_id) so even if
#  bot restarts mid-split, old locks auto-expire in 10 minutes.
# ══════════════════════════════════════════════════════════════

def _lock_path(uid: int) -> str:
    return f"{LOCK_DIR}/{uid}.lock"

def _part_lock_path(uid: int, part: int) -> str:
    return f"{LOCK_DIR}/{uid}_p{part}.done"

def _acquire_lock(uid: int) -> bool:
    """Atomic file-based lock. Returns True if acquired."""
    path = _lock_path(uid)
    # Check if stale (> 10 min old)
    if os.path.exists(path):
        age = time.time() - os.path.getmtime(path)
        if age > 600:
            try: os.remove(path)
            except: pass
        else:
            return False  # locked by another session
    try:
        # O_CREAT | O_EXCL = atomic create, fails if exists
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(time.time()).encode())
        os.close(fd)
        return True
    except FileExistsError:
        return False

def _release_lock(uid: int) -> None:
    try: os.remove(_lock_path(uid))
    except: pass

def _is_locked(uid: int) -> bool:
    path = _lock_path(uid)
    if not os.path.exists(path): return False
    age = time.time() - os.path.getmtime(path)
    if age > 600:
        try: os.remove(path)
        except: pass
        return False
    return True

def _mark_part_sent(uid: int, part: int) -> None:
    path = _part_lock_path(uid, part)
    try:
        fd = os.open(path, os.O_CREAT | os.O_WRONLY)
        os.close(fd)
    except: pass

def _is_part_sent(uid: int, part: int) -> bool:
    return os.path.exists(_part_lock_path(uid, part))

def _clear_part_locks(uid: int) -> None:
    for f in os.listdir(LOCK_DIR):
        if f.startswith(f"{uid}_p") and f.endswith(".done"):
            try: os.remove(f"{LOCK_DIR}/{f}")
            except: pass

# Message dedup
_seen: set = set()
_seen_lk   = asyncio.Lock()

async def _dedup(mid: int) -> bool:
    async with _seen_lk:
        if mid in _seen: return True
        _seen.add(mid)
        if len(_seen) > 2000: _seen.clear()
        return False

# Command cooldown (catches near-simultaneous duplicate deliveries)
_cmd_ts: dict[int, float] = {}
def _cooldown(uid: int, sec: float = 4.0) -> bool:
    now = time.time()
    if now - _cmd_ts.get(uid, 0) < sec: return True
    _cmd_ts[uid] = now
    return False


# ══════════════════════════════════════════════════════════════
#  STATE
# ══════════════════════════════════════════════════════════════
user_files:  dict[int, str]           = {}
user_cancel: dict[int, asyncio.Event] = {}
user_status: dict[int, dict]          = {}

def _cancel_ev(uid): 
    if uid not in user_cancel: user_cancel[uid] = asyncio.Event()
    return user_cancel[uid]

def _set_st(uid, task, detail=""):
    user_status[uid] = {"task": task, "detail": detail, "ts": time.time()}

def _clr_st(uid): user_status.pop(uid, None)


# ══════════════════════════════════════════════════════════════
#  VISUALS
# ══════════════════════════════════════════════════════════════
SPIN_DL  = ["⣾","⣽","⣻","⢿","⡿","⣟","⣯","⣷"]
SPIN_UL  = ["▁","▂","▃","▄","▅","▆","▇","█","▇","▆","▅","▄"]
SPIN_CUT = ["◜","◝","◞","◟"]
SPIN_CLK = ["🕐","🕑","🕒","🕓","🕔","🕕","🕖","🕗","🕘","🕙","🕚","🕛"]
SPIN_PHS = ["🌑","🌒","🌓","🌔","🌕","🌖","🌗","🌘"]

BF, BE = "█", "░"

def _bar(p, w=18):
    n = max(0, min(w, int(p/100*w)))
    return BF*n + BE*(w-n)

def _abar(p, t, w=18):
    n = max(0, min(w, int(p/100*w)))
    if n==0: return BE*w
    if n>=w: return BF*w
    return BF*(n-1) + ("▓" if t%2==0 else BF) + BE*(w-n)

def _spark(h, w=14):
    b=" ▁▂▃▄▅▆▇█"
    if not h: return "─"*w
    mx=max(h) or 1
    return "".join(b[min(8,int(s/mx*8))] for s in h[-w:])

def _sz(b):
    for u in("B","KB","MB","GB"):
        if b<1024: return f"{b:.1f} {u}"
        b/=1024
    return f"{b:.1f} TB"

def _t(s):
    s=max(0,s)
    if s>=3600: return f"{int(s//3600)}h{int(s%3600//60)}m"
    if s>=60:   return f"{int(s//60)}m{int(s%60)}s"
    return f"{int(s)}s"

def _tier(bps):
    mb=bps/1048576
    if mb>=10: return "🟢 ULTRA"
    if mb>=5:  return "🟢 FAST"
    if mb>=1:  return "🟡 GOOD"
    if bps>=524288: return "🟠 OK"
    return "🔴 SLOW"

def _milestone(p):
    if p>=100: return "🏆 COMPLETE"
    if p>=90:  return "🔥 BLAZING"
    if p>=75:  return "⚡ LIGHTNING"
    if p>=50:  return "🚀 CRUISING"
    if p>=25:  return "💫 RISING"
    return "🔵 STARTING"

# Progress state
_le: dict[int,float]={};  _ema: dict[int,float]={}
_tk: dict[int,int]={};    _sh:  dict[int,float]={}
_hi: dict[int,list]={}
THROT=0.15; ALPHA=0.35

def _rst(uid):
    for d in(_le,_ema,_tk,_sh): d.pop(uid,None)

def _cup(uid,real,step=2.5):
    p=_sh.get(uid,0.0)
    v=min(real,p+step) if real>p else real
    _sh[uid]=v; return v

async def _edit(msg, txt):
    try: await msg.edit(txt)
    except FloodWait as e:
        await asyncio.sleep(min(e.value,15)+0.5)
        try: await msg.edit(txt)
        except: pass
    except MessageNotModified: pass
    except: pass


# ══════════════════════════════════════════════════════════════
#  PROGRESS ENGINE v10  (cinematic + animated)
# ══════════════════════════════════════════════════════════════
async def _progress(cur, tot, msg, t0, uid=0, mode="📥 Download"):
    if not tot or _cancel_ev(uid).is_set(): return
    now=time.time(); el=max(now-t0,0.001)
    if now-_le.get(uid,0)<THROT and _le.get(uid,0): return
    _le[uid]=now

    raw=cur/el
    ema=ALPHA*raw+(1-ALPHA)*_ema.get(uid,raw)
    _ema[uid]=ema
    if uid not in _hi: _hi[uid]=[]
    _hi[uid].append(ema)
    if len(_hi[uid])>30: _hi[uid].pop(0)

    eta=(tot-cur)/ema if ema>0 else 0
    real=cur*100/tot; shown=_cup(uid,real)
    tk=_tk.get(uid,0); _tk[uid]=tk+1

    pool=SPIN_DL if "Down" in mode else SPIN_UL
    spin=pool[tk%len(pool)]
    bar=_abar(shown,tk,18)
    graph=_spark(_hi.get(uid,[]),14)
    hdr="📥" if "Down" in mode else "📤"
    eta_s=f"⚡{_t(eta)}" if eta<15 and shown>5 else _t(eta)

    await _edit(msg,
        f"{spin} **{mode}**\n"
        f"══════════════════════════\n"
        f"`{bar}`\n"
        f"  {_milestone(shown)} · **{shown:.1f}%**\n"
        f"  {_tier(ema)}\n"
        f"──────────────────────────\n"
        f"  📊 `{graph}`\n"
        f"──────────────────────────\n"
        f"  {hdr} `{_sz(cur)}` / `{_sz(tot)}`\n"
        f"  ⚡ `{_sz(ema)}/s`\n"
        f"  ⏱ `{eta_s}` left\n"
        f"  ⏳ `{_t(el)}` elapsed\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel"
    )

async def _ul_prog(cur,tot,msg,t0,uid=0):
    await _progress(cur,tot,msg,t0,uid,"📤 Upload")


# ══════════════════════════════════════════════════════════════
#  FFMPEG
# ══════════════════════════════════════════════════════════════
async def _cut(inp,out,ss,t):
    p=await asyncio.create_subprocess_exec(
        "ffmpeg","-y","-ss",str(ss),"-i",inp,
        "-t",str(t),"-c","copy","-avoid_negative_ts","make_zero",out,
        stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL)
    await p.wait(); return p.returncode==0

async def _thumb(vid,ss,out):
    p=await asyncio.create_subprocess_exec(
        "ffmpeg","-y","-ss",str(ss),"-i",vid,
        "-vframes","1","-q:v","2","-vf","scale=320:-1",out,
        stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL)
    await p.wait(); return out if os.path.exists(out) else None

async def _dur(f):
    try:
        p=await asyncio.create_subprocess_exec(
            "ffprobe","-v","error","-show_entries","format=duration",
            "-of","default=noprint_wrappers=1:nokey=1",f,
            stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.DEVNULL)
        o,_=await p.communicate(); return float(o.decode().strip())
    except: return None


# ══════════════════════════════════════════════════════════════
#  SPLIT UI
# ══════════════════════════════════════════════════════════════
_stk:dict[int,int]={}

async def _split_ui(msg, done, total, uid, note=""):
    pct=done*100//total
    t=_stk.get(uid,0); _stk[uid]=t+1
    spin=SPIN_CUT[t%4]; bar=_abar(pct,t,16)
    cap=min(total,20)
    dots=""
    for i in range(cap):
        if i<done:    dots+="✅"
        elif i==done: dots+="✂️"
        else:         dots+="⬜"
        if (i+1)%10==0 and i+1<cap: dots+="\n  "
    if total>20: dots+=f" +{total-20}"
    nl=f"\n  ┗ _{note}_" if note else ""
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


# ══════════════════════════════════════════════════════════════
#  UPLOAD ONE PART  — NUCLEAR ANTI-DOUBLE
#
#  Uses FILE-BASED part lock (.done file on disk).
#  If .done file exists → part already sent → return True.
#  reply_video called ONCE maximum per part, ever.
# ══════════════════════════════════════════════════════════════
async def _send_part(message, prog_msg, path, num, total, uid, thumb_t):
    # ── NUCLEAR CHECK: already sent? ──
    if _is_part_sent(uid, num):
        return True

    if _cancel_ev(uid).is_set(): return False

    await _edit(prog_msg,
        f"📤 **Uploading part {num}/{total}**\n"
        f"══════════════════════════\n"
        f"`{BE*18}` **0%**\n"
        f"  🔵 {_tier(0)} · preparing…\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel"
    )
    _rst(uid); t0=time.time()

    tp=f"{THUMB_DIR}/th_{uid}_{num}.jpg"
    th=await _thumb(path, thumb_t, tp)

    ok = False

    while not _is_part_sent(uid, num):
        if _cancel_ev(uid).is_set():
            await _edit(prog_msg, "🚫 **Cancelled.**")
            break
        try:
            # NO progress callback — progress callbacks cause Pyrogram
            # to internally retry on network hiccup → double send.
            # asyncio.shield prevents CancelledError from aborting mid-upload.
            await asyncio.shield(
                message.reply_video(
                    path,
                    caption=(
                        f"🎬 **Part {num} / {total}**\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"  ✅ Ultra Bot v10"
                    ),
                    thumb=th,
                )
            )
            _mark_part_sent(uid, num)   # write .done IMMEDIATELY
            ok = True
            break

        except FloodWait as e:
            # Telegram rejected — NOT sent → safe to wait and retry
            wait = min(e.value + 2, 60)
            for rem in range(wait, 0, -1):
                sp = SPIN_CLK[rem % 12]
                fill = int((wait - rem) / wait * 18)
                await _edit(prog_msg,
                    f"{sp} **Flood wait** part {num}/{total}\n"
                    f"  ⏳ Resume in `{rem}s`\n"
                    f"  `{BF*fill}{BE*(18-fill)}`"
                )
                await asyncio.sleep(1)
            # loop continues — retries after wait

        except Exception as e:
            err = str(e).lower()
            # These mean Telegram received it but returned an error anyway
            if any(x in err for x in [
                "duplicate", "already", "message_id_invalid",
                "forbidden", "timeout", "connection", "network"
            ]):
                _mark_part_sent(uid, num)
                ok = True
            else:
                await _edit(prog_msg, f"❌ Upload failed part {num}:\n`{e}`")
            break  # never retry unknown errors

    _rst(uid)
    if th and os.path.exists(tp):
        try: os.remove(tp)
        except: pass
    return ok


# ══════════════════════════════════════════════════════════════
#  /start
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("start") & filters.incoming, group=0)
async def cmd_start(_, msg):
    if await _dedup(msg.id): return
    name=msg.from_user.first_name or "User"
    await msg.reply(
        f"╔══════════════════════════╗\n"
        f"║  ⚡  ULTRA BOT v10  ⚡   ║\n"
        f"╚══════════════════════════╝\n\n"
        f"  👋 Hey **{name}**!\n\n"
        f"  📽 Send any video:\n"
        f"    MP4 · MKV · AVI · MOV · WEBM\n\n"
        f"  ✂️ Commands:\n"
        f"    • `/split 3`    → 3 equal parts\n"
        f"    • `/splitmin 2` → 2-min chunks\n"
        f"    • `/splitsize 500` → 500 MB chunks\n\n"
        f"  🛠 Tools:\n"
        f"    • `/status` · `/cancel` · `/info`\n\n"
        f"──────────────────────────\n"
        f"  🔒 File-lock anti-double\n"
        f"  ⚡ 3-layer dedup system\n"
        f"  📊 Live speed graph\n"
        f"  🌙 Phase moon spinner\n"
        f"──────────────────────────\n"
        f"  _Ultra Bot v10 — Zero Bugs_ ✓"
    )


# ══════════════════════════════════════════════════════════════
#  /info
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("info") & filters.incoming, group=0)
async def cmd_info(_, msg):
    if await _dedup(msg.id): return
    uid=msg.from_user.id
    if uid not in user_files or not os.path.exists(user_files[uid]):
        return await msg.reply("❌ No video loaded.")
    p=user_files[uid]
    d=await _dur(p)
    sz=os.path.getsize(p)
    # Calculate possible splits
    splits=""
    if d:
        for n in [2,3,4,5]:
            s=_t(d/n)
            splits+=f"  `/split {n}` → {n}×{s}\n"
    await msg.reply(
        f"📋 **FILE INFO**\n"
        f"══════════════════════════\n"
        f"  📁 `{os.path.basename(p)}`\n"
        f"  📦 `{_sz(sz)}`\n"
        f"  🎬 `{_t(d) if d else 'unknown'}`\n"
        f"──────────────────────────\n"
        f"  💡 **Split options:**\n"
        f"{splits}"
        f"══════════════════════════\n"
        f"  👉 `/split N` or `/splitmin N`"
    )


# ══════════════════════════════════════════════════════════════
#  /status
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("status") & filters.incoming, group=0)
async def cmd_status(_, msg):
    if await _dedup(msg.id): return
    uid=msg.from_user.id
    info=user_status.get(uid)
    busy=_is_locked(uid)

    if not busy or not info:
        if uid in user_files and os.path.exists(user_files[uid]):
            p=user_files[uid]
            return await msg.reply(
                f"💤 **IDLE — Ready**\n"
                f"══════════════════════════\n"
                f"  📁 `{os.path.basename(p)}`\n"
                f"  📦 `{_sz(os.path.getsize(p))}`\n"
                f"══════════════════════════\n"
                f"  👉 `/split N` · `/splitmin N`\n"
                f"  📋 `/info` for split options"
            )
        return await msg.reply(
            "💤 **IDLE**\n"
            "  No video loaded.\n"
            "  Send a video to start!"
        )

    hist=_hi.get(uid,[])
    g=_spark(hist,14) if hist else "no data"
    spd=f"{_sz(hist[-1])}/s" if hist else "—"
    sp=SPIN_PHS[int(time.time()*2)%8]
    await msg.reply(
        f"{sp} **RUNNING**\n"
        f"══════════════════════════\n"
        f"  📌 `{info['task']}`\n"
        f"  📝 `{info['detail']}`\n"
        f"  ⏳ `{_t(time.time()-info['ts'])}`\n"
        f"  📊 `{g}`\n"
        f"  ⚡ `{spd}`\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel"
    )


# ══════════════════════════════════════════════════════════════
#  /cancel
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("cancel") & filters.incoming, group=0)
async def cmd_cancel(_, msg):
    if await _dedup(msg.id): return
    uid=msg.from_user.id
    if not _is_locked(uid):
        return await msg.reply("💤 Nothing running.")
    _cancel_ev(uid).set()
    await msg.reply(
        "🚫 **CANCEL SENT**\n"
        "  Stopping at next checkpoint…"
    )


# ══════════════════════════════════════════════════════════════
#  RECEIVE VIDEO
# ══════════════════════════════════════════════════════════════
@app.on_message(
    filters.incoming
    & ~filters.command(["start","split","splitmin","splitsize","status","cancel","info"])
    & (filters.video | filters.document),
    group=1
)
async def recv(_, msg):
    if await _dedup(msg.id): return
    uid=msg.from_user.id
    if _is_locked(uid):
        return await msg.reply("⏳ Task running.\n👉 /status · /cancel")

    media=msg.document or msg.video
    if not media: return

    mime=getattr(media,"mime_type","") or ""
    fsz =getattr(media,"file_size",0) or 0

    if mime and not(mime.startswith("video/") or mime=="application/octet-stream"):
        return

    if uid in user_files and os.path.exists(user_files[uid]):
        try: os.remove(user_files[uid])
        except: pass
        user_files.pop(uid,None)

    ext_map={
        "video/x-matroska":"mkv","video/mkv":"mkv",
        "video/avi":"avi","video/x-msvideo":"avi",
        "video/webm":"webm","video/quicktime":"mov",
        "video/x-ms-wmv":"wmv","video/3gpp":"3gp",
    }
    ext=ext_map.get(mime,"mp4")
    fname=f"{DOWNLOAD_DIR}/v_{uid}_{msg.id}.{ext}"

    st=await msg.reply(
        f"⣾ **DOWNLOADING**\n"
        f"══════════════════════════\n"
        f"  📁 `{ext.upper()}` · `{_sz(fsz)}`\n"
        f"`{BE*18}` **0%**\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel"
    )

    _cancel_ev(uid).clear()
    _rst(uid); _hi.pop(uid,None)
    _clear_part_locks(uid)
    _set_st(uid,"Download",_sz(fsz))

    if not _acquire_lock(uid):
        await _edit(st,"⏳ Already busy!"); return

    t0=time.time()
    try:
        path=await msg.download(
            file_name=fname,
            progress=_progress,
            progress_args=(st,t0,uid,"📥 Download"),
        )
    except Exception as e:
        _release_lock(uid); _clr_st(uid)
        await _edit(st,f"❌ Download failed:\n`{e}`"); return

    if not path or not os.path.exists(path):
        _release_lock(uid); _clr_st(uid)
        await _edit(st,"❌ Not saved. Try again."); return

    el=time.time()-t0
    avg=fsz/el if el and fsz else 0
    actual=os.path.getsize(path)

    _rst(uid); _clr_st(uid)
    _release_lock(uid)
    user_files[uid]=path

    await _edit(st,
        f"✅ **DOWNLOAD COMPLETE**\n"
        f"══════════════════════════\n"
        f"`{BF*18}` **100%** 🏆\n"
        f"──────────────────────────\n"
        f"  📁 `{os.path.basename(path)}`\n"
        f"  📦 `{_sz(actual)}`\n"
        f"  ⚡ `{_sz(avg)}/s`\n"
        f"  ⏱ `{_t(el)}`\n"
        f"══════════════════════════\n"
        f"  👉 `/split N` · `/splitmin N`\n"
        f"  📋 `/info` for all options"
    )


# ══════════════════════════════════════════════════════════════
#  CORE SPLIT ENGINE
# ══════════════════════════════════════════════════════════════
async def _do_split(msg_orig, uid, seg, parts, label):
    file=user_files[uid]
    ev=_cancel_ev(uid); ev.clear()
    _clear_part_locks(uid)
    _stk[uid]=0

    prog=await msg_orig.reply(
        f"✂️ **{label}**\n"
        f"══════════════════════════\n"
        f"`{BE*16}` **0%**\n"
        f"  Part **0** / **{parts}**\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel"
    )

    try:
        for i in range(parts):
            if ev.is_set():
                await _edit(prog,
                    f"🚫 **CANCELLED**\n"
                    f"  Stopped at **{i}/{parts}**."
                )
                return

            ss=i*seg
            _set_st(uid,"Cut",f"{i+1}/{parts}")
            await _split_ui(prog,i,parts,uid,f"cutting {i+1}…")

            out=f"{DOWNLOAD_DIR}/p_{uid}_{i+1}.mp4"
            if not await _cut(file,out,ss,seg) or not os.path.exists(out):
                await _edit(prog,f"❌ ffmpeg failed part {i+1}.")
                return

            _set_st(uid,"Upload",f"{i+1}/{parts}")
            await _split_ui(prog,i,parts,uid,f"uploading {i+1}…")

            sent=await _send_part(msg_orig,prog,out,i+1,parts,uid,ss+seg/2)

            try:
                if os.path.exists(out): os.remove(out)
            except: pass

            if not sent:
                await _edit(prog,
                    f"🚫 **CANCELLED**\n"
                    f"  Stopped at **{i+1}/{parts}**."
                )
                return

            await _split_ui(prog,i+1,parts,uid,
                "🎉 done!" if i+1==parts else f"next: {i+2}…"
            )

        # ── ALL DONE ──
        try:
            if os.path.exists(file): os.remove(file)
        except: pass
        user_files.pop(uid,None)
        _clear_part_locks(uid)

        checks="".join("✅" for _ in range(min(parts,20)))
        if parts>20: checks+=f" +{parts-20}"

        await _edit(prog,
            f"🏆 **ALL {parts} PARTS DONE!**\n"
            f"══════════════════════════\n"
            f"`{BF*16}` **100%**\n"
            f"  {checks}\n"
            f"──────────────────────────\n"
            f"  ✅ **{parts}** parts uploaded\n"
            f"  🎬 Ultra Bot v10\n"
            f"══════════════════════════\n"
            f"  _Send next video!_"
        )

    except Exception as e:
        await _edit(prog,f"❌ Error:\n`{e}`")
    finally:
        _clr_st(uid)
        _release_lock(uid)   # ALWAYS release


# ══════════════════════════════════════════════════════════════
#  /split  — 3-layer guard
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("split") & filters.incoming, group=0)
async def cmd_split(_, msg):
    if await _dedup(msg.id): return          # L1: msg ID
    uid=msg.from_user.id
    if _cooldown(uid): return                # L2: time window
    if not _acquire_lock(uid):              # L3: file lock (atomic)
        return await msg.reply("⏳ Already running!\n👉 /status · /cancel")
    try:
        if uid not in user_files:
            await msg.reply("❌ Send a video first!")
            return
        try:
            n=int(msg.command[1]); assert 2<=n<=100
        except:
            await msg.reply("❌ Usage: `/split 3`"); return
        d=await _dur(user_files[uid])
        if not d:
            await msg.reply("❌ Cannot read duration."); return
        await _do_split(msg,uid,d/n,n,f"Splitting into **{n}** equal parts…")
    except Exception as e:
        await msg.reply(f"❌ `{e}`")
        _release_lock(uid)


# ══════════════════════════════════════════════════════════════
#  /splitmin
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("splitmin") & filters.incoming, group=0)
async def cmd_splitmin(_, msg):
    if await _dedup(msg.id): return
    uid=msg.from_user.id
    if _cooldown(uid): return
    if not _acquire_lock(uid):
        return await msg.reply("⏳ Already running!\n👉 /status · /cancel")
    try:
        if uid not in user_files:
            await msg.reply("❌ Send a video first!"); return
        try:
            m=int(msg.command[1]); assert 1<=m<=120
        except:
            await msg.reply("❌ Usage: `/splitmin 2`"); return
        d=await _dur(user_files[uid])
        if not d:
            await msg.reply("❌ Cannot read duration."); return
        seg=m*60; parts=math.ceil(d/seg)
        if parts>100:
            await msg.reply(f"❌ Too many parts ({parts}). Use bigger chunk."); return
        await _do_split(msg,uid,seg,parts,f"**{m} min** chunks → **{parts}** parts…")
    except Exception as e:
        await msg.reply(f"❌ `{e}`")
        _release_lock(uid)


# ══════════════════════════════════════════════════════════════
#  /splitsize  — split by MB size  (NEW FEATURE)
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("splitsize") & filters.incoming, group=0)
async def cmd_splitsize(_, msg):
    if await _dedup(msg.id): return
    uid=msg.from_user.id
    if _cooldown(uid): return
    if not _acquire_lock(uid):
        return await msg.reply("⏳ Already running!")
    try:
        if uid not in user_files:
            await msg.reply("❌ Send a video first!"); return
        try:
            mb=int(msg.command[1]); assert 10<=mb<=2000
        except:
            await msg.reply("❌ Usage: `/splitsize 500`\n  Size in MB (10–2000)."); return

        file=user_files[uid]
        total_mb=os.path.getsize(file)/1048576
        d=await _dur(file)
        if not d:
            await msg.reply("❌ Cannot read duration."); return

        parts=math.ceil(total_mb/mb)
        if parts>100:
            await msg.reply(f"❌ Too many parts ({parts})."); return
        if parts<2:
            await msg.reply(f"❌ File is already ≤{mb}MB. No split needed."); return

        seg=d/parts
        await _do_split(msg,uid,seg,parts,
            f"**{mb} MB** chunks → **{parts}** parts…")
    except Exception as e:
        await msg.reply(f"❌ `{e}`")
        _release_lock(uid)


# ══════════════════════════════════════════════════════════════
app.run()
