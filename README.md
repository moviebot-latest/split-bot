# Video Split Pro V10 — Render Ready

V10 adds restart-safe job recovery and persistent checkpoints.

## Required
- `API_ID`
- `API_HASH`
- `BOT_TOKEN`

## AI (optional)
- `GROQ_API_KEY`
- `GROQ_MODEL`
- `GROQ_VISION_MODEL`
- `GROQ_WHISPER_MODEL`

## Strong recovery on Render
Set **`DATABASE_URL`** to a persistent PostgreSQL/Neon connection string. V10 stores only small job metadata/checkpoints in the database; the large video stays in Telegram/temporary server storage.

If `DATABASE_URL` is not set, V10 falls back to local SQLite. That is useful for local testing, but it is **not restart-durable on Render Free** because the local filesystem can be reset.

## Recovery behavior
- Original Telegram chat/message ID is saved.
- Split parts checkpoint after every successful upload.
- Scene scan stores detected cuts before splitting.
- Highlight pipeline stores transcript/candidates/vision results by stage.
- On process restart, unfinished jobs are detected and resumed automatically.
- If the temporary source file is missing, V10 downloads the original again from Telegram.
- `/retry` and `/resume` manually trigger recovery for the latest unfinished job.
- Completed jobs are marked done; cancelled jobs are not auto-restarted.

## Render start
`python bot.py`
