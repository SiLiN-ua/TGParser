# TGParser

24/7 Telegram OSINT collector:

- Durable, resumable backfill with per-message retry + dead-letter queue
- Canonical chat ids (via `telethon.utils.get_peer_id`)
- FloodWait-tolerant iterator that resumes from checkpoint, not restarts
- PostgreSQL storage with versioned migrations, transparent reconnect/retry
- Media download queue with dedup, many-to-many (`message_media`), retries, disk-space guard
- User profile TTL + change history + avatar history by `photo_file_id`
- Reactions aggregates + authors (admin-only in most chats) with rate-limited queue
- Memberships: initial sync + live join/leave with `is_complete`/`limitation_reason`
- Deletions: stub rows when `chat_id` is known, `deletion_events` log when not
- Background tasks are tracked, awaited on shutdown, errors recorded
- Per-process lock file (single instance per session)
- Health check reports `healthy`/`degraded`/`unhealthy` with PID, uptime, PG/TG status

## Install

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Postgres 17 locally (one-time setup); keep the DB user to `localhost` until you
rotate the password, then move it to a non-trivial secret in `.env` before
opening the port on the network.

## Config

Secrets in `.env` (never committed):

```
TG_API_ID=...
TG_API_HASH=...
TG_PHONE=+7...
TG_SESSION_NAME=tgparser
PG_DSN=postgresql://tgparser:<pw>@127.0.0.1:5432/tgparser
```

Runtime in `config.yaml`:

```yaml
storage:
  media_dir: "./media"
  download_media: true
  max_media_mb: 500
  media_concurrency: 3
targets:
  - "@durov"
options:
  backfill_on_start: true
  backfill_limit: null                  # null = full history
  follow_linked_discussion: true
  user_profile_ttl_hours: 24
  reaction_authors: false
  reaction_authors_limit: 100
  reaction_authors_concurrency: 2
  participants_sync: true
  per_message_retries: 3
  bg_concurrency: 10
```

## Run

```powershell
.\.venv\Scripts\python.exe run.py
```

First launch prompts for QR scan (`login_qr.png`) + 2FA. Session saved to
`tgparser.session`.

Health snapshot (exit 0 = healthy, 1 = degraded, 2 = unhealthy):

```powershell
.\.venv\Scripts\python.exe health.py
```

Tests:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

## 24/7 (Windows, user scope — no admin)

```powershell
.\scripts\install_task.ps1          # elevated; registers a proper /SC ONLOGON task
```

User-scope variant (no admin required) is scripted explicitly:

```powershell
schtasks /Create /TN "TGParser" `
  /TR "powershell.exe -ExecutionPolicy Bypass -WindowStyle Hidden -File `"C:\Users\<you>\Desktop\TGParser\scripts\ensure_running.ps1`"" `
  /SC DAILY /ST 00:01 /RI 1 /DU 24:00 /F

$startup = [Environment]::GetFolderPath("Startup")
$lnk = Join-Path $startup "TGParser.lnk"
$ws = New-Object -ComObject WScript.Shell
$s = $ws.CreateShortcut($lnk)
$s.TargetPath = "powershell.exe"
$s.Arguments  = "-ExecutionPolicy Bypass -WindowStyle Hidden -File `"C:\Users\<you>\Desktop\TGParser\scripts\ensure_running.ps1`""
$s.WorkingDirectory = "C:\Users\<you>\Desktop\TGParser"
$s.WindowStyle = 7
$s.Save()
```

`ensure_running.ps1` is a watchdog: it starts `pythonw.exe run.py` only when
the lock file is stale or missing, so repeated invocations never create a
second instance. The Startup shortcut handles login-time start; the scheduled
task repeats every minute as a crash-restart mechanism.

## 24/7 (Linux systemd)

`/etc/systemd/system/tgparser.service`:

```ini
[Unit]
Description=TGParser
After=network.target postgresql.service
Wants=postgresql.service

[Service]
Type=simple
User=tgparser
WorkingDirectory=/opt/tgparser
EnvironmentFile=/opt/tgparser/.env
ExecStart=/opt/tgparser/.venv/bin/python run.py
Restart=always
RestartSec=10
TimeoutStopSec=30
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now tgparser
journalctl -u tgparser -f
```

## Backups

```powershell
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$bk = "C:\backups\tgparser_$ts"
New-Item -ItemType Directory -Path $bk -Force | Out-Null
$env:PGPASSWORD="<pw>"
& "C:\Program Files\PostgreSQL\17\bin\pg_dump.exe" -U tgparser -h 127.0.0.1 -d tgparser -F c -f "$bk\tgparser.dump"
Copy-Item .\tgparser.session "$bk\"
# media/ grows large — sync separately (robocopy / rsync).
```

## Data model (v3)

| table | purpose |
|---|---|
| `chats` | canonical chat records, `linked_chat_id`, `is_forum` |
| `users` | profile + `full_fetched_at` TTL + `user_profile_history` change log |
| `avatar_history` | one row per (`user_id`, `photo_file_id`) with `local_path` |
| `memberships` | join/leave/role events, `source` = initial_sync/chat_action |
| `sync_state` | per-chat `last_backfilled_message_id`, `participants_synced`/`_is_complete`/`_limitation` |
| `messages` | `(chat_id, message_id)` canonical, all metadata + `raw` JSONB |
| `message_versions` | every observed version, deduped by `(chat_id, message_id, edit_date)` |
| `message_media` | many-to-many: one Telegram file can attach to many messages |
| `media` | one row per `file_id` with status pending/done/skipped/failed, attempts |
| `message_reactions` | aggregate counts per emoji |
| `message_reaction_authors` | per-peer authors (admin-only in many chats) |
| `backfill_queue` | durable retry for messages that failed in backfill; blocks checkpoint |
| `deletion_events` | append-only log of `UpdateDeleteMessages` without a `chat_id` |
| `chat_actions` | ChatAction events (join/leave/pin/rename…) |
| `errors` | isolated collector exceptions with traceback |
| `runtime_state` | pid / status / targets / started_at / heartbeat_at |

## Durability guarantees

- Backfill checkpoint `last_backfilled_message_id` is NEVER advanced past any
  message currently `pending` in `backfill_queue` (tested in
  `tests/test_collector_flow.py::test_backfill_dlq_then_recovery`).
- Per-message failures are retried 3 times by default, then dead-lettered.
- `retry_dead_letter` is invoked once after each backfill pass and re-fetches
  each failed message by id.
- On process restart, `MediaQueue.sweep_retryable()` re-enqueues every
  `pending`/`failed` row that still has a `(chat_id, message_id)` pointer.
- A FloodWait in the middle of `iter_messages` sleeps then re-opens the
  iterator from the live checkpoint; the current message is retried.
- Transient Postgres connection errors are retried with exponential backoff and
  trigger a pool recreate (tested by terminating all backends mid-op).

## Telegram API limitations

- **Full membership lists** for big private groups are capped by Telegram
  (~10k) and may require admin; recorded in `sync_state.participants_limitation`.
- **Reaction authors** are hidden in channels and many groups unless you're
  admin, and are rate-limited with pagination.
- **Historical edits or deletions** that happened before we were observing are
  not retrievable.
- **`UpdateDeleteMessages` without `chat_id`**: Telegram sometimes sends these;
  we log the ids to `deletion_events` but cannot associate them with rows.
- **Private chats you're not in**: no way, by anyone.
- **`bio`**: depends on each user's privacy setting; `null` is a valid answer.
- **Historical profile changes** before first observation are lost; only
  changes seen after the collector started appear in `user_profile_history`.
- **Hard bans**: aggressive polling or scraping may still get the session
  banned. Use a dedicated number.
