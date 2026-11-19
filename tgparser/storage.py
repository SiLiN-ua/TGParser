import asyncio
import json
import logging
import random
import traceback
from datetime import datetime, timezone
from functools import wraps
from typing import Any
import asyncpg

log = logging.getLogger("tgparser.storage")


def _j(v) -> str:
    return json.dumps(v if v is not None else {}, default=str, ensure_ascii=False)


# PostgreSQL transient exceptions that are safe to retry for idempotent ops.
_RETRYABLE_PG = (
    asyncpg.PostgresConnectionError,
    asyncpg.CannotConnectNowError,
    asyncpg.ConnectionDoesNotExistError,
    asyncpg.ConnectionFailureError,
    asyncpg.InterfaceError,
    asyncpg.exceptions.DeadlockDetectedError,
    asyncpg.exceptions.SerializationError,
    asyncpg.exceptions._base.InternalClientError,
    ConnectionError,
    ConnectionResetError,
    OSError,
    asyncio.TimeoutError,
)


def _with_db_retry(idempotent: bool = True, max_tries: int = 6,
                   base: float = 0.5, cap: float = 15.0):
    """Wrap a Storage method with exponential-backoff retry on transient Postgres errors.

    idempotent=True is safe to retry freely. For non-idempotent operations we
    rely on ON CONFLICT upserts / idempotency keys; this is still safe because
    every write in this module is an upsert or an append to an event log.
    """
    def deco(fn):
        @wraps(fn)
        async def wrapper(self, *a, **kw):
            attempt = 0
            while True:
                attempt += 1
                if self.pool is None:
                    try:
                        await self._ensure_pool()
                    except Exception:
                        pass
                try:
                    return await fn(self, *a, **kw)
                except _RETRYABLE_PG as e:
                    if attempt >= max_tries:
                        log.error("db op %s failed after %s tries: %s",
                                  fn.__name__, attempt, e)
                        raise
                    delay = min(cap, base * (2 ** (attempt - 1))) + random.uniform(0, 0.3)
                    log.warning("db op %s transient %s, retry %d/%d in %.1fs",
                                fn.__name__, type(e).__name__, attempt, max_tries, delay)
                    await asyncio.sleep(delay)
                    try:
                        await self._ensure_pool(force_recreate=True)
                    except Exception:
                        pass
        return wrapper
    return deco


class Storage:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: asyncpg.Pool | None = None
        self._pool_lock = asyncio.Lock()

    async def connect(self):
        await self._ensure_pool()

    async def _ensure_pool(self, force_recreate: bool = False):
        async with self._pool_lock:
            if self.pool is not None and not force_recreate:
                try:
                    async with self.pool.acquire() as con:
                        await con.execute("SELECT 1")
                    return
                except Exception:
                    force_recreate = True
            if force_recreate and self.pool is not None:
                try:
                    await asyncio.wait_for(self.pool.close(), timeout=3)
                except Exception:
                    try:
                        self.pool.terminate()
                    except Exception:
                        pass
                self.pool = None
            self.pool = await asyncpg.create_pool(
                self.dsn, min_size=1, max_size=5, ssl=False,
                command_timeout=60,
            )

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def ping(self) -> bool:
        try:
            async with self.pool.acquire() as con:
                return (await con.fetchval("SELECT 1")) == 1
        except Exception:
            return False

    # --- chats ---------------------------------------------------------------
    @_with_db_retry()
    async def upsert_chat(self, c: dict):
        q = """
        INSERT INTO chats (id, type, title, username, about, participants_count,
                           linked_chat_id, is_forum, is_broadcast, raw, last_updated)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,NOW())
        ON CONFLICT (id) DO UPDATE SET
            type=EXCLUDED.type,
            title=COALESCE(EXCLUDED.title, chats.title),
            username=COALESCE(EXCLUDED.username, chats.username),
            about=COALESCE(EXCLUDED.about, chats.about),
            participants_count=COALESCE(EXCLUDED.participants_count, chats.participants_count),
            linked_chat_id=COALESCE(EXCLUDED.linked_chat_id, chats.linked_chat_id),
            is_forum=EXCLUDED.is_forum,
            is_broadcast=EXCLUDED.is_broadcast,
            raw=EXCLUDED.raw,
            last_updated=NOW();
        """
        async with self.pool.acquire() as con:
            await con.execute(q, c["id"], c.get("type"), c.get("title"), c.get("username"),
                              c.get("about"), c.get("participants_count"), c.get("linked_chat_id"),
                              bool(c.get("is_forum")), c.get("is_broadcast"), _j(c.get("raw")))

    # --- users ---------------------------------------------------------------
    @_with_db_retry()
    async def get_user(self, user_id: int) -> asyncpg.Record | None:
        async with self.pool.acquire() as con:
            return await con.fetchrow("SELECT * FROM users WHERE id=$1", user_id)

    @_with_db_retry()
    async def user_needs_refresh(self, user_id: int, ttl_hours: int) -> bool:
        async with self.pool.acquire() as con:
            row = await con.fetchrow(
                "SELECT full_fetched_at FROM users WHERE id=$1", user_id)
            if row is None or row["full_fetched_at"] is None:
                return True
            age = datetime.now(timezone.utc) - row["full_fetched_at"]
            return age.total_seconds() > ttl_hours * 3600

    @_with_db_retry()
    async def upsert_user(self, u: dict, record_history: bool = True):
        q = """
        INSERT INTO users (id, username, first_name, last_name, phone, bio,
            is_bot, is_premium, is_scam, is_fake, is_verified, is_deleted,
            lang_code, photo_path, photo_dc, photo_file_id, raw,
            full_fetched_at, last_updated)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17::jsonb,$18,NOW())
        ON CONFLICT (id) DO UPDATE SET
            username=COALESCE(EXCLUDED.username, users.username),
            first_name=COALESCE(EXCLUDED.first_name, users.first_name),
            last_name=COALESCE(EXCLUDED.last_name, users.last_name),
            phone=COALESCE(EXCLUDED.phone, users.phone),
            bio=COALESCE(EXCLUDED.bio, users.bio),
            is_bot=COALESCE(EXCLUDED.is_bot, users.is_bot),
            is_premium=COALESCE(EXCLUDED.is_premium, users.is_premium),
            is_scam=COALESCE(EXCLUDED.is_scam, users.is_scam),
            is_fake=COALESCE(EXCLUDED.is_fake, users.is_fake),
            is_verified=COALESCE(EXCLUDED.is_verified, users.is_verified),
            is_deleted=COALESCE(EXCLUDED.is_deleted, users.is_deleted),
            lang_code=COALESCE(EXCLUDED.lang_code, users.lang_code),
            photo_path=COALESCE(EXCLUDED.photo_path, users.photo_path),
            photo_dc=COALESCE(EXCLUDED.photo_dc, users.photo_dc),
            photo_file_id=COALESCE(EXCLUDED.photo_file_id, users.photo_file_id),
            raw=EXCLUDED.raw,
            full_fetched_at=COALESCE(EXCLUDED.full_fetched_at, users.full_fetched_at),
            last_updated=NOW();
        """
        async with self.pool.acquire() as con:
            prev = None
            if record_history:
                prev = await con.fetchrow(
                    "SELECT username, first_name, last_name, bio, photo_file_id FROM users WHERE id=$1",
                    u["id"])
            await con.execute(
                q, u["id"], u.get("username"), u.get("first_name"), u.get("last_name"),
                u.get("phone"), u.get("bio"), u.get("is_bot"), u.get("is_premium"),
                u.get("is_scam"), u.get("is_fake"), u.get("is_verified"), u.get("is_deleted"),
                u.get("lang_code"), u.get("photo_path"), u.get("photo_dc"), u.get("photo_file_id"),
                _j(u.get("raw")), u.get("full_fetched_at"))
            if record_history and prev is not None:
                for field in ("username", "first_name", "last_name", "bio", "photo_file_id"):
                    old = prev[field]
                    new = u.get(field)
                    if new is None:
                        continue
                    if (old or "") != (str(new) if new is not None else ""):
                        await con.execute(
                            "INSERT INTO user_profile_history (user_id, field, old_value, new_value) VALUES ($1,$2,$3,$4)",
                            u["id"], field,
                            None if old is None else str(old),
                            None if new is None else str(new))

    # --- messages ------------------------------------------------------------
    @_with_db_retry()
    async def upsert_message(self, m: dict, record_version: bool = True):
        q = """
        INSERT INTO messages (
            chat_id, message_id, sender_user_id, sender_chat_id, date, edit_date,
            reply_to_message_id, reply_to_top_id, reply_to_peer_id, thread_id, post_author,
            text, entities, media_type, media_path, media_file_id, media_group_id,
            fwd_from_user_id, fwd_from_chat_id, fwd_from_name, fwd_from_msg_id, fwd_date, fwd_channel_post,
            views, forwards, replies_count, reactions_total,
            is_service, service_action, service_payload,
            geo, contact, poll, webpage, raw)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14,$15,$16,$17,
                $18,$19,$20,$21,$22,$23,
                $24,$25,$26,$27,
                $28,$29,$30::jsonb,
                $31::jsonb,$32::jsonb,$33::jsonb,$34::jsonb,$35::jsonb)
        ON CONFLICT (chat_id, message_id) DO UPDATE SET
            edit_date=EXCLUDED.edit_date,
            text=EXCLUDED.text,
            entities=EXCLUDED.entities,
            media_type=COALESCE(EXCLUDED.media_type, messages.media_type),
            media_path=COALESCE(EXCLUDED.media_path, messages.media_path),
            media_file_id=COALESCE(EXCLUDED.media_file_id, messages.media_file_id),
            views=COALESCE(EXCLUDED.views, messages.views),
            forwards=COALESCE(EXCLUDED.forwards, messages.forwards),
            replies_count=COALESCE(EXCLUDED.replies_count, messages.replies_count),
            reactions_total=COALESCE(EXCLUDED.reactions_total, messages.reactions_total),
            raw=EXCLUDED.raw;
        """
        async with self.pool.acquire() as con:
            async with con.transaction():
                existing = await con.fetchrow(
                    "SELECT text, entities, media_file_id, edit_date FROM messages WHERE chat_id=$1 AND message_id=$2",
                    m["chat_id"], m["message_id"])
                await con.execute(q,
                    m["chat_id"], m["message_id"], m.get("sender_user_id"), m.get("sender_chat_id"),
                    m.get("date"), m.get("edit_date"), m.get("reply_to_message_id"),
                    m.get("reply_to_top_id"), m.get("reply_to_peer_id"), m.get("thread_id"),
                    m.get("post_author"), m.get("text"), _j(m.get("entities")),
                    m.get("media_type"), m.get("media_path"), m.get("media_file_id"),
                    m.get("media_group_id"),
                    m.get("fwd_from_user_id"), m.get("fwd_from_chat_id"), m.get("fwd_from_name"),
                    m.get("fwd_from_msg_id"), m.get("fwd_date"), m.get("fwd_channel_post"),
                    m.get("views"), m.get("forwards"), m.get("replies_count"), m.get("reactions_total"),
                    bool(m.get("is_service")), m.get("service_action"), _j(m.get("service_payload")),
                    _j(m.get("geo")), _j(m.get("contact")), _j(m.get("poll")), _j(m.get("webpage")),
                    _j(m.get("raw")))
                if record_version:
                    is_initial = existing is None
                    changed = (
                        existing is None
                        or (existing["text"] or "") != (m.get("text") or "")
                        or (existing["media_file_id"] or "") != (m.get("media_file_id") or "")
                    )
                    if changed:
                        await con.execute("""
                            INSERT INTO message_versions (chat_id, message_id, edit_date, text, entities, media_type, media_path, media_file_id, is_initial)
                            VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9)
                            ON CONFLICT DO NOTHING
                        """, m["chat_id"], m["message_id"], m.get("edit_date"), m.get("text"),
                             _j(m.get("entities")), m.get("media_type"), m.get("media_path"),
                             m.get("media_file_id"), is_initial)

    @_with_db_retry()
    async def mark_deleted(self, chat_id: int, message_ids: list[int]):
        if not message_ids:
            return
        q = """
        UPDATE messages SET is_deleted=TRUE, deleted_at=COALESCE(deleted_at, NOW())
        WHERE chat_id=$1 AND message_id = ANY($2::bigint[]);
        """
        async with self.pool.acquire() as con:
            await con.execute(q, chat_id, message_ids)

    @_with_db_retry()
    async def ensure_message_stub_deleted(self, chat_id: int, message_ids: list[int]):
        if not message_ids:
            return
        q = """
        INSERT INTO messages (chat_id, message_id, is_deleted, deleted_at)
        SELECT $1, mid, TRUE, NOW() FROM unnest($2::bigint[]) AS mid
        ON CONFLICT (chat_id, message_id) DO UPDATE SET
            is_deleted=TRUE, deleted_at=COALESCE(messages.deleted_at, NOW());
        """
        async with self.pool.acquire() as con:
            await con.execute(q, chat_id, message_ids)

    @_with_db_retry()
    async def record_deletion_event(self, deleted_ids: list[int],
                                    channel_id: int | None = None,
                                    update_type: str | None = None,
                                    raw_payload: dict | None = None):
        async with self.pool.acquire() as con:
            await con.execute(
                "INSERT INTO deletion_events (deleted_ids, channel_id, update_type, raw_payload) "
                "VALUES ($1::jsonb, $2, $3, $4::jsonb)",
                _j(deleted_ids), channel_id, update_type, _j(raw_payload))

    @_with_db_retry()
    async def resolve_deletion_events(self, chat_id: int,
                                      deleted_ids: list[int]) -> int:
        """Best-effort association: for each unresolved event with matching
        channel_id and any overlapping id in deleted_ids, mark it resolved
        and stub the messages. Returns number of resolved events."""
        if not deleted_ids:
            return 0
        async with self.pool.acquire() as con:
            async with con.transaction():
                await con.execute("""
                    INSERT INTO messages (chat_id, message_id, is_deleted, deleted_at)
                    SELECT $1, mid, TRUE, NOW() FROM unnest($2::bigint[]) AS mid
                    ON CONFLICT (chat_id, message_id) DO UPDATE SET
                        is_deleted=TRUE, deleted_at=COALESCE(messages.deleted_at, NOW());
                """, chat_id, deleted_ids)
                return await con.fetchval("""
                    WITH upd AS (
                        UPDATE deletion_events d
                           SET resolved_at = NOW()
                         WHERE d.channel_id = $1
                           AND d.resolved_at IS NULL
                           AND EXISTS (
                               SELECT 1
                                 FROM jsonb_array_elements_text(d.deleted_ids) AS e
                                WHERE e.value::bigint = ANY($2::bigint[])
                           )
                        RETURNING 1
                    )
                    SELECT COUNT(*)::int FROM upd;
                """, chat_id, deleted_ids)

    @_with_db_retry()
    async def deletion_events_unresolved(self) -> int:
        async with self.pool.acquire() as con:
            return await con.fetchval(
                "SELECT COUNT(*) FROM deletion_events WHERE resolved_at IS NULL")

    # --- reactions -----------------------------------------------------------
    @_with_db_retry()
    async def replace_reactions(self, chat_id: int, message_id: int, items: list[dict]):
        async with self.pool.acquire() as con:
            async with con.transaction():
                await con.execute(
                    "DELETE FROM message_reactions WHERE chat_id=$1 AND message_id=$2",
                    chat_id, message_id)
                if items:
                    await con.executemany("""
                        INSERT INTO message_reactions (chat_id, message_id, reaction, count, chosen_order)
                        VALUES ($1,$2,$3,$4,$5)
                        ON CONFLICT (chat_id, message_id, reaction) DO UPDATE
                        SET count=EXCLUDED.count, chosen_order=EXCLUDED.chosen_order, updated_at=NOW()
                    """, [(chat_id, message_id, it["reaction"], it["count"], it.get("chosen_order"))
                          for it in items])

    @_with_db_retry()
    async def replace_reaction_authors(self, chat_id: int, message_id: int,
                                       rows: list[dict], is_complete: bool):
        """Replace author list atomically when Telegram returned the complete set."""
        async with self.pool.acquire() as con:
            async with con.transaction():
                if is_complete:
                    await con.execute(
                        "DELETE FROM message_reaction_authors WHERE chat_id=$1 AND message_id=$2",
                        chat_id, message_id)
                if rows:
                    await con.executemany("""
                        INSERT INTO message_reaction_authors (chat_id, message_id, reaction, peer_id, peer_kind, reacted_at)
                        VALUES ($1,$2,$3,$4,$5,$6)
                        ON CONFLICT (chat_id, message_id, reaction, peer_id) DO UPDATE
                        SET reacted_at = COALESCE(EXCLUDED.reacted_at, message_reaction_authors.reacted_at),
                            peer_kind=EXCLUDED.peer_kind;
                    """, [(chat_id, message_id, r["reaction"], r["peer_id"],
                           r.get("peer_kind"), r.get("reacted_at")) for r in rows])

    # kept for backwards compat with tests/older code
    async def upsert_reaction_authors(self, chat_id: int, message_id: int, rows: list[dict]):
        await self.replace_reaction_authors(chat_id, message_id, rows, is_complete=False)

    # --- sync state ----------------------------------------------------------
    @_with_db_retry()
    async def get_sync_state(self, chat_id: int) -> asyncpg.Record | None:
        async with self.pool.acquire() as con:
            return await con.fetchrow(
                "SELECT * FROM sync_state WHERE chat_id=$1", chat_id)

    @_with_db_retry()
    async def init_sync_state(self, chat_id: int):
        async with self.pool.acquire() as con:
            await con.execute(
                "INSERT INTO sync_state (chat_id) VALUES ($1) ON CONFLICT DO NOTHING", chat_id)

    @_with_db_retry()
    async def set_backfill_checkpoint(self, chat_id: int, last_id: int, first_id: int | None = None):
        q = """
        INSERT INTO sync_state (chat_id, last_backfilled_message_id, first_message_id, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (chat_id) DO UPDATE SET
            last_backfilled_message_id = GREATEST(sync_state.last_backfilled_message_id,
                                                  EXCLUDED.last_backfilled_message_id),
            first_message_id = COALESCE(LEAST(sync_state.first_message_id, EXCLUDED.first_message_id),
                                        sync_state.first_message_id,
                                        EXCLUDED.first_message_id),
            updated_at = NOW();
        """
        async with self.pool.acquire() as con:
            await con.execute(q, chat_id, last_id, first_id)

    @_with_db_retry()
    async def mark_backfill_complete(self, chat_id: int):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE sync_state SET backfill_complete=TRUE, "
                "backfill_status='complete', updated_at=NOW() WHERE chat_id=$1",
                chat_id)

    @_with_db_retry()
    async def set_backfill_status(self, chat_id: int, status: str,
                                  mark_complete: bool | None = None):
        """Set truthful backfill_status. Never marks `backfill_complete` TRUE
        unless `mark_complete=True` AND status='complete'."""
        assert status in {"running", "syncing", "complete", "incomplete",
                          "blocked", "complete_with_errors",
                          "stalled", "degraded",
                          "syncing_paused_media_backpressure"}
        async with self.pool.acquire() as con:
            if mark_complete and status == "complete":
                await con.execute(
                    "UPDATE sync_state SET backfill_status=$2, backfill_complete=TRUE, "
                    "updated_at=NOW() WHERE chat_id=$1",
                    chat_id, status)
            else:
                await con.execute(
                    "UPDATE sync_state SET backfill_status=$2, backfill_complete=FALSE, "
                    "updated_at=NOW() WHERE chat_id=$1",
                    chat_id, status)

    @_with_db_retry()
    async def set_participants_sync(self, chat_id: int, offset: int,
                                    is_complete: bool, limitation: str | None):
        async with self.pool.acquire() as con:
            await con.execute("""
                INSERT INTO sync_state (chat_id, participants_offset, participants_synced,
                                        participants_is_complete, participants_limitation,
                                        participants_updated_at, updated_at)
                VALUES ($1,$2,TRUE,$3,$4,NOW(),NOW())
                ON CONFLICT (chat_id) DO UPDATE SET
                    participants_offset = EXCLUDED.participants_offset,
                    participants_synced = TRUE,
                    participants_is_complete = EXCLUDED.participants_is_complete,
                    participants_limitation = EXCLUDED.participants_limitation,
                    participants_updated_at = NOW(),
                    updated_at = NOW();
            """, chat_id, offset, is_complete, limitation)

    # --- backfill durability -------------------------------------------------
    @_with_db_retry()
    async def backfill_queue_upsert(self, chat_id: int, message_id: int,
                                    error: str, attempts: int, next_retry_at: datetime,
                                    status: str = "pending"):
        async with self.pool.acquire() as con:
            await con.execute("""
                INSERT INTO backfill_queue
                    (chat_id, message_id, attempts, last_error,
                     last_attempt_at, next_retry_at, status)
                VALUES ($1,$2,$3,$4,NOW(),$5,$6)
                ON CONFLICT (chat_id, message_id) DO UPDATE SET
                    attempts = backfill_queue.attempts + 1,
                    last_error = EXCLUDED.last_error,
                    last_attempt_at = NOW(),
                    next_retry_at = EXCLUDED.next_retry_at,
                    status = EXCLUDED.status;
            """, chat_id, message_id, attempts, error[:1000], next_retry_at, status)

    @_with_db_retry()
    async def backfill_queue_mark_done(self, chat_id: int, message_id: int):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE backfill_queue SET status='done', last_attempt_at=NOW() "
                "WHERE chat_id=$1 AND message_id=$2",
                chat_id, message_id)

    @_with_db_retry()
    async def backfill_queue_get_pending(self, chat_id: int, limit: int = 100):
        async with self.pool.acquire() as con:
            return await con.fetch(
                "SELECT chat_id, message_id, attempts, last_error FROM backfill_queue "
                "WHERE chat_id=$1 AND status='pending' "
                "AND (next_retry_at IS NULL OR next_retry_at <= NOW()) "
                "ORDER BY message_id LIMIT $2",
                chat_id, limit)

    @_with_db_retry()
    async def backfill_queue_min_pending(self, chat_id: int) -> int | None:
        async with self.pool.acquire() as con:
            return await con.fetchval(
                "SELECT MIN(message_id) FROM backfill_queue "
                "WHERE chat_id=$1 AND status='pending'",
                chat_id)

    @_with_db_retry()
    async def backfill_queue_counts(self, chat_id: int | None = None) -> dict:
        async with self.pool.acquire() as con:
            if chat_id is not None:
                rows = await con.fetch(
                    "SELECT status, COUNT(*) c FROM backfill_queue WHERE chat_id=$1 GROUP BY status",
                    chat_id)
            else:
                rows = await con.fetch(
                    "SELECT status, COUNT(*) c FROM backfill_queue GROUP BY status")
            return {r["status"]: r["c"] for r in rows}

    # --- memberships / participants -----------------------------------------
    @_with_db_retry()
    async def upsert_membership(self, chat_id: int, user_id: int, role: str,
                                joined_at=None, left_at=None,
                                source: str | None = None,
                                is_bot: bool | None = None,
                                raw: dict | None = None):
        q = """
        INSERT INTO memberships (chat_id, user_id, role, joined_at, left_at, source, is_bot, raw, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,NOW())
        ON CONFLICT (chat_id, user_id) DO UPDATE SET
            role=EXCLUDED.role,
            joined_at=COALESCE(EXCLUDED.joined_at, memberships.joined_at),
            left_at=COALESCE(EXCLUDED.left_at, memberships.left_at),
            source=COALESCE(EXCLUDED.source, memberships.source),
            is_bot=COALESCE(EXCLUDED.is_bot, memberships.is_bot),
            raw=EXCLUDED.raw,
            updated_at=NOW();
        """
        async with self.pool.acquire() as con:
            await con.execute(q, chat_id, user_id, role, joined_at, left_at,
                              source, is_bot, _j(raw))

    # --- media ---------------------------------------------------------------
    @_with_db_retry()
    async def upsert_media_record(self, rec: dict):
        q = """
        INSERT INTO media (file_id, chat_id, message_id, media_type, mime_type, size,
                           file_name, local_path, status, attempts, last_error,
                           duration, width, height, sha256, completed_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
        ON CONFLICT (file_id) DO UPDATE SET
            chat_id=COALESCE(EXCLUDED.chat_id, media.chat_id),
            message_id=COALESCE(EXCLUDED.message_id, media.message_id),
            media_type=COALESCE(EXCLUDED.media_type, media.media_type),
            mime_type=COALESCE(EXCLUDED.mime_type, media.mime_type),
            size=COALESCE(EXCLUDED.size, media.size),
            file_name=COALESCE(EXCLUDED.file_name, media.file_name),
            local_path=COALESCE(EXCLUDED.local_path, media.local_path),
            status=CASE
                WHEN media.status='done' AND EXCLUDED.status <> 'done' THEN media.status
                ELSE EXCLUDED.status
            END,
            attempts=GREATEST(EXCLUDED.attempts, media.attempts),
            last_error=COALESCE(EXCLUDED.last_error, media.last_error),
            duration=COALESCE(EXCLUDED.duration, media.duration),
            width=COALESCE(EXCLUDED.width, media.width),
            height=COALESCE(EXCLUDED.height, media.height),
            sha256=COALESCE(EXCLUDED.sha256, media.sha256),
            completed_at=COALESCE(EXCLUDED.completed_at, media.completed_at);
        """
        async with self.pool.acquire() as con:
            await con.execute(q,
                rec["file_id"], rec.get("chat_id"), rec.get("message_id"),
                rec.get("media_type"), rec.get("mime_type"), rec.get("size"),
                rec.get("file_name"), rec.get("local_path"),
                rec.get("status", "pending"), rec.get("attempts", 0), rec.get("last_error"),
                rec.get("duration"), rec.get("width"), rec.get("height"), rec.get("sha256"),
                rec.get("completed_at"))

    @_with_db_retry()
    async def media_increment_attempt(self, file_id: str, error: str | None = None):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE media SET attempts = attempts + 1, "
                "last_error = COALESCE($2, last_error) WHERE file_id=$1",
                file_id, (error or "")[:500] if error else None)

    @_with_db_retry()
    async def media_existing_path(self, file_id: str) -> tuple[str | None, str | None]:
        async with self.pool.acquire() as con:
            row = await con.fetchrow(
                "SELECT status, local_path FROM media WHERE file_id=$1", file_id)
            if row is None:
                return None, None
            return row["status"], row["local_path"]

    @_with_db_retry()
    async def media_get_retryable(self, limit: int = 200) -> list[asyncpg.Record]:
        async with self.pool.acquire() as con:
            return await con.fetch("""
                SELECT m.file_id, m.chat_id, m.message_id, m.media_type, m.size, m.attempts
                FROM media m
                WHERE m.status IN ('pending','failed')
                  AND m.attempts < 5
                  AND m.chat_id IS NOT NULL AND m.message_id IS NOT NULL
                ORDER BY m.first_seen ASC
                LIMIT $1
            """, limit)

    @_with_db_retry()
    async def link_message_media(self, chat_id: int, message_id: int, file_id: str):
        async with self.pool.acquire() as con:
            await con.execute(
                "INSERT INTO message_media (chat_id, message_id, file_id) VALUES ($1,$2,$3) "
                "ON CONFLICT DO NOTHING",
                chat_id, message_id, file_id)

    @_with_db_retry()
    async def attach_media_to_message(self, chat_id: int, message_id: int,
                                      media_type: str, file_id: str, local_path: str):
        async with self.pool.acquire() as con:
            async with con.transaction():
                await con.execute(
                    "UPDATE messages SET media_type=$3, media_path=$4, media_file_id=$5 "
                    "WHERE chat_id=$1 AND message_id=$2",
                    chat_id, message_id, media_type, local_path, file_id)
                await con.execute(
                    "INSERT INTO message_media (chat_id, message_id, file_id) VALUES ($1,$2,$3) "
                    "ON CONFLICT DO NOTHING",
                    chat_id, message_id, file_id)

    # --- avatars -------------------------------------------------------------
    @_with_db_retry()
    async def avatar_lookup(self, user_id: int, photo_file_id: int | None) -> str | None:
        async with self.pool.acquire() as con:
            return await con.fetchval(
                "SELECT local_path FROM avatar_history WHERE user_id=$1::bigint "
                "AND COALESCE(photo_file_id, 0::bigint)=COALESCE($2::bigint, 0::bigint)",
                user_id, photo_file_id)

    @_with_db_retry()
    async def avatar_record(self, user_id: int, photo_file_id: int | None, local_path: str):
        async with self.pool.acquire() as con:
            await con.execute("""
                INSERT INTO avatar_history (user_id, photo_file_id, local_path)
                VALUES ($1::bigint, $2::bigint, $3)
                ON CONFLICT (user_id, COALESCE(photo_file_id, 0::bigint)) DO UPDATE
                SET local_path = COALESCE(EXCLUDED.local_path, avatar_history.local_path);
            """, user_id, photo_file_id, local_path)

    # --- chat actions / errors ----------------------------------------------
    @_with_db_retry()
    async def record_action(self, chat_id: int, message_id: int | None,
                            actor_user_id: int | None, action: str, payload: dict | None):
        q = """
        INSERT INTO chat_actions (chat_id, message_id, actor_user_id, action, payload)
        VALUES ($1,$2,$3,$4,$5::jsonb);
        """
        async with self.pool.acquire() as con:
            await con.execute(q, chat_id, message_id, actor_user_id, action, _j(payload))

    async def record_error(self, kind: str, chat_id: int | None, message_id: int | None,
                           user_id: int | None, exc: BaseException):
        q = """
        INSERT INTO errors (kind, chat_id, message_id, user_id, message, traceback)
        VALUES ($1,$2,$3,$4,$5,$6);
        """
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-8000:]
        try:
            async with self.pool.acquire() as con:
                await con.execute(q, kind, chat_id, message_id, user_id, str(exc)[:1000], tb)
        except Exception as e:
            log.warning("could not record_error: %s", e)

    # --- runtime -------------------------------------------------------------
    @_with_db_retry()
    async def set_runtime(self, pid: int | None, status: str, targets: list[int] | None,
                          note: str | None, started: bool = False):
        async with self.pool.acquire() as con:
            if started:
                await con.execute("""
                    INSERT INTO runtime_state (id, pid, started_at, heartbeat_at, status, targets, note)
                    VALUES (1,$1,NOW(),NOW(),$2,$3::jsonb,$4)
                    ON CONFLICT (id) DO UPDATE SET
                        pid=EXCLUDED.pid, started_at=EXCLUDED.started_at,
                        heartbeat_at=EXCLUDED.heartbeat_at, status=EXCLUDED.status,
                        targets=EXCLUDED.targets, note=EXCLUDED.note;
                """, pid, status, _j(targets or []), note)
            else:
                await con.execute("""
                    UPDATE runtime_state SET pid=$1, heartbeat_at=NOW(),
                        status=$2, targets=$3::jsonb, note=COALESCE($4, note)
                    WHERE id=1;
                """, pid, status, _j(targets or []), note)

    @_with_db_retry()
    async def heartbeat(self, status: str | None = None):
        async with self.pool.acquire() as con:
            if status:
                await con.execute(
                    "UPDATE runtime_state SET heartbeat_at=NOW(), status=$1 WHERE id=1",
                    status)
            else:
                await con.execute(
                    "UPDATE runtime_state SET heartbeat_at=NOW() WHERE id=1")

    @_with_db_retry()
    async def get_runtime(self) -> asyncpg.Record | None:
        async with self.pool.acquire() as con:
            return await con.fetchrow("SELECT * FROM runtime_state WHERE id=1")

    @_with_db_retry()
    async def last_message_at(self) -> datetime | None:
        async with self.pool.acquire() as con:
            return await con.fetchval(
                "SELECT GREATEST(MAX(collected_at), MAX(date)) FROM messages")

    # --- v4: target_state ----------------------------------------------------
    @_with_db_retry()
    async def target_upsert(self, chat_id: int, target: str, status: str,
                            last_error: str | None = None):
        err = last_error[:1000] if last_error else None
        async with self.pool.acquire() as con:
            await con.execute("""
                INSERT INTO target_state (chat_id, target, backfill_status,
                    last_success_at, last_error_at, last_error, updated_at)
                VALUES ($1::bigint, $2::text, $3::text,
                    CASE WHEN $3::text IN ('complete','complete_with_errors')
                        THEN NOW() END,
                    CASE WHEN $4::text IS NOT NULL THEN NOW() END,
                    $4::text, NOW())
                ON CONFLICT (chat_id) DO UPDATE SET
                    target = EXCLUDED.target,
                    backfill_status = EXCLUDED.backfill_status,
                    last_success_at = COALESCE(
                        EXCLUDED.last_success_at, target_state.last_success_at),
                    last_error_at = COALESCE(
                        EXCLUDED.last_error_at, target_state.last_error_at),
                    last_error = COALESCE(
                        EXCLUDED.last_error, target_state.last_error),
                    updated_at = NOW();
            """, chat_id, target, status, err)

    @_with_db_retry()
    async def target_update_counts(self, chat_id: int,
                                   pending: int, dead: int):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE target_state SET pending_count=$2, dead_count=$3, "
                "updated_at=NOW() WHERE chat_id=$1",
                chat_id, pending, dead)

    @_with_db_retry()
    async def targets_all(self) -> list[asyncpg.Record]:
        async with self.pool.acquire() as con:
            return await con.fetch(
                "SELECT chat_id, target, backfill_status, last_success_at, "
                "last_error_at, last_error, pending_count, dead_count, "
                "enabled, disabled_at, disabled_reason "
                "FROM target_state ORDER BY chat_id")

    @_with_db_retry()
    async def targets_active(self) -> list[asyncpg.Record]:
        async with self.pool.acquire() as con:
            return await con.fetch(
                "SELECT chat_id, target, backfill_status, last_success_at, "
                "last_error_at, last_error, pending_count, dead_count "
                "FROM target_state WHERE enabled=TRUE ORDER BY chat_id")

    @_with_db_retry()
    async def target_disable(self, chat_id: int, reason: str = "removed_from_config"):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE target_state SET enabled=FALSE, disabled_at=NOW(), "
                "disabled_reason=$2, updated_at=NOW() WHERE chat_id=$1",
                chat_id, reason)

    @_with_db_retry()
    async def target_enable(self, chat_id: int):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE target_state SET enabled=TRUE, disabled_at=NULL, "
                "disabled_reason=NULL, updated_at=NOW() WHERE chat_id=$1",
                chat_id)

    @_with_db_retry()
    async def record_batch_progress(self, chat_id: int):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE sync_state SET last_batch_at=NOW(), "
                "batches_completed = batches_completed + 1 WHERE chat_id=$1",
                chat_id)

    # --- v5: durable user profile retry queue --------------------------------
    @_with_db_retry()
    async def user_profile_queue_upsert(self, user_id: int, error: str,
                                        hint_chat_id: int | None = None,
                                        hint_message_id: int | None = None,
                                        retry_in_minutes: int = 10):
        async with self.pool.acquire() as con:
            await con.execute("""
                INSERT INTO user_profile_queue
                    (user_id, attempts, last_error, last_attempt_at,
                     next_retry_at, hint_chat_id, hint_message_id)
                VALUES ($1::bigint, 1, $2, NOW(),
                        NOW() + ($3 || ' minutes')::interval, $4, $5)
                ON CONFLICT (user_id) DO UPDATE SET
                    attempts = user_profile_queue.attempts + 1,
                    last_error = EXCLUDED.last_error,
                    last_attempt_at = NOW(),
                    next_retry_at = NOW() + ($3 || ' minutes')::interval,
                    hint_chat_id = COALESCE(user_profile_queue.hint_chat_id, EXCLUDED.hint_chat_id),
                    hint_message_id = COALESCE(user_profile_queue.hint_message_id, EXCLUDED.hint_message_id);
            """, user_id, (error or "")[:500], str(retry_in_minutes),
                 hint_chat_id, hint_message_id)

    @_with_db_retry()
    async def user_profile_queue_mark_done(self, user_id: int):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE user_profile_queue SET status='done', last_attempt_at=NOW() "
                "WHERE user_id=$1::bigint",
                user_id)

    @_with_db_retry()
    async def user_profile_queue_mark_dead(self, user_id: int):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE user_profile_queue SET status='dead', last_attempt_at=NOW() "
                "WHERE user_id=$1::bigint",
                user_id)

    @_with_db_retry()
    async def user_profile_queue_pending(self, limit: int = 100,
                                         max_attempts: int = 5):
        async with self.pool.acquire() as con:
            return await con.fetch(
                "SELECT user_id, attempts, hint_chat_id, hint_message_id "
                "FROM user_profile_queue "
                "WHERE status='pending' AND attempts < $2 "
                "AND (next_retry_at IS NULL OR next_retry_at <= NOW()) "
                "ORDER BY first_seen LIMIT $1",
                limit, max_attempts)

    @_with_db_retry()
    async def user_profile_queue_stats(self) -> dict:
        async with self.pool.acquire() as con:
            rows = await con.fetch(
                "SELECT status, count(*) FROM user_profile_queue GROUP BY status")
            return {r["status"]: r["count"] for r in rows}

    # --- v5: error audit ----------------------------------------------------
    @_with_db_retry()
    async def errors_unresolved_by_group(self) -> list[dict]:
        async with self.pool.acquire() as con:
            rows = await con.fetch(
                "SELECT COALESCE(error_group, kind) AS group, COUNT(*) AS n "
                "FROM errors WHERE resolved_at IS NULL "
                "GROUP BY 1 ORDER BY 2 DESC")
            return [dict(r) for r in rows]

    @_with_db_retry()
    async def errors_resolve_group(self, group: str, resolution: str):
        async with self.pool.acquire() as con:
            return await con.execute(
                "UPDATE errors SET resolved_at=NOW(), resolution=$2 "
                "WHERE error_group=$1 AND resolved_at IS NULL",
                group, resolution)

    # --- v5: message throughput & batch metrics -----------------------------
    @_with_db_retry()
    async def messages_per_minute(self, window_minutes: int = 5) -> float:
        async with self.pool.acquire() as con:
            n = await con.fetchval(
                f"SELECT COUNT(*) FROM messages WHERE collected_at > NOW() - "
                f"interval '{int(window_minutes)} minutes'")
            return float(n) / max(window_minutes, 1)

    @_with_db_retry()
    async def media_oldest_pending_age(self) -> int | None:
        async with self.pool.acquire() as con:
            row = await con.fetchval(
                "SELECT EXTRACT(EPOCH FROM (NOW() - MIN(first_seen)))::int "
                "FROM media WHERE status IN ('pending','failed','needs_refresh')")
            return row

    @_with_db_retry()
    async def media_throughput(self) -> dict:
        async with self.pool.acquire() as con:
            row = await con.fetchrow("""
                SELECT COUNT(*) FILTER (WHERE completed_at > NOW() - interval '1 minute')  AS m1,
                       COUNT(*) FILTER (WHERE completed_at > NOW() - interval '5 minutes') AS m5,
                       COUNT(*) FILTER (WHERE completed_at > NOW() - interval '15 minutes') AS m15
                FROM media WHERE status='done'
            """)
            return {"m1": row["m1"], "m5": row["m5"]/5.0, "m15": row["m15"]/15.0}

    @_with_db_retry()
    async def media_status_breakdown(self) -> dict:
        async with self.pool.acquire() as con:
            rows = await con.fetch(
                "SELECT status, COUNT(*) FROM media GROUP BY status")
            return {r["status"]: r["count"] for r in rows}

    # --- v6: coordinator progress ------------------------------------------
    @_with_db_retry()
    async def coordinator_batch_start(self, chat_id: int, start_message_id: int):
        async with self.pool.acquire() as con:
            await con.execute("""
                INSERT INTO coordinator_progress (chat_id, batch_started_at,
                    batch_last_progress_at, batch_last_message_id, batch_running,
                    coordinator_heartbeat_at, updated_at)
                VALUES ($1, NOW(), NOW(), $2, TRUE, NOW(), NOW())
                ON CONFLICT (chat_id) DO UPDATE SET
                    batch_started_at = NOW(),
                    batch_last_progress_at = NOW(),
                    batch_last_message_id = EXCLUDED.batch_last_message_id,
                    batch_running = TRUE,
                    coordinator_heartbeat_at = NOW(),
                    updated_at = NOW();
            """, chat_id, start_message_id)

    @_with_db_retry()
    async def coordinator_batch_progress(self, chat_id: int, message_id: int):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE coordinator_progress SET "
                "batch_last_progress_at=NOW(), batch_last_message_id=$2, "
                "coordinator_heartbeat_at=NOW(), updated_at=NOW() WHERE chat_id=$1",
                chat_id, message_id)

    @_with_db_retry()
    async def coordinator_batch_end(self, chat_id: int, added: int,
                                    duration_sec: int, success: bool,
                                    reason: str | None = None):
        async with self.pool.acquire() as con:
            if success:
                await con.execute("""
                    UPDATE coordinator_progress SET
                        batch_running=FALSE,
                        consecutive_batch_failures=0,
                        last_batch_duration_sec=$2,
                        last_batch_added=$3,
                        coordinator_heartbeat_at=NOW(),
                        updated_at=NOW()
                    WHERE chat_id=$1
                """, chat_id, duration_sec, added)
            else:
                await con.execute("""
                    UPDATE coordinator_progress SET
                        batch_running=FALSE,
                        consecutive_batch_failures=consecutive_batch_failures+1,
                        last_batch_duration_sec=$2,
                        last_batch_added=$3,
                        last_recovery_at=NOW(),
                        last_recovery_reason=$4,
                        coordinator_heartbeat_at=NOW(),
                        updated_at=NOW()
                    WHERE chat_id=$1
                """, chat_id, duration_sec, added, (reason or "")[:300])

    @_with_db_retry()
    async def coordinator_heartbeat(self, chat_id: int):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE coordinator_progress SET coordinator_heartbeat_at=NOW() "
                "WHERE chat_id=$1", chat_id)

    @_with_db_retry()
    async def coordinator_progress_all(self) -> list:
        async with self.pool.acquire() as con:
            return [dict(r) for r in await con.fetch(
                "SELECT chat_id, batch_started_at, batch_last_progress_at, "
                "batch_last_message_id, batch_running, "
                "consecutive_batch_failures, last_batch_duration_sec, "
                "last_batch_added, last_recovery_at, last_recovery_reason, "
                "coordinator_heartbeat_at FROM coordinator_progress")]

    # --- v6: media worker heartbeats ---------------------------------------
    @_with_db_retry()
    async def worker_heartbeat(self, worker_id: int, pid: int | None = None,
                               current_file_id: str | None = None,
                               completed_delta: int = 0, failed_delta: int = 0,
                               last_error: str | None = None):
        async with self.pool.acquire() as con:
            await con.execute("""
                INSERT INTO media_worker_state (worker_id, pid, started_at,
                    heartbeat_at, current_file_id, completed_count, failed_count,
                    last_error, updated_at)
                VALUES ($1, $2, NOW(), NOW(), $3, $4, $5, $6, NOW())
                ON CONFLICT (worker_id) DO UPDATE SET
                    pid = COALESCE(EXCLUDED.pid, media_worker_state.pid),
                    heartbeat_at = NOW(),
                    current_file_id = EXCLUDED.current_file_id,
                    completed_count = media_worker_state.completed_count + $4,
                    failed_count = media_worker_state.failed_count + $5,
                    last_error = COALESCE(EXCLUDED.last_error, media_worker_state.last_error),
                    updated_at = NOW();
            """, worker_id, pid, current_file_id, completed_delta,
                 failed_delta, (last_error or None))

    @_with_db_retry()
    async def workers_all(self) -> list:
        async with self.pool.acquire() as con:
            return [dict(r) for r in await con.fetch(
                "SELECT worker_id, pid, "
                "EXTRACT(EPOCH FROM (NOW()-heartbeat_at))::int AS heartbeat_age_sec, "
                "current_file_id, completed_count, failed_count, last_error "
                "FROM media_worker_state ORDER BY worker_id")]

    @_with_db_retry()
    async def media_mark_unavailable(self, file_id: str, reason: str):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE media SET status='unavailable', last_error=$2, "
                "attempts=attempts+1 WHERE file_id=$1",
                file_id, reason[:300])

    @_with_db_retry()
    async def media_needs_refresh(self, limit: int = 100) -> list:
        async with self.pool.acquire() as con:
            return [dict(r) for r in await con.fetch(
                "SELECT file_id, chat_id, message_id, media_type, attempts "
                "FROM media WHERE status='needs_refresh' "
                "AND chat_id IS NOT NULL AND message_id IS NOT NULL "
                "AND attempts < 5 ORDER BY first_seen LIMIT $1", limit)]

    # --- v4: telegram heartbeat ---------------------------------------------
    @_with_db_retry()
    async def record_tg_ok(self):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE runtime_state SET telegram_connected=TRUE, "
                "telegram_last_ok_at=NOW(), telegram_last_error=NULL "
                "WHERE id=1")

    @_with_db_retry()
    async def record_tg_error(self, err: str):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE runtime_state SET telegram_connected=FALSE, "
                "telegram_last_error=$1 WHERE id=1",
                err[:500] if err else None)

    @_with_db_retry()
    async def record_update_received(self):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE runtime_state SET update_last_received_at=NOW() WHERE id=1")

    # --- v4: backfill queue ack ---------------------------------------------
    @_with_db_retry()
    async def backfill_queue_acknowledge(self, chat_id: int,
                                         message_id: int, by: str = "user"):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE backfill_queue SET acknowledged=TRUE, "
                "acknowledged_at=NOW(), acknowledged_by=$3 "
                "WHERE chat_id=$1 AND message_id=$2",
                chat_id, message_id, by)

    @_with_db_retry()
    async def backfill_queue_stats(self, chat_id: int | None = None) -> dict:
        async with self.pool.acquire() as con:
            if chat_id is not None:
                rows = await con.fetch(
                    "SELECT status, acknowledged, COUNT(*) c "
                    "FROM backfill_queue WHERE chat_id=$1 "
                    "GROUP BY status, acknowledged",
                    chat_id)
            else:
                rows = await con.fetch(
                    "SELECT status, acknowledged, COUNT(*) c "
                    "FROM backfill_queue GROUP BY status, acknowledged")
            out = {"pending": 0, "dead_unacknowledged": 0,
                   "dead_acknowledged": 0, "done": 0}
            for r in rows:
                if r["status"] == "pending":
                    out["pending"] += r["c"]
                elif r["status"] == "dead":
                    if r["acknowledged"]:
                        out["dead_acknowledged"] += r["c"]
                    else:
                        out["dead_unacknowledged"] += r["c"]
                elif r["status"] == "done":
                    out["done"] += r["c"]
            return out

    @_with_db_retry()
    async def backfill_queue_problem_ids(self, chat_id: int,
                                         limit: int = 20) -> list[int]:
        async with self.pool.acquire() as con:
            rows = await con.fetch(
                "SELECT message_id FROM backfill_queue "
                "WHERE chat_id=$1 AND status IN ('pending','dead') "
                "AND NOT acknowledged ORDER BY message_id LIMIT $2",
                chat_id, limit)
            return [r["message_id"] for r in rows]

    # --- v4: media metadata_only --------------------------------------------
    @_with_db_retry()
    async def media_set_metadata_only(self, file_id: str, media_type: str,
                                      chat_id: int, message_id: int,
                                      raw: dict | None = None):
        async with self.pool.acquire() as con:
            async with con.transaction():
                await con.execute("""
                    INSERT INTO media (file_id, chat_id, message_id, media_type,
                        status, is_downloadable, first_seen)
                    VALUES ($1,$2,$3,$4,'metadata_only',FALSE,NOW())
                    ON CONFLICT (file_id) DO UPDATE SET
                        chat_id = COALESCE(EXCLUDED.chat_id, media.chat_id),
                        message_id = COALESCE(EXCLUDED.message_id, media.message_id),
                        status = CASE WHEN media.status='done' THEN media.status
                                      ELSE 'metadata_only' END,
                        is_downloadable = FALSE;
                """, file_id, chat_id, message_id, media_type)
                await con.execute(
                    "INSERT INTO message_media (chat_id, message_id, file_id) "
                    "VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
                    chat_id, message_id, file_id)
                if raw is not None:
                    await con.execute(
                        "UPDATE messages SET webpage = COALESCE(webpage, $3::jsonb) "
                        "WHERE chat_id=$1 AND message_id=$2",
                        chat_id, message_id, _j(raw))
