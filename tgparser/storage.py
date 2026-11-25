import asyncio
import hashlib
import ipaddress
import json
import logging
import re
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
    async def media_get_retryable(self, limit: int = 200,
                                  chat_ids: list[int] | None = None) -> list[asyncpg.Record]:
        async with self.pool.acquire() as con:
            return await con.fetch("""
                SELECT m.file_id, m.chat_id, m.message_id, m.media_type, m.size, m.attempts
                FROM media m
                WHERE m.status IN ('pending','failed')
                  AND m.attempts < 5
                  AND m.chat_id IS NOT NULL AND m.message_id IS NOT NULL
                  AND ($2::bigint[] IS NULL OR m.chat_id = ANY($2::bigint[]))
                ORDER BY m.first_seen ASC
                LIMIT $1
            """, limit, chat_ids)

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

    # --- v7.1: user identity enrichment --------------------------------------
    # entity_quality rank (for name/bio/username downgrade protection)
    _QUALITY_RANK = {"min": 1, "normal": 2, "full": 3}

    # access_hash priority rank (official Telegram rules at
    # https://core.telegram.org/api/peers "Access hash"):
    #   full > from_message > min > zero > unknown
    _AH_PRIORITY = {"full": 4, "from_message": 3, "min": 2, "zero": 1, "unknown": 0}

    @staticmethod
    def _hash_fp(ah: int | None) -> str | None:
        if ah is None:
            return None
        import hashlib
        # md5 matches the DB fingerprint we seeded in migration 0010
        return hashlib.md5(str(ah).encode()).hexdigest()

    @_with_db_retry()
    async def upsert_user_from_entity(self, data: dict, record_history: bool = True):
        """Store a user row from a Telethon-style entity dict.

        Enforces official Telegram access_hash priority rules AND the broader
        entity_quality downgrade protection.
        """
        uid = data["id"]
        new_q = data.get("entity_quality") or "normal"
        new_rank = self._QUALITY_RANK.get(new_q, 1)

        # Access-hash classification. Callers pass access_hash_kind explicitly;
        # default to 'unknown' when not stated (never promotes).
        new_ah = data.get("access_hash")
        new_ah_kind = data.get("access_hash_kind") or (
            "full" if new_q == "full" else
            ("min" if data.get("is_min") else
             ("from_message" if data.get("entity_source") == "message_sender" else "unknown"))
        )
        new_ah_prio = self._AH_PRIORITY.get(new_ah_kind, 0)
        new_auth_fp = data.get("access_hash_auth_fingerprint") or "default"

        async with self.pool.acquire() as con:
            prev = await con.fetchrow(
                "SELECT username, first_name, last_name, bio, photo_file_id, "
                "entity_quality, access_hash, access_hash_kind, "
                "access_hash_priority, access_hash_auth_fingerprint "
                "FROM users WHERE id=$1::bigint", uid)
            prev_q = (prev["entity_quality"] if prev else None) or "min"
            prev_rank = self._QUALITY_RANK.get(prev_q, 0)
            prev_ah_prio = (prev["access_hash_priority"] if prev else 0) or 0
            prev_auth_fp = (prev["access_hash_auth_fingerprint"] if prev else None) or "default"

            # Decide whether the new access_hash is ALLOWED to become active.
            # Rule 1: never from a different auth session.
            # Rule 2: lower priority never beats higher.
            # Rule 3: same priority is allowed to refresh (hash may have rotated).
            # Rule 4: null doesn't clear an existing active hash.
            ah_applies = False
            if new_ah is not None:
                if prev is None:
                    # new user row - always apply
                    ah_applies = True
                elif new_auth_fp == prev_auth_fp:
                    ah_applies = new_ah_prio >= prev_ah_prio

            # Always record every observed hash in history, even if not active.
            # v7.3: the whole history write + authoritative users update runs
            # inside a single transaction so promotion is atomic -- no window
            # where two rows per (user_id, auth_fingerprint) are both active.
            if new_ah is not None:
                async with con.transaction():
                    new_fp = self._hash_fp(new_ah)
                    # If the incoming hash is going to become active, retire
                    # every other is_active row for this user. Normative rule
                    # (v7.3): exactly ONE is_active history row per user
                    # mirrors the single authoritative users.access_hash slot.
                    # Rows from a different auth session stay in the append-
                    # only history (never deleted) but are moved out of
                    # is_active=TRUE: foreign-session hashes are not usable in
                    # the current session.
                    if ah_applies:
                        await con.execute("""
                            UPDATE access_hash_history
                               SET is_active = FALSE,
                                   replaced_at = NOW(),
                                   replacement_reason = CASE
                                     WHEN auth_fingerprint = $2
                                     THEN 'promotion_by_higher_priority'
                                     ELSE 'superseded_by_current_session' END
                             WHERE user_id = $1::bigint
                               AND is_active = TRUE
                               AND NOT (hash_fingerprint = $3
                                        AND auth_fingerprint = $2)
                        """, uid, new_auth_fp, new_fp)
                    await con.execute("""
                        INSERT INTO access_hash_history
                            (user_id, hash_fingerprint, access_hash, kind, priority,
                             source, auth_fingerprint, source_chat_id, source_message_id,
                             first_seen, last_seen, became_active_at, is_active)
                        VALUES ($1::bigint, $2, $3::bigint, $4, $5, $6, $7, $8, $9,
                                NOW(), NOW(),
                                CASE WHEN $10 THEN NOW() END, $10)
                        ON CONFLICT (user_id, hash_fingerprint, auth_fingerprint)
                        DO UPDATE SET last_seen=NOW(),
                            is_active = access_hash_history.is_active OR EXCLUDED.is_active,
                            became_active_at = COALESCE(access_hash_history.became_active_at,
                                                         EXCLUDED.became_active_at),
                            replaced_at = CASE
                                WHEN access_hash_history.is_active OR EXCLUDED.is_active
                                THEN NULL ELSE access_hash_history.replaced_at END;
                    """, uid, new_fp, new_ah, new_ah_kind, new_ah_prio,
                         data.get("entity_source"), new_auth_fp,
                         data.get("source_chat_id"), data.get("source_message_id"),
                         ah_applies)

            if prev and new_rank < prev_rank:
                # downgrade of OVERALL quality: refresh only technical fields.
                # Still apply ah if rules permit.
                await con.execute("""
                    UPDATE users SET
                        access_hash = CASE WHEN $3 THEN $2::bigint ELSE access_hash END,
                        access_hash_kind = CASE WHEN $3 THEN $4 ELSE access_hash_kind END,
                        access_hash_priority = CASE WHEN $3 THEN $5 ELSE access_hash_priority END,
                        access_hash_updated_at = CASE WHEN $3 THEN NOW()
                                                     ELSE access_hash_updated_at END,
                        access_hash_source_chat_id = CASE WHEN $3 THEN $6
                                                         ELSE access_hash_source_chat_id END,
                        access_hash_source_message_id = CASE WHEN $3 THEN $7
                                                            ELSE access_hash_source_message_id END,
                        access_hash_is_stale = CASE WHEN $3 THEN FALSE
                                                   ELSE access_hash_is_stale END,
                        last_seen = NOW(),
                        last_updated = NOW()
                     WHERE id = $1::bigint
                """, uid, new_ah, ah_applies, new_ah_kind, new_ah_prio,
                     data.get("source_chat_id"), data.get("source_message_id"))
                return

            # upsert with new quality taking over. access_hash columns obey
            # the priority rule (ah_applies), computed above.
            await con.execute("""
                INSERT INTO users (
                    id, access_hash, access_hash_updated_at,
                    access_hash_kind, access_hash_priority,
                    access_hash_auth_fingerprint,
                    access_hash_source_chat_id, access_hash_source_message_id,
                    username, first_name, last_name, phone, bio,
                    is_bot, is_premium, is_scam, is_fake, is_verified, is_deleted,
                    is_contact, is_mutual_contact, is_close_friend, is_min,
                    lang_code, photo_path, photo_dc, photo_file_id,
                    entity_source, entity_quality,
                    raw, full_fetched_at, first_seen, last_seen, last_updated)
                VALUES ($1::bigint,
                    CASE WHEN $26 THEN $2::bigint END,
                    CASE WHEN $26 AND $2 IS NOT NULL THEN NOW() END,
                    CASE WHEN $26 THEN $27::text END,
                    CASE WHEN $26 THEN $28::int ELSE 0 END,
                    CASE WHEN $26 THEN $29::text END,
                    CASE WHEN $26 THEN $30::bigint END,
                    CASE WHEN $26 THEN $31::bigint END,
                    $3,$4,$5,$6,$7,
                    $8,$9,$10,$11,$12,$13,
                    $14,$15,$16,$17,
                    $18,$19,$20,$21::bigint,
                    $22,$23,
                    $24::jsonb,$25,NOW(),NOW(),NOW())
                ON CONFLICT (id) DO UPDATE SET
                    access_hash = CASE WHEN $26 THEN $2::bigint ELSE users.access_hash END,
                    access_hash_kind = CASE WHEN $26 THEN $27::text ELSE users.access_hash_kind END,
                    access_hash_priority = CASE WHEN $26 THEN $28::int ELSE users.access_hash_priority END,
                    access_hash_auth_fingerprint = CASE WHEN $26 THEN $29::text ELSE users.access_hash_auth_fingerprint END,
                    access_hash_source_chat_id = CASE WHEN $26 THEN $30::bigint ELSE users.access_hash_source_chat_id END,
                    access_hash_source_message_id = CASE WHEN $26 THEN $31::bigint ELSE users.access_hash_source_message_id END,
                    access_hash_is_stale = CASE WHEN $26 THEN FALSE ELSE users.access_hash_is_stale END,
                    access_hash_updated_at = CASE
                        WHEN $26 AND $2 IS NOT NULL
                             AND ($2::bigint IS DISTINCT FROM users.access_hash)
                        THEN NOW()
                        ELSE users.access_hash_updated_at END,
                    username   = COALESCE(EXCLUDED.username,  users.username),
                    first_name = COALESCE(EXCLUDED.first_name, users.first_name),
                    last_name  = COALESCE(EXCLUDED.last_name,  users.last_name),
                    phone      = COALESCE(EXCLUDED.phone,     users.phone),
                    bio        = COALESCE(EXCLUDED.bio,       users.bio),
                    is_bot     = COALESCE(EXCLUDED.is_bot,    users.is_bot),
                    is_premium = COALESCE(EXCLUDED.is_premium,users.is_premium),
                    is_scam    = COALESCE(EXCLUDED.is_scam,   users.is_scam),
                    is_fake    = COALESCE(EXCLUDED.is_fake,   users.is_fake),
                    is_verified= COALESCE(EXCLUDED.is_verified,users.is_verified),
                    is_deleted = COALESCE(EXCLUDED.is_deleted,users.is_deleted),
                    is_contact = COALESCE(EXCLUDED.is_contact,users.is_contact),
                    is_mutual_contact = COALESCE(EXCLUDED.is_mutual_contact, users.is_mutual_contact),
                    is_close_friend   = COALESCE(EXCLUDED.is_close_friend,   users.is_close_friend),
                    is_min     = COALESCE(EXCLUDED.is_min,    users.is_min),
                    lang_code  = COALESCE(EXCLUDED.lang_code, users.lang_code),
                    photo_path     = COALESCE(EXCLUDED.photo_path,     users.photo_path),
                    photo_dc       = COALESCE(EXCLUDED.photo_dc,       users.photo_dc),
                    photo_file_id  = COALESCE(EXCLUDED.photo_file_id,  users.photo_file_id),
                    entity_source  = EXCLUDED.entity_source,
                    entity_quality = EXCLUDED.entity_quality,
                    raw = EXCLUDED.raw,
                    full_fetched_at = COALESCE(EXCLUDED.full_fetched_at, users.full_fetched_at),
                    last_seen = NOW(),
                    last_updated = NOW();
            """, uid, new_ah,
                 data.get("username"), data.get("first_name"), data.get("last_name"),
                 data.get("phone"), data.get("bio"),
                 data.get("is_bot"), data.get("is_premium"), data.get("is_scam"),
                 data.get("is_fake"), data.get("is_verified"), data.get("is_deleted"),
                 data.get("is_contact"), data.get("is_mutual_contact"),
                 data.get("is_close_friend"), data.get("is_min"),
                 data.get("lang_code"), data.get("photo_path"),
                 data.get("photo_dc"), data.get("photo_file_id"),
                 data.get("entity_source"), new_q,
                 _j(data.get("raw")), data.get("full_fetched_at"),
                 ah_applies, new_ah_kind, new_ah_prio, new_auth_fp,
                 data.get("source_chat_id"), data.get("source_message_id"))

            if record_history and prev is not None:
                for field in ("username", "first_name", "last_name", "bio",
                              "photo_file_id"):
                    old = prev[field]
                    new = data.get(field)
                    if new is None:
                        continue
                    if (old or "") != (str(new) if new is not None else ""):
                        await con.execute(
                            "INSERT INTO user_profile_history "
                            "(user_id, field, old_value, new_value, source_chat_id, source_message_id) "
                            "VALUES ($1::bigint,$2,$3,$4,$5,$6)",
                            uid, field,
                            None if old is None else str(old),
                            None if new is None else str(new),
                            data.get("source_chat_id"),
                            data.get("source_message_id"))

    @_with_db_retry()
    async def record_user_observation(self, user_id: int, chat_id: int,
                                      message_id: int | None,
                                      source_type: str, entity_quality: str,
                                      username_at_time: str | None,
                                      display_name_at_time: str | None,
                                      access_hash_present: bool,
                                      raw_meta: dict | None = None):
        async with self.pool.acquire() as con:
            # v7.1: dedup via UNIQUE(user_id,chat_id,COALESCE(message_id,0),source_type)
            await con.execute("""
                INSERT INTO user_observations
                  (user_id, chat_id, message_id, source_type, entity_quality,
                   username_at_time, display_name_at_time, access_hash_present,
                   raw_meta)
                VALUES ($1::bigint,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
                ON CONFLICT (user_id, chat_id, (COALESCE(message_id, 0)), source_type)
                DO UPDATE SET observed_at=NOW(),
                    entity_quality=EXCLUDED.entity_quality,
                    access_hash_present=user_observations.access_hash_present
                                        OR EXCLUDED.access_hash_present
            """, user_id, chat_id, message_id, source_type, entity_quality,
                 username_at_time, display_name_at_time, access_hash_present,
                 _j(raw_meta))

    # --- v7.1: access_hash aggregates / stale flag ---------------------------
    @_with_db_retry()
    async def access_hash_aggregates(self) -> dict:
        async with self.pool.acquire() as con:
            row = await con.fetchrow("""
                SELECT COUNT(*) AS users_total,
                   COUNT(*) FILTER (WHERE access_hash IS NOT NULL
                                      AND access_hash_kind='full' AND NOT access_hash_is_stale) AS with_full,
                   COUNT(*) FILTER (WHERE access_hash IS NOT NULL
                                      AND access_hash_kind='from_message' AND NOT access_hash_is_stale) AS with_from_message,
                   COUNT(*) FILTER (WHERE access_hash IS NOT NULL
                                      AND access_hash_kind='min' AND NOT access_hash_is_stale) AS with_min,
                   COUNT(*) FILTER (WHERE access_hash IS NOT NULL
                                      AND access_hash_kind='zero' AND NOT access_hash_is_stale) AS with_zero,
                   COUNT(*) FILTER (WHERE access_hash IS NOT NULL
                                      AND access_hash_kind='unknown' AND NOT access_hash_is_stale) AS with_unknown,
                   COUNT(*) FILTER (WHERE access_hash IS NULL) AS without,
                   COUNT(*) FILTER (WHERE access_hash_is_stale) AS stale
                FROM users WHERE id > 0
            """)
            return dict(row)

    @_with_db_retry()
    async def access_hash_mark_stale(self, user_id: int, reason: str):
        async with self.pool.acquire() as con:
            async with con.transaction():
                await con.execute(
                    "UPDATE users SET access_hash_is_stale=TRUE WHERE id=$1::bigint",
                    user_id)
                await con.execute(
                    "UPDATE access_hash_history SET replaced_at=NOW(), "
                    "replacement_reason=$2, is_active=FALSE "
                    "WHERE user_id=$1::bigint AND is_active=TRUE",
                    user_id, (reason or "")[:300])

    # --- v7.1: identity enrichment queue ---------------------------------
    @_with_db_retry()
    async def identity_enqueue(self, user_id: int, priority: int = 1000,
                               hint_chat_id: int | None = None,
                               hint_message_id: int | None = None):
        async with self.pool.acquire() as con:
            await con.execute("""
                INSERT INTO identity_enrichment_queue
                    (user_id, priority, hint_chat_id, hint_message_id)
                VALUES ($1::bigint, $2, $3, $4)
                ON CONFLICT (user_id) DO UPDATE SET
                    priority = LEAST(identity_enrichment_queue.priority, EXCLUDED.priority),
                    hint_chat_id = COALESCE(identity_enrichment_queue.hint_chat_id, EXCLUDED.hint_chat_id),
                    hint_message_id = COALESCE(identity_enrichment_queue.hint_message_id, EXCLUDED.hint_message_id);
            """, user_id, priority, hint_chat_id, hint_message_id)

    @_with_db_retry()
    async def identity_pending(self, limit: int = 20, max_attempts: int = 5) -> list:
        async with self.pool.acquire() as con:
            return [dict(r) for r in await con.fetch(
                "SELECT user_id, priority, attempts, hint_chat_id, hint_message_id "
                "FROM identity_enrichment_queue "
                "WHERE status IN ('pending','waiting_entity') "
                "AND attempts < $2 "
                "AND (next_retry_at IS NULL OR next_retry_at <= NOW()) "
                "ORDER BY priority, first_seen LIMIT $1", limit, max_attempts)]

    @_with_db_retry()
    async def identity_mark(self, user_id: int, status: str,
                            error: str | None = None, retry_in_minutes: int = 10):
        async with self.pool.acquire() as con:
            await con.execute("""
                UPDATE identity_enrichment_queue
                   SET status=$2,
                       attempts=attempts+1,
                       last_error=COALESCE($3, last_error),
                       last_attempt_at=NOW(),
                       next_retry_at=NOW() + ($4 || ' minutes')::interval
                 WHERE user_id=$1::bigint
            """, user_id, status, (error or "")[:500] if error else None,
                 str(retry_in_minutes))

    @_with_db_retry()
    async def identity_stats(self) -> dict:
        async with self.pool.acquire() as con:
            rows = await con.fetch(
                "SELECT status, count(*) FROM identity_enrichment_queue GROUP BY status")
            return {r["status"]: r["count"] for r in rows}

    @_with_db_retry()
    async def user_access_hash(self, user_id: int) -> int | None:
        async with self.pool.acquire() as con:
            return await con.fetchval(
                "SELECT access_hash FROM users WHERE id=$1::bigint", user_id)

    @_with_db_retry()
    async def users_enrichment_stats(self) -> dict:
        async with self.pool.acquire() as con:
            row = await con.fetchrow("""
                SELECT COUNT(*) AS total,
                       COUNT(access_hash) AS with_access_hash,
                       COUNT(username) AS with_username,
                       COUNT(first_name) AS with_first_name,
                       COUNT(last_name) AS with_last_name,
                       COUNT(bio) AS with_bio,
                       COUNT(phone) AS with_phone,
                       COUNT(photo_file_id) AS with_avatar,
                       COUNT(*) FILTER (WHERE entity_quality='min') AS q_min,
                       COUNT(*) FILTER (WHERE entity_quality='normal') AS q_normal,
                       COUNT(*) FILTER (WHERE entity_quality='full') AS q_full,
                       COUNT(*) FILTER (WHERE full_fetched_at IS NOT NULL) AS with_full_fetch
                FROM users WHERE id > 0
            """)
            return dict(row)

    @_with_db_retry()
    async def users_multi_chat(self) -> int:
        async with self.pool.acquire() as con:
            return await con.fetchval("""
                SELECT COUNT(*) FROM (
                    SELECT user_id FROM user_observations
                    GROUP BY user_id HAVING COUNT(DISTINCT chat_id) > 1
                ) x
            """)

    @_with_db_retry()
    async def users_negative_id_count(self) -> int:
        async with self.pool.acquire() as con:
            return await con.fetchval("SELECT COUNT(*) FROM users WHERE id < 0")

    @_with_db_retry()
    async def media_needs_refresh(self, limit: int = 100,
                                  chat_ids: list[int] | None = None) -> list:
        async with self.pool.acquire() as con:
            return [dict(r) for r in await con.fetch(
                "SELECT file_id, chat_id, message_id, media_type, attempts "
                "FROM media WHERE status='needs_refresh' "
                "AND chat_id IS NOT NULL AND message_id IS NOT NULL "
                "AND ($2::bigint[] IS NULL OR chat_id = ANY($2::bigint[])) "
                "AND attempts < 5 ORDER BY first_seen LIMIT $1", limit, chat_ids)]

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

    # ========================================================================
    # v11: dynamic targets foundation. Every method here requires
    # schema_version() >= 11; callers check that first.
    # ========================================================================

    @_with_db_retry()
    async def schema_version(self) -> int:
        """max(schema_migrations.version); 0 if the table does not exist."""
        async with self.pool.acquire() as con:
            exists = await con.fetchval(
                "SELECT to_regclass('public.schema_migrations') IS NOT NULL")
            if not exists:
                return 0
            v = await con.fetchval("SELECT max(version) FROM schema_migrations")
            return int(v or 0)

    # --- gate / running engine ---------------------------------------------
    @_with_db_retry()
    async def dynamic_gate_enabled(self) -> bool:
        async with self.pool.acquire() as con:
            exists = await con.fetchval(
                "SELECT to_regclass('public.deployment_gates') IS NOT NULL")
            if not exists:
                return False
            v = await con.fetchval(
                "SELECT enabled FROM deployment_gates WHERE key='dynamic_targets'")
            return v is True

    @_with_db_retry()
    async def set_runtime_dynamic_engine(self, on: bool) -> None:
        """Record the engine this process actually runs. Raises if the value
        could not be written and read back, so callers can fail closed."""
        want = bool(on)
        async with self.pool.acquire() as con:
            got = await con.fetchval(
                "INSERT INTO runtime_state (id, dyn_engine, dyn_engine_set_at) "
                "VALUES (1, $1, NOW()) "
                "ON CONFLICT (id) DO UPDATE SET dyn_engine=EXCLUDED.dyn_engine, "
                "  dyn_engine_set_at=EXCLUDED.dyn_engine_set_at "
                "RETURNING dyn_engine", want)
        if got is not want:
            raise RuntimeError("runtime_state.dyn_engine write not confirmed")

    # --- intent: config / preferences / rules -------------------------------
    @_with_db_retry()
    async def get_collection_config(self) -> dict:
        async with self.pool.acquire() as con:
            row = await con.fetchrow("SELECT * FROM collection_config WHERE id=1")
            return dict(row) if row else {}

    @_with_db_retry()
    async def get_target_preference(self, chat_id: int) -> asyncpg.Record | None:
        async with self.pool.acquire() as con:
            return await con.fetchrow(
                "SELECT * FROM target_preferences WHERE chat_id=$1", chat_id)

    @_with_db_retry()
    async def upsert_target_preference(self, chat_id: int, *,
                                       selected: bool | None = None,
                                       excluded: bool | None = None,
                                       note: str | None = None,
                                       updated_by: str = "ui"):
        """Patch one preference in a single statement (one policy bump at
        most). Unspecified fields keep their current value."""
        if selected is True and excluded is True:
            raise ValueError("selected and excluded cannot both be true")
        if note is not None and len(note) > 500:
            raise ValueError("note too long")
        async with self.pool.acquire() as con:
            await con.execute(
                "INSERT INTO target_preferences "
                "  (chat_id, selected, excluded, note, updated_at, updated_by) "
                "VALUES ($1, COALESCE($2, FALSE), COALESCE($3, FALSE), $4, NOW(), $5) "
                "ON CONFLICT (chat_id) DO UPDATE SET "
                "  selected = COALESCE($2, target_preferences.selected), "
                "  excluded = COALESCE($3, target_preferences.excluded), "
                "  note = COALESCE($4, target_preferences.note), "
                "  updated_at = NOW(), updated_by = $5",
                chat_id, selected, excluded, note, updated_by[:80])

    @_with_db_retry()
    async def list_title_exclude_rules(self) -> list[asyncpg.Record]:
        async with self.pool.acquire() as con:
            return await con.fetch(
                "SELECT id, pattern, created_at, created_by "
                "FROM collection_title_rules WHERE action='exclude' ORDER BY id")

    # --- discovery: request / claim -----------------------------------------
    DISCOVERY_MIN_SPACING_SEC = 60

    @_with_db_retry()
    async def request_discovery(self, requested_by: str = "ui") -> int:
        async with self.pool.acquire() as con:
            return int(await con.fetchval(
                "UPDATE discovery_control "
                "SET requested_seq = requested_seq + 1, "
                "    last_requested_at = NOW(), last_requested_by = $1 "
                "WHERE id=1 RETURNING requested_seq", requested_by[:80]))

    @_with_db_retry()
    async def claim_discovery_run(self, trigger_kind: str) -> int | None:
        """Create the one and only running discovery run, or return None.

        Single transaction under discovery_control FOR UPDATE:
          1. any run still 'running' (live or crashed) -> None;
          2. next_allowed_at in the future -> None;
          3. trigger 'request' with nothing pending -> None;
          4. INSERT run; for 'request' consume requested_seq; set last_run_id
             and next_allowed_at = NOW() + min spacing; commit.
        An INSERT failure rolls back the whole transaction, so the request is
        not consumed. The partial unique index uq_ddr_one_running backs (1)."""
        if trigger_kind not in ("scheduled", "request", "startup"):
            raise ValueError(f"bad trigger_kind {trigger_kind!r}")
        async with self.pool.acquire() as con:
            async with con.transaction():
                ctl = await con.fetchrow(
                    "SELECT requested_seq, consumed_seq, "
                    "       (next_allowed_at IS NOT NULL AND next_allowed_at > NOW()) AS throttled "
                    "FROM discovery_control WHERE id=1 FOR UPDATE")
                if await con.fetchval(
                        "SELECT EXISTS (SELECT 1 FROM dialog_discovery_runs "
                        "WHERE status='running')"):
                    return None
                if ctl["throttled"]:
                    return None
                if trigger_kind == "request" and ctl["requested_seq"] <= ctl["consumed_seq"]:
                    return None
                run_id = await con.fetchval(
                    "INSERT INTO dialog_discovery_runs (status, trigger_kind) "
                    "VALUES ('running', $1) RETURNING id", trigger_kind)
                await con.execute(
                    "UPDATE discovery_control SET "
                    "  consumed_seq = CASE WHEN $2 THEN requested_seq ELSE consumed_seq END, "
                    "  last_consumed_at = CASE WHEN $2 THEN NOW() ELSE last_consumed_at END, "
                    "  last_run_id = $1, "
                    "  next_allowed_at = NOW() + make_interval(secs => $3) "
                    "WHERE id=1",
                    run_id, trigger_kind == "request", float(self.DISCOVERY_MIN_SPACING_SEC))
                return int(run_id)

    # --- discovery: staging --------------------------------------------------
    _CATEGORIES = frozenset({"megagroup", "channel", "group", "private",
                             "bot", "saved", "unknown", "forbidden"})

    @_with_db_retry()
    async def stage_discovery_items(self, run_id: int, items: list[dict]) -> int:
        """Write dialogs seen by the current run into staging only. Live
        discovered_dialogs and the policy version are untouched. Returns the
        number of rows staged (0 if the run is not running)."""
        ids, cats, titles, users, arch, parts = [], [], [], [], [], []
        for it in items:
            cat = it.get("category")
            ids.append(int(it["chat_id"]))
            cats.append(cat if cat in self._CATEGORIES else "unknown")
            t = it.get("title")
            u = it.get("username")
            titles.append(t[:256] if t else None)
            users.append(u[:64] if u else None)
            arch.append(bool(it.get("is_archived")))
            pc = it.get("participants_count")
            parts.append(int(pc) if pc is not None else None)
        if not ids:
            return 0
        async with self.pool.acquire() as con:
            res = await con.execute(
                "INSERT INTO dialog_discovery_items "
                "  (run_id, chat_id, category, title, username, is_archived, participants_count) "
                "SELECT $1, u.chat_id, u.category, u.title, u.username, u.is_archived, u.pc "
                "FROM unnest($2::bigint[], $3::text[], $4::text[], $5::text[], "
                "            $6::bool[], $7::int[]) "
                "     AS u(chat_id, category, title, username, is_archived, pc) "
                "WHERE EXISTS (SELECT 1 FROM dialog_discovery_runs "
                "              WHERE id=$1 AND status='running') "
                "ON CONFLICT (run_id, chat_id) DO UPDATE SET "
                "  category=EXCLUDED.category, title=EXCLUDED.title, "
                "  username=EXCLUDED.username, is_archived=EXCLUDED.is_archived, "
                "  participants_count=EXCLUDED.participants_count, staged_at=NOW()",
                run_id, ids, cats, titles, users, arch, parts)
            return int(res.split()[-1])

    @_with_db_retry()
    async def mark_discovery_pass_done(self, run_id: int, pass_kind: str) -> bool:
        if pass_kind not in ("normal", "archived"):
            raise ValueError("pass_kind must be 'normal' or 'archived'")
        col = "normal_pass_done" if pass_kind == "normal" else "archived_pass_done"
        async with self.pool.acquire() as con:
            r = await con.fetchval(
                f"UPDATE dialog_discovery_runs SET {col}=TRUE "
                "WHERE id=$1 AND status='running' RETURNING id", run_id)
            return r is not None

    # --- discovery: finish ---------------------------------------------------
    @_with_db_retry()
    async def finish_discovery_run_success(self, run_id: int) -> dict:
        """Publish the staged snapshot atomically. Gone is computed here as
        (live, not already gone) minus (full staged snapshot); callers cannot
        pass gone ids. Returns {applied, reason, requested_version, seen,
        gone}. A run that is not 'running' or lacks a pass is not applied
        and does not bump the version."""
        async with self.pool.acquire() as con:
            async with con.transaction():
                run = await con.fetchrow(
                    "SELECT status, normal_pass_done, archived_pass_done "
                    "FROM dialog_discovery_runs WHERE id=$1 FOR UPDATE", run_id)
                ver = await con.fetchval(
                    "SELECT requested_version FROM policy_state WHERE id=1")
                if run is None:
                    return {"applied": False, "reason": "no_such_run",
                            "requested_version": int(ver), "seen": 0, "gone": 0}
                if run["status"] != "running":
                    return {"applied": False, "reason": f"status_{run['status']}",
                            "requested_version": int(ver), "seen": 0, "gone": 0}
                if not (run["normal_pass_done"] and run["archived_pass_done"]):
                    return {"applied": False, "reason": "pass_incomplete",
                            "requested_version": int(ver), "seen": 0, "gone": 0}
                seen = int(await con.fetchval(
                    "WITH m AS ("
                    "  INSERT INTO discovered_dialogs AS d "
                    "    (chat_id, category, title, username, is_archived, "
                    "     participants_count, last_seen_run_id, first_seen_at, "
                    "     last_seen_at, gone_at) "
                    "  SELECT chat_id, category, title, username, is_archived, "
                    "         participants_count, run_id, NOW(), NOW(), NULL "
                    "  FROM dialog_discovery_items WHERE run_id=$1 "
                    "  ON CONFLICT (chat_id) DO UPDATE SET "
                    "    category=EXCLUDED.category, title=EXCLUDED.title, "
                    "    username=EXCLUDED.username, is_archived=EXCLUDED.is_archived, "
                    "    participants_count=EXCLUDED.participants_count, "
                    "    last_seen_run_id=EXCLUDED.last_seen_run_id, "
                    "    last_seen_at=NOW(), gone_at=NULL "
                    "  RETURNING 1) SELECT COUNT(*) FROM m", run_id))
                gone = int(await con.fetchval(
                    "WITH g AS ("
                    "  UPDATE discovered_dialogs d SET gone_at=NOW() "
                    "  WHERE d.gone_at IS NULL AND NOT EXISTS ("
                    "    SELECT 1 FROM dialog_discovery_items i "
                    "    WHERE i.run_id=$1 AND i.chat_id=d.chat_id) "
                    "  RETURNING 1) SELECT COUNT(*) FROM g", run_id))
                await con.execute(
                    "UPDATE dialog_discovery_runs SET status='complete', "
                    "  finished_at=NOW(), dialogs_seen=$2, gone_detected=$3 "
                    "WHERE id=$1", run_id, seen, gone)
                await con.execute(
                    "DELETE FROM dialog_discovery_items WHERE run_id=$1", run_id)
                new_ver = await con.fetchval(
                    "UPDATE policy_state SET requested_version=requested_version+1, "
                    "  requested_at=NOW() WHERE id=1 RETURNING requested_version")
                return {"applied": True, "reason": "complete",
                        "requested_version": int(new_ver), "seen": seen, "gone": gone}

    STAGING_RETAIN_FAILED_RUNS = 3

    @_with_db_retry()
    async def finish_discovery_run_fail(self, run_id: int, reason: str,
                                        cancelled: bool = False,
                                        retry_after_sec: int | None = None) -> bool:
        """Mark a running run failed/cancelled. Live snapshot, gone_at and
        policy version are untouched. Staging of the newest few failed runs
        is kept for diagnostics; older staging is pruned."""
        status = "cancelled" if cancelled else "failed"
        async with self.pool.acquire() as con:
            async with con.transaction():
                r = await con.fetchval(
                    "UPDATE dialog_discovery_runs SET status=$2, finished_at=NOW(), "
                    "  error=$3 WHERE id=$1 AND status='running' RETURNING id",
                    run_id, status, (reason or "")[:500])
                if r is None:
                    return False
                if retry_after_sec:
                    await con.execute(
                        "UPDATE discovery_control SET next_allowed_at = GREATEST("
                        "  COALESCE(next_allowed_at, NOW()), "
                        "  NOW() + make_interval(secs => $1)) WHERE id=1",
                        float(retry_after_sec))
                await con.execute(
                    "DELETE FROM dialog_discovery_items i USING dialog_discovery_runs r "
                    "WHERE i.run_id=r.id AND r.status IN ('failed','cancelled') "
                    "  AND r.id NOT IN (SELECT id FROM dialog_discovery_runs "
                    "    WHERE status IN ('failed','cancelled') "
                    "    ORDER BY id DESC LIMIT $1)",
                    self.STAGING_RETAIN_FAILED_RUNS)
                return True

    @_with_db_retry()
    async def recover_stale_discovery_runs(self) -> int:
        """Startup only: a 'running' row belongs to a dead process. Mark it
        failed and drop its claim spacing so a fresh run can be claimed."""
        async with self.pool.acquire() as con:
            async with con.transaction():
                n = int(await con.fetchval(
                    "WITH upd AS ("
                    "  UPDATE dialog_discovery_runs SET status='failed', "
                    "    finished_at=NOW(), error='stale: process restart while running' "
                    "  WHERE status='running' RETURNING 1) SELECT COUNT(*) FROM upd"))
                if n:
                    await con.execute(
                        "UPDATE discovery_control SET next_allowed_at=NULL WHERE id=1")
                return n

    @_with_db_retry()
    async def get_discovery_control(self) -> asyncpg.Record | None:
        async with self.pool.acquire() as con:
            return await con.fetchrow("SELECT * FROM discovery_control WHERE id=1")

    # --- policy version -------------------------------------------------------
    @_with_db_retry()
    async def get_policy_state(self) -> asyncpg.Record | None:
        async with self.pool.acquire() as con:
            return await con.fetchrow("SELECT * FROM policy_state WHERE id=1")

    @_with_db_retry()
    async def apply_policy_version(self, version: int):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE policy_state SET applied_version = GREATEST(applied_version, $1), "
                "  applied_at = NOW() WHERE id=1 AND $1 <= requested_version",
                int(version))

    # --- mass-gone block ------------------------------------------------------
    @staticmethod
    def _gone_fingerprint(chat_ids) -> str:
        ids = sorted({int(x) for x in chat_ids})
        return hashlib.sha256(",".join(str(i) for i in ids).encode("ascii")).hexdigest()

    @staticmethod
    def _should_mass_block(gone_count: int, active_count: int) -> bool:
        """block if gone_count > 5 OR gone_count * 5 > active_count;
        active_count == 0 never blocks. 100 active / 20 gone -> True."""
        if active_count <= 0:
            return False
        return gone_count > 5 or gone_count * 5 > active_count

    _GONE_SET_SQL = (
        "SELECT ts.chat_id FROM target_state ts "
        "JOIN discovered_dialogs d ON d.chat_id = ts.chat_id "
        "WHERE ts.enabled = TRUE AND d.gone_at IS NOT NULL ORDER BY ts.chat_id")

    @_with_db_retry()
    async def evaluate_mass_gone_block(self) -> dict:
        """Compute the gone set (enabled targets whose dialog is gone) inside
        the transaction and update the CURRENT block. Confirmation is valid
        only for the exact (requested_version, fingerprint). Returns
        {state: none|blocked|confirmed, version, fingerprint, gone, active}."""
        async with self.pool.acquire() as con:
            async with con.transaction():
                ps = await con.fetchrow("SELECT * FROM policy_state WHERE id=1 FOR UPDATE")
                ver = int(ps["requested_version"])
                gone_ids = [r["chat_id"] for r in await con.fetch(self._GONE_SET_SQL)]
                active = int(await con.fetchval(
                    "SELECT COUNT(*) FROM target_state WHERE enabled=TRUE"))
                fp = self._gone_fingerprint(gone_ids)
                out = {"version": ver, "fingerprint": fp,
                       "gone": len(gone_ids), "active": active}
                if not self._should_mass_block(len(gone_ids), active):
                    if ps["blocked_version"] is not None:
                        await self._clear_block(con, ps, "collector")
                    return {**out, "state": "none"}
                if ps["confirmed_version"] == ver and ps["confirmed_fingerprint"] == fp:
                    return {**out, "state": "confirmed"}
                if not (ps["blocked_version"] == ver and ps["blocked_fingerprint"] == fp):
                    await con.execute(
                        "UPDATE policy_state SET blocked_version=$1, blocked_reason='mass_gone', "
                        "  blocked_count=$2, blocked_fingerprint=$3, blocked_at=NOW() WHERE id=1",
                        ver, len(gone_ids), fp)
                    await con.execute(
                        "INSERT INTO policy_block_history "
                        "(event, policy_version, fingerprint, gone_count, active_count, actor) "
                        "VALUES ('blocked',$1,$2,$3,$4,'collector')",
                        ver, fp, len(gone_ids), active)
                return {**out, "state": "blocked"}

    @staticmethod
    async def _clear_block(con, ps, actor: str):
        await con.execute(
            "UPDATE policy_state SET blocked_version=NULL, blocked_reason=NULL, "
            "  blocked_count=NULL, blocked_fingerprint=NULL, blocked_at=NULL WHERE id=1")
        await con.execute(
            "INSERT INTO policy_block_history "
            "(event, policy_version, fingerprint, gone_count, actor) "
            "VALUES ('cleared',$1,$2,$3,$4)",
            ps["blocked_version"], ps["blocked_fingerprint"], ps["blocked_count"], actor)

    @_with_db_retry()
    async def confirm_mass_gone(self, policy_version: int, fingerprint: str,
                                confirmed_by: str = "admin_cli") -> bool:
        """Confirm the CURRENT block, only if (version, fingerprint) match it
        exactly. Previous confirmations are overwritten in policy_state but
        remain in policy_block_history."""
        if confirmed_by != "admin_cli":
            raise ValueError("only admin_cli may confirm")
        async with self.pool.acquire() as con:
            async with con.transaction():
                ps = await con.fetchrow("SELECT * FROM policy_state WHERE id=1 FOR UPDATE")
                if (ps["blocked_version"] != policy_version
                        or ps["blocked_fingerprint"] != fingerprint
                        or ps["requested_version"] != policy_version):
                    return False
                await con.execute(
                    "UPDATE policy_state SET confirmed_version=$1, confirmed_fingerprint=$2, "
                    "  confirmed_at=NOW(), confirmed_by=$3 WHERE id=1",
                    policy_version, fingerprint, confirmed_by)
                await con.execute(
                    "INSERT INTO policy_block_history "
                    "(event, policy_version, fingerprint, gone_count, actor) "
                    "VALUES ('confirmed',$1,$2,$3,'admin_cli')",
                    policy_version, fingerprint, ps["blocked_count"])
                return True

    @_with_db_retry()
    async def clear_mass_gone_block(self, policy_version: int, fingerprint: str,
                                    actor: str = "collector") -> bool:
        """Clear the CURRENT block (e.g. once the confirmed version is
        applied). Exact match required; confirmation fields and history stay."""
        async with self.pool.acquire() as con:
            async with con.transaction():
                ps = await con.fetchrow("SELECT * FROM policy_state WHERE id=1 FOR UPDATE")
                if (ps["blocked_version"] != policy_version
                        or ps["blocked_fingerprint"] != fingerprint):
                    return False
                await self._clear_block(con, ps, actor)
                return True

    @staticmethod
    def mass_gone_state(ps) -> str:
        """none | blocked | confirmed, from a policy_state row. Confirmation
        counts only for the current block of the CURRENT requested_version;
        a block left behind by a newer policy change reads as blocked until
        the next evaluate_mass_gone_block() refreshes or clears it."""
        if ps is None or ps["blocked_version"] is None:
            return "none"
        if (ps["blocked_version"] == ps["requested_version"]
                and ps["confirmed_version"] == ps["blocked_version"]
                and ps["confirmed_fingerprint"] == ps["blocked_fingerprint"]):
            return "confirmed"
        return "blocked"

    # --- YAML import -----------------------------------------------------------
    @staticmethod
    def _yaml_target(raw: str) -> str:
        raw = (raw or "").strip()
        if not 1 <= len(raw) <= 300 or any(ord(ch) < 32 or ord(ch) == 127 for ch in raw):
            raise ValueError("bad yaml target")
        return raw

    @staticmethod
    def _error_class(name: str) -> str:
        name = re.sub(r"[^A-Za-z0-9_.:]", "_", name or "Error")[:120]
        return name or "Error"

    @_with_db_retry()
    async def yaml_import_upsert(self, raw_target: str) -> int:
        """YAML only adds. An existing row keeps its status (a target removed
        in the UI and marked terminal never resurrects)."""
        t = self._yaml_target(raw_target)
        async with self.pool.acquire() as con:
            return int(await con.fetchval(
                "WITH ins AS (INSERT INTO yaml_target_imports (raw_target) VALUES ($1) "
                "  ON CONFLICT (raw_target) DO NOTHING RETURNING id) "
                "SELECT id FROM ins UNION ALL "
                "SELECT id FROM yaml_target_imports WHERE raw_target=$1 LIMIT 1", t))

    @_with_db_retry()
    async def yaml_import_record_error(self, raw_target: str, error_class: str,
                                       next_retry_at=None):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE yaml_target_imports SET attempts=LEAST(attempts+1, 1000), "
                "  last_error_class=$2, last_attempt_at=NOW(), next_retry_at=$3, "
                "  status='retry' WHERE raw_target=$1 AND status IN ('pending','retry')",
                self._yaml_target(raw_target), self._error_class(error_class), next_retry_at)

    @_with_db_retry()
    async def yaml_import_mark_selected(self, raw_target: str, resolved_chat_id: int):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE yaml_target_imports SET status='selected', resolved_chat_id=$2, "
                "  resolved_at=NOW() WHERE raw_target=$1 AND status IN ('pending','retry')",
                self._yaml_target(raw_target), int(resolved_chat_id))

    @_with_db_retry()
    async def yaml_import_mark_terminal(self, raw_target: str, error_class: str):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE yaml_target_imports SET status='terminal', last_error_class=$2, "
                "  last_attempt_at=NOW() WHERE raw_target=$1",
                self._yaml_target(raw_target), self._error_class(error_class))

    # --- UI audit ----------------------------------------------------------------
    AUDIT_ACTORS = frozenset({"ui", "admin_cli", "system"})
    AUDIT_OUTCOMES = frozenset({"ok", "rejected", "error"})
    AUDIT_DETAIL_KEYS = frozenset({
        "chat_id", "field", "old", "new", "mode", "category", "rule_id",
        "pattern_len", "version", "seq", "service_action", "count", "reason",
        "status_code"})
    _AUDIT_ACTION_RX = re.compile(r"^[a-z][a-z0-9_.:-]{1,63}$")
    _SECRETISH_RX = re.compile(r"(postgres(ql)?://|api_hash|access_hash|session|"
                               r"password|token|\+?\d{10,15}$)", re.I)

    @classmethod
    def _audit_detail(cls, detail: dict | None) -> dict:
        out = {}
        for k, v in (detail or {}).items():
            if k not in cls.AUDIT_DETAIL_KEYS:
                raise ValueError(f"audit detail key not allowed: {k!r}")
            if v is not None and not isinstance(v, (bool, int, float, str)):
                raise ValueError(f"audit detail value must be scalar: {k!r}")
            if isinstance(v, str):
                if len(v) > 120:
                    raise ValueError(f"audit detail value too long: {k!r}")
                if cls._SECRETISH_RX.search(v):
                    raise ValueError(f"audit detail value looks sensitive: {k!r}")
            out[k] = v
        return out

    @_with_db_retry()
    async def ui_audit(self, actor: str, action: str, outcome: str,
                       detail: dict | None = None, client_ip: str | None = None,
                       user_agent: str | None = None, error: str | None = None):
        """Bounded audit record. Only whitelisted scalar detail keys; never a
        request body. Oversized or malformed values raise instead of being
        silently truncated past the DB constraints."""
        if actor not in self.AUDIT_ACTORS:
            raise ValueError("bad audit actor")
        if outcome not in self.AUDIT_OUTCOMES:
            raise ValueError("bad audit outcome")
        if not self._AUDIT_ACTION_RX.match(action or ""):
            raise ValueError("bad audit action")
        d = self._audit_detail(detail)
        ip = str(ipaddress.ip_address(client_ip)) if client_ip else None
        async with self.pool.acquire() as con:
            await con.execute(
                "INSERT INTO ui_audit_log "
                "(actor, action, outcome, detail, client_ip, user_agent, error) "
                "VALUES ($1,$2,$3,$4::jsonb,$5::inet,$6,$7)",
                actor, action, outcome, json.dumps(d, ensure_ascii=False), ip,
                (user_agent[:200] if user_agent else None),
                (error[:500] if error else None))

    # --- read-only policy dry-run -------------------------------------------------
    async def policy_dry_run(self) -> dict:
        """READ ONLY transaction: compares v_target_policy with target_state.
        Never writes runtime, policy, discovery or targets."""
        async with self.pool.acquire() as con:
            async with con.transaction(readonly=True):
                schema = int(await con.fetchval(
                    "SELECT COALESCE(max(version),0) FROM schema_migrations"))
                gate = (await con.fetchval(
                    "SELECT enabled FROM deployment_gates WHERE key='dynamic_targets'")) is True
                rt = await con.fetchrow("SELECT dyn_engine, pid, status FROM runtime_state WHERE id=1")
                engine = bool(rt and rt["dyn_engine"])
                cfg = await con.fetchrow("SELECT * FROM collection_config WHERE id=1")
                ps = await con.fetchrow("SELECT * FROM policy_state WHERE id=1")
                any_complete = await con.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM dialog_discovery_runs WHERE status='complete')")
                rows = await con.fetch("SELECT * FROM v_target_policy ORDER BY chat_id")
                gone_ids = [r["chat_id"] for r in await con.fetch(self._GONE_SET_SQL)]
                active = int(await con.fetchval(
                    "SELECT COUNT(*) FROM target_state WHERE enabled=TRUE"))
        would_enable, would_disable = [], []
        reasons: dict[str, int] = {}
        keep = 0
        for r in rows:
            reasons[r["reason"]] = reasons.get(r["reason"], 0) + 1
            item = {k: r[k] for k in ("chat_id", "category", "title", "discovered",
                                      "has_preference", "selected", "excluded",
                                      "in_target_state", "current_enabled",
                                      "reason", "desired")}
            if r["desired"] and not r["current_enabled"]:
                would_enable.append(item)
            elif r["current_enabled"] and not r["desired"]:
                would_disable.append(item)
            else:
                keep += 1
        mode = cfg["mode"]
        awaiting = mode == "all" and not any_complete
        block_state = self.mass_gone_state(ps)
        mass_rule = self._should_mass_block(len(gone_ids), active)
        suppressed = []
        if awaiting:
            suppressed.append("awaiting_discovery")
        if block_state == "blocked" or (mass_rule and block_state == "none"):
            suppressed.append("mass_gone_block")
        return {
            "schema_version": schema,
            "gate_enabled": gate,
            "engine_active": engine,
            "requires_restart": gate != engine,
            "mode": mode,
            "include_archived": bool(cfg["include_archived"]),
            "requested_version": int(ps["requested_version"]),
            "applied_version": int(ps["applied_version"]),
            "awaiting_discovery": awaiting,
            "block": {
                "state": block_state,
                "blocked_version": ps["blocked_version"],
                "blocked_count": ps["blocked_count"],
                "blocked_fingerprint": ps["blocked_fingerprint"],
                "confirmed_version": ps["confirmed_version"],
                "rule_would_block_now": mass_rule,
                "gone_enabled_count": len(gone_ids),
                "active_count": active,
            },
            "reconcile": {
                "version": ps["reconcile_version"],
                "state": ps["reconcile_state"],
                "error": ps["reconcile_error"],
                "at": ps["reconciled_at"],
            },
            "disable_suppressed_by": suppressed,
            "would_enable": would_enable,
            "would_disable": would_disable,
            "would_keep_count": keep,
            "reason_counts": reasons,
            "enabled_without_discovery": [r["chat_id"] for r in rows
                                          if r["current_enabled"] and not r["discovered"]],
            "enabled_without_preference": [r["chat_id"] for r in rows
                                           if r["current_enabled"] and not r["has_preference"]],
        }

    # ========================================================================
    # v11: dynamic engine runtime primitives
    # ========================================================================

    @_with_db_retry()
    async def policy_snapshot(self) -> dict:
        """One consistent read (REPEATABLE READ) of requested_version, mode,
        whether any discovery snapshot is complete, and every policy row."""
        async with self.pool.acquire() as con:
            async with con.transaction(isolation="repeatable_read", readonly=True):
                ps = await con.fetchrow(
                    "SELECT requested_version, applied_version FROM policy_state WHERE id=1")
                mode = await con.fetchval("SELECT mode FROM collection_config WHERE id=1")
                complete = await con.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM dialog_discovery_runs WHERE status='complete')")
                rows = await con.fetch(
                    "SELECT chat_id, desired, reason, title, username, current_enabled, "
                    "in_target_state FROM v_target_policy")
                enabled = [r["chat_id"] for r in await con.fetch(
                    "SELECT chat_id FROM target_state WHERE enabled=TRUE")]
        return {"requested_version": int(ps["requested_version"]),
                "applied_version": int(ps["applied_version"]),
                "mode": mode, "any_complete_discovery": bool(complete),
                "rows": [dict(r) for r in rows], "enabled": set(enabled)}

    @_with_db_retry()
    async def enable_dynamic_target(self, chat_id: int, label: str) -> None:
        """Step 1 of enabling: target_state enabled=TRUE and a sync_state row.
        An existing checkpoint, backfill status and label are preserved."""
        label = (label or str(chat_id))[:200]
        async with self.pool.acquire() as con:
            async with con.transaction():
                await con.execute(
                    "INSERT INTO target_state (chat_id, target, backfill_status, enabled, "
                    "  label, updated_at) VALUES ($1,$2,'running',TRUE,$2,NOW()) "
                    "ON CONFLICT (chat_id) DO UPDATE SET enabled=TRUE, disabled_at=NULL, "
                    "  disabled_reason=NULL, updated_at=NOW()",
                    chat_id, label)
                await con.execute(
                    "INSERT INTO sync_state (chat_id) VALUES ($1) ON CONFLICT DO NOTHING",
                    chat_id)
                await con.execute(
                    "INSERT INTO coordinator_progress (chat_id, batch_running, updated_at) "
                    "VALUES ($1, FALSE, NOW()) ON CONFLICT (chat_id) DO NOTHING", chat_id)

    async def disable_target_ack(self, chat_id: int, reason: str,
                                 lock_timeout_ms: int = 30_000) -> None:
        """Disable ACK. Takes the EXCLUSIVE advisory xact lock on chat_id (the
        key the write guard takes SHARED), so it waits for guarded writes that
        already started; then enabled=FALSE. COMMIT is the ACK: from then on
        the guard rejects every write for this chat (dynamic engine).
        Not retried automatically: the reconciler retries the whole step."""
        async with self.pool.acquire() as con:
            async with con.transaction():
                await con.execute(f"SET LOCAL lock_timeout = {int(lock_timeout_ms)}")
                await con.execute("SELECT pg_advisory_xact_lock($1::bigint)", chat_id)
                await con.execute(
                    "UPDATE target_state SET enabled=FALSE, disabled_at=NOW(), "
                    "  disabled_reason=$2, updated_at=NOW() WHERE chat_id=$1",
                    chat_id, (reason or "policy")[:120])

    @_with_db_retry()
    async def set_backfill_checkpoint_if_enabled(self, chat_id: int, last_id: int,
                                                 first_id: int | None = None) -> bool:
        """Dynamic engine checkpoint: same GREATEST semantics as
        set_backfill_checkpoint, but under the shared advisory lock and only
        while target_state.enabled is TRUE, so a disabled target's checkpoint
        cannot move after its disable ACK."""
        async with self.pool.acquire() as con:
            async with con.transaction():
                await con.execute("SELECT pg_advisory_xact_lock_shared($1::bigint)", chat_id)
                res = await con.execute(
                    "UPDATE sync_state SET "
                    "  last_backfilled_message_id = GREATEST(last_backfilled_message_id, $2), "
                    "  first_message_id = COALESCE(LEAST(first_message_id, $3), "
                    "                              first_message_id, $3), "
                    "  updated_at = NOW() "
                    "WHERE chat_id=$1 AND EXISTS (SELECT 1 FROM target_state t "
                    "  WHERE t.chat_id=$1 AND t.enabled IS TRUE)",
                    chat_id, last_id, first_id)
                return res.endswith(" 1")

    @_with_db_retry()
    async def record_reconcile(self, version: int, state: str, error: str | None = None):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE policy_state SET reconcile_version=$1, reconcile_state=$2, "
                "  reconcile_error=$3, reconciled_at=NOW() WHERE id=1",
                int(version), state, (error[:500] if error else None))

    @_with_db_retry()
    async def media_pending_count_for(self, chat_ids: list[int]) -> int:
        async with self.pool.acquire() as con:
            return int(await con.fetchval(
                "SELECT COUNT(*) FROM media WHERE status='pending' "
                "AND chat_id = ANY($1::bigint[])", chat_ids))

    @_with_db_retry()
    async def mark_deleted_existing_unique(self, chat_ids: list[int],
                                           message_ids: list[int]) -> dict:
        """Deletion WITHOUT chat id (private / basic-group ids are account
        wide). Only already stored, non-channel messages of the given active
        chats are touched; an id matching more than one chat is ambiguous and
        left alone. No stubs are created. Returns {matched, ambiguous,
        unknown} id lists."""
        ids = sorted({int(x) for x in message_ids})
        cands = [c for c in chat_ids if int(c) > -10**12]   # not a channel
        if not ids:
            return {"matched": [], "ambiguous": [], "unknown": []}
        async with self.pool.acquire() as con:
            async with con.transaction():
                rows = await con.fetch(
                    "SELECT message_id, array_agg(chat_id) chats FROM messages "
                    "WHERE message_id = ANY($1::bigint[]) AND chat_id = ANY($2::bigint[]) "
                    "GROUP BY message_id", ids, cands)
                matched, ambiguous = [], []
                pairs_chat, pairs_msg = [], []
                for r in rows:
                    if len(r["chats"]) == 1:
                        matched.append(r["message_id"])
                        pairs_chat.append(r["chats"][0])
                        pairs_msg.append(r["message_id"])
                    else:
                        ambiguous.append(r["message_id"])
                if pairs_msg:
                    await con.execute(
                        "UPDATE messages m SET is_deleted=TRUE, "
                        "  deleted_at=COALESCE(m.deleted_at, NOW()) "
                        "FROM unnest($1::bigint[], $2::bigint[]) AS p(chat_id, message_id) "
                        "WHERE m.chat_id=p.chat_id AND m.message_id=p.message_id",
                        pairs_chat, pairs_msg)
        seen = set(matched) | set(ambiguous)
        return {"matched": sorted(matched), "ambiguous": sorted(ambiguous),
                "unknown": [i for i in ids if i not in seen]}

    # --- YAML import worker -----------------------------------------------------
    @_with_db_retry()
    async def yaml_import_due(self, limit: int) -> list[asyncpg.Record]:
        async with self.pool.acquire() as con:
            return await con.fetch(
                "SELECT id, raw_target, status, attempts FROM yaml_target_imports "
                "WHERE status IN ('pending','retry') "
                "  AND (next_retry_at IS NULL OR next_retry_at <= NOW()) "
                "ORDER BY COALESCE(next_retry_at, created_at), id LIMIT $1", int(limit))

    @_with_db_retry()
    async def yaml_import_resolved(self, raw_target: str, chat_id: int) -> bool:
        """Resolved: mark the import 'selected' and create a selected
        preference ONLY if the chat has no preference yet (a later manual
        choice is never overwritten). Returns True if a preference was
        created. One transaction, so at most one policy bump."""
        t = self._yaml_target(raw_target)
        async with self.pool.acquire() as con:
            async with con.transaction():
                upd = await con.fetchval(
                    "UPDATE yaml_target_imports SET status='selected', resolved_chat_id=$2, "
                    "  resolved_at=NOW(), last_attempt_at=NOW() "
                    "WHERE raw_target=$1 AND status IN ('pending','retry') RETURNING id",
                    t, int(chat_id))
                if upd is None:
                    return False
                created = await con.fetchval(
                    "INSERT INTO target_preferences (chat_id, selected, excluded, updated_by) "
                    "VALUES ($1, TRUE, FALSE, 'yaml_import') "
                    "ON CONFLICT (chat_id) DO NOTHING RETURNING chat_id", int(chat_id))
                return created is not None

    # ========================================================================
    # v11: atomic admin-CLI wrappers (mutation + audit in one transaction).
    # Each returns a result dict with ok/reason and the fields the CLI prints.
    # An audit row is written inside the same transaction for every outcome
    # (ok / rejected). If the audit INSERT or read-back raises, the whole
    # transaction rolls back together with the mutation.
    # ========================================================================
    async def _admin_audit(self, con, actor: str, action: str, outcome: str,
                           detail: dict):
        if actor not in self.AUDIT_ACTORS:
            raise ValueError("bad audit actor")
        if outcome not in self.AUDIT_OUTCOMES:
            raise ValueError("bad audit outcome")
        if not self._AUDIT_ACTION_RX.match(action or ""):
            raise ValueError("bad audit action")
        d = self._audit_detail(detail)
        await con.execute(
            "INSERT INTO ui_audit_log (actor, action, outcome, detail) "
            "VALUES ($1,$2,$3,$4::jsonb)",
            actor, action, outcome, json.dumps(d, ensure_ascii=False))

    @_with_db_retry()
    async def admin_gate_update(self, *, enable: bool, from_enabled: bool,
                                actor: str, action: str, note: str | None,
                                detail: dict, expected_dyn_engine: bool) -> dict:
        """Atomic: deployment_gates UPDATE + ui_audit_log INSERT in one tx.
        Preconditions enforced inside the tx against the gates row FOR UPDATE:
          - schema_version == 11;
          - runtime_state.dyn_engine == expected_dyn_engine;
          - deployment_gates.enabled == from_enabled;
          - UPDATE affects exactly 1 row;
          - read-back confirms enabled == enable.
        Any failing precondition writes a 'rejected' audit and returns
        ok=False. A read-back or audit INSERT failure rolls back the UPDATE."""
        async with self.pool.acquire() as con:
            async with con.transaction():
                row = await con.fetchrow(
                    "SELECT enabled FROM deployment_gates WHERE key='dynamic_targets' "
                    "FOR UPDATE")
                schema = int(await con.fetchval(
                    "SELECT COALESCE(max(version),0) FROM schema_migrations"))
                if schema != 11:
                    await self._admin_audit(con, actor, action, "rejected",
                                            {**detail, "reason": "schema_not_v11"})
                    return {"ok": False, "reason": "schema_not_v11",
                            "schema": schema}
                rt_dyn = await con.fetchval(
                    "SELECT dyn_engine FROM runtime_state WHERE id=1")
                if bool(rt_dyn) is not bool(expected_dyn_engine):
                    await self._admin_audit(con, actor, action, "rejected",
                                            {**detail, "reason": "dyn_engine_mismatch"})
                    return {"ok": False, "reason": "dyn_engine_mismatch",
                            "dyn_engine": bool(rt_dyn),
                            "expected_dyn_engine": bool(expected_dyn_engine)}
                if bool(row["enabled"]) is not bool(from_enabled):
                    await self._admin_audit(con, actor, action, "rejected",
                                            {**detail, "reason": "gate_state_mismatch"})
                    return {"ok": False, "reason": "gate_state_mismatch",
                            "current": bool(row["enabled"])}
                res = await con.execute(
                    "UPDATE deployment_gates SET enabled=$1, updated_at=NOW(), "
                    "updated_by=$2, note=$3 WHERE key='dynamic_targets' "
                    "AND enabled=$4",
                    enable, actor[:80], (note or "")[:1000], from_enabled)
                if not res.endswith(" 1"):
                    await self._admin_audit(con, actor, action, "rejected",
                                            {**detail, "reason": "update_rowcount"})
                    return {"ok": False, "reason": "update_rowcount"}
                new = await con.fetchrow(
                    "SELECT enabled, updated_at, updated_by, note "
                    "FROM deployment_gates WHERE key='dynamic_targets'")
                if bool(new["enabled"]) is not bool(enable):
                    raise RuntimeError("gate read-back mismatch")
                await self._admin_audit(con, actor, action, "ok", detail)
                return {"ok": True, "enabled": bool(new["enabled"]),
                        "updated_at": new["updated_at"],
                        "updated_by": new["updated_by"]}

    @_with_db_retry()
    async def admin_preference_update(self, *, chat_id: int,
                                      selected: bool | None,
                                      excluded: bool | None,
                                      note: str | None,
                                      actor: str, action: str,
                                      detail: dict,
                                      expected_requested_version: int | None
                                      ) -> dict:
        """Atomic: target_preferences upsert + audit in one tx. Checks
        policy_state.requested_version == expected (when expected is not
        None) under FOR UPDATE and refuses on mismatch. Returns new
        requested_version after the upsert (may equal old if the write was
        idempotent and no intent trigger bumped)."""
        if selected is True and excluded is True:
            raise ValueError("selected and excluded cannot both be true")
        async with self.pool.acquire() as con:
            async with con.transaction():
                ps = await con.fetchrow(
                    "SELECT requested_version FROM policy_state WHERE id=1 FOR UPDATE")
                before = int(ps["requested_version"])
                if expected_requested_version is not None \
                        and before != int(expected_requested_version):
                    await self._admin_audit(con, actor, action, "rejected",
                                            {**detail, "reason": "stale_version",
                                             "version": before})
                    return {"ok": False, "reason": "stale_version",
                            "current_requested_version": before}
                await con.execute(
                    "INSERT INTO target_preferences "
                    "  (chat_id, selected, excluded, note, updated_at, updated_by) "
                    "VALUES ($1, COALESCE($2, FALSE), COALESCE($3, FALSE), $4, NOW(), $5) "
                    "ON CONFLICT (chat_id) DO UPDATE SET "
                    "  selected = COALESCE($2, target_preferences.selected), "
                    "  excluded = COALESCE($3, target_preferences.excluded), "
                    "  note = COALESCE($4, target_preferences.note), "
                    "  updated_at = NOW(), updated_by = $5",
                    chat_id, selected, excluded, note, actor[:80])
                after = int(await con.fetchval(
                    "SELECT requested_version FROM policy_state WHERE id=1"))
                await self._admin_audit(con, actor, action, "ok",
                                        {**detail, "version": after})
                return {"ok": True,
                        "requested_version_before": before,
                        "requested_version_after": after,
                        "chat_id": int(chat_id)}

    @_with_db_retry()
    async def admin_confirm_mass_gone(self, *, policy_version: int,
                                      fingerprint: str, actor: str,
                                      action: str, detail: dict) -> dict:
        """Atomic: confirm_mass_gone + audit. Exact match required on
        blocked_version, blocked_fingerprint and requested_version."""
        async with self.pool.acquire() as con:
            async with con.transaction():
                ps = await con.fetchrow(
                    "SELECT blocked_version, blocked_fingerprint, requested_version, "
                    "       blocked_count FROM policy_state WHERE id=1 FOR UPDATE")
                if (ps["blocked_version"] != policy_version
                        or ps["blocked_fingerprint"] != fingerprint
                        or ps["requested_version"] != policy_version):
                    await self._admin_audit(con, actor, action, "rejected",
                                            {**detail, "reason": "stale_block",
                                             "version": int(ps["requested_version"])})
                    return {"ok": False, "reason": "stale_block",
                            "blocked_version": ps["blocked_version"],
                            "requested_version": int(ps["requested_version"])}
                await con.execute(
                    "UPDATE policy_state SET confirmed_version=$1, "
                    "confirmed_fingerprint=$2, confirmed_at=NOW(), "
                    "confirmed_by=$3 WHERE id=1",
                    policy_version, fingerprint, actor)
                await con.execute(
                    "INSERT INTO policy_block_history "
                    "(event, policy_version, fingerprint, gone_count, actor) "
                    "VALUES ('confirmed',$1,$2,$3,$4)",
                    policy_version, fingerprint, ps["blocked_count"], actor)
                await self._admin_audit(con, actor, action, "ok",
                                        {**detail, "version": policy_version})
                return {"ok": True, "confirmed_version": policy_version}

    @_with_db_retry()
    async def admin_request_discovery(self, *, actor: str, action: str,
                                      detail: dict) -> dict:
        """Atomic: discovery request + audit. Refuses if a pending request is
        not yet consumed, or next_allowed_at is in the future."""
        async with self.pool.acquire() as con:
            async with con.transaction():
                ctl = await con.fetchrow(
                    "SELECT requested_seq, consumed_seq, last_requested_at, "
                    "       next_allowed_at, "
                    "       (next_allowed_at IS NOT NULL "
                    "        AND next_allowed_at > NOW()) AS throttled "
                    "FROM discovery_control WHERE id=1 FOR UPDATE")
                if ctl["requested_seq"] > ctl["consumed_seq"]:
                    await self._admin_audit(con, actor, action, "rejected",
                                            {**detail, "reason": "pending_request",
                                             "seq": int(ctl["requested_seq"])})
                    return {"ok": False, "reason": "pending_request",
                            "requested_seq": int(ctl["requested_seq"]),
                            "consumed_seq": int(ctl["consumed_seq"])}
                if ctl["throttled"]:
                    await self._admin_audit(con, actor, action, "rejected",
                                            {**detail, "reason": "throttled"})
                    return {"ok": False, "reason": "throttled",
                            "next_allowed_at": ctl["next_allowed_at"]}
                new_seq = int(await con.fetchval(
                    "UPDATE discovery_control SET "
                    "  requested_seq = requested_seq + 1, "
                    "  last_requested_at = NOW(), last_requested_by=$1 "
                    "WHERE id=1 RETURNING requested_seq",
                    actor[:80]))
                await self._admin_audit(con, actor, action, "ok",
                                        {**detail, "seq": new_seq})
                return {"ok": True, "requested_seq": new_seq}
