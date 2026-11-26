"""All SQL for the UI. READ-ONLY by contract.

Rules enforced by tests/web/test_readonly.py:
  - every string literal in this module that starts with SQL must be SELECT;
  - no INSERT/UPDATE/DELETE/TRUNCATE/ALTER/CREATE/DROP/GRANT anywhere;
  - all user-controlled values go through $1..$N positional parameters;
  - secret columns (access_hash, access_hash_auth_fingerprint,
    access_hash_source_*, raw.user info, …) are NEVER selected.

Datetime strings are returned as ISO in Europe/Kyiv via the view layer.
"""
from __future__ import annotations

from typing import Any, Iterable

from .db import pool


# --- dashboard -----------------------------------------------------------

async def graph_expand(chat=None,user=None,offset=0):
    async with pool().acquire() as con:
        if chat is None:
            rows=await con.fetch('''WITH ids AS (
                    SELECT c.id FROM chats c
                     WHERE EXISTS(SELECT 1 FROM messages m WHERE m.chat_id=c.id)
                    UNION SELECT chat_id FROM target_state)
                SELECT i.id, COALESCE(c.title,d.title) AS title,
                       COALESCE(c.type,d.category) AS type
                  FROM ids i
                  LEFT JOIN chats c ON c.id=i.id
                  LEFT JOIN discovered_dialogs d ON d.chat_id=i.id
                 ORDER BY i.id LIMIT 101 OFFSET $1''',offset)
        elif user is None:
            rows=await con.fetch('''SELECT m.sender_user_id AS id,
                coalesce(nullif(concat_ws(' ',u.first_name,u.last_name),''),u.username,m.sender_user_id::text) AS title,count(*) AS n
                FROM messages m LEFT JOIN users u ON u.id=m.sender_user_id
                WHERE m.chat_id=$1 AND m.sender_user_id IS NOT NULL
                GROUP BY m.sender_user_id,u.first_name,u.last_name,u.username
                ORDER BY n DESC,m.sender_user_id LIMIT 101 OFFSET $2''',chat,offset)
        else:
            rows=await con.fetch('''SELECT message_id AS id,substr(coalesce(text,''),1,500) AS title,
                date,media_type,is_deleted,reply_to_message_id FROM messages WHERE chat_id=$1 AND sender_user_id=$2
                ORDER BY message_id DESC LIMIT 101 OFFSET $3''',chat,user,offset)
    return {'rows':[dict(r) for r in rows[:100]],'more':len(rows)>100,'offset':offset+100}

async def graph_relations(chat):
    """Directed, evidenced interactions, never inferred from shared membership."""
    async with pool().acquire() as con:
        rows = await con.fetch('''SELECT source,target,kind,count(*) AS weight,
            max(message_id) AS message_id FROM (
            SELECT m.sender_user_id AS source,p.sender_user_id AS target,
                'reply' AS kind,m.message_id
            FROM messages m JOIN messages p
              ON p.chat_id=coalesce(m.reply_to_peer_id,m.chat_id)
             AND p.message_id=m.reply_to_message_id
            WHERE m.chat_id=$1 AND m.sender_user_id IS NOT NULL
              AND p.sender_user_id IS NOT NULL AND m.sender_user_id<>p.sender_user_id
            UNION ALL
            SELECT sender_user_id,fwd_from_user_id,'forward',message_id
            FROM messages WHERE chat_id=$1 AND sender_user_id IS NOT NULL
              AND fwd_from_user_id IS NOT NULL AND sender_user_id<>fwd_from_user_id
        ) events GROUP BY source,target,kind ORDER BY weight DESC,source,target,kind''',chat)
    return {'edges':[dict(r) for r in rows]}

async def graph_rows(user_id=None):
    async with pool().acquire() as con:
        rows = await con.fetch('''SELECT m.chat_id, COALESCE(c.title,d.title) AS title, m.sender_user_id,
            coalesce(nullif(concat_ws(' ',u.first_name,u.last_name),''),u.username,m.sender_user_id::text) AS name,
            count(*) AS n, max(m.message_id) AS message_id
            FROM messages m LEFT JOIN chats c ON c.id=m.chat_id
            LEFT JOIN discovered_dialogs d ON d.chat_id=m.chat_id
            LEFT JOIN users u ON u.id=m.sender_user_id
            WHERE m.sender_user_id IS NOT NULL AND ($1::bigint IS NULL OR m.sender_user_id=$1)
            GROUP BY m.chat_id,c.title,d.title,m.sender_user_id,u.first_name,u.last_name,u.username
            ORDER BY n DESC LIMIT 80''', user_id)
        relations = await con.fetch('''WITH events AS (
          SELECT m.sender_user_id AS source,p.sender_user_id AS target,'reply' AS kind,m.chat_id,m.message_id
          FROM messages m JOIN messages p ON p.chat_id=coalesce(m.reply_to_peer_id,m.chat_id) AND p.message_id=m.reply_to_message_id
          WHERE m.sender_user_id IS NOT NULL AND p.sender_user_id IS NOT NULL AND m.sender_user_id<>p.sender_user_id
          UNION ALL
          SELECT sender_user_id,fwd_from_user_id,'forward',chat_id,message_id FROM messages
          WHERE sender_user_id IS NOT NULL AND fwd_from_user_id IS NOT NULL
        ), counts AS (
          SELECT source,target,kind,count(*) n,(array_agg(chat_id ORDER BY chat_id,message_id DESC))[1] chat_id,
          (array_agg(message_id ORDER BY chat_id,message_id DESC))[1] message_id
          FROM events WHERE ($1::bigint IS NULL OR source=$1 OR target=$1) GROUP BY source,target,kind
        ) SELECT e.*,coalesce(nullif(concat_ws(' ',a.first_name,a.last_name),''),a.username,e.source::text) source_label,
        coalesce(nullif(concat_ws(' ',b.first_name,b.last_name),''),b.username,e.target::text) target_label
        FROM counts e LEFT JOIN users a ON a.id=e.source LEFT JOIN users b ON b.id=e.target
        ORDER BY n DESC,source,target LIMIT 80''',user_id)
    return [dict(r,kind='publication') for r in rows]+[dict(r) for r in relations]

Q_RUNTIME = """
SELECT pid, started_at, heartbeat_at, status, telegram_connected,
       telegram_last_ok_at, update_last_received_at, note
  FROM runtime_state WHERE id=1
"""

Q_TOTALS = """
SELECT
  (SELECT count(*) FROM messages)                         AS messages,
  (SELECT count(*) FROM users)                            AS users,
  (SELECT count(*) FROM media)                            AS media,
  (SELECT count(*) FROM (
       SELECT chat_id AS id FROM discovered_dialogs
       UNION SELECT chat_id FROM target_preferences
       UNION SELECT chat_id FROM target_state
       UNION SELECT id FROM chats) k
     WHERE NOT EXISTS (SELECT 1 FROM target_preferences p
                        WHERE p.chat_id = k.id AND p.excluded))  AS chats,
  (SELECT count(*) FROM message_versions)                 AS versions,
  (SELECT count(*) FROM message_reactions)                AS reactions,
  (SELECT count(*) FROM backfill_queue WHERE status='pending') AS dlq_pending,
  (SELECT count(*) FROM backfill_queue WHERE status='dead')    AS dlq_dead
"""

Q_MEDIA_STATUS = """
SELECT status, count(*)::bigint AS n
  FROM media GROUP BY status ORDER BY status
"""

Q_MSGS_LAST_24H = """
SELECT date_trunc('hour', date AT TIME ZONE $1)::text AS bucket,
       count(*)::bigint AS n
  FROM messages
 WHERE date >= now() - interval '24 hours'
 GROUP BY 1 ORDER BY 1
"""

Q_MSGS_PER_MIN = """
SELECT count(*)::bigint AS n
  FROM messages
 WHERE collected_at >= now() - interval '1 minute'
"""

Q_RECENT_ERRORS = """
SELECT id, at, kind, chat_id, message_id, user_id,
       substr(coalesce(message,''), 1, 400) AS message,
       error_group, occurrence_count, resolved_at
  FROM errors
 ORDER BY at DESC LIMIT $1
"""

Q_TARGETS = """
SELECT t.chat_id, t.target, t.backfill_status, t.pending_count, t.dead_count,
       t.enabled, t.last_success_at, t.last_error_at,
       substr(coalesce(t.last_error,''), 1, 200) AS last_error,
       s.last_backfilled_message_id, s.backfill_complete,
       s.participants_synced, s.participants_is_complete,
       s.participants_limitation,
       COALESCE(c.title, d.title)       AS title,
       COALESCE(c.username, d.username) AS username
  FROM target_state t
  LEFT JOIN sync_state s ON s.chat_id = t.chat_id
  LEFT JOIN chats c ON c.id = t.chat_id
  LEFT JOIN discovered_dialogs d ON d.chat_id = t.chat_id
 ORDER BY t.chat_id
"""


async def dashboard() -> dict[str, Any]:
    p = pool()
    async with p.acquire() as con:
        runtime = await con.fetchrow(Q_RUNTIME)
        totals = await con.fetchrow(Q_TOTALS)
        media_status = await con.fetch(Q_MEDIA_STATUS)
        msgs_per_min = await con.fetchrow(Q_MSGS_PER_MIN)
        # Dashboard is cheap; cap at 20 err rows.
        errors = await con.fetch(Q_RECENT_ERRORS, 20)
        targets = await con.fetch(Q_TARGETS)
    return {
        "runtime": dict(runtime) if runtime else None,
        "totals": dict(totals) if totals else {},
        "media_status": [dict(r) for r in media_status],
        "msgs_per_min": msgs_per_min["n"] if msgs_per_min else 0,
        "errors": [dict(r) for r in errors],
        "targets": [dict(r) for r in targets],
    }


async def control_chats() -> list[dict[str, Any]]:
    """Known chats that can be selected as collection targets.

    Pulls the UNION of chat ids from discovered_dialogs, target_preferences,
    target_state AND the legacy chats table — the discovery pipeline only
    touches discovered_dialogs, it does NOT backfill rows into chats until
    a message actually arrives. So right after `sync_once` runs there can be
    many discovered_dialogs rows without a matching chats row; those still
    need to show up in the picker so the operator can pick them.
    """
    async with pool().acquire() as con:
        rows = await con.fetch("""
            WITH ids AS (
                SELECT chat_id AS id FROM discovered_dialogs
                UNION SELECT chat_id FROM target_preferences
                UNION SELECT chat_id FROM target_state
                UNION SELECT id      FROM chats
            )
            SELECT
                i.id,
                COALESCE(c.title,    d.title)    AS title,
                COALESCE(c.username, d.username) AS username,
                COALESCE(c.type, d.category, 'chat') AS type,
                COALESCE(p.selected, FALSE) AS selected,
                COALESCE(p.excluded, FALSE) AS excluded,
                COALESCE(t.enabled,  FALSE) AS enabled,
                (d.chat_id IS NOT NULL)      AS discovered,
                COALESCE(d.gone_at IS NOT NULL, FALSE) AS gone,
                COALESCE(d.is_archived, FALSE) AS archived,
                (SELECT COUNT(*) FROM messages m WHERE m.chat_id = i.id) AS messages
              FROM ids i
              LEFT JOIN chats               c ON c.id       = i.id
              LEFT JOIN discovered_dialogs  d ON d.chat_id  = i.id
              LEFT JOIN target_preferences  p ON p.chat_id  = i.id
              LEFT JOIN target_state        t ON t.chat_id  = i.id
             WHERE NOT COALESCE(p.excluded, FALSE)
          ORDER BY COALESCE(c.title, d.title, c.username, d.username, i.id::text),
                   i.id
        """)
    return [dict(r) for r in rows]


async def chart_messages_24h(tz: str) -> list[dict[str, Any]]:
    p = pool()
    async with p.acquire() as con:
        rows = await con.fetch(Q_MSGS_LAST_24H, tz)
    return [dict(r) for r in rows]


# --- messages ------------------------------------------------------------
#
# Keyset pagination: (date DESC, chat_id DESC, message_id DESC).
# Cursor is encoded as "<date_iso>|<chat_id>|<message_id>" in the URL.

MSG_LIST_SELECT = """
SELECT chat_id, message_id, sender_user_id, sender_chat_id, date, edit_date,
       substr(coalesce(text,''), 1, 400) AS text_preview,
       media_type, media_file_id,
       COALESCE((SELECT title FROM chats WHERE id=messages.chat_id), (SELECT title FROM discovered_dialogs WHERE chat_id=messages.chat_id)) AS chat_title,
       is_deleted, deleted_at,
       views, forwards, reactions_total
  FROM messages
"""


async def messages_list(
    *,
    cursor: tuple | None,
    text: str | None,
    chat_id: int | None,
    sender_user_id: int | None,
    date_from: str | None,     # ISO date, parsed by caller
    date_to: str | None,
    with_media: bool | None,
    only_deleted: bool | None,
    limit: int,
) -> list[dict[str, Any]]:
    """
    Parameters are all passed as positional SQL args. The WHERE clause is
    composed from a fixed set of predicates — never from string
    concatenation of user input.
    """
    where: list[str] = []
    args: list[Any] = []
    def bind(v: Any) -> str:
        args.append(v); return f"${len(args)}"

    if text:
        where.append(f"text ILIKE {bind('%' + text + '%')}")
    if chat_id is not None:
        where.append(f"chat_id = {bind(chat_id)}")
    if sender_user_id is not None:
        where.append(f"sender_user_id = {bind(sender_user_id)}")
    if date_from:
        where.append(f"date >= {bind(date_from)}::timestamptz")
    if date_to:
        where.append(f"date <= {bind(date_to)}::timestamptz")
    if with_media is True:
        where.append("media_type IS NOT NULL")
    elif with_media is False:
        where.append("media_type IS NULL")
    if only_deleted:
        where.append("is_deleted = TRUE")

    if cursor:
        c_date, c_chat, c_msg = cursor
        where.append(
            f"(date, chat_id, message_id) < "
            f"({bind(c_date)}::timestamptz, {bind(c_chat)}, {bind(c_msg)})"
        )

    sql = MSG_LIST_SELECT
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY date DESC, chat_id DESC, message_id DESC"
    sql += f" LIMIT {bind(limit)}"

    p = pool()
    async with p.acquire() as con:
        rows = await con.fetch(sql, *args)
    return [dict(r) for r in rows]


Q_MSG_ONE = """
SELECT chat_id, message_id, sender_user_id, sender_chat_id,
       COALESCE((SELECT c.title FROM chats c WHERE c.id=m.chat_id), (SELECT d.title FROM discovered_dialogs d WHERE d.chat_id=m.chat_id)) AS chat_title,
       (SELECT coalesce(nullif(concat_ws(' ',u.first_name,u.last_name),''),u.username,u.id::text) FROM users u WHERE u.id=m.sender_user_id) AS sender_name,
       date, edit_date, text, entities,
       reply_to_message_id, reply_to_top_id, reply_to_peer_id, thread_id,
       post_author,
       media_type, media_path, media_file_id, media_group_id,
       fwd_from_user_id, fwd_from_chat_id, fwd_from_name, fwd_from_msg_id,
       fwd_date, fwd_channel_post,
       views, forwards, replies_count, reactions_total,
       is_service, service_action, service_payload,
       geo, contact, poll, webpage,
       raw, is_deleted, deleted_at, collected_at
  FROM messages m WHERE chat_id=$1 AND message_id=$2
"""

Q_MSG_VERSIONS = """
SELECT id, edit_date, text, media_type, is_initial, captured_at
  FROM message_versions
 WHERE chat_id=$1 AND message_id=$2
 ORDER BY captured_at ASC, id ASC
"""

Q_MSG_MEDIA = """
SELECT mm.file_id, mm.attached_at,
       m.media_type, m.mime_type, m.size, m.file_name, m.status,
       m.width, m.height, m.duration, m.sha256, m.completed_at
  FROM message_media mm
  LEFT JOIN media m ON m.file_id = mm.file_id
 WHERE mm.chat_id=$1 AND mm.message_id=$2
"""

Q_MSG_REACTIONS = """
SELECT reaction, count, chosen_order, updated_at
  FROM message_reactions
 WHERE chat_id=$1 AND message_id=$2
 ORDER BY count DESC, reaction
"""


async def message_detail(chat_id: int, message_id: int) -> dict[str, Any] | None:
    p = pool()
    async with p.acquire() as con:
        row = await con.fetchrow(Q_MSG_ONE, chat_id, message_id)
        if row is None:
            return None
        versions = await con.fetch(Q_MSG_VERSIONS, chat_id, message_id)
        media = await con.fetch(Q_MSG_MEDIA, chat_id, message_id)
        reactions = await con.fetch(Q_MSG_REACTIONS, chat_id, message_id)
    return {
        "msg": dict(row),
        "versions": [dict(r) for r in versions],
        "media": [dict(r) for r in media],
        "reactions": [dict(r) for r in reactions],
    }


# --- users ---------------------------------------------------------------
#
# SELECT list NEVER includes access_hash, access_hash_auth_fingerprint,
# access_hash_source_* or raw.access_hash — only "profile" fields.

USER_FIELDS_PUBLIC = """
  id, username, first_name, last_name, phone, bio, lang_code,
  is_bot, is_premium, is_scam, is_fake, is_verified, is_deleted,
  is_contact, is_mutual_contact, is_close_friend, is_min,
  photo_path, photo_dc, photo_file_id, entity_source, entity_quality,
  full_fetched_at, first_seen, last_updated, last_seen
"""


async def users_list(
    *,
    cursor_id: int | None,
    query: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    where: list[str] = []
    args: list[Any] = []
    def bind(v): args.append(v); return f"${len(args)}"

    if query:
        if query.isdigit():
            where.append(f"id = {bind(int(query))}")
        else:
            needle = f"%{query}%"
            where.append(
                "(username ILIKE " + bind(needle) +
                " OR first_name ILIKE " + bind(needle) +
                " OR last_name ILIKE " + bind(needle) +
                " OR phone ILIKE " + bind(needle) + ")"
            )
    if cursor_id is not None:
        where.append(f"id < {bind(cursor_id)}")

    sql = f"SELECT {USER_FIELDS_PUBLIC} FROM users"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC"
    sql += f" LIMIT {bind(limit)}"

    p = pool()
    async with p.acquire() as con:
        rows = await con.fetch(sql, *args)
    return [dict(r) for r in rows]


Q_USER_ONE = f"SELECT {USER_FIELDS_PUBLIC} FROM users WHERE id=$1"

Q_USER_PROFILE_HISTORY = """
SELECT field, old_value, new_value, changed_at,
       source_chat_id, source_message_id
  FROM user_profile_history WHERE user_id=$1
 ORDER BY changed_at DESC LIMIT 200
"""

Q_USER_AVATARS = """
SELECT photo_file_id, local_path, first_seen
  FROM avatar_history WHERE user_id=$1
 ORDER BY first_seen DESC LIMIT 100
"""

Q_USER_RECENT_MSGS = """
SELECT chat_id, message_id, date, media_type,
       COALESCE((SELECT title FROM chats WHERE id=messages.chat_id), (SELECT title FROM discovered_dialogs WHERE chat_id=messages.chat_id)) AS chat_title,
       substr(coalesce(text,''), 1, 300) AS text_preview
  FROM messages WHERE sender_user_id=$1
 ORDER BY date DESC LIMIT 50
"""

Q_USER_OBSERVATIONS = """
SELECT chat_id, message_id, observed_at, source_type, entity_quality,
       COALESCE((SELECT title FROM chats WHERE id=user_observations.chat_id), (SELECT title FROM discovered_dialogs WHERE chat_id=user_observations.chat_id)) AS chat_title,
       username_at_time, display_name_at_time
  FROM user_observations WHERE user_id=$1
 ORDER BY observed_at DESC LIMIT 100
"""


async def user_detail(user_id: int) -> dict[str, Any] | None:
    p = pool()
    async with p.acquire() as con:
        row = await con.fetchrow(Q_USER_ONE, user_id)
        if row is None:
            return None
        hist = await con.fetch(Q_USER_PROFILE_HISTORY, user_id)
        avatars = await con.fetch(Q_USER_AVATARS, user_id)
        msgs = await con.fetch(Q_USER_RECENT_MSGS, user_id)
        obs = await con.fetch(Q_USER_OBSERVATIONS, user_id)
    return {
        "user": dict(row),
        "history": [dict(r) for r in hist],
        "avatars": [dict(r) for r in avatars],
        "messages": [dict(r) for r in msgs],
        "observations": [dict(r) for r in obs],
    }


# --- media ---------------------------------------------------------------

async def media_list(
    *,
    cursor: tuple | None,
    status: str | None,
    media_type: str | None,
    date_from: str | None,
    date_to: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    where: list[str] = []
    args: list[Any] = []
    def bind(v): args.append(v); return f"${len(args)}"

    # Only allow a fixed whitelist of status values.
    if status:
        allowed = {"pending", "done", "failed", "skipped",
                   "metadata_only", "needs_refresh", "unavailable"}
        if status in allowed:
            where.append(f"status = {bind(status)}")
    if media_type:
        where.append(f"media_type = {bind(media_type)}")
    if date_from:
        where.append(f"first_seen >= {bind(date_from)}::timestamptz")
    if date_to:
        where.append(f"first_seen <= {bind(date_to)}::timestamptz")
    if cursor:
        c_seen, c_fid = cursor
        where.append(
            f"(first_seen, file_id) < ({bind(c_seen)}::timestamptz, {bind(c_fid)})"
        )

    sql = """
    SELECT file_id, chat_id, message_id, media_type, mime_type, size,
           file_name, status, width, height, duration, sha256,
           first_seen, completed_at, verified_at
      FROM media
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY first_seen DESC, file_id DESC"
    sql += f" LIMIT {bind(limit)}"

    p = pool()
    async with p.acquire() as con:
        rows = await con.fetch(sql, *args)
    return [dict(r) for r in rows]


Q_MEDIA_ONE = """
SELECT file_id, chat_id, message_id, media_type, mime_type, size,
       file_name, local_path, status, attempts,
       substr(coalesce(last_error,''), 1, 500) AS last_error,
       width, height, duration, sha256,
       first_seen, completed_at, verified_at, is_downloadable, media_group_id
  FROM media WHERE file_id = $1
"""


async def media_detail(file_id: str) -> dict[str, Any] | None:
    p = pool()
    async with p.acquire() as con:
        row = await con.fetchrow(Q_MEDIA_ONE, file_id)
    return dict(row) if row else None


# --- errors --------------------------------------------------------------

async def errors_list(
    *,
    cursor_id: int | None,
    kind: str | None,
    unresolved_only: bool,
    limit: int,
) -> list[dict[str, Any]]:
    where: list[str] = []
    args: list[Any] = []
    def bind(v): args.append(v); return f"${len(args)}"

    if kind:
        where.append(f"kind = {bind(kind)}")
    if unresolved_only:
        where.append("resolved_at IS NULL")
    if cursor_id is not None:
        where.append(f"id < {bind(cursor_id)}")

    sql = """
    SELECT id, at, kind, chat_id, message_id, user_id,
           substr(coalesce(message,''), 1, 400) AS message,
           error_group, occurrence_count, resolved_at
      FROM errors
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC"
    sql += f" LIMIT {bind(limit)}"

    p = pool()
    async with p.acquire() as con:
        rows = await con.fetch(sql, *args)
    return [dict(r) for r in rows]
