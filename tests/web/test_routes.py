"""Routes, templates, filters, pagination, security headers."""
from __future__ import annotations

import pytest


def test_dashboard_renders(ui_env):
    client, *_ = ui_env
    r = client.get("/")
    assert r.status_code == 200
    # Ukrainian UI and local collector controls
    assert "Панель" in r.text
    assert "Запустити парсер" in r.text
    assert "Усі відомі чати" in r.text
    assert "Test" in r.text
    # Security headers
    assert r.headers.get("X-Frame-Options") == "DENY"
    assert r.headers.get("X-Content-Type-Options") == "nosniff"
    assert "default-src 'self'" in r.headers.get("Content-Security-Policy", "")


def test_graph_progressive_expansion(ui_env):
    client, *_ = ui_env
    assert client.get('/graph').status_code == 200
    chats = client.get('/graph/data').json()
    assert any(r['id'] == -1001 and r['title'] == 'Test' for r in chats['rows'])
    authors = client.get('/graph/data', params={'chat': -1001}).json()
    assert authors['rows'][0]['id'] == 42
    assert authors['rows'][0]['n'] == 3
    messages = client.get('/graph/data', params={'chat': -1001, 'user': 42}).json()
    assert [r['id'] for r in messages['rows']] == [102,101,100]
    assert messages['more'] is False
    assert client.get('/graph/data', params={'chat': -1001, 'user': 999}).json()['rows'] == []
    assert client.get('/graph/data', params={'offset': -1}).status_code == 422


def test_messages_list_and_filter(ui_env):
    client, *_ = ui_env
    r = client.get("/messages")
    assert r.status_code == 200
    assert "Повідомлення" in r.text
    assert "#100" in r.text and "#101" in r.text and "#102" in r.text
    # Filter by text
    r2 = client.get("/messages", params={"q": "world #1"})
    assert r2.status_code == 200
    assert "#101" in r2.text


def test_messages_sql_injection_input_is_safe(ui_env):
    """Classic injection fragments go through parameter binding; no 500s,
    no DROP TABLE side effects. The seeded `messages` table must still be
    intact after the request, and the payload must be HTML-escaped when
    reflected back into the form."""
    import asyncio, asyncpg
    from web.config import load as load_cfg
    client, *_ = ui_env
    r = client.get("/messages", params={"q": "' OR 1=1; DROP TABLE messages;--"})
    assert r.status_code == 200
    # Payload is reflected only in the form input, HTML-escaped. Raw
    # quote char must not appear as an unescaped attribute terminator.
    assert "&#39;" in r.text  # ' → &#39;
    # Verify the messages table is still there with its rows.
    cfg = load_cfg()
    async def check():
        con = await asyncpg.connect(cfg.pg_dsn)
        try:
            return await con.fetchval("SELECT count(*) FROM messages")
        finally:
            await con.close()
    assert asyncio.get_event_loop().run_until_complete(check()) == 3


def test_message_detail_xss_escaped(ui_env):
    client, *_ = ui_env
    r = client.get("/messages/-1001/102")
    assert r.status_code == 200
    # The seeded text contains "<b>hello</b> world #2 ') DROP TABLE users;".
    # Jinja2 autoescape must render it as &lt;b&gt;… not a real tag.
    assert "<b>hello</b>" not in r.text
    assert "&lt;b&gt;hello&lt;/b&gt;" in r.text


def test_users_list(ui_env):
    client, *_ = ui_env
    r = client.get("/users")
    assert r.status_code == 200
    assert "alice" in r.text and "Alice" in r.text


def test_user_detail_never_exposes_access_hash(ui_env):
    client, *_ = ui_env
    r = client.get("/users/42")
    assert r.status_code == 200
    body = r.text.lower()
    for forbidden in ("access_hash", "auth_fingerprint", "pg_dsn",
                      "tg_api_hash", "tg_api_id", "session"):
        assert forbidden not in body, f"leaked: {forbidden!r}"


def test_media_list_and_raw(ui_env):
    client, media_dir, *_ = ui_env
    r = client.get("/media")
    assert r.status_code == 200
    assert "photo:1" in r.text
    r2 = client.get("/media/raw/photo:1")
    assert r2.status_code == 200
    assert r2.content == b"png-ish bytes"
    # inline for image
    cd = r2.headers.get("Content-Disposition", "")
    assert cd.startswith("inline")


def test_media_raw_path_traversal_blocked(ui_env, tmp_path):
    client, media_dir, *_ = ui_env
    # Try to request a file we know exists OUTSIDE media_dir.
    # The DB stores local_path per file_id, so even if we craft a URL, the
    # server queries the DB and uses its local_path. We exercise the
    # containment check via the DB-stored local_path of an injected row.
    import asyncpg
    import asyncio
    from web.config import load as load_cfg
    cfg = load_cfg()
    async def inject():
        con = await asyncpg.connect(cfg.pg_dsn)
        try:
            await con.execute(
                """INSERT INTO media(file_id, media_type, mime_type, size,
                                     local_path, status, first_seen)
                   VALUES ($1,$2,$3,$4,$5,$6, NOW())""",
                "escape:1", "document", "application/octet-stream", 1,
                "../../../etc/passwd", "done")
        finally:
            await con.close()
    asyncio.get_event_loop().run_until_complete(inject())
    r = client.get("/media/raw/escape:1")
    assert r.status_code == 404


def test_chats_page(ui_env):
    client, *_ = ui_env
    r = client.get("/chats")
    assert r.status_code == 200


def test_errors_page(ui_env):
    client, *_ = ui_env
    r = client.get("/errors")
    assert r.status_code == 200


def test_openapi_and_docs_disabled(ui_env):
    client, *_ = ui_env
    for p in ("/openapi.json", "/docs", "/redoc"):
        r = client.get(p)
        assert r.status_code == 404


def test_control_requires_local_csrf_token(ui_env):
    client, *_ = ui_env
    assert client.post("/control/start", json={"mode": "all", "chats": []}).status_code == 403
    assert client.post("/control/stop", json={}).status_code == 403


def test_graph_relations_evidence(ui_env):
    """A reply needs its saved original; hidden forwards never invent a person."""
    import asyncio
    import asyncpg
    client, _, dsn = ui_env
    async def seed():
        con = await asyncpg.connect(dsn)
        try:
            await con.execute("INSERT INTO users(id,first_name) VALUES (43,'Bob')")
            await con.execute("UPDATE messages SET sender_user_id=43,reply_to_message_id=100 WHERE message_id=101")
            await con.execute("UPDATE messages SET fwd_from_user_id=43,reply_to_message_id=999999 WHERE message_id=102")
        finally:
            await con.close()
    asyncio.get_event_loop().run_until_complete(seed())
    response = client.get('/graph/relations', params={'chat': -1001})
    assert response.status_code == 200
    edges = response.json()['edges']
    assert {(e['source'],e['target'],e['kind'],e['weight'],e['message_id']) for e in edges} == {
        (43,42,'reply',1,101), (42,43,'forward',1,102)}
    assert client.get('/graph/relations', params={'chat': -999}).json()['edges'] == []
