"""FastAPI application for the TGParser UI. Read-only, 127.0.0.1-only."""
from __future__ import annotations

import datetime as _dt
import html
import json
import logging
import mimetypes
import os
import secrets
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware

from . import queries
from .config import load as load_cfg
from .db import init_pool, close_pool
from . import control
from tgparser.storage import Storage

log = logging.getLogger("tgparser.ui")

PROJ = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
STATIC_DIR = Path(__file__).resolve().parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = load_cfg()
    app.state.cfg = cfg
    await init_pool(cfg)
    app.state.control_storage = Storage(cfg.pg_dsn)
    await app.state.control_storage.connect()
    app.state.control_token = secrets.token_urlsafe(32)
    log.info("UI started; bind=%s:%s; db=%s",
             cfg.bind_host, cfg.bind_port, cfg.sanitized_pg_target())
    yield
    await app.state.control_storage.close()
    await close_pool()


app = FastAPI(
    title="TGParser UI",
    docs_url=None, redoc_url=None, openapi_url=None,  # no API surface
    debug=False,
    lifespan=lifespan,
)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# --- helpers / Jinja filters ---------------------------------------------

def _kyiv(dt: Any) -> str:
    if dt is None:
        return ""
    if isinstance(dt, str):
        try:
            dt = _dt.datetime.fromisoformat(dt)
        except Exception:
            return dt
    if not isinstance(dt, _dt.datetime):
        return str(dt)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    return dt.astimezone(ZoneInfo("Europe/Kyiv")).strftime("%Y-%m-%d %H:%M:%S")


def _nice_int(n: Any) -> str:
    try:
        return f"{int(n):,}".replace(",", " ")
    except Exception:
        return str(n)


def _pretty_json(v: Any) -> str:
    """Return canonical, HTML-safe JSON for raw blocks."""
    try:
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", errors="replace")
        if isinstance(v, str):
            try:
                v = json.loads(v)
            except Exception:
                pass
        text = json.dumps(v, ensure_ascii=False, indent=2, sort_keys=True,
                          default=str)
    except Exception:
        text = str(v)
    return html.escape(text)


def _short(s: Any, n: int = 100) -> str:
    if s is None:
        return ""
    s = str(s)
    return s if len(s) <= n else s[:n] + "…"


templates.env.filters["kyiv"] = _kyiv
templates.env.filters["nice_int"] = _nice_int
templates.env.filters["pretty_json"] = _pretty_json
templates.env.filters["short"] = _short

def _telegram_link(chat_id, message_id):
    cid = str(chat_id)
    return f"https://t.me/c/{cid[4:]}/{int(message_id)}" if cid.startswith('-100') and message_id else None

templates.env.globals['telegram_link'] = _telegram_link

@app.get('/graph', response_class=HTMLResponse)
async def graph_page(request: Request, user: int | None = None):
    return templates.TemplateResponse(request, 'explorer.html', {'title': 'Граф зв’язків'})

@app.get('/graph/data')
async def graph_data(chat: int | None = None, user: int | None = None, offset: int = Query(0, ge=0)):
    from fastapi.encoders import jsonable_encoder
    return JSONResponse(jsonable_encoder(await queries.graph_expand(chat,user,offset)))


@app.get('/graph/relations')
async def graph_relations(chat: int):
    return JSONResponse(await queries.graph_relations(chat))


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; img-src 'self' data:; "
            "style-src 'self'; script-src 'self'; "
            "media-src 'self'; frame-ancestors 'none'; base-uri 'none'"
        )
        response.headers["Permissions-Policy"] = "interest-cohort=()"
        return response


app.add_middleware(SecurityHeadersMiddleware)


# --- generic error handler (never leak internals) -------------------------

@app.exception_handler(Exception)
async def _any(request: Request, exc: Exception):
    # Log server-side with full detail; return a scrubbed page to the user.
    log.exception("unhandled error on %s", request.url.path)
    return templates.TemplateResponse(
        request, "error.html",
        {"title": "Помилка", "detail": "Внутрішня помилка сервера."},
        status_code=500,
    )


# --- dashboard -----------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    data = await queries.dashboard()
    cfg = request.app.state.cfg
    chart = await queries.chart_messages_24h(cfg.tz)
    control_chats = await queries.control_chats()
    return templates.TemplateResponse(
        request, "dashboard.html",
        {"title": "Панель", "data": data, "chart": chart,
         "control_chats": control_chats,
         "control_token": request.app.state.control_token},
    )


def _control_request_ok(request: Request) -> bool:
    host = request.client.host if request.client else ""
    origin = request.headers.get("origin", "")
    expected = f"http://{request.app.state.cfg.bind_host}:{request.app.state.cfg.bind_port}"
    return (host in {"127.0.0.1", "::1", "testclient"}
            and origin in {expected, "http://127.0.0.1:8080"}
            and request.headers.get("x-tgparser-control") == request.app.state.control_token)


@app.post("/control/start")
async def control_start(request: Request):
    if not _control_request_ok(request):
        raise HTTPException(status_code=403, detail="Неприпустимий запит керування")
    body = await request.json()
    mode = body.get("mode")
    chats = body.get("chats", [])
    if mode not in {"all", "selected"} or not isinstance(chats, list) or len(chats) > 500:
        raise HTTPException(status_code=422, detail="Некоректний вибір чатів")
    try:
        chat_ids = [int(x) for x in chats]
        state = await control.configure_and_start(
            request.app.state.control_storage, chat_ids, mode == "all")
    except (ValueError, control.ControlError) as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=409)
    return {"ok": True, "message": "Збір запущено", "pid": state.pid}


@app.post("/control/stop")
async def control_stop(request: Request):
    if not _control_request_ok(request):
        raise HTTPException(status_code=403, detail="Неприпустимий запит керування")
    try:
        state = await control.stop_service(request.app.state.control_storage)
    except control.ControlError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=409)
    return {"ok": True, "message": "Збір зупинено", "pid": state.pid}


@app.post("/control/add_chat")
async def control_add_chat(request: Request):
    if not _control_request_ok(request):
        raise HTTPException(status_code=403, detail="Неприпустимий запит керування")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=422, detail="Некоректний запит")
    raw_target = body.get("raw_target") if isinstance(body, dict) else None
    if not isinstance(raw_target, str):
        raise HTTPException(status_code=422, detail="Відсутнє поле raw_target")
    try:
        res = await control.add_chat(request.app.state.control_storage,
                                     raw_target)
    except control.ControlError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=409)
    service = await control.service_state()
    if res["inserted"]:
        message = "Чат додано; перевіряємо доступ у Telegram…"
    elif res["status"] == "selected":
        message = "Цей чат уже додано раніше."
    else:
        message = "Чат уже в черзі; перевіряємо доступ у Telegram…"
    return {"ok": True, "message": message,
            "inserted": res["inserted"],
            "status": res["status"],
            "resolved_chat_id": res["resolved_chat_id"],
            "service_running": service.running}


def _parse_chat_id(request_json) -> int | None:
    cid = request_json.get("chat_id") if isinstance(request_json, dict) else None
    try:
        return int(cid)
    except Exception:
        return None


@app.post("/control/delete_chat")
async def control_delete_chat(request: Request):
    if not _control_request_ok(request):
        raise HTTPException(status_code=403, detail="Неприпустимий запит керування")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=422, detail="Некоректний запит")
    chat_id = _parse_chat_id(body)
    if chat_id is None:
        raise HTTPException(status_code=422, detail="Відсутнє або некоректне поле chat_id")
    try:
        await control.delete_chat(request.app.state.control_storage, chat_id)
    except control.ControlError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=409)
    return {"ok": True, "chat_id": chat_id, "message": "Чат видалено з платформи."}


@app.post("/control/sync_status")
async def control_sync_status(request: Request):
    if not _control_request_ok(request):
        raise HTTPException(status_code=403, detail="Неприпустимий запит керування")
    return {"ok": True, **await control.sync_status(request.app.state.control_storage)}


@app.post("/control/sync_dialogs")
async def control_sync_dialogs(request: Request):
    if not _control_request_ok(request):
        raise HTTPException(status_code=403, detail="Неприпустимий запит керування")
    try:
        res = await control.sync_dialogs(request.app.state.control_storage)
    except control.ControlError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=409)
    mode = res.get("mode")
    if mode == "queued":
        if not res["ok"]:
            reason = res.get("reason")
            msgs = {
                "pending_request":
                    "Попередній запит на синхронізацію ще не оброблений.",
                "throttled":
                    "Синхронізація обмежена за часом; спробуйте згодом.",
            }
            return JSONResponse(
                {"ok": False, "message": msgs.get(reason, "Запит відхилено"),
                 "reason": reason, "service_running": True}, status_code=409)
        return {"ok": True,
                "message": "Синхронізацію заплановано; оновлення з'явиться найближчим часом.",
                "mode": "queued", "requested_seq": res.get("requested_seq"),
                "service_running": True}
    # mode == "ran" — the one-shot process finished.
    resolver = res.get("resolver", {})
    disc = res.get("discovery", {})
    parts = [f"Синхронізацію виконано: діалогів {disc.get('seen', 0)}",
             f"нових {disc.get('new', 0)}", f"зникло {disc.get('gone', 0)}"]
    if resolver.get("selected"):
        parts.append(f"додано вручну {resolver['selected']}")
    if resolver.get("terminal"):
        parts.append(f"недоступних {resolver['terminal']}")
    return {"ok": True,
            "message": ", ".join(parts) + ".",
            "mode": "ran",
            "service_running": False,
            "discovery": res.get("discovery"),
            "resolver": resolver,
            "duration_s": res.get("duration_s")}


# --- messages ------------------------------------------------------------

def _parse_messages_cursor(c: str | None) -> tuple | None:
    if not c:
        return None
    try:
        a, b, d = c.split("|", 2)
        return a, int(b), int(d)
    except Exception:
        return None


def _parse_int(v: str | None) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except Exception:
        return None


def _parse_date(v: str | None) -> str | None:
    if not v:
        return None
    try:
        # Accept YYYY-MM-DD and full ISO forms.
        _dt.datetime.fromisoformat(v)
        return v
    except Exception:
        return None


@app.get("/messages", response_class=HTMLResponse)
async def messages_page(
    request: Request,
    q: str | None = Query(None, max_length=200),
    chat_id: str | None = Query(None, max_length=32),
    sender: str | None = Query(None, max_length=32),
    date_from: str | None = Query(None, max_length=32),
    date_to: str | None = Query(None, max_length=32),
    with_media: str | None = Query(None, max_length=10),
    only_deleted: str | None = Query(None, max_length=10),
    cursor: str | None = Query(None, max_length=120),
):
    cfg = request.app.state.cfg
    wm = {"1": True, "0": False}.get(with_media, None)
    rows = await queries.messages_list(
        cursor=_parse_messages_cursor(cursor),
        text=(q or None),
        chat_id=_parse_int(chat_id),
        sender_user_id=_parse_int(sender),
        date_from=_parse_date(date_from),
        date_to=_parse_date(date_to),
        with_media=wm,
        only_deleted=(only_deleted == "1"),
        limit=cfg.page_size,
    )
    next_cursor = ""
    if rows and len(rows) == cfg.page_size:
        last = rows[-1]
        d = last["date"].isoformat() if last["date"] else ""
        next_cursor = f"{d}|{last['chat_id']}|{last['message_id']}"
    return templates.TemplateResponse(
        request, "messages.html",
        {"title": "Повідомлення", "rows": rows, "next_cursor": next_cursor,
         "q": q or "", "chat_id": chat_id or "", "sender": sender or "",
         "date_from": date_from or "", "date_to": date_to or "",
         "with_media": with_media or "", "only_deleted": only_deleted or ""},
    )


@app.get("/messages/{chat_id}/{message_id}", response_class=HTMLResponse)
async def message_detail(request: Request, chat_id: str, message_id: str):
    # Starlette's built-in `int` converter rejects negative numbers (which
    # all supergroup/channel chat_ids are), so parse manually.
    try:
        chat_id = int(chat_id); message_id = int(message_id)
    except ValueError:
        raise HTTPException(status_code=404)
    data = await queries.message_detail(chat_id, message_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Повідомлення не знайдено")
    return templates.TemplateResponse(
        request, "message_detail.html",
        {"title": f"#{message_id}", "d": data},
    )


# --- users ---------------------------------------------------------------

@app.get("/users", response_class=HTMLResponse)
async def users_page(
    request: Request,
    q: str | None = Query(None, max_length=200),
    cursor: str | None = Query(None, max_length=32),
):
    cfg = request.app.state.cfg
    rows = await queries.users_list(
        cursor_id=_parse_int(cursor),
        query=(q or None),
        limit=cfg.page_size,
    )
    next_cursor = ""
    if rows and len(rows) == cfg.page_size:
        next_cursor = str(rows[-1]["id"])
    return templates.TemplateResponse(
        request, "users.html",
        {"title": "Користувачі", "rows": rows,
         "next_cursor": next_cursor, "q": q or ""},
    )


@app.get("/users/{user_id}", response_class=HTMLResponse)
async def user_detail(request: Request, user_id: str):
    try:
        user_id = int(user_id)
    except ValueError:
        raise HTTPException(status_code=404)
    data = await queries.user_detail(user_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Користувача не знайдено")
    return templates.TemplateResponse(
        request, "user_detail.html",
        {"title": f"@{data['user'].get('username') or user_id}", "d": data},
    )


# --- media ---------------------------------------------------------------

def _parse_media_cursor(c: str | None) -> tuple | None:
    if not c:
        return None
    try:
        a, b = c.split("|", 1)
        return a, b
    except Exception:
        return None


@app.get("/media", response_class=HTMLResponse)
async def media_page(
    request: Request,
    status: str | None = Query(None, max_length=30),
    media_type: str | None = Query(None, max_length=30),
    date_from: str | None = Query(None, max_length=32),
    date_to: str | None = Query(None, max_length=32),
    cursor: str | None = Query(None, max_length=200),
):
    cfg = request.app.state.cfg
    rows = await queries.media_list(
        cursor=_parse_media_cursor(cursor),
        status=status or None,
        media_type=media_type or None,
        date_from=_parse_date(date_from),
        date_to=_parse_date(date_to),
        limit=cfg.page_size,
    )
    next_cursor = ""
    if rows and len(rows) == cfg.page_size:
        last = rows[-1]
        d = last["first_seen"].isoformat() if last["first_seen"] else ""
        next_cursor = f"{d}|{last['file_id']}"
    return templates.TemplateResponse(
        request, "media.html",
        {"title": "Медіа", "rows": rows, "next_cursor": next_cursor,
         "status": status or "", "media_type": media_type or "",
         "date_from": date_from or "", "date_to": date_to or ""},
    )


@app.get("/media/f/{file_id}", response_class=HTMLResponse)
async def media_card(request: Request, file_id: str):
    d = await queries.media_detail(file_id)
    if d is None:
        raise HTTPException(status_code=404, detail="Файл не знайдено")
    return templates.TemplateResponse(
        request, "media_detail.html",
        {"title": file_id, "d": d},
    )


@app.get("/media/raw/{file_id}")
async def media_raw(request: Request, file_id: str):
    """Serve the physical file after strict path-containment checks."""
    cfg = request.app.state.cfg
    d = await queries.media_detail(file_id)
    if d is None or not d.get("local_path"):
        raise HTTPException(status_code=404, detail="Файл не знайдено")

    rel = d["local_path"]
    # Reject any obvious escape attempt before touching the filesystem.
    if rel.startswith("/") or "\x00" in rel or ".." in Path(rel).parts:
        raise HTTPException(status_code=404)

    media_root = cfg.media_dir.resolve(strict=False)
    candidate = (media_root / rel).resolve(strict=False)
    try:
        candidate.relative_to(media_root)        # containment check
    except ValueError:
        raise HTTPException(status_code=404)

    # Follow no symlinks.
    try:
        st = os.lstat(candidate)
    except FileNotFoundError:
        raise HTTPException(status_code=404)
    import stat as _s
    if _s.S_ISLNK(st.st_mode) or not _s.S_ISREG(st.st_mode):
        raise HTTPException(status_code=404)

    ctype = d.get("mime_type") or mimetypes.guess_type(str(candidate))[0] or \
            "application/octet-stream"
    # Documents / unknown types are forced to download, never inlined.
    disp = "inline" if ctype.startswith(("image/", "video/", "audio/")) else "attachment"
    headers = {
        "Content-Disposition":
            f'{disp}; filename="{Path(rel).name}"',
    }
    return FileResponse(str(candidate), media_type=ctype, headers=headers)


# --- chats, errors, health -----------------------------------------------

@app.get("/chats", response_class=HTMLResponse)
async def chats_page(request: Request):
    data = await queries.dashboard()
    return templates.TemplateResponse(
        request, "chats.html",
        {"title": "Чати", "targets": data["targets"]},
    )


@app.get("/errors", response_class=HTMLResponse)
async def errors_page(
    request: Request,
    kind: str | None = Query(None, max_length=60),
    unresolved: str | None = Query(None, max_length=4),
    cursor: str | None = Query(None, max_length=32),
):
    cfg = request.app.state.cfg
    rows = await queries.errors_list(
        cursor_id=_parse_int(cursor),
        kind=kind or None,
        unresolved_only=(unresolved == "1"),
        limit=cfg.page_size,
    )
    next_cursor = ""
    if rows and len(rows) == cfg.page_size:
        next_cursor = str(rows[-1]["id"])
    return templates.TemplateResponse(
        request, "errors.html",
        {"title": "Помилки", "rows": rows, "next_cursor": next_cursor,
         "kind": kind or "", "unresolved": unresolved or ""},
    )


@app.get("/healthz")
async def healthz():
    """Readiness ping. No DB touches so it stays cheap. Returns JSON only."""
    return JSONResponse({"ok": True})
