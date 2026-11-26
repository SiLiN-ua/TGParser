"""UI configuration. Reads only the whitelisted keys from .env — never
sources the file as shell. Secrets never leave this module."""
from __future__ import annotations

import os
import re
import urllib.parse as u
from dataclasses import dataclass
from pathlib import Path

PROJ = Path(__file__).resolve().parent.parent


def _read_env_value(key: str) -> str | None:
    env_path = PROJ / ".env"
    if not env_path.is_file():
        return None
    rx = re.compile(rf"^\s*{re.escape(key)}\s*=\s*(.*?)\s*$")
    with env_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if line.lstrip().startswith("#"):
                continue
            m = rx.match(line)
            if m:
                v = m.group(1)
                if (v.startswith('"') and v.endswith('"')) or \
                   (v.startswith("'") and v.endswith("'")):
                    v = v[1:-1]
                return v
    return None


@dataclass(frozen=True)
class UIConfig:
    pg_dsn: str
    media_dir: Path
    bind_host: str
    bind_port: int
    page_size: int
    max_query_length: int
    tz: str

    def sanitized_pg_target(self) -> str:
        """`host:port/db` — never includes user or password; safe to log."""
        p = u.urlparse(self.pg_dsn)
        host = p.hostname or "127.0.0.1"
        port = p.port or 5432
        db = (p.path or "/").lstrip("/") or "?"
        return f"{host}:{port}/{db}"


def load() -> UIConfig:
    # UI_PG_DSN overrides, then PG_DSN from env, then .env file.
    dsn = (os.environ.get("UI_PG_DSN")
           or os.environ.get("PG_DSN")
           or _read_env_value("PG_DSN"))
    if not dsn:
        raise RuntimeError("UI_PG_DSN / PG_DSN is not set")

    media_dir = Path(os.environ.get("TGPARSER_MEDIA_DIR", PROJ / "media")).resolve()
    if not media_dir.is_dir():
        # Still construct cfg — media endpoints will return 404 safely.
        pass

    return UIConfig(
        pg_dsn=dsn,
        media_dir=media_dir,
        bind_host=os.environ.get("UI_HOST", "127.0.0.1"),
        bind_port=int(os.environ.get("UI_PORT", "8080")),
        page_size=int(os.environ.get("UI_PAGE_SIZE", "50")),
        max_query_length=int(os.environ.get("UI_MAX_QUERY_LEN", "200")),
        tz=os.environ.get("UI_TZ", "Europe/Kyiv"),
    )
