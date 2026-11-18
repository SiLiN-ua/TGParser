import os
from pathlib import Path
import yaml
from dotenv import load_dotenv

SECRET_KEYS = {"api_hash", "phone", "dsn", "password"}


def load(config_path: str = "config.yaml", env_path: str = ".env") -> dict:
    if Path(env_path).exists():
        load_dotenv(env_path)
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    cfg.setdefault("telegram", {})
    cfg["telegram"].update({
        "api_id": int(_env("TG_API_ID", required=True)),
        "api_hash": _env("TG_API_HASH", required=True),
        "phone": _env("TG_PHONE", default=""),
        "session_name": _env("TG_SESSION_NAME", default="tgparser"),
    })
    cfg.setdefault("postgres", {})["dsn"] = _env("PG_DSN", required=True)
    return cfg


def _env(name: str, default=None, required=False) -> str:
    v = os.environ.get(name, default)
    if required and (v is None or v == ""):
        raise RuntimeError(f"Required env var {name} not set (put it in .env)")
    return v


def redact(value, key_hint: str = "") -> str:
    s = str(value)
    if not s:
        return s
    kh = key_hint.lower()
    if any(k in kh for k in SECRET_KEYS) or "api_id" in kh and len(s) > 4:
        return s[:2] + "***" + s[-2:] if len(s) > 4 else "***"
    return s
