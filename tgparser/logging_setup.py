import logging
import logging.handlers
import os
import re
import sys
from pathlib import Path

_SECRET_RX = re.compile(
    r"(?i)(api_hash|password|dsn|phone|token|session)[\"'= :]+([^\s\"']+)"
)


class RedactFilter(logging.Filter):
    def filter(self, record):
        try:
            record.msg = _SECRET_RX.sub(r"\1=***", str(record.msg))
        except Exception:
            pass
        return True


def setup(cfg: dict) -> logging.Logger:
    lg = cfg.get("logging", {})
    level = getattr(logging, lg.get("level", "INFO").upper(), logging.INFO)
    log_dir = Path(lg.get("dir", "./logs"))
    log_dir.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s | %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addFilter(RedactFilter())

    fh = logging.handlers.RotatingFileHandler(
        log_dir / "tgparser.log",
        maxBytes=lg.get("max_bytes", 10_485_760),
        backupCount=lg.get("backups", 5),
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    fh.addFilter(RedactFilter())
    root.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    sh.addFilter(RedactFilter())
    root.addHandler(sh)

    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    return logging.getLogger("tgparser")
