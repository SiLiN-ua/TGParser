import logging
import re
from pathlib import Path
import asyncpg

log = logging.getLogger("tgparser.migrations")

MIG_RX = re.compile(r"^(\d+)_.*\.sql$")


async def apply(pool: asyncpg.Pool, migrations_dir: str = "migrations") -> list[int]:
    applied: list[int] = []
    async with pool.acquire() as con:
        await con.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INT PRIMARY KEY,
                applied_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        done = {r["version"] for r in await con.fetch("SELECT version FROM schema_migrations")}
        files = sorted(Path(migrations_dir).glob("*.sql"))
        for f in files:
            m = MIG_RX.match(f.name)
            if not m:
                continue
            ver = int(m.group(1))
            if ver in done:
                continue
            sql = f.read_text(encoding="utf-8")
            log.info("Applying migration %s (%s)", ver, f.name)
            async with con.transaction():
                await con.execute(sql)
                await con.execute(
                    "INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING",
                    ver,
                )
            applied.append(ver)
    log.info("Migrations done: %s", applied or "nothing new")
    return applied
