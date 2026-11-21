"""Sample 10 users with ah-but-no-kind, check their characteristics (no secrets).
Figure out which path created them."""
import asyncio, sys
sys.path.insert(0, r"E:\TGParser")
import asyncpg
from tgparser.config import load as cfg_load

async def main():
    cfg = cfg_load()
    conn = await asyncpg.connect(cfg["postgres"]["dsn"])
    rows = await conn.fetch("""
      SELECT first_seen, last_updated, entity_source, entity_quality,
             access_hash_updated_at,
             access_hash_priority, access_hash_auth_fingerprint,
             access_hash_source_chat_id IS NOT NULL AS has_src_chat,
             is_min, is_contact
      FROM users
      WHERE access_hash IS NOT NULL AND access_hash_kind IS NULL
      ORDER BY first_seen ASC
      LIMIT 10
    """)
    print("=== first 10 oldest unclassified ===")
    for r in rows:
        print(dict(r))
    rows = await conn.fetch("""
      SELECT first_seen, last_updated, entity_source, entity_quality,
             access_hash_updated_at,
             access_hash_priority, access_hash_auth_fingerprint
      FROM users
      WHERE access_hash IS NOT NULL AND access_hash_kind IS NULL
      ORDER BY first_seen DESC LIMIT 10
    """)
    print("\n=== 10 most recent unclassified ===")
    for r in rows:
        print(dict(r))
    agg = await conn.fetch("""
      SELECT entity_source, entity_quality, COUNT(*) AS n
      FROM users
      WHERE access_hash IS NOT NULL AND access_hash_kind IS NULL
      GROUP BY 1,2 ORDER BY n DESC
    """)
    print("\n=== aggregate by entity_source/quality ===")
    for r in agg:
        print(dict(r))
    agg2 = await conn.fetch("""
      SELECT access_hash_priority, COUNT(*) AS n
      FROM users
      WHERE access_hash IS NOT NULL AND access_hash_kind IS NULL
      GROUP BY 1
    """)
    print("\n=== priority ===")
    for r in agg2:
        print(dict(r))
    # Compare: when was migration 0010 applied?
    mig = await conn.fetch("""
      SELECT * FROM schema_migrations WHERE filename LIKE '0010%'
    """)
    print("\n=== migration 0010 record ===")
    for r in mig:
        print(dict(r))
    await conn.close()

asyncio.run(main())
