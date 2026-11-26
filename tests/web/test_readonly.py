"""Static guarantee: web/queries.py contains SELECTs only.

If a future change ever adds INSERT/UPDATE/DELETE/TRUNCATE/ALTER/CREATE/
DROP/GRANT/REVOKE (as the first keyword of a SQL statement), this test
fails. The check is a lexical safeguard, not a semantic one, but it is
enough to catch accidental introductions of writes into the UI layer.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path


FORBIDDEN = {"INSERT", "UPDATE", "DELETE", "TRUNCATE", "ALTER",
             "CREATE", "DROP", "GRANT", "REVOKE", "MERGE"}


def _iter_sql_strings(src: str):
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            s = node.value
            if len(s) > 4 and re.search(r"(?i)\b(SELECT|WITH|INSERT|UPDATE|DELETE|TRUNCATE|ALTER|CREATE|DROP|GRANT|REVOKE|MERGE|COPY)\b", s):
                yield s


def test_queries_contains_only_selects():
    src = (Path(__file__).resolve().parents[2] / "web" / "queries.py").read_text("utf-8")
    bad = []
    for s in _iter_sql_strings(src):
        # Extract *statement* start keywords. A single multi-line heredoc may
        # contain several statements separated by ';'. For each piece, the
        # first real keyword must be SELECT or WITH (CTEs) or be a pure
        # expression fragment (we skip those by requiring it match a keyword
        # at all).
        for stmt in [p for p in re.split(r";\s*", s) if p.strip()]:
            m = re.search(r"(?i)\b([A-Z]+)\b", stmt)
            if not m:
                continue
            kw = m.group(1).upper()
            if kw in FORBIDDEN:
                bad.append(stmt.strip()[:120])
    assert not bad, f"Forbidden SQL keyword(s) found in web/queries.py: {bad}"
