from sqlalchemy import text as sa_text

def _table_exists(conn, table: str) -> bool:
    return bool(conn.execute(sa_text(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=:t"
    ), {"t": table}).fetchone())

def _has_col(conn, table: str, col: str) -> bool:
    rows = conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()
    return any(r[1] == col for r in rows)

def _cols(conn, table: str) -> set[str]:
    return {r[1] for r in conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()}

def _count(conn, sql: str, params: dict) -> int:
    row = conn.execute(sa_text(sql), params).fetchone()
    return int(row[0]) if row and row[0] is not None else 0
