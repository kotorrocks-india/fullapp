# app/screens/assignments.py
from __future__ import annotations
import streamlit as st
from sqlalchemy import text as sa_text, exc as sa_exc
from core.settings import load_settings
from core.db import get_engine, init_db, SessionLocal
from core.forms import tagline, success, warn

# --- schema guards (idempotent) ------------------------------------------------

def _ensure_assignments_schema(engine):
    """Create assignments table & constraints; add columns if missing."""
    with engine.begin() as conn:
        # base table
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS assignments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                degree_code TEXT NOT NULL,
                assignment_code TEXT NOT NULL,
                title TEXT,
                max_marks INTEGER DEFAULT 100,
                require_approval INTEGER NOT NULL DEFAULT 0,
                approved INTEGER NOT NULL DEFAULT 0,
                status TEXT DEFAULT 'draft',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """))
        # unique composite for your UPSERT to work
        conn.execute(sa_text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_assignments_degree_code_assignment_code
            ON assignments(degree_code, assignment_code)
        """))

        # ensure columns exist (future migrations)
        cols = {r[1] for r in conn.exec_driver_sql("PRAGMA table_info(assignments)").fetchall()}
        if "approved" not in cols:
            conn.exec_driver_sql("ALTER TABLE assignments ADD COLUMN approved INTEGER NOT NULL DEFAULT 0")
        if "require_approval" not in cols:
            conn.exec_driver_sql("ALTER TABLE assignments ADD COLUMN require_approval INTEGER NOT NULL DEFAULT 0")
        if "updated_at" not in cols:
            conn.exec_driver_sql("ALTER TABLE assignments ADD COLUMN updated_at DATETIME DEFAULT CURRENT_TIMESTAMP")


def render():
    st.title("Assignments")
    tagline()
    settings = load_settings()
    engine = get_engine(settings.db.url)
    init_db(engine)
    SessionLocal.configure(bind=engine)

    # make sure table/columns/indexes exist (and avoid UnboundLocalError on 'text')
    _ensure_assignments_schema(engine)

    # degrees to target
    with engine.begin() as conn:
        degree_rows = conn.execute(sa_text("SELECT code FROM degrees WHERE status='active' ORDER BY code")).fetchall()
        degree_codes = [r[0] for r in degree_rows]

    if not degree_codes:
        warn("No active degrees found. Add a degree first.")
        return

    degree_code = st.selectbox("Degree", degree_codes, index=0)
    assignment_code = st.text_input("Assignment code", value="ASSGN-01")
    title = st.text_input("Title", value="TOS: Load Paths & Diagrams")
    max_marks = st.number_input("Max marks", min_value=10, max_value=500, value=100, step=5)
    require_approval = st.checkbox("Require approval before publishing?", value=False)

    if st.button("Save Assignment"):
        with engine.begin() as conn:
            conn.execute(
                sa_text("""
                INSERT INTO assignments (degree_code, assignment_code, title, max_marks, require_approval, status)
                VALUES (:degree_code, :assignment_code, :title, :max_marks, :require_approval, 'draft')
                ON CONFLICT(degree_code, assignment_code) DO UPDATE
                SET title=excluded.title,
                    max_marks=excluded.max_marks,
                    require_approval=excluded.require_approval,
                    updated_at=CURRENT_TIMESTAMP
                """),
                dict(
                    degree_code=degree_code,
                    assignment_code=assignment_code,
                    title=title,
                    max_marks=int(max_marks),
                    require_approval=int(require_approval),
                ),
            )
        success("Assignment saved.")

    st.subheader("Assignments List (latest first)")
    with engine.begin() as conn:
        rows = conn.execute(sa_text("""
            SELECT degree_code, assignment_code, title, max_marks, require_approval, approved, status, updated_at
            FROM assignments
            ORDER BY updated_at DESC
        """)).fetchall()
    st.dataframe([dict(r._mapping) for r in rows])

    st.subheader("Workflow / Approvals")
    with engine.begin() as conn:
        codes = [r[0] for r in conn.execute(sa_text("SELECT code FROM degrees WHERE status='active' ORDER BY code")).fetchall()]
    if not codes:
        warn("No active degrees to manage.")
        return

    dsel = st.selectbox("Degree (manage)", codes, index=0, key="assgn_wf_deg")
    with engine.begin() as conn:
        items = [
            dict(r._mapping)
            for r in conn.execute(
                sa_text("""SELECT id, assignment_code, title, status, require_approval, approved
                           FROM assignments
                           WHERE degree_code=:d
                           ORDER BY updated_at DESC"""),
                dict(d=dsel)
            ).fetchall()
        ]

    if not items:
        warn("No assignments in this degree.")
        return

    labels = [
        f"{it['assignment_code']} — {it['title']} [{it['status']}]"
        f"{' (requires approval)' if it['require_approval'] else ''}"
        f"{' (approved)' if it.get('approved') else ''}"
        for it in items
    ]
    pick = st.selectbox("Assignment", labels, index=0, key="assgn_wf_pick")
    it = items[labels.index(pick)]

    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("Move to READY"):
            with engine.begin() as conn:
                conn.execute(sa_text("UPDATE assignments SET status='ready', updated_at=CURRENT_TIMESTAMP WHERE id=:i"), dict(i=it['id']))
            st.success("Moved to READY"); st.rerun()

    with col2:
        if st.button("Approve"):
            with engine.begin() as conn:
                conn.execute(sa_text("UPDATE assignments SET approved=1, updated_at=CURRENT_TIMESTAMP WHERE id=:i"), dict(i=it['id']))
            st.success("Approved"); st.rerun()

    with col3:
        if st.button("Publish"):
            if it['require_approval'] and not it.get('approved'):
                st.error("This assignment requires approval before publishing.")
            else:
                with engine.begin() as conn:
                    conn.execute(sa_text("UPDATE assignments SET status='published', updated_at=CURRENT_TIMESTAMP WHERE id=:i"), dict(i=it['id']))
                st.success("Published"); st.rerun()
