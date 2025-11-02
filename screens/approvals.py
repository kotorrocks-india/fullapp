# app/screens/approvals.py
from __future__ import annotations

import json
import pandas as pd
import streamlit as st
from sqlalchemy import text as sa_text

# Core plumbing
from core.settings import load_settings
from core.db import get_engine, init_db
from core.policy import require_page, approver_roles_for, rule_for
from core.theme_apply import apply_theme_for_degree         # same as Degrees page
from core.theme_toggle import render_theme_toggle           # inline dark/light toggle
from core.ui import render_footer_global
from core.rbac import user_roles

# ────────────────── Small schema helpers ──────────────────
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

# ────────────────── Program child discovery (schema-adaptive) ──────────────────
def _program_children_counts(conn, program_code: str) -> dict:
    """
    Return counts of common children tied to a program. Handles both program_code and program_id styles
    and only checks tables/columns that exist in the current DB.
    """
    out = {}

    # branches
    if _table_exists(conn, "branches"):
        c = _cols(conn, "branches")
        if "program_code" in c:
            out["branches"] = _count(conn,
                "SELECT COUNT(*) FROM branches WHERE LOWER(program_code)=LOWER(:pc)",
                {"pc": program_code})
        elif "program_id" in c and _table_exists(conn, "programs"):
            pid_row = conn.execute(sa_text(
                "SELECT id FROM programs WHERE LOWER(program_code)=LOWER(:pc)"
            ), {"pc": program_code}).fetchone()
            pid = pid_row[0] if pid_row else None
            if pid is not None:
                out["branches"] = _count(conn,
                    "SELECT COUNT(*) FROM branches WHERE program_id=:pid",
                    {"pid": pid})

    # semesters (if modeled by program_code)
    if _table_exists(conn, "semesters") and "program_code" in _cols(conn, "semesters"):
        out["semesters"] = _count(conn,
            "SELECT COUNT(*) FROM semesters WHERE LOWER(program_code)=LOWER(:pc)",
            {"pc": program_code})

    # curriculum_groups (optional future)
    if _table_exists(conn, "curriculum_groups") and "program_code" in _cols(conn, "curriculum_groups"):
        out["curriculum_groups"] = _count(conn,
            "SELECT COUNT(*) FROM curriculum_groups WHERE LOWER(program_code)=LOWER(:pc)",
            {"pc": program_code})

    # subjects/offerings/enrollments (if present)
    for tbl, fld in [
        ("subjects", "program_code"),
        ("offerings", "program_code"),
        ("enrollments", "program_code"),
    ]:
        if _table_exists(conn, tbl) and (fld in _cols(conn, tbl)):
            out[tbl] = _count(conn,
                f"SELECT COUNT(*) FROM {tbl} WHERE LOWER({fld})=LOWER(:pc)",
                {"pc": program_code})

    return out

def _program_delete_cascade(conn, program_code: str):
    """
    Hard-delete children then the program. Only call when payload.cascade==true.
    """
    # Delete branches
    if _table_exists(conn, "branches"):
        c = _cols(conn, "branches")
        if "program_code" in c:
            conn.execute(sa_text(
                "DELETE FROM branches WHERE LOWER(program_code)=LOWER(:pc)"
            ), {"pc": program_code})
        elif "program_id" in c and _table_exists(conn, "programs"):
            pid_row = conn.execute(sa_text(
                "SELECT id FROM programs WHERE LOWER(program_code)=LOWER(:pc)"
            ), {"pc": program_code}).fetchone()
            if pid_row:
                conn.execute(sa_text(
                    "DELETE FROM branches WHERE program_id=:pid"
                ), {"pid": pid_row[0]})

    # Delete semesters tied to program (if modeled)
    if _table_exists(conn, "semesters") and "program_code" in _cols(conn, "semesters"):
        conn.execute(sa_text(
            "DELETE FROM semesters WHERE LOWER(program_code)=LOWER(:pc)"
        ), {"pc": program_code})

    # Delete curriculum groups (optional)
    if _table_exists(conn, "curriculum_groups") and "program_code" in _cols(conn, "curriculum_groups"):
        conn.execute(sa_text(
            "DELETE FROM curriculum_groups WHERE LOWER(program_code)=LOWER(:pc)"
        ), {"pc": program_code})

    # Finally delete program
    conn.execute(sa_text(
        "DELETE FROM programs WHERE LOWER(program_code)=LOWER(:pc)"
    ), {"pc": program_code})

# ────────────────── Data loading ──────────────────
def _fetch_open_approvals(engine) -> pd.DataFrame:
    with engine.begin() as conn:
        has_payload = _has_col(conn, "approvals", "payload")
        # canonical columns per your latest schema
        cols = "id, object_type, object_id, action, status, requester, note, created_at"
        if has_payload:
            cols += ", payload"
        rows = conn.execute(sa_text(f"""
            SELECT {cols}
              FROM approvals
             WHERE status IN ('pending','under_review')
             ORDER BY created_at DESC, id DESC
        """)).fetchall()
    if not rows:
        base = ["id","object_type","object_id","action","status","requester","note","created_at"]
        if has_payload: base.append("payload")
        return pd.DataFrame(columns=base)
    return pd.DataFrame([dict(r._mapping) for r in rows])

# ────────────────── Approval finalize (votes + status) ──────────────────
def _record_vote_and_finalize(engine, approval_id: int, decision: str, actor_email: str, note: str):
    """
    Record a vote and finalize approval status.
    - approvals_votes.decision must be 'approve' | 'reject' (per CHECK constraint)
    - approvals.status we keep as 'approved' | 'rejected'
    """
    d_norm = (decision or "").strip().lower()
    vote_val = "approve" if d_norm in ("approve", "approved") else "reject"
    status_val = "approved" if vote_val == "approve" else "rejected"

    with engine.begin() as conn:
        # record the vote if table/cols exist
        cols = _cols(conn, "approvals_votes") if _table_exists(conn, "approvals_votes") else set()
        if {"approval_id","voter_email","decision","note"}.issubset(cols):
            conn.execute(sa_text("""
                INSERT INTO approvals_votes(approval_id, voter_email, decision, note)
                VALUES (:aid, :actor, :dec, :note)
            """), {"aid": approval_id, "actor": actor_email, "dec": vote_val, "note": note})

        conn.execute(sa_text("""
            UPDATE approvals
               SET status=:st, approver=:actor, decided_at=CURRENT_TIMESTAMP
             WHERE id=:id
        """), {"st": status_val, "actor": actor_email, "id": approval_id})

# ────────────────── Policy helpers ──────────────────
# In the _allowed_to_act function in approvals.py
def _allowed_to_act(roles_set: set[str], row: dict) -> tuple[bool, set[str], str]:
    """
    Ask central policy which roles may approve this object/action and what rule applies.
    MR is intentionally not in approver set; they view only.
    """
    # Get engine from session state
    engine = st.session_state.get("engine")
    
    # Get active degree for policy context
    active_degree = st.session_state.get("active_degree")
    
    # Pass engine and degree as keyword arguments
    allowed = approver_roles_for(row["object_type"], row["action"], engine=engine, degree=active_degree)
    rule = rule_for(row["object_type"], row["action"], engine=engine, degree=active_degree) or "either_one"
    return (not roles_set.isdisjoint(set(allowed))), set(allowed), rule

# ────────────────── Switchboard: apply the underlying effect ──────────────────
def _perform_action(conn, row: dict) -> None:
    """
    Apply the underlying effect after approval.

    SAFE deletes:
      • Programs: if children exist → block with clear message,
        unless approval payload has {"cascade": true} ⇒ cascade delete children then program.
      • Branches: simple delete (extend with child checks if needed).
    """
    otype = (row.get("object_type") or "").strip().lower()
    action = (row.get("action") or "").strip().lower()
    object_id = row.get("object_id")
    raw = row.get("payload")
    payload = {}
    if raw:
        try:
            payload = json.loads(raw)
        except Exception:
            payload = {}

    # PROGRAM: delete
    if otype == "program" and action == "delete":
        pc = str(object_id or "").strip()
        if not pc:
            raise ValueError("Program code missing in approval row.")

        counts = _program_children_counts(conn, pc)
        total_children = sum(counts.values())
        allow_cascade = bool(payload.get("cascade"))

        if total_children > 0 and not allow_cascade:
            parts = [f"{k}={v}" for k, v in counts.items() if v]
            summary = ", ".join(parts) if parts else "children present"
            raise Exception(
                f"Cannot delete program '{pc}': dependent data exists ({summary}). "
                f"Either deactivate it or submit a delete approval with payload.cascade=true."
            )

        if total_children > 0 and allow_cascade:
            _program_delete_cascade(conn, pc)
        else:
            conn.execute(sa_text(
                "DELETE FROM programs WHERE LOWER(program_code)=LOWER(:pc)"
            ), {"pc": pc})
        return

    # BRANCH: delete (basic – extend later if needed)
    if otype == "branch" and action == "delete":
        bc = str(object_id or "").strip()
        if not bc:
            raise ValueError("Branch code missing in approval row.")
        conn.execute(sa_text(
            "DELETE FROM branches WHERE LOWER(branch_code)=LOWER(:bc)"
        ), {"bc": bc})
        return

    # SEMESTER: delete
    if otype == "semester" and action == "delete":
        try:
            deg, semno = (object_id or "").split(":", 1)
            conn.execute(sa_text("DELETE FROM semesters WHERE degree_code=:d AND semester_no=:n"),
                         {"d": deg, "n": int(semno)})
        except Exception:
            if str(object_id).isdigit():
                conn.execute(sa_text("DELETE FROM semesters WHERE id=:id"), {"id": int(object_id)})
        return

    # SEMESTER: edit
    if otype == "semester" and action == "edit":
        allowed = {"degree_code","semester_no","title","start_date","end_date","status","active","sort_order","description"}
        safe = {k: v for k, v in (payload or {}).items() if k in allowed}
        if "degree_code" in safe and "semester_no" in safe:
            conn.execute(sa_text("""
                UPDATE semesters
                   SET title=:title, start_date=:start_date, end_date=:end_date, status=:status,
                       active=:active, sort_order=:sort_order, description=:description,
                       updated_at=CURRENT_TIMESTAMP
                 WHERE degree_code=:degree_code AND semester_no=:semester_no
            """), {
                "title": safe.get("title"),
                "start_date": safe.get("start_date"),
                "end_date": safe.get("end_date"),
                "status": safe.get("status"),
                "active": int(safe.get("active")) if safe.get("active") is not None else None,
                "sort_order": safe.get("sort_order"),
                "description": safe.get("description"),
                "degree_code": safe.get("degree_code"),
                "semester_no": int(safe.get("semester_no")),
            })
        return

# ────────────────── Public render() API ──────────────────
@require_page("Approvals")
def render():
    # Bootstrap: engine & session
    settings = load_settings()
    engine = get_engine(settings.db.url)
    init_db(engine)
    st.session_state["engine"] = engine

    # Who's logged in
    user = st.session_state.get("user") or {}
    email = (user.get("email") or "").strip().lower()
    roles = user_roles(engine, email)

    # Current “active” degree (not used visually on this page)
    active_degree = st.session_state.get("active_degree")

    # Theme (no degree logo; degree tokens + per-user mode)
    theme_cfg = apply_theme_for_degree(engine, active_degree, email)

    # Inline dark/light toggle (in-page)
    render_theme_toggle(engine, theme_cfg, key="approvals_theme_toggle", location="inline", label="Dark mode")

    st.title("📬 Approvals Inbox")

    # Inbox
    df = _fetch_open_approvals(engine)
    st.caption(f"Showing {len(df)} pending/under_review items.")
    st.dataframe(df, use_container_width=True, hide_index=True)

    if df.empty:
        #render_footer_global()
        return

    st.subheader("Review an approval")

    ids = df["id"].tolist()
    sel = st.selectbox("Select approval ID", options=ids, key="ap_sel_id")
    row = df[df["id"] == sel].iloc[0].to_dict()

    st.write(
        f"**Object:** `{row['object_type']}` • **ID:** `{row['object_id']}` • "
        f"**Action:** `{row['action']}` • **Requested by:** `{row['requester']}`"
    )
    st.write(f"**Current status:** `{row['status']}`")
    if str(row.get("note") or "").strip():
        st.info(f"Requester note: {row['note']}")

    # Per-item policy (who can act)
    eligible, approver_set, policy_rule = _allowed_to_act(set(roles), row)
    if not eligible:
        st.error(f"You are not an approver for this item. Allowed roles: {', '.join(sorted(approver_set))}")
        #render_footer_global()
        return
    st.caption(f"Policy: approver roles = {', '.join(sorted(approver_set))}; rule = {policy_rule}")

    decision_note = st.text_area("Decision note (optional)", placeholder="Reason for approval/rejection…", key="ap_dec_note")

    # Optional: mark under review
    if st.button("🕒 Mark Under Review", disabled=(row["status"] == "under_review"), key="ap_under_review"):
        with engine.begin() as conn:
            conn.execute(sa_text("UPDATE approvals SET status='under_review' WHERE id=:id"), {"id": int(sel)})
        st.success("Marked as under_review.")
        st.rerun()

    c1, c2, _ = st.columns([1, 1, 2])
    with c1:
        if st.button("✅ Approve", key="ap_btn_approve"):
            try:
                with engine.begin() as conn:
                    _perform_action(conn, row)   # apply change
                _record_vote_and_finalize(engine, int(sel), "approved", email, decision_note or "")
                st.success(f"Approved #{sel} and applied the change.")
                st.rerun()
            except Exception as ex:
                st.error(str(ex))
    with c2:
        if st.button("⛔ Reject", key="ap_btn_reject"):
            try:
                _record_vote_and_finalize(engine, int(sel), "rejected", email, decision_note or "")
                st.success(f"Rejected #{sel}.")
                st.rerun()
            except Exception as ex:
                st.error(str(ex))

    st.markdown("---")
    #render_footer_global()
render()
