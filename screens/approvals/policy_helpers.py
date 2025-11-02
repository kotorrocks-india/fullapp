import streamlit as st
from sqlalchemy import text as sa_text  # Add this import
from core.policy import approver_roles_for, rule_for
from .schema_helpers import _table_exists, _cols

def _allowed_to_act(roles_set: set[str], row: dict) -> tuple[bool, set[str], str]:
    """
    Ask central policy which roles may approve this object/action and what rule applies.
    """
    engine = st.session_state.get("engine")
    active_degree = st.session_state.get("active_degree")

    allowed = approver_roles_for(row["object_type"], row["action"], engine=engine, degree=active_degree)
    rule = rule_for(row["object_type"], row["action"], engine=engine, degree=active_degree) or "either_one"
    return (not roles_set.isdisjoint(set(allowed))), set(allowed), rule

def _record_vote_and_finalize(engine, approval_id: int, decision: str, actor_email: str, note: str):
    """
    Record a vote and finalize approval status.
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
