import json
import streamlit as st
from sqlalchemy import text as sa_text  # Add this import
from .data_loader import get_affiliation_details
from .ui_registry import get_detail_renderer
from .schema_helpers import _has_col

def render_approval_details(row, engine):
    """Render detailed information about the selected approval."""
    # Use registered detail renderer
    detail_renderer = get_detail_renderer(row["object_type"], row["action"])
    detail_renderer(row, engine)
    
    # Common details
    st.write(f"**Current status:** `{row['status']}`")
    if str(row.get("note") or "").strip():
        st.info(f"Requester note: {row['note']}")

def render_approval_actions(approval_id, row, email, decision_note, engine):
    """Render approval action buttons and logic."""
    # Mark under review button
    if st.button("🕒 Mark Under Review", disabled=(row["status"] == "under_review"), key="ap_under_review"):
        with engine.begin() as conn:
            conn.execute(sa_text("UPDATE approvals SET status='under_review' WHERE id=:id"), {"id": int(approval_id)})
        st.success("Marked as under_review.")
        st.rerun()

    c1, c2, _ = st.columns([1, 1, 2])
    with c1:
        if st.button("✅ Approve", key="ap_btn_approve"):
            return "approve"
    with c2:
        if st.button("⛔ Reject", key="ap_btn_reject"):
            return "reject"
    
    return None

# Register custom detail renderers
from .ui_registry import register_detail_renderer

@register_detail_renderer("affiliation", "edit_in_use")
def render_affiliation_details(row, engine):
    """Render affiliation edit details."""
    affiliation_id = int(row["object_id"]) if row["object_id"] and row["object_id"].isdigit() else None
    if affiliation_id:
        affiliation_details = get_affiliation_details(engine, affiliation_id)
        if affiliation_details:
            st.info(f"""
            **Affiliation Details:**
            - Faculty: {affiliation_details.get('faculty_name', 'N/A')} ({affiliation_details.get('email', 'N/A')})
            - Degree: {affiliation_details.get('degree_name', 'N/A')} ({affiliation_details.get('degree_code', 'N/A')})
            - Branch: {affiliation_details.get('branch_code', 'N/A')}
            - Current Designation: {affiliation_details.get('designation', 'N/A')}
            - Type: {affiliation_details.get('type', 'N/A')}
            """)
    
    # Fallback to default rendering
    from .ui_registry import _default_detail_renderer
    _default_detail_renderer(row, engine)


# --- Faculty delete detail renderer (added) ---
from .ui_registry import register_detail_renderer as _reg_facdel

def _resolve_faculty_id_for_ui(engine, row) -> int | None:
    payload = {}
    raw = row.get("payload")
    if raw:
        try:
            payload = json.loads(raw) or {}
        except Exception:
            payload = {}
    with engine.begin() as conn:
        # 1) payload.faculty_id
        if payload.get("faculty_id") is not None:
            try:
                return int(payload["faculty_id"])
            except Exception:
                pass
        # 2) payload.email
        email = (payload.get("email") or "").strip().lower()
        if email:
            r = conn.execute(sa_text("SELECT id FROM faculty_profiles WHERE LOWER(email)=LOWER(:e)"), {"e": email}).fetchone()
            return int(r[0]) if r else None
        # 3) object_id (id or email)
        oid = (row.get("object_id") or "").strip()
        if oid.isdigit():
            return int(oid)
        if oid:
            r = conn.execute(sa_text("SELECT id FROM faculty_profiles WHERE LOWER(email)=LOWER(:e)"), {"e": oid.lower()}).fetchone()
            return int(r[0]) if r else None
    return None

def _dep_count(conn, table, col, fid):
    try:
        r = conn.execute(sa_text(f"SELECT COUNT(*) FROM {table} WHERE {col}=:fid"), {"fid": fid}).fetchone()
        return int(r[0]) if r and r[0] is not None else 0
    except Exception:
        return 0

@_reg_facdel("faculty", "delete")
def render_faculty_delete_details(row, engine):
    fid = _resolve_faculty_id_for_ui(engine, row)
    if fid is None:
        st.warning("Couldn’t resolve faculty record from this approval. Check payload/object_id.")
        return

    with engine.begin() as conn:
        # Always-safe columns
        info = conn.execute(
            sa_text("SELECT id, name, email FROM faculty_profiles WHERE id=:fid"),
            {"fid": fid}
        ).fetchone()

        if not info:
            st.error(f"Faculty id {fid} not found (might already be deleted).")
            return

        # Optional user mapping (only if the column exists)
        user_id_val = None
        try:
            if _has_col(conn, "faculty_profiles", "user_id"):
                u = conn.execute(
                    sa_text("SELECT user_id FROM faculty_profiles WHERE id=:fid"),
                    {"fid": fid}
                ).fetchone()
                user_id_val = u[0] if u else None
        except Exception:
            # ignore any lookup failure; this is best-effort UI info
            pass

        # dependent counts (best-effort)
        counts = {}
        for tbl, col in [
            ("faculty_affiliations", "faculty_id"),
            ("faculty_custom_field_values", "faculty_id"),
            ("faculty_roles", "faculty_id"),
            ("faculty_initial_credentials", "faculty_id"),
            ("faculty_teachings", "faculty_id"),
            ("faculty_workloads", "faculty_id"),
            ("faculty_documents", "faculty_id"),
            ("faculty_tags_map", "faculty_id"),
        ]:
            try:
                r = conn.execute(
                    sa_text(f"SELECT COUNT(*) FROM {tbl} WHERE {col}=:fid"),
                    {"fid": fid}
                ).fetchone()
                counts[tbl] = int(r[0]) if r and r[0] is not None else 0
            except Exception:
                # table might not exist in a given deployment — ignore
                pass

    st.info(
        f"**Faculty to delete:** {getattr(info, 'name', None) or 'N/A'}  \n"
        f"**Email:** `{getattr(info, 'email', None) or 'N/A'}`  \n"
        f"**Internal ID:** `{getattr(info, 'id', None)}`  \n"
        f"**Linked user_id:** `{user_id_val if user_id_val is not None else '—'}`"
    )

    if counts:
        total = sum(counts.values())
        if total:
            st.warning(
                "This action will hard-delete dependent rows (best-effort):  \n"
                + "  \n".join([f"- {k}: **{v}**" for k, v in counts.items() if v])
            )
        else:
            st.info("No dependents found for this faculty.")
