# action_handlers.py

import json
from typing import Dict, Any, Optional

from sqlalchemy import text as sa_text

from .action_registry import register_action_handler
from .cascade_handlers import (
    _program_children_counts,
    _program_delete_cascade,
    _degree_delete_cascade,
    _rebuild_semesters_for_approval,
    _faculty_delete_cascade,  # NEW: cascade helper for faculty
)
from .schema_helpers import _table_exists, _has_col


# ───────────────────────────────────────────────────────────────────────────────
# Registered Handlers
# ───────────────────────────────────────────────────────────────────────────────

@register_action_handler("degree", "delete")
def handle_degree_delete(conn, degree_code: str, payload: dict) -> None:
    """Handle degree deletion (with cascade)."""
    _degree_delete_cascade(conn, degree_code)


@register_action_handler("program", "delete")
def handle_program_delete(conn, program_code: str, payload: dict) -> None:
    """
    Handle program deletion with cascade logic.
    If children exist and payload['cascade'] is not truthy, raise.
    """
    counts = _program_children_counts(conn, program_code)
    total_children = sum(counts.values())
    allow_cascade = bool((payload or {}).get("cascade"))

    if total_children > 0 and not allow_cascade:
        raise Exception(
            f"Cannot delete program '{program_code}': dependent data exists "
            f"({', '.join([f'{k}={v}' for k, v in counts.items() if v])})."
        )

    if total_children > 0 and allow_cascade:
        _program_delete_cascade(conn, program_code)
    else:
        conn.execute(
            sa_text("DELETE FROM programs WHERE LOWER(program_code)=LOWER(:pc)"),
            {"pc": program_code},
        )


@register_action_handler("branch", "delete")
def handle_branch_delete(conn, object_id: str, payload: dict) -> None:
    """
    Handle branch deletion, by numeric ID (if branches.id exists) or by branch_code.
    """
    if object_id.isdigit() and _has_col(conn, "branches", "id"):
        conn.execute(sa_text("DELETE FROM branches WHERE id=:id"), {"id": int(object_id)})
    else:
        conn.execute(
            sa_text("DELETE FROM branches WHERE LOWER(branch_code)=LOWER(:bc)"),
            {"bc": object_id},
        )


@register_action_handler("semester", "delete")
def handle_semester_delete(conn, object_id: str, payload: dict) -> None:
    """Handle semester deletion by composite key 'DEGREE:NUM' or by id."""
    try:
        deg, semno = object_id.split(":", 1)
        conn.execute(
            sa_text(
                "DELETE FROM semesters WHERE degree_code=:d AND semester_no=:n"
            ),
            {"d": deg, "n": int(semno)},
        )
    except Exception:
        if object_id.isdigit():
            conn.execute(
                sa_text("DELETE FROM semesters WHERE id=:id"), {"id": int(object_id)}
            )


@register_action_handler("semester", "edit")
def handle_semester_edit(conn, object_id: str, payload: dict) -> None:
    """
    Handle semester edits for a small set of whitelisted fields.
    object_id is not used here since degree_code + semester_no are inside payload.
    """
    allowed = {
        "degree_code",
        "semester_no",
        "title",
        "start_date",
        "end_date",
        "status",
        "active",
        "sort_order",
        "description",
    }
    safe = {k: v for k, v in (payload or {}).items() if k in allowed}

    if "degree_code" in safe and "semester_no" in safe:
        conn.execute(
            sa_text(
                """
                UPDATE semesters
                   SET title=:title,
                       start_date=:start_date,
                       end_date=:end_date,
                       status=:status,
                       active=:active,
                       sort_order=:sort_order,
                       description=:description,
                       updated_at=CURRENT_TIMESTAMP
                 WHERE degree_code=:degree_code
                   AND semester_no=:semester_no
                """
            ),
            {
                "title": safe.get("title"),
                "start_date": safe.get("start_date"),
                "end_date": safe.get("end_date"),
                "status": safe.get("status"),
                "active": int(safe.get("active")) if safe.get("active") is not None else None,
                "sort_order": safe.get("sort_order"),
                "description": safe.get("description"),
                "degree_code": safe.get("degree_code"),
                "semester_no": int(safe.get("semester_no")),
            },
        )


@register_action_handler("semesters", "binding_change")
def handle_binding_change(conn, degree_code: str, payload: dict) -> None:
    """
    Handle semester binding mode changes at the degree level and rebuild if requested.
    payload may contain: {from, to|new_binding|binding_mode, auto_rebuild}
    """
    from_binding = (payload or {}).get("from")
    to_binding = (payload or {}).get("to")
    if to_binding is None:
        to_binding = (payload or {}).get("new_binding") or (payload or {}).get("binding_mode") or "degree"

    if to_binding not in ["degree", "program", "branch"]:
        raise ValueError(f"Invalid binding mode: {to_binding}")

    conn.execute(
        sa_text(
            """
            UPDATE semester_binding
               SET binding_mode=:bm, updated_at=CURRENT_TIMESTAMP
             WHERE degree_code=:dc
            """
        ),
        {"bm": to_binding, "dc": degree_code},
    )

    if (payload or {}).get("auto_rebuild", True):
        row = conn.execute(
            sa_text(
                "SELECT label_mode FROM semester_binding WHERE degree_code=:dc"
            ),
            {"dc": degree_code},
        ).fetchone()
        label_mode = row[0] if row else "year_term"
        _rebuild_semesters_for_approval(conn, degree_code, to_binding, label_mode)


@register_action_handler("semesters", "edit_structure")
def handle_structure_edit(conn, target_key: str, payload: dict) -> None:
    """
    Handle semester structure edits against degree/program/branch structure tables,
    then rebuild the semesters for the affected degree.

    target_key format: "degree:DEGREE_CODE" | "program:PROGRAM_ID" | "branch:BRANCH_ID"
    payload: {years_to, tpy_to}
    """
    if ":" not in target_key:
        return

    target, key = target_key.split(":", 1)
    table_map = {
        "degree": "degree_semester_struct",
        "program": "program_semester_struct",
        "branch": "branch_semester_struct",
    }

    if target not in table_map:
        return

    table = table_map[target]
    key_col = "degree_code" if target == "degree" else f"{target}_id"

    years_to = (payload or {}).get("years_to")
    tpy_to = (payload or {}).get("tpy_to")
    if years_to and tpy_to:
        conn.execute(
            sa_text(
                f"""
                UPDATE {table}
                   SET years=:years,
                       terms_per_year=:tpy,
                       updated_at=CURRENT_TIMESTAMP
                 WHERE {key_col}=:key
                """
            ),
            {"years": int(years_to), "tpy": int(tpy_to), "key": key},
        )

        # Resolve degree_code to rebuild
        degree_code: Optional[str] = key if target == "degree" else None
        if not degree_code and target == "program":
            row = conn.execute(
                sa_text("SELECT degree_code FROM programs WHERE id=:id"), {"id": key}
            ).fetchone()
            degree_code = row[0] if row else None
        elif not degree_code and target == "branch":
            row = conn.execute(
                sa_text("SELECT degree_code FROM branches WHERE id=:id"), {"id": key}
            ).fetchone()
            degree_code = row[0] if row else None

        if degree_code:
            binding_row = conn.execute(
                sa_text(
                    "SELECT binding_mode, label_mode FROM semester_binding WHERE degree_code=:dc"
                ),
                {"dc": degree_code},
            ).fetchone()
            if binding_row:
                binding_mode, label_mode = binding_row
                _rebuild_semesters_for_approval(conn, degree_code, binding_mode, label_mode)


@register_action_handler("affiliation", "edit_in_use")
def handle_affiliation_edit(conn, affiliation_id: str, payload: dict) -> None:
    """
    Handle faculty affiliation edits while the affiliation is in use elsewhere.
    payload: {"updates": {field: value, ...}}
    """
    updates = (payload or {}).get("updates", {})
    if updates:
        set_clauses = []
        params = {"aff_id": int(affiliation_id)}
        for field, value in updates.items():
            if field in ["designation", "type", "custom_type_code", "allowed_credit_override", "active"]:
                set_clauses.append(f"{field}=:{field}")
                params[field] = value

        if set_clauses:
            sql = f"UPDATE faculty_affiliations SET {', '.join(set_clauses)} WHERE id=:aff_id"
            conn.execute(sa_text(sql), params)

    # If an auxiliary table tracks approval status, mark approved there too.
    if _table_exists(conn, "affiliation_edit_approvals"):
        conn.execute(
            sa_text(
                """
                UPDATE affiliation_edit_approvals
                   SET status='approved', updated_at=CURRENT_TIMESTAMP
                 WHERE affiliation_id=:aff_id
                   AND status='pending'
                """
            ),
            {"aff_id": int(affiliation_id)},
        )


# ───────────────────────────────────────────────────────────────────────────────
# NEW: Faculty delete via Approvals
# ───────────────────────────────────────────────────────────────────────────────

def _resolve_faculty_id(conn, object_id: str, payload: dict) -> int:
    """
    Resolve a faculty ID from either the approval.object_id or approval.payload.
    Accepts numeric id or an email in object_id; payload may include faculty_id or email.
    """
    # 1) payload.faculty_id wins if present
    fid = (payload or {}).get("faculty_id")
    if fid is not None:
        return int(fid)

    # 2) payload.email next
    email = ((payload or {}).get("email") or "").strip().lower()
    if email:
        row = conn.execute(
            sa_text("SELECT id FROM faculty_profiles WHERE LOWER(email)=LOWER(:e)"),
            {"e": email},
        ).fetchone()
        if not row:
            raise ValueError(f"Faculty not found for email: {email}")
        return int(row[0])

    # 3) object_id can be numeric id or an email
    if object_id:
        if object_id.isdigit():
            return int(object_id)
        row = conn.execute(
            sa_text("SELECT id FROM faculty_profiles WHERE LOWER(email)=LOWER(:e)"),
            {"e": object_id.strip().lower()},
        ).fetchone()
        if row:
            return int(row[0])

    raise ValueError("faculty.delete requires faculty_id or email in payload or object_id")


@register_action_handler("faculty", "delete")
def handle_faculty_delete(conn, object_id: str, payload: dict) -> Dict[str, Any]:
    """
    On approval: hard-delete a faculty record and its dependent rows, then optionally
    deactivate a linked user. Uses _faculty_delete_cascade for dependents.
    """
    fid = _resolve_faculty_id(conn, object_id, payload)

    # Delete dependents (custom field values, affiliations, roles, etc.)
    _faculty_delete_cascade(conn, fid)

    # Finally delete the faculty row
    conn.execute(sa_text("DELETE FROM faculty_profiles WHERE id=:fid"), {"fid": fid})

    # Optional: deactivate a linked user if such a mapping exists
    if _table_exists(conn, "users") and _has_col(conn, "faculty_profiles", "user_id"):
        try:
            conn.execute(
                sa_text(
                    """
                    UPDATE users
                       SET active=0, updated_at=CURRENT_TIMESTAMP
                     WHERE id=(SELECT user_id FROM faculty_profiles WHERE id=:fid)
                    """
                ),
                {"fid": fid},
            )
        except Exception:
            # If FK is already gone or mapping doesn't exist, ignore.
            pass

    return {"status": "ok", "deleted_faculty_id": fid}


# ───────────────────────────────────────────────────────────────────────────────
# Main Action Router entrypoint (called by Approvals page after approval)
# ───────────────────────────────────────────────────────────────────────────────

def perform_action(conn, row: dict) -> None:
    """
    Apply the underlying effect after approval.
    Routes to registered handlers based on object_type and action.
    """
    from .action_registry import get_action_handler

    otype = (row.get("object_type") or "").strip().lower()
    action = (row.get("action") or "").strip().lower()
    object_id = str(row.get("object_id") or "")

    raw = row.get("payload")
    payload = {}
    if raw:
        try:
            payload = json.loads(raw) or {}
        except Exception:
            payload = {}

    # Get and execute the appropriate handler
    handler = get_action_handler(otype, action)
    handler(conn, object_id, payload)
