# core/universal_delete.py
"""
Universal Delete Handler

ONE function to rule all deletes! Just call it with different object types.

Usage:
    # In degrees screen
    show_delete_form(engine, "degree", degree_code, user_email, 
                     display_name=degree_name)
    
    # In semesters screen  
    show_delete_form(engine, "semester", f"{degree_code}:{sem_no}", user_email,
                     display_name=f"Semester {sem_no}")
    
    # In programs screen
    show_delete_form(engine, "program", program_code, user_email,
                     display_name=program_name)
"""

import streamlit as st
from typing import Optional, Dict, Any, Callable
from datetime import datetime
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

from core.approval_handler_enhanced import ApprovalHandler


# ============================================================================
# CONFIGURATION: Define behavior for each object type
# ============================================================================

DELETE_CONFIG = {
    "degree": {
        "display_name": "Degree",
        "display_name_plural": "Degrees",
        "icon": "🎓",
        "check_dependencies": True,
        "dependency_tables": [
            ("programs", "degree_code", "Programs"),
            ("semesters", "degree_code", "Semesters"),
            ("branches", "degree_code", "Branches"),
        ],
        "warning_message": "This will delete the degree and all its programs, semesters, and branches.",
        "require_reason": True,
        "confirmation_required": True,
    },
    "program": {
        "display_name": "Program",
        "display_name_plural": "Programs",
        "icon": "📚",
        "check_dependencies": True,
        "dependency_tables": [
            ("branches", "program_code", "Branches"),
            ("semesters", "program_code", "Semesters"),
        ],
        "warning_message": "This will delete the program and all its branches and semesters.",
        "require_reason": True,
        "confirmation_required": True,
    },
    "branch": {
        "display_name": "Branch",
        "display_name_plural": "Branches",
        "icon": "🌿",
        "check_dependencies": True,
        "dependency_tables": [
            ("semesters", "branch_code", "Semesters"),
        ],
        "warning_message": "This will delete the branch and all its semesters.",
        "require_reason": True,
        "confirmation_required": True,
    },
    "semester": {
        "display_name": "Semester",
        "display_name_plural": "Semesters",
        "icon": "📅",
        "check_dependencies": False,
        "warning_message": "This will permanently delete this semester.",
        "require_reason": True,
        "confirmation_required": True,
    },
    "faculty": {
        "display_name": "Faculty",
        "display_name_plural": "Faculty",
        "icon": "👨‍🏫",
        "check_dependencies": True,
        "dependency_tables": [
            ("faculty_affiliations", "faculty_id", "Affiliations"),
            ("faculty_teachings", "faculty_id", "Teaching Assignments"),
            ("faculty_workloads", "faculty_id", "Workload Records"),
        ],
        "warning_message": "This will delete the faculty member and all their records.",
        "require_reason": True,
        "confirmation_required": True,
    },
    "subject": {
        "display_name": "Subject",
        "display_name_plural": "Subjects",
        "icon": "📖",
        "check_dependencies": True,
        "dependency_tables": [
            ("offerings", "subject_code", "Offerings"),
            ("enrollments", "subject_code", "Enrollments"),
        ],
        "warning_message": "This will delete the subject and all its offerings.",
        "require_reason": True,
        "confirmation_required": False,
    },
    "affiliation": {
        "display_name": "Affiliation",
        "display_name_plural": "Affiliations",
        "icon": "🔗",
        "check_dependencies": False,
        "warning_message": "This will remove this faculty affiliation.",
        "require_reason": True,
        "confirmation_required": False,
    },
}


# ============================================================================
# DEPENDENCY CHECKER
# ============================================================================

def check_dependencies(
    engine: Engine,
    object_type: str,
    object_id: str,
) -> Dict[str, int]:
    """
    Check if object has dependencies in other tables.
    
    Returns:
        Dict mapping table names to counts, e.g., {"programs": 3, "semesters": 12}
    """
    config = DELETE_CONFIG.get(object_type, {})
    
    if not config.get("check_dependencies"):
        return {}
    
    dependencies = {}
    dependency_tables = config.get("dependency_tables", [])
    
    with engine.begin() as conn:
        for table, column, display_name in dependency_tables:
            # Check if table exists
            table_exists = conn.execute(sa_text(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
            )).fetchone()
            
            if not table_exists:
                continue
            
            # Count dependencies
            try:
                count = conn.execute(sa_text(
                    f"SELECT COUNT(*) FROM {table} WHERE {column} = :oid"
                ), {"oid": object_id}).fetchone()[0]
                
                if count > 0:
                    dependencies[display_name] = count
            except Exception:
                # If query fails, skip this dependency
                pass
    
    return dependencies


# ============================================================================
# AUDIT TRAIL
# ============================================================================

def log_delete_request(
    engine: Engine,
    object_type: str,
    object_id: str,
    user_email: str,
    reason: str,
    display_name: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Log delete request to audit trail.
    
    Returns:
        Log entry ID
    """
    with engine.begin() as conn:
        # Create audit_log table if it doesn't exist
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action_type TEXT NOT NULL,
                object_type TEXT NOT NULL,
                object_id TEXT NOT NULL,
                object_display_name TEXT,
                user_email TEXT NOT NULL,
                reason TEXT,
                metadata TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Insert log entry
        result = conn.execute(sa_text("""
            INSERT INTO audit_log (
                action_type, object_type, object_id, object_display_name,
                user_email, reason, metadata
            ) VALUES (
                'delete_request', :obj_type, :obj_id, :display_name,
                :email, :reason, :metadata
            )
        """), {
            "obj_type": object_type,
            "obj_id": object_id,
            "display_name": display_name,
            "email": user_email,
            "reason": reason,
            "metadata": str(metadata) if metadata else None
        })
        
        return result.lastrowid


# ============================================================================
# UNIVERSAL DELETE FORM & HANDLER
# ============================================================================

def show_delete_form(
    engine: Engine,
    object_type: str,
    object_id: str,
    user_email: str,
    display_name: Optional[str] = None,
    degree_code: Optional[str] = None,
    on_success: Optional[Callable] = None,
    custom_payload: Optional[Dict[str, Any]] = None,
    form_key: Optional[str] = None,
) -> None:
    """
    Universal delete form with approval workflow.
    
    This ONE function handles delete for ANY object type!
    
    Args:
        engine: Database engine
        object_type: Type of object (degree, program, semester, faculty, etc.)
        object_id: ID of the object to delete
        user_email: Current user's email
        display_name: Display name for the object (e.g., "Bachelor of Science")
        degree_code: Optional degree code for scope
        on_success: Optional callback to run on successful submission
        custom_payload: Optional additional data for the approval
        form_key: Optional key for the form (auto-generated if not provided)
    
    Example:
        # In degrees screen
        show_delete_form(
            engine, "degree", "CS", user_email,
            display_name="Computer Science"
        )
        
        # In semesters screen
        show_delete_form(
            engine, "semester", "CS:1", user_email,
            display_name="Semester 1",
            degree_code="CS"
        )
    """
    # Get configuration for this object type
    config = DELETE_CONFIG.get(object_type)
    
    if not config:
        st.error(f"Delete not configured for object type: {object_type}")
        return
    
    # Auto-generate form key if not provided
    if not form_key:
        form_key = f"delete_{object_type}_{object_id}".replace(":", "_").replace(" ", "_")
    
    # Get display names
    icon = config["icon"]
    obj_display = config["display_name"]
    display_name = display_name or object_id
    
    # Check for dependencies
    dependencies = check_dependencies(engine, object_type, object_id)
    
    # Show delete form
    st.markdown(f"### {icon} Delete {obj_display}: {display_name}")
    
    # Warning about dependencies
    if dependencies:
        st.warning(
            f"⚠️ **This {obj_display.lower()} has dependencies:**\n\n" +
            "\n".join([f"- **{count} {name}**" for name, count in dependencies.items()])
        )
    
    # Warning message
    if config.get("warning_message"):
        st.info(config["warning_message"])
    
    # Create form
    with st.form(form_key):
        # Reason field (if required)
        reason = ""
        if config.get("require_reason"):
            reason = st.text_area(
                f"📝 Reason for deleting this {obj_display.lower()}",
                placeholder=f"Please explain why you want to delete this {obj_display.lower()}...",
                help="This will be reviewed by approvers",
                key=f"{form_key}_reason"
            )
        
        # Confirmation checkbox (if required)
        confirmed = True
        if config.get("confirmation_required"):
            confirmed = st.checkbox(
                f"⚠️ I understand this will permanently delete this {obj_display.lower()}",
                key=f"{form_key}_confirm"
            )
        
        # Submit button
        col1, col2 = st.columns([1, 3])
        with col1:
            submitted = st.form_submit_button(
                f"🗑️ Request Deletion",
                type="primary",
                use_container_width=True
            )
        
        # Handle submission
        if submitted:
            # Validate inputs
            errors = []
            
            if config.get("require_reason") and not reason.strip():
                errors.append("Please provide a reason for deletion")
            
            if config.get("confirmation_required") and not confirmed:
                errors.append("Please confirm you understand the consequences")
            
            if errors:
                for error in errors:
                    st.error(error)
                return
            
            # Prepare payload
            payload = {
                "object_type": object_type,
                "object_id": object_id,
                "display_name": display_name,
                "dependencies": dependencies,
                "requested_at": datetime.now().isoformat(),
                "requested_by": user_email,
            }
            
            # Add degree code if provided
            if degree_code:
                payload["degree_code"] = degree_code
            
            # Merge custom payload
            if custom_payload:
                payload.update(custom_payload)
            
            try:
                # Create approval handler
                handler = ApprovalHandler(
                    engine,
                    object_type,
                    degree_code=degree_code
                )
                
                # Request approval
                approval_id = handler.request_approval(
                    object_id=object_id,
                    action="delete",
                    requester_email=user_email,
                    reason=reason,
                    payload=payload
                )
                
                # Log to audit trail
                log_delete_request(
                    engine,
                    object_type,
                    object_id,
                    user_email,
                    reason,
                    display_name,
                    payload
                )
                
                # Success message
                st.success(
                    f"✅ **Deletion request submitted successfully!**\n\n"
                    f"{icon} {obj_display}: **{display_name}**\n\n"
                    f"📋 Approval Request: **#{approval_id}**\n\n"
                    f"Your request will be reviewed by authorized approvers. "
                    f"You will be notified once a decision is made."
                )
                
                # Show who can approve
                from core.approvals_policy import get_assigned_approvers
                approvers = get_assigned_approvers(
                    engine, object_type, "delete",
                    degree_code=degree_code
                )
                
                if approvers:
                    st.info(
                        f"👥 **Approvers**: " +
                        ", ".join([a.split('@')[0] for a in list(approvers)[:3]]) +
                        (f" and {len(approvers) - 3} more" if len(approvers) > 3 else "")
                    )
                
                # Run success callback if provided
                if on_success:
                    on_success(approval_id)
                
            except ValueError as e:
                st.error(f"❌ **Validation Error**: {e}")
            except Exception as e:
                st.error(f"❌ **Error**: {e}")
                st.exception(e)


# ============================================================================
# BULK DELETE FORM
# ============================================================================

def show_bulk_delete_form(
    engine: Engine,
    object_type: str,
    object_ids: list[str],
    user_email: str,
    display_names: Optional[Dict[str, str]] = None,
    degree_code: Optional[str] = None,
    on_success: Optional[Callable] = None,
) -> None:
    """
    Universal bulk delete form.
    
    Args:
        engine: Database engine
        object_type: Type of objects
        object_ids: List of object IDs to delete
        user_email: Current user's email
        display_names: Optional dict mapping object_id -> display name
        degree_code: Optional degree code for scope
        on_success: Optional callback on success
    
    Example:
        show_bulk_delete_form(
            engine, "semester",
            ["CS:1", "CS:2", "CS:3"],
            user_email,
            display_names={"CS:1": "Semester 1", "CS:2": "Semester 2"},
            degree_code="CS"
        )
    """
    if not object_ids:
        st.warning("No objects selected for deletion")
        return
    
    config = DELETE_CONFIG.get(object_type, {})
    if not config:
        st.error(f"Delete not configured for object type: {object_type}")
        return
    
    icon = config["icon"]
    obj_display = config["display_name_plural"]
    
    st.markdown(f"### {icon} Bulk Delete {obj_display}")
    st.info(f"Selected {len(object_ids)} {obj_display.lower()} for deletion")
    
    # Show list of objects
    with st.expander(f"View {len(object_ids)} selected {obj_display.lower()}"):
        for obj_id in object_ids:
            display_name = (display_names or {}).get(obj_id, obj_id)
            st.write(f"- {display_name}")
    
    # Form
    with st.form(f"bulk_delete_{object_type}"):
        reason = st.text_area(
            f"📝 Reason for deleting these {obj_display.lower()}",
            placeholder=f"Please explain why you want to delete these {obj_display.lower()}...",
        )
        
        confirmed = st.checkbox(
            f"⚠️ I understand this will permanently delete {len(object_ids)} {obj_display.lower()}",
        )
        
        submitted = st.form_submit_button(
            f"🗑️ Request Bulk Deletion",
            type="primary"
        )
        
        if submitted:
            if not reason.strip():
                st.error("Please provide a reason")
                return
            
            if not confirmed:
                st.error("Please confirm you understand the consequences")
                return
            
            # Create approval requests for each object
            approval_ids = []
            
            for obj_id in object_ids:
                display_name = (display_names or {}).get(obj_id, obj_id)
                
                try:
                    handler = ApprovalHandler(engine, object_type, degree_code=degree_code)
                    
                    approval_id = handler.request_approval(
                        object_id=obj_id,
                        action="delete",
                        requester_email=user_email,
                        reason=reason,
                        payload={
                            "display_name": display_name,
                            "bulk_operation": True,
                            "bulk_count": len(object_ids),
                        }
                    )
                    
                    approval_ids.append(approval_id)
                    
                    # Log to audit trail
                    log_delete_request(
                        engine, object_type, obj_id, user_email,
                        reason, display_name,
                        {"bulk_operation": True}
                    )
                    
                except Exception as e:
                    st.error(f"Error for {display_name}: {e}")
            
            if approval_ids:
                st.success(
                    f"✅ **Bulk deletion request submitted!**\n\n"
                    f"Created {len(approval_ids)} approval requests\n\n"
                    f"Request IDs: {', '.join([f'#{id}' for id in approval_ids[:5]])}"
                    + (f" and {len(approval_ids) - 5} more" if len(approval_ids) > 5 else "")
                )
                
                if on_success:
                    on_success(approval_ids)


# ============================================================================
# QUICK CHECK: CAN USER DELETE?
# ============================================================================

def can_user_request_delete(
    engine: Engine,
    user_email: str,
    object_type: str,
) -> bool:
    """
    Check if user can request deletion for this object type.
    
    Returns:
        True if user can request, False otherwise
    """
    from core.policy import user_roles, can_request
    
    roles = user_roles(engine, user_email)
    return can_request(object_type, "delete", roles)


# ============================================================================
# SHOW DELETE BUTTON (Simple wrapper)
# ============================================================================

def show_delete_button(
    engine: Engine,
    object_type: str,
    object_id: str,
    user_email: str,
    display_name: Optional[str] = None,
    degree_code: Optional[str] = None,
    button_label: Optional[str] = None,
    button_key: Optional[str] = None,
) -> None:
    """
    Show delete button that opens form in expander.
    
    This is the SIMPLEST way to add delete functionality.
    
    Example:
        # Just one line!
        show_delete_button(engine, "degree", code, user_email, display_name=name)
    """
    config = DELETE_CONFIG.get(object_type, {})
    icon = config.get("icon", "🗑️")
    obj_display = config.get("display_name", object_type)
    
    button_label = button_label or f"{icon} Delete"
    button_key = button_key or f"delete_btn_{object_type}_{object_id}".replace(":", "_")
    
    if st.button(button_label, key=button_key, type="secondary"):
        with st.expander(f"Delete {obj_display}: {display_name or object_id}", expanded=True):
            show_delete_form(
                engine, object_type, object_id, user_email,
                display_name=display_name,
                degree_code=degree_code,
                form_key=f"form_{button_key}"
            )


# ============================================================================
# CONVENIENCE FUNCTIONS FOR COMMON TYPES
# ============================================================================

def delete_degree(engine, degree_code, user_email, degree_name=None):
    """Shortcut for deleting a degree."""
    show_delete_form(engine, "degree", degree_code, user_email, display_name=degree_name)


def delete_program(engine, program_code, user_email, program_name=None, degree_code=None):
    """Shortcut for deleting a program."""
    show_delete_form(engine, "program", program_code, user_email, 
                     display_name=program_name, degree_code=degree_code)


def delete_branch(engine, branch_code, user_email, branch_name=None, degree_code=None):
    """Shortcut for deleting a branch."""
    show_delete_form(engine, "branch", branch_code, user_email,
                     display_name=branch_name, degree_code=degree_code)


def delete_semester(engine, semester_id, user_email, semester_name=None, degree_code=None):
    """Shortcut for deleting a semester."""
    show_delete_form(engine, "semester", semester_id, user_email,
                     display_name=semester_name, degree_code=degree_code)


def delete_faculty(engine, faculty_id, user_email, faculty_name=None):
    """Shortcut for deleting faculty."""
    show_delete_form(engine, "faculty", str(faculty_id), user_email,
                     display_name=faculty_name)
