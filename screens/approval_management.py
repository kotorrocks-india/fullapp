# screens/approval_management.py
"""
Approval Management Page (Superadmin Only)

This page allows superadmins to:
1. View all defined approval rules
2. Assign specific users as approvers for each action type
3. Enable/disable approver assignments
4. View approver statistics
5. Configure approval policies
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Optional

# Import enhanced policy functions
from core.approvals_policy import (
    assign_approver,
    revoke_approver,
    list_all_approver_assignments,
    get_approver_stats,
    get_approval_config,
    get_assigned_approvers,
    get_role_based_approvers,
)
from core.settings import load_settings
from core.db import get_engine, init_db
from core.policy import require_page, user_roles
from sqlalchemy import text as sa_text

PAGE_KEY = "Approval Management"


def _get_all_users(engine) -> list[dict]:
    """Get all active users from the system, with a schema-aware roles column."""
    with engine.begin() as conn:
        # Inspect the user_roles table to see what column it actually has
        try:
            cols_rows = conn.execute(sa_text("PRAGMA table_info(user_roles)")).fetchall()
            cols = {r[1] for r in cols_rows}  # r[1] is the column name
        except Exception:
            cols = set()

        # Decide which column to use for roles
        if "role_name" in cols:
            role_expr = "ur.role_name"
        elif "role_code" in cols:
            role_expr = "ur.role_code"
        elif cols:
            # Table exists but no obvious name/code column
            role_expr = "CAST(NULL AS TEXT)"
        else:
            # No user_roles table at all – no join, just return users
            rows = conn.execute(sa_text("""
                SELECT u.id, u.email, u.full_name,
                       '' AS roles
                FROM users u
                WHERE u.active = 1
                ORDER BY u.full_name, u.email
            """)).fetchall()
            return [dict(r._mapping) for r in rows]

        sql = f"""
            SELECT u.id, u.email, u.full_name, 
                   GROUP_CONCAT(DISTINCT {role_expr}) as roles
            FROM users u
            LEFT JOIN user_roles ur ON u.id = ur.user_id
            WHERE u.active = 1
            GROUP BY u.id, u.email, u.full_name
            ORDER BY u.full_name, u.email
        """
        rows = conn.execute(sa_text(sql)).fetchall()

    return [dict(r._mapping) for r in rows]


def _get_all_action_types(engine) -> list[tuple]:
    """Get all unique object_type and action combinations."""
    # Predefined action types
    action_types = [
        ("degree", "delete"),
        ("degree", "edit"),
        ("degree", "create"),
        ("program", "delete"),
        ("program", "edit"),
        ("program", "create"),
        ("branch", "delete"),
        ("branch", "edit"),
        ("branch", "create"),
        ("faculty", "delete"),
        ("faculty", "edit"),
        ("faculty", "create"),
        ("semester", "delete"),
        ("semester", "edit"),
        ("semesters", "binding_change"),
        ("semesters", "edit_structure"),
        ("affiliation", "edit_in_use"),
        ("subject", "delete"),
        ("subject", "edit"),
        ("office_admin", "export_data"),    # added
        ("office_admin", "delete_student"), # added
    ]

    # Also fetch any from database
    with engine.begin() as conn:
        rows = conn.execute(sa_text("""
            SELECT DISTINCT object_type, action 
            FROM approvals
            ORDER BY object_type, action
        """)).fetchall()

        for row in rows:
            obj_type, action = row[0], row[1]
            if (obj_type, action) not in action_types:
                action_types.append((obj_type, action))

    return sorted(action_types)


def _render_overview(engine):
    """Render overview statistics."""
    # --- MODIFIED: This is now just a component, not a full subheader ---
    with engine.begin() as conn:
        # Count total assignments
        total_assignments = conn.execute(sa_text("""
            SELECT COUNT(*) FROM approver_assignments WHERE is_active = 1
        """)).fetchone()[0]

        # Count unique approvers
        unique_approvers = conn.execute(sa_text("""
            SELECT COUNT(DISTINCT approver_email) 
            FROM approver_assignments WHERE is_active = 1
        """)).fetchone()[0]

        # Count action types with assigned approvers
        action_types_covered = conn.execute(sa_text("""
            SELECT COUNT(DISTINCT object_type || '.' || action)
            FROM approver_assignments WHERE is_active = 1
        """)).fetchone()[0]

        # Count pending approvals
        pending_approvals = conn.execute(sa_text("""
            SELECT COUNT(*) FROM approvals 
            WHERE status IN ('pending', 'under_review')
        """)).fetchone()[0]

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Active Assignments", total_assignments)

    with col2:
        st.metric("Unique Approvers", unique_approvers)

    with col3:
        st.metric("Action Types Covered", action_types_covered)

    with col4:
        st.metric("Pending Approvals", pending_approvals)


def _render_assignment_matrix(engine, admin_email):
    """Render the main approver assignment matrix."""
    st.subheader("👥 Approver Assignment Matrix")
    st.caption("Assign specific users as approvers for each action type")

    # Get data
    users = _get_all_users(engine)
    action_types = _get_all_action_types(engine)
    current_assignments = list_all_approver_assignments(engine, active_only=True)

    # Create assignment lookup
    assignment_lookup = {}
    for assign in current_assignments:
        key = (assign['object_type'], assign['action'], assign['approver_email'].lower())
        assignment_lookup[key] = assign

    # Select action type to configure
    st.markdown("### Select Action Type to Configure")

    col1, col2 = st.columns(2)
    with col1:
        object_types = sorted(set(ot for ot, _ in action_types))
        selected_object_type = st.selectbox("Object Type", object_types)

    with col2:
        actions = sorted(set(act for ot, act in action_types if ot == selected_object_type))
        
        # --- Helper function for friendly names ---
        def format_action_name(action_code):
            name_map = {
                "edit_in_use": "Edit In-Use Record",
                "binding_change": "Change Semester Binding",
                "edit_structure": "Edit Structure",
                "export_data": "Export Data",
                "delete_student": "Delete Student"
            }
            # Get the friendly name, or just clean up the code name
            return name_map.get(action_code, action_code.replace("_", " ").title())

        # --- Applied format_func to the selectbox ---
        selected_action = st.selectbox(
            "Action", 
            actions,
            format_func=format_action_name  # Use the new formatter
        )

    # Show current configuration
    config = get_approval_config(engine, selected_object_type, selected_action)

    with st.expander("⚙️ Configuration Settings"):
        col1, col2, col3 = st.columns(3)

        with col1:
            st.write(f"**Requires Reason:** {config['requires_reason']}")
            st.write(f"**Min Approvers:** {config['min_approvers']}")

        with col2:
            st.write(f"**Approval Rule:** {config['approval_rule']}")
            st.write(f"**Require User Assignment:** {config['require_user_assignment']}")

        with col3:
            st.write(f"**Fallback to Roles:** {config['fallback_to_roles']}")

            # Show fallback roles if applicable
            if config['fallback_to_roles']:
                fallback_roles = get_role_based_approvers(engine, selected_object_type, selected_action)
                st.caption(f"Fallback Roles: {', '.join(fallback_roles)}")

    st.markdown(f"### Assign Approvers for: `{selected_object_type}.{selected_action}`")

    # Get currently assigned users for this action
    assigned_emails = get_assigned_approvers(
        engine, selected_object_type, selected_action
    )

    # Display users with checkboxes
    if not users:
        st.warning("No active users found in the system.")
        return

    st.markdown("**Select users who can approve this action:**")

    changes_made = False

    for user in users:
        email = user['email'].lower()
        is_currently_assigned = email in assigned_emails

        col1, col2, col3 = st.columns([3, 2, 1])

        with col1:
            # Checkbox for assignment
            # --- Changed label to be "Name (Email)" ---
            is_checked = st.checkbox(
                f"{user['full_name'] or 'N/A'} ({user['email']})",
                value=is_currently_assigned,
                key=f"assign_{selected_object_type}_{selected_action}_{email}"
            )

        with col2:
            # Show user's roles
            roles_text = user.get('roles', '') or 'No roles'
            st.caption(f"Roles: {roles_text}")

        with col3:
            # Show statistics
            if is_currently_assigned:
                stats = get_approver_stats(engine, email)
                st.caption(f"📋 {stats['pending_count']} pending")

        # Handle state change
        if is_checked != is_currently_assigned:
            changes_made = True

            if is_checked:
                # Assign approver
                try:
                    assign_approver(
                        engine,
                        selected_object_type,
                        selected_action,
                        email,
                        admin_email,
                        notes=f"Assigned via Approval Management page"
                    )
                    st.success(f"✅ Assigned {user['full_name'] or email}")
                except Exception as e:
                    st.error(f"Error assigning {email}: {e}")
            else:
                # Revoke approver
                try:
                    # Find assignment ID
                    key = (selected_object_type, selected_action, email)
                    if key in assignment_lookup:
                        revoke_approver(
                            engine,
                            assignment_lookup[key]['id'],
                            admin_email
                        )
                        st.success(f"❌ Revoked {user['full_name'] or email}")
                except Exception as e:
                    st.error(f"Error revoking {email}: {e}")

    if changes_made:
        st.rerun()


def _render_current_assignments(engine):
    """Show all current assignments in a table."""
    st.subheader("📋 Current Approver Assignments")

    assignments = list_all_approver_assignments(engine, active_only=True)

    if not assignments:
        st.info("No active approver assignments found.")
        return

    df = pd.DataFrame(assignments)

    # Format for display
    display_df = df[[
        'object_type', 'action', 'approver_email', 'approver_name',
        'assigned_by', 'assigned_at'
    ]].copy()

    display_df.columns = [
        'Object Type', 'Action', 'Approver Email', 'Approver Name',
        'Assigned By', 'Assigned At'
    ]

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Assigned At": st.column_config.DatetimeColumn(
                "Assigned At",
                format="YYYY-MM-DD hh:mm A"
            )
        }
    )

    # Download option
    if st.button("📥 Download Assignments as CSV"):
        csv = display_df.to_csv(index=False)
        st.download_button(
            "Download CSV",
            csv,
            "approver_assignments.csv",
            "text/csv",
            key="download_csv"
        )


def _render_approver_stats(engine):
    """Show statistics for each approver."""
    st.subheader("📈 Approver Statistics")

    with engine.begin() as conn:
        rows = conn.execute(sa_text("""
            SELECT 
                aa.approver_email,
                aa.approver_name,
                COUNT(DISTINCT aa.object_type || '.' || aa.action) as action_types,
                COUNT(DISTINCT a.id) as pending_count
            FROM approver_assignments aa
            LEFT JOIN approvals a 
                ON a.object_type = aa.object_type 
                AND a.action = aa.action
                AND a.status IN ('pending', 'under_review')
            WHERE aa.is_active = 1
            GROUP BY aa.approver_email, aa.approver_name
            ORDER BY action_types DESC, pending_count DESC
        """)).fetchall()

    if not rows:
        st.info("No approver statistics available.")
        return

    data = []
    for row in rows:
        email = row[0]
        stats = get_approver_stats(engine, email)

        data.append({
            'Approver': row[1] or row[0],
            'Email': row[0],
            'Action Types': row[2],
            'Pending': row[3],
            'Approved': stats['approved_count'],
            'Rejected': stats['rejected_count'],
            'Total Decisions': stats['approved_count'] + stats['rejected_count']
        })

    df = pd.DataFrame(data)
    st.dataframe(df, use_container_width=True, hide_index=True)


def _render_page_access_management(engine, admin_email):
    """Render the UI for managing View/Edit page access."""
    # --- MODIFIED: Removed the subheader from this function ---
    st.caption("Control which roles can view and edit each page.")

    try:
        # Get all unique pages and roles from the DB
        with engine.begin() as conn:
            pages = [r[0] for r in conn.execute(sa_text(
                "SELECT DISTINCT page_name FROM page_access_rules ORDER BY page_name"
            )).fetchall()]

            roles = [r[0] for r in conn.execute(sa_text(
                "SELECT name FROM roles ORDER BY name"
            )).fetchall()]

            rules = conn.execute(sa_text(
                "SELECT page_name, permission_type, role_name FROM page_access_rules"
            )).fetchall()

        if not pages or not roles:
            st.error("Page access rules or roles table is empty. Run schema migration.")
            return

        # Create a lookup for existing rules
        rules_lookup = {(r[0], r[1], r[2]) for r in rules}

        # Create the data for the permissions matrix
        data = []
        for page in pages:
            page_data = {"Page": page}
            for role in roles:
                key = f"{role}"
                # Check view
                page_data[f"{key} (View)"] = (page, 'view', role) in rules_lookup
                # Check edit
                page_data[f"{key} (Edit)"] = (page, 'edit', role) in rules_lookup
            data.append(page_data)

        df = pd.DataFrame(data).set_index("Page")

        st.info("Changes are saved instantly when you check/uncheck a box.")

        # Use st.data_editor to make it interactive
        edited_df = st.data_editor(
            df,
            use_container_width=True,
            disabled=[col for col in df.columns if '(View)' not in col and '(Edit)' not in col]
        )

        # Process any changes
        # This is complex, so we check for diffs
        for page in edited_df.index:
            for col in edited_df.columns:
                role, perm_type = col.replace(")", "").split(" (")

                new_value = edited_df.loc[page, col]
                old_value = df.loc[page, col]

                if new_value != old_value:
                    try:
                        with engine.begin() as conn:
                            if new_value:
                                # Add the permission
                                conn.execute(sa_text("""
                                    INSERT OR IGNORE INTO page_access_rules
                                        (page_name, permission_type, role_name, created_by)
                                    VALUES (:page, :perm, :role, :admin)
                                """), {
                                    "page": page,
                                    "perm": perm_type.lower(),
                                    "role": role,
                                    "admin": admin_email
                                })
                                st.toast(f"Granted {perm_type} for {role} on {page}")
                            else:
                                # Remove the permission
                                conn.execute(sa_text("""
                                    DELETE FROM page_access_rules
                                    WHERE page_name = :page 
                                      AND permission_type = :perm 
                                      AND role_name = :role
                                """), {
                                    "page": page,
                                    "perm": perm_type.lower(),
                                    "role": role
                                })
                                st.toast(f"Revoked {perm_type} for {role} on {page}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error updating {page}: {e}")

    except Exception as e:
        st.error("Could not load page access rules. Have you run the schema migration?")
        st.exception(e)


def _render_bulk_operations(engine, admin_email):
    """Bulk operations for managing approvers."""
    st.subheader("⚡ Bulk Operations")

    st.warning("⚠️ Use bulk operations carefully - they affect multiple assignments at once.")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Assign User to Multiple Actions")

        users = _get_all_users(engine)
        user_options = {
            f"{u['full_name'] or u['email']} ({u['email']})": u['email']
            for u in users
        }

        selected_user = st.selectbox("Select User", list(user_options.keys()), key="bulk_user")

        action_types = _get_all_action_types(engine)
        action_options = [f"{ot}.{act}" for ot, act in action_types]

        selected_actions = st.multiselect(
            "Select Actions",
            action_options,
            key="bulk_actions"
        )

        if st.button("Assign to All Selected Actions", key="bulk_assign"):
            if selected_user and selected_actions:
                user_email = user_options[selected_user]
                success_count = 0

                for action_str in selected_actions:
                    obj_type, action = action_str.split(".", 1)
                    try:
                        assign_approver(
                            engine, obj_type, action, user_email, admin_email,
                            notes="Bulk assigned"
                        )
                        success_count += 1
                    except Exception as e:
                        st.error(f"Error assigning {action_str}: {e}")

                st.success(f"✅ Assigned {user_email} to {success_count}/{len(selected_actions)} actions")
                st.rerun()

    with col2:
        st.markdown("### Revoke All Assignments for User")

        users = _get_all_users(engine)
        user_options = {
            f"{u['full_name'] or u['email']} ({u['email']})": u['email']
            for u in users
        }

        selected_user_revoke = st.selectbox(
            "Select User",
            list(user_options.keys()),
            key="bulk_revoke_user"
        )

        if selected_user_revoke:
            user_email = user_options[selected_user_revoke]

            # Show current assignments
            assignments = [
                a for a in list_all_approver_assignments(engine)
                if a['approver_email'].lower() == user_email.lower()
                and a['is_active']
            ]

            if assignments:
                st.write(f"Current assignments: **{len(assignments)}**")

                if st.button("⚠️ Revoke All Assignments", key="bulk_revoke", type="secondary"):
                    success_count = 0
                    for assign in assignments:
                        try:
                            revoke_approver(engine, assign['id'], admin_email)
                            success_count += 1
                        except Exception as e:
                            st.error(f"Error revoking assignment {assign['id']}: {e}")

                    st.success(f"✅ Revoked {success_count}/{len(assignments)} assignments")
                    st.rerun()
            else:
                st.info("No active assignments for this user")


@require_page(PAGE_KEY)
def render():
    """Main render function for approval management page."""
    # --- MODIFIED: Changed title to be more general ---
    st.title("⚙️ Site Administration")
    st.caption("Manage approval workflows and page access (Superadmin Only)")

    # Setup
    settings = load_settings()
    engine = get_engine(settings.db.url)
    init_db(engine)

    # Get admin info
    user = st.session_state.get("user", {})
    admin_email = user.get("email", "")
    roles = user_roles(engine, admin_email)

    # Check permissions
    if "superadmin" not in roles:
        st.error("🔒 Access Denied")
        st.warning("This page is only accessible to superadmins.")
        return

    # --- NEW STRUCTURE: Section 1 (Approvals) ---
    with st.container(border=True):
        st.subheader("Approval Workflow Management")
        st.write("Configure users who can approve specific actions, view statistics, and perform bulk assignments.")
        
        # Render overview metrics *inside* this section
        _render_overview(engine)
        
        st.markdown("---")

        # Create tabs *inside* this section
        tab1, tab2, tab3, tab4 = st.tabs([
            "👥 Assign Approvers",
            "📋 View All Assignments",
            "📈 Approver Statistics",
            "⚡ Bulk Operations"
        ])
        
        with tab1:
            _render_assignment_matrix(engine, admin_email)

        with tab2:
            _render_current_assignments(engine)

        with tab3:
            _render_approver_stats(engine)

        with tab4:
            _render_bulk_operations(engine, admin_email)

    # Add a visual separator
    st.markdown("<br>", unsafe_allow_html=True) 

    # --- NEW STRUCTURE: Section 2 (Page Access) ---
    with st.container(border=True):
        st.subheader("🔒 Page Role Access Management")
        st.write("Control which user roles can view or edit each page across the entire application.")
        
        # Call the render function directly
        # (We removed the subheader from the function itself)
        _render_page_access_management(engine, admin_email)


if __name__ == "__main__":
    render()
