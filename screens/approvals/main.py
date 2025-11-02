# screens/approvals/main.py
import streamlit as st
import sys
import os
from pathlib import Path

# Add the project root directory to Python path so absolute imports work
project_root = Path(__file__).parent.parent.parent #
if str(project_root) not in sys.path: #
    sys.path.insert(0, str(project_root)) #

# Core plumbing
try:
    from core.settings import load_settings #
    from core.db import get_engine, init_db #
    from core.policy import require_page #
    from core.theme_apply import apply_theme_for_degree #
    from core.rbac import user_roles #
    CORE_IMPORTS_OK = True #
except ImportError as e: #
    CORE_IMPORTS_OK = False #
    CORE_IMPORT_ERROR = str(e) #

# Local modules - use absolute imports with path adjustment
try:
    # --- MODIFIED IMPORT ---
    from screens.approvals.data_loader import _fetch_open_approvals, _fetch_completed_approvals #
    DATA_LOADER_OK = True #
except ImportError as e: #
    DATA_LOADER_OK = False #
    DATA_LOADER_ERROR = str(e) #

try:
    from screens.approvals.policy_helpers import _allowed_to_act, _record_vote_and_finalize #
    POLICY_HELPERS_OK = True #
except ImportError as e: #
    POLICY_HELPERS_OK = False #
    POLICY_HELPERS_ERROR = str(e) #

try:
    from screens.approvals.action_handlers import perform_action #
    ACTION_HANDLERS_OK = True #
except ImportError as e: #
    ACTION_HANDLERS_OK = False #
    ACTION_HANDLERS_ERROR = str(e) #

try:
    from screens.approvals.ui_components import render_approval_details, render_approval_actions #
    UI_COMPONENTS_OK = True #
except ImportError as e: #
    UI_COMPONENTS_OK = False #
    UI_COMPONENTS_ERROR = str(e) #

@require_page("Approvals") #
def render():
    # This debug code can now be removed
    st.sidebar.subheader("🕵️‍♂️ Session State Debug") #
    if "user" in st.session_state: #
        st.sidebar.write("User object found in session:") #
        st.sidebar.json(st.session_state.user) #
    else: #
        st.sidebar.warning("User object NOT found in session.") #

    # Show debug info in sidebar
    with st.sidebar: #
        st.write("---") #
        st.write("🔍 DEBUG: Approvals Imports") #
        st.write(f"Core: {'✅' if CORE_IMPORTS_OK else '❌'}") #
        if not CORE_IMPORTS_OK: #
            st.write(f"Error: {CORE_IMPORT_ERROR}") #
        
        st.write(f"Data Loader: {'✅' if DATA_LOADER_OK else '❌'}") #
        if not DATA_LOADER_OK: #
            st.write(f"Error: {DATA_LOADER_ERROR}") #
            
        st.write(f"Policy Helpers: {'✅' if POLICY_HELPERS_OK else '❌'}") #
        if not POLICY_HELPERS_OK: #
            st.write(f"Error: {POLICY_HELPERS_ERROR}") #
            
        st.write(f"Action Handlers: {'✅' if ACTION_HANDLERS_OK else '❌'}") #
        if not ACTION_HANDLERS_OK: #
            st.write(f"Error: {ACTION_HANDLERS_ERROR}") #
            
        st.write(f"UI Components: {'✅' if UI_COMPONENTS_OK else '❌'}") #
        if not UI_COMPONENTS_OK: #
            st.write(f"Error: {UI_COMPONENTS_ERROR}") #

    st.title("📬 Approvals Inbox") #
    
    # Check if all imports are successful
    if not all([CORE_IMPORTS_OK, DATA_LOADER_OK, POLICY_HELPERS_OK, ACTION_HANDLERS_OK, UI_COMPONENTS_OK]): #
        st.error("Some imports failed. Check the sidebar for details.") #
        return #

    # If all imports are successful, proceed with normal execution
    try: #
        settings = load_settings() #
        engine = get_engine(settings.db.url) #
        init_db(engine) #
        st.session_state["engine"] = engine #

        # Who's logged in
        user = st.session_state.get("user") or {} #
        email = (user.get("email") or "").strip().lower() #
        roles = user_roles(engine, email) #

        # Current "active" degree
        active_degree = st.session_state.get("active_degree") #

        # Theme
        theme_cfg = apply_theme_for_degree(engine, active_degree, email) #

        # --- EXISTING INBOX ---
        df = _fetch_open_approvals(engine) #
        st.caption(f"Showing {len(df)} pending/under_review items.") #
        st.dataframe(df, use_container_width=True, hide_index=True) #

        if df.empty: #
            st.info("No pending approvals found.") #
            # We don't return here anymore, so the history section can show
        else: #
            st.subheader("Review an approval") #

            ids = df["id"].tolist() #
            sel = st.selectbox("Select approval ID", options=ids, key="ap_sel_id") #
            row = df[df["id"] == sel].iloc[0].to_dict() #

            # Render approval details
            render_approval_details(row, engine) #

            # Per-item policy (who can act)
            eligible, approver_set, policy_rule = _allowed_to_act(set(roles), row) #
            if not eligible: #
                st.error(f"You are not an approver for this item. Allowed roles: {', '.join(sorted(approver_set))}") #
                # We don't return here either
            else: #
                st.caption(f"Policy: approver roles = {', '.join(sorted(approver_set))}; rule = {policy_rule}") #

                decision_note = st.text_area("Decision note (optional)", placeholder="Reason for approval/rejection…", key="ap_dec_note") #

                # Render actions and handle responses
                action = render_approval_actions(sel, row, email, decision_note, engine) #
                
                if action == "approve": #
                    try: #
                        with engine.begin() as conn: #
                            perform_action(conn, row)   # apply change
                        _record_vote_and_finalize(engine, int(sel), "approved", email, decision_note or "") #
                        st.success(f"Approved #{sel} and applied the change.") #
                        st.rerun() #
                    except Exception as ex: #
                        st.error(str(ex)) #
                
                elif action == "reject": #
                    try: #
                        _record_vote_and_finalize(engine, int(sel), "rejected", email, decision_note or "") #
                        st.success(f"Rejected #{sel}.") #
                        st.rerun() #
                    except Exception as ex: #
                        st.error(str(ex)) #

        st.markdown("---") #
        
        # --- NEW APPROVAL HISTORY (AUDIT LOG) ---
        st.title("🏛️ Approval History")
        
        # Use the new function to get completed items
        df_completed = _fetch_completed_approvals(engine)
        
        if df_completed.empty:
            st.info("No completed approval history found.")
        else:
            # Get all unique object types from the history
            object_types = sorted(df_completed["object_type"].unique())
            
            # Create a tab for each object type
            tabs = st.tabs([f"{t.capitalize()} History" for t in object_types])
            
            for i, tab in enumerate(tabs):
                with tab:
                    obj_type = object_types[i]
                    
                    df_filtered = df_completed[df_completed["object_type"] == obj_type]
                    
                    # Define columns to show in the audit log
                    audit_cols = ["id", "object_id", "action", "status", "requester"]
                    if "approver" in df_filtered.columns:
                        audit_cols.append("approver")
                    if "decided_at" in df_filtered.columns:
                        audit_cols.append("decided_at")
                    
                    st.dataframe(
                        df_filtered[audit_cols],
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "decided_at": st.column_config.DatetimeColumn("Decided At", format="YYYY-MM-DD hh:mm A"),
                            "status": st.column_config.SelectboxColumn("Status", options=["approved", "rejected"])
                        }
                    )

        st.markdown("---")
        # --- END OF NEW SECTION ---
        
    except Exception as e: #
        st.error(f"Error in approvals system: {e}") #

# This allows the page to be run standalone
# (e.g., `streamlit run screens/approvals/main.py`)
# but also safely imported by a main app router.
if __name__ == "__main__":
    render()
