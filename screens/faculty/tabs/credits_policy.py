# app/screens/faculty/tabs/credits_policy.py
from __future__ import annotations
from typing import Set

import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

from screens.faculty.utils import _handle_error
from screens.faculty.db import _designation_catalog, _designation_enabled

def render(engine: Engine, degree: str, roles: Set[str], can_edit: bool, key_prefix: str):
    st.subheader("Credits Policy (per degree)")
    
    # FIXED: Just verify table exists, don't try to create it here
    # The table should be created by schema.py during app initialization
    try:
        with engine.begin() as conn:
            # Check if table exists
            table_check = conn.execute(sa_text("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='faculty_credits_policy'
            """)).fetchone()
            
            if not table_check:
                st.error("⚠️ Faculty credits policy table doesn't exist!")
                st.info("💡 **This is a schema issue.** The table should be created during app initialization.")
                st.info("🔧 **To fix:** Restart the application. The schema installer should create this table automatically.")
                
                with st.expander("🔍 Technical Details"):
                    st.write("The `faculty_credits_policy` table should be created by:")
                    st.code("screens.faculty.schema.install_credits_policy(engine)")
                    st.write("This is called during app startup in the Faculty page render function.")
                
                return
    except Exception as e:
        st.error(f"Failed to check table existence: {e}")
        import traceback
        st.code(traceback.format_exc())
        return

    # --- FIXED: Load designations for THIS tab ---
    # We need regular enabled designations + admin designations for this degree
    catalog, enabled = [], []
    try:
        with engine.begin() as conn:
            # 1. Get regular enabled designations (from catalog, which excludes admins)
            catalog = _designation_catalog(conn)
            enabled_regular = [d for d in catalog if _designation_enabled(conn, degree, d)]

            # 2. Get active admin designations for THIS degree (Principal/Director)
            admin_desgs_rows = conn.execute(sa_text("""
                SELECT DISTINCT fa.designation
                FROM faculty_affiliations fa
                JOIN users u ON lower(u.email) = lower(fa.email)
                JOIN academic_admins aa ON u.id = aa.user_id
                WHERE lower(fa.degree_code) = lower(:degree)
                AND fa.active = 1
                AND aa.fixed_role IN ('principal', 'director')
            """), {"degree": degree}).fetchall()
            
            admin_desgs = [r[0] for r in admin_desgs_rows if r[0]]

            # 3. Combine the lists
            enabled = sorted(list(set(enabled_regular + admin_desgs)))

    except Exception as e:
        _handle_error(e, "Could not load designation catalog.")
        st.error(f"Detailed error: {e}")
        import traceback
        st.code(traceback.format_exc())
        catalog, enabled = [], [] # Fallback to empty lists

    if not enabled:
        st.warning(f"⚠️ No designations are enabled or active for degree '{degree}'")
        st.info("💡 **Next Steps:**\n"
               "1. Go to the **'Designation Catalog'** tab and enable designations.\n"
               "2. Ensure **Principal/Director** roles are set in **User Roles** (they sync automatically).")
        
        with st.expander("🔍 Available Regular Designations in System"):
            if catalog:
                st.write(f"Total regular designations in catalog: {len(catalog)}")
                st.write("Designations:", catalog)
            else:
                st.write("No regular designations found in the system.")
        return

    # Show current policies in a table with edit capability
    st.markdown("### Current Credit Policies")
    
    try:
        with engine.begin() as conn:
            rows = conn.execute(sa_text("""
                SELECT designation, required_credits, allowed_credit_override
                FROM faculty_credits_policy
                WHERE degree_code=:d
                ORDER BY designation
            """), {"d": degree}).fetchall()
        
        if rows:
            # Create editable dataframe
            df = pd.DataFrame(rows, columns=["Designation", "Required Credits", "Allowed Override"])
            
            # Show as editable data editor if user can edit
            if can_edit:
                st.info("💡 Click any cell below to edit values directly, then click 'Save Changes'")
                edited_df = st.data_editor(
                    df,
                    use_container_width=True,
                    hide_index=True,
                    key=f"{key_prefix}_policy_editor",
                    disabled=["Designation"],  # Don't allow changing the designation
                )
                
                # Save button for edited data
                if st.button("💾 Save Changes", key=f"{key_prefix}_save_edits"):
                    try:
                        with engine.begin() as conn:
                            for _, row in edited_df.iterrows():
                                conn.execute(sa_text("""
                                    UPDATE faculty_credits_policy 
                                    SET required_credits=:r, allowed_credit_override=:o
                                    WHERE degree_code=:d AND designation=:g
                                """), {
                                    "d": degree,
                                    "g": row["Designation"],
                                    "r": int(row["Required Credits"]),
                                    "o": int(row["Allowed Override"])
                                })
                        st.success("✅ Changes saved!")
                        st.rerun()
                    except Exception as e:
                        _handle_error(e, "Failed to save changes")
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.caption("View-only (no edit permission)")
        else:
            st.info(f"No credit policies defined yet for degree '{degree}'")
    except Exception as e:
        _handle_error(e, "Could not load current policies.")
        st.error(f"**Detailed error:** {e}")
        import traceback
        st.code(traceback.format_exc())

    # Teaching Load Summary Section
    st.divider()
    st.markdown("### 📊 Faculty Teaching Load Summary (with Administrative Relief)")
    st.caption("Shows base required credits, administrative credit relief, and effective required credits")
    
    try:
        with engine.begin() as conn:
            # Import the helper function
            from screens.faculty.db import _calculate_effective_teaching_load, _people_for_degree_including_positions
            
            # Get all people with affiliations OR positions relevant to this degree
            all_people = _people_for_degree_including_positions(conn, degree)
            
            # Get all credit policies for this degree into a lookup map
            policy_rows = conn.execute(sa_text("""
                SELECT lower(designation), required_credits
                FROM faculty_credits_policy
                WHERE lower(degree_code) = lower(:d)
            """), {"d": degree}).fetchall()
            policy_map = {desg: credits for desg, credits in policy_rows}

            # Get a map of administrative positions for all relevant faculty
            position_rows = conn.execute(sa_text("""
                SELECT lower(pa.assignee_email) as email, ap.position_title
                FROM position_assignments pa
                JOIN administrative_positions ap ON ap.position_code = pa.position_code
                WHERE pa.is_active = 1
                  AND (pa.start_date IS NULL OR DATE(pa.start_date) <= DATE('now'))
                  AND (pa.end_date   IS NULL OR DATE(pa.end_date)   >= DATE('now'))
                  AND (
                    ap.scope = 'institution' OR lower(pa.degree_code) = lower(:degree)
                  )
            """), {"degree": degree}).fetchall()
            
            position_map = {}
            for email, title in position_rows:
                if email not in position_map:
                    position_map[email] = []
                position_map[email].append(title)

            if all_people:
                load_data = []
                faculty_with_relief = 0

                for person in all_people:
                    name = person['name']
                    email = person['email']
                    
                    # --- MODIFIED: Separate Designation and Position ---
                    # Get teaching designation
                    designation_str = person.get('designation') or "N/A"
                    
                    # Get administrative positions
                    positions_list = position_map.get(email.lower(), [])
                    position_str = ", ".join(sorted(list(set(positions_list)))) or "N/A"
                    
                    # Base credits are based *only* on the teaching designation
                    base_credits = int(policy_map.get(designation_str.lower(), 0))
                    # --- END MODIFIED ---

                    load_info = _calculate_effective_teaching_load(
                        conn, email, degree, base_credits
                    )
                    
                    # --- MODIFIED: Add separate columns to data ---
                    load_data.append({
                        "Faculty": name,
                        "Teaching Designation": designation_str,
                        "Administrative Position": position_str,
                        "Base Required": load_info['base_required'],
                        "Admin Relief": load_info['admin_relief'],
                        "Effective Required": load_info['effective_required'],
                    })
                    # --- END MODIFIED ---
                    
                    if load_info['admin_relief'] > 0:
                        faculty_with_relief += 1
                
                if load_data:
                    # Create DataFrame
                    load_df = pd.DataFrame(load_data)
                    
                    # Display the table
                    # --- MODIFIED: Update column_config ---
                    st.dataframe(
                        load_df,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Faculty": st.column_config.TextColumn("Faculty Name", width="medium"),
                            "Teaching Designation": st.column_config.TextColumn("Teaching Designation", width="medium"),
                            "Administrative Position": st.column_config.TextColumn("Administrative Position", width="medium"),
                            "Base Required": st.column_config.NumberColumn(
                                "Base Required",
                                help="Base teaching credits required for this designation",
                                format="%d credits"
                            ),
                            "Admin Relief": st.column_config.NumberColumn(
                                "Admin Relief",
                                help="Credit reduction due to administrative positions",
                                format="%d credits"
                            ),
                            "Effective Required": st.column_config.NumberColumn(
                                "Effective Required", 
                                help="Actual teaching credits required after admin relief",
                                format="%d credits"
                            )
                        }
                    )
                    # --- END MODIFIED ---
                    
                    # Summary statistics
                    total_base = sum(d['Base Required'] for d in load_data)
                    total_relief = sum(d['Admin Relief'] for d in load_data)
                    total_effective = sum(d['Effective Required'] for d in load_data)
                    
                    st.markdown("#### Summary Statistics")
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Total Faculty", len(load_data))
                    with col2:
                        st.metric("Total Base Load", f"{total_base} credits")
                    with col3:
                        st.metric(
                            "Total Admin Relief", 
                            f"{total_relief} credits",
                            delta=f"-{total_relief}" if total_relief > 0 else "0",
                            delta_color="inverse"
                        )
                    with col4:
                        st.metric("Total Effective Load", f"{total_effective} credits")
                    
                    # Show faculty with admin relief
                    if faculty_with_relief > 0:
                        st.info(
                            f"ℹ️ **{faculty_with_relief}** faculty member(s) have administrative "
                            f"positions that reduce their teaching load by a total of **{total_relief}** credits."
                        )
                        
                        with st.expander("💡 Understanding Administrative Credit Relief"):
                            st.markdown("""
                            **Administrative credit relief** reduces the teaching load for faculty 
                            members who hold administrative positions such as:
                            
                            - **Principal/Director** (6 credits) - Institution-wide leadership
                            - **Dean** (4 credits) - Degree program administration  
                            - **Head of Department** (3 credits) - Branch/department leadership
                            - **Program Coordinator** (2 credits) - Program coordination duties
                            
                            **Formula:**
                            ```
                            Effective Required = Base Required - Admin Relief
                            ```
                            
                            **Example:** If a Professor (Base: 12 credits) is also a Dean (Relief: 4 credits),
                            their effective teaching requirement becomes 8 credits.
                            """)
                else:
                    st.info("No faculty with credit policies found for this degree")
            else:
                st.info("No faculty with affiliations or positions found for this degree")
    except Exception as e:
        st.warning(f"Could not load teaching load summary: {e}")

    if not can_edit:
        return
    
    # Add new policy section
    st.divider()
    st.markdown("### Add New Policy")
    
    # Get designations that don't have policies yet
    try:
        with engine.begin() as conn:
            existing_designations = conn.execute(sa_text("""
                SELECT designation FROM faculty_credits_policy
                WHERE degree_code=:d
            """), {"d": degree}).fetchall()
            existing = {d[0].lower() for d in existing_designations}
            available = [d for d in enabled if d.lower() not in existing] # Use the 'enabled' list from this tab
    except:
        available = enabled

    if not available:
        st.info("✅ All enabled and active designations already have credit policies defined.")
        return

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        desg = st.selectbox("Designation", options=available, key=f"{key_prefix}_desg")
    with c2:
        req = st.number_input("Required credits", 0, 10000, 0, key=f"{key_prefix}_req")
    with c3:
        ovr = st.number_input("Allowed credit override", 0, 10000, 0, key=f"{key_prefix}_ovr")

    if st.button("➕ Add Policy", key=f"{key_prefix}_save"):
        if not desg:
            st.error("Designation is required.")
            return
        try:
            with engine.begin() as conn:
                conn.execute(sa_text("""
                    INSERT INTO faculty_credits_policy(degree_code, designation, required_credits, allowed_credit_override)
                    VALUES(:d, :g, :r, :o)
                    ON CONFLICT(degree_code, designation) DO UPDATE SET
                      required_credits=excluded.required_credits,
                      allowed_credit_override=excluded.allowed_override
                """), {"d": degree, "g": desg, "r": int(req), "o": int(ovr)})
            st.success("✅ Policy added!")
            st.rerun()
        except Exception as e:
            _handle_error(e, "Failed to add policy.")
