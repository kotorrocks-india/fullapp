# app/screens/faculty/tabs/admin_credits_policy.py
"""
Admin Credits Policy Tab

Manages credit requirements for institution-level academic administrators
(Principal and Director) who are not tied to specific degree programs.
"""
from __future__ import annotations
from typing import Set

import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine


def render(
    engine: Engine,
    degree: str,
    roles: Set[str],
    can_edit: bool,
    key_prefix: str
):
    """
    Render the Academic Admin Credits Policy tab.
    
    Args:
        engine: Database engine
        degree: Current degree context (not used for admin policy)
        roles: User's roles
        can_edit: Whether user can edit policies
        key_prefix: Unique key prefix for Streamlit widgets
    """
    st.subheader("Academic Admin Credits Policy")
    
    st.info("""
    📚 **Institution-Level Credits Policy**
    
    Set credit requirements for Principal and Director roles.
    These are institution-wide and not tied to specific degrees.
    """)
    
    # Load current policies
    try:
        with engine.begin() as conn:
            rows = conn.execute(sa_text("""
                SELECT fixed_role, required_credits, allowed_credit_override
                FROM academic_admin_credits_policy
                ORDER BY fixed_role
            """)).fetchall()
        
        if rows:
            # Convert to DataFrame with proper column names
            df = pd.DataFrame(rows, columns=["Role", "Required Credits", "Allowed Override"])
            
            # Capitalize role names for display
            df["Role"] = df["Role"].str.title()
            
            if can_edit:
                st.info("💡 Click any cell to edit values, then click 'Save Changes'")
                edited_df = st.data_editor(
                    df,
                    use_container_width=True,
                    hide_index=True,
                    key=f"{key_prefix}_admin_policy_editor",
                    disabled=["Role"],  # Can't change the role names
                    column_config={
                        "Role": st.column_config.TextColumn(
                            "Admin Role",
                            help="Institution-level administrator role",
                            width="medium"
                        ),
                        "Required Credits": st.column_config.NumberColumn(
                            "Required Credits",
                            help="Minimum teaching credits required per semester",
                            min_value=0,
                            max_value=40,
                            step=1,
                            width="medium"
                        ),
                        "Allowed Override": st.column_config.NumberColumn(
                            "Allowed Override",
                            help="Maximum additional credits allowed beyond required",
                            min_value=0,
                            max_value=20,
                            step=1,
                            width="medium"
                        )
                    }
                )
                
                col1, col2 = st.columns([1, 5])
                with col1:
                    if st.button("💾 Save Changes", key=f"{key_prefix}_save_admin_policy", type="primary"):
                        try:
                            with engine.begin() as conn:
                                for _, row in edited_df.iterrows():
                                    # Convert role back to lowercase for database
                                    role = row["Role"].lower()
                                    conn.execute(sa_text("""
                                        UPDATE academic_admin_credits_policy 
                                        SET required_credits=:r, 
                                            allowed_credit_override=:o,
                                            updated_at=CURRENT_TIMESTAMP
                                        WHERE fixed_role=:role
                                    """), {
                                        "role": role,
                                        "r": int(row["Required Credits"]),
                                        "o": int(row["Allowed Override"])
                                    })
                            st.success("✅ Admin credits policy updated!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed to save: {e}")
            else:
                # Read-only view
                st.dataframe(
                    df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Role": "Admin Role",
                        "Required Credits": "Required Credits",
                        "Allowed Override": "Allowed Override"
                    }
                )
                st.caption("🔒 View-only (no edit permission)")
        else:
            st.warning("⚠️ No admin credit policies defined")
    
    except Exception as e:
        st.error(f"Could not load policies: {e}")
    
    # Explanation section
    with st.expander("ℹ️ About Admin Credits Policy"):
        st.markdown("""
        ### How This Works:
        
        **Principal and Director** are institution-level roles that:
        - Oversee all degree programs
        - May have teaching responsibilities  
        - Don't belong to specific degrees/branches
        
        **This policy sets:**
        - Minimum required teaching credits per semester
        - Flexibility for overload assignments
        
        **Note:** Other academic admins (those with custom designations) follow 
        regular faculty credits policies based on their designation.
        
        ---
        
        ### Credit Policy Lookup:
        
        **Regular Faculty:**
        ```
        Designation: Professor
        Degree: BSC
        → Uses: faculty_credits_policy table
        ```
        
        **Immutable Admin (Principal/Director):**
        ```
        Fixed Role: principal
        → Uses: academic_admin_credits_policy table
        ```
        
        **Custom Designation Admin:**
        ```
        Fixed Role: NULL
        Designation: Dean
        Degree: BSC
        → Uses: faculty_credits_policy table
        ```
        """)
