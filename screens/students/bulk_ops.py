# app/screens/students/bulk_ops.py
# FIXED: Properly organized tabs without duplication

import streamlit as st
from sqlalchemy.engine import Engine
from sqlalchemy import text as sa_text

from screens.students.importer import (
    _add_student_import_export_section,
    _add_student_mover_section,
    _add_student_credential_export_section
)


def render(engine: Engine):
    """
    Renders three-tab UI for student bulk operations.
    """
    
    # Check if degrees exist
    with engine.begin() as conn:
        degree_check = conn.execute(sa_text(
            "SELECT COUNT(*) FROM degrees WHERE active = 1"
        )).scalar()
        has_degrees = degree_check and degree_check > 0

    if not has_degrees:
        st.warning("⚠️ No degrees found. Set up degrees first.")
        st.info("""
### 🚀 Getting Started

1. Create Degrees (with duration)
2. Import Students
3. Manage Students

Go to Degrees page to get started.
        """)
        return

    # Create tabs
    st.markdown("## 🔥 Student Bulk Operations")
    
    tab1, tab2, tab3 = st.tabs([
        "📤 Import Students",
        "🚚 Student Mover", 
        "🔑 Export Credentials"
    ])

    # Tab 1: Import
    with tab1:
        _add_student_import_export_section(engine)

    # Tab 2: Mover
    with tab2:
        _add_student_mover_section(engine)

    # Tab 3: Credentials
    with tab3:
        _add_student_credential_export_section(engine)
