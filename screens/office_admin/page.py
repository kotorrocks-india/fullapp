# screens/office_admin/page.py
"""
Office Administration - Account Management
Entry point for managing office admin accounts who can manage students and run reports.
"""
from __future__ import annotations
from screens.office_admin.ui import render_office_admin

# Streamlit navigation will call this file directly
render_office_admin()
