# screens/office_admin/__init__.py
"""
Office Admin module for managing office administrator accounts.
Office admins can manage students, run reports, but cannot configure system settings.
"""
from screens.office_admin.ui import render_office_admin

__all__ = ["render_office_admin"]
