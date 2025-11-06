# core/approval_handler_enhanced.py
"""
Enhanced Approval Handler

Updated to support dynamic user-based approver assignments in addition
to role-based approvals.

Usage remains the same, but now checks:
1. Specific user assignments first (if configured)
2. Role-based rules as fallback (if configured)
"""

from __future__ import annotations
import json
import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

# Import enhanced policy functions
try:
    from core.approvals_policy import (
        approver_roles as get_approver_roles,
        rule as get_approval_rule,
        requires_reason,
        can_user_approve,
    )
except ImportError:
    # Fallback to original policy if enhanced not available
    from core.policy import (
        approver_roles_for as get_approver_roles,
        rule_for as get_approval_rule,
        requires_reason,
    )
    
    def can_user_approve(engine, user_email, user_roles, object_type, action, **kwargs):
        """Fallback implementation."""
        approvers = get_approver_roles(object_type, action, engine=engine, **kwargs)
        # Check if user email matches (user-based)
        if f"user:{user_email.lower()}" in approvers:
            return True
        # Check if user role matches (role-based)
        return bool(user_roles & approvers)


class ApprovalHandler:
    """
    Enhanced approval handler with support for dynamic user assignment.
    
    Changes from original:
    - Uses enhanced policy system
    - Checks user-specific assignments
    - Falls back to role-based when configured
    """
    
    def __init__(
        self,
        engine: Engine,
        object_type: str,
        degree_code: Optional[str] = None,
        program_code: Optional[str] = None,
        branch_code: Optional[str] = None,
        filter_pattern: Optional[str] = None,
    ):
        """
        Initialize the approval handler.
        
        Args:
            engine: Database engine
            object_type: Type of object (e.g., "degree", "semester", "faculty")
            degree_code: Optional degree code to filter by
            program_code: Optional program code to filter by
            branch_code: Optional branch code to filter by
            filter_pattern: Optional custom SQL LIKE pattern for object_id filtering
        """
        self.engine = engine
        self.object_type = object_type
        self.degree_code = degree_code
        self.program_code = program_code
        self.branch_code = branch_code
        
        # Build filter pattern
        if filter_pattern:
            self.filter_pattern = filter_pattern
        elif degree_code:
            self.filter_pattern = f"%{degree_code}%"
        else:
            self.filter_pattern = None
    
    # ==================== DATA FETCHING ====================
    
    def get_pending_approvals(self, user_email: Optional[str] = None, user_roles: Optional[set] = None) -> list[dict]:
        """
        Fetch all pending approvals for this object type and context.
        
        If user_email and user_roles provided, only returns approvals this user can act on.
        """
        with self.engine.begin() as conn:
            if self.filter_pattern:
                rows = conn.execute(sa_text("""
                    SELECT id, object_type, object_id, action, status, requester, 
                           requester_email, created_at, payload, reason_note, rule
                    FROM approvals 
                    WHERE object_type = :obj_type
                      AND (object_id LIKE :pattern OR object_id = :degree)
                      AND status IN ('pending', 'under_review')
                    ORDER BY created_at DESC
                """), {
                    "obj_type": self.object_type,
                    "pattern": self.filter_pattern,
                    "degree": self.degree_code or ""
                }).fetchall()
            else:
                rows = conn.execute(sa_text("""
                    SELECT id, object_type, object_id, action, status, requester,
                           requester_email, created_at, payload, reason_note, rule
                    FROM approvals 
                    WHERE object_type = :obj_type
                      AND status IN ('pending', 'under_review')
                    ORDER BY created_at DESC
                """), {"obj_type": self.object_type}).fetchall()
        
        results = []
        for r in rows:
            d = dict(r._mapping)
            # Parse payload JSON
            try:
                d['payload'] = json.loads(d.get('payload') or '{}')
            except (json.JSONDecodeError, TypeError):
                d['payload'] = {}
            
            # Filter by user permissions if provided
            if user_email and user_roles:
                if self._can_user_act_on_approval(d, user_email, user_roles):
                    results.append(d)
            else:
                results.append(d)
        
        return results
    
    def _can_user_act_on_approval(self, approval: dict, user_email: str, user_roles: set) -> bool:
        """Check if user can act on this specific approval."""
        return can_user_approve(
            self.engine,
            user_email,
            user_roles,
            approval['object_type'],
            approval['action'],
            degree=self.degree_code,
            program=self.program_code,
            branch=self.branch_code
        )
    
    def get_approved_changes(self, days: int = 1) -> list[dict]:
        """Fetch recently approved changes."""
        with self.engine.begin() as conn:
            if self.filter_pattern:
                rows = conn.execute(sa_text("""
                    SELECT id, object_type, object_id, action, status, 
                           requester, requester_email, approver, decided_at, reason_note
                    FROM approvals 
                    WHERE object_type = :obj_type
                      AND (object_id LIKE :pattern OR object_id = :degree)
                      AND status = 'approved'
                      AND decided_at >= datetime('now', :days)
                    ORDER BY decided_at DESC
                """), {
                    "obj_type": self.object_type,
                    "pattern": self.filter_pattern,
                    "degree": self.degree_code or "",
                    "days": f"-{days} day"
                }).fetchall()
            else:
                rows = conn.execute(sa_text("""
                    SELECT id, object_type, object_id, action, status,
                           requester, requester_email, approver, decided_at, reason_note
                    FROM approvals 
                    WHERE object_type = :obj_type
                      AND status = 'approved'
                      AND decided_at >= datetime('now', :days)
                    ORDER BY decided_at DESC
                """), {
                    "obj_type": self.object_type,
                    "days": f"-{days} day"
                }).fetchall()
        
        return [dict(r._mapping) for r in rows]
    
    def get_all_completed(self) -> pd.DataFrame:
        """Fetch all completed approvals (approved/rejected) for history."""
        with self.engine.begin() as conn:
            if self.filter_pattern:
                rows = conn.execute(sa_text("""
                    SELECT id, object_type, object_id, action, status,
                           requester, requester_email, approver, decided_at, reason_note
                    FROM approvals 
                    WHERE object_type = :obj_type
                      AND (object_id LIKE :pattern OR object_id = :degree)
                      AND status IN ('approved', 'rejected')
                    ORDER BY decided_at DESC
                """), {
                    "obj_type": self.object_type,
                    "pattern": self.filter_pattern,
                    "degree": self.degree_code or ""
                }).fetchall()
            else:
                rows = conn.execute(sa_text("""
                    SELECT id, object_type, object_id, action, status,
                           requester, requester_email, approver, decided_at, reason_note
                    FROM approvals 
                    WHERE object_type = :obj_type
                      AND status IN ('approved', 'rejected')
                    ORDER BY decided_at DESC
                """), {"obj_type": self.object_type}).fetchall()
        
        if not rows:
            return pd.DataFrame()
        
        return pd.DataFrame([dict(r._mapping) for r in rows])
    
    # ==================== APPROVAL REQUESTS ====================
    
    def request_approval(
        self,
        object_id: str,
        action: str,
        requester_email: str,
        reason: str = "",
        payload: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Create an approval request.
        
        Returns:
            approval_id: The ID of the created approval request
        
        Raises:
            ValueError: If reason is required but not provided
        """
        # Check if reason is required
        if requires_reason(self.engine, self.object_type, action, degree=self.degree_code) and not reason.strip():
            raise ValueError("Reason is required for this action")
        
        # Get approval metadata
        approver_set = get_approver_roles(
            self.engine, self.object_type, action,
            degree=self.degree_code,
            program=self.program_code,
            branch=self.branch_code
        )
        rule_name = get_approval_rule(
            self.engine, self.object_type, action,
            degree=self.degree_code
        )
        
        # Prepare payload
        payload_json = json.dumps(payload or {})
        
        with self.engine.begin() as conn:
            # Detect which columns exist in approvals table
            cols = {c[1] for c in conn.execute(sa_text("PRAGMA table_info(approvals)")).fetchall()}
            
            fields = ["object_type", "object_id", "action", "status", "payload"]
            params = {
                "object_type": self.object_type,
                "object_id": object_id,
                "action": action,
                "status": "pending",
                "payload": payload_json,
            }
            
            # Add requester email (support both column names)
            if "requester_email" in cols:
                fields.append("requester_email")
                params["requester_email"] = requester_email
            elif "requester" in cols:
                fields.append("requester")
                params["requester"] = requester_email
            
            # Add rule if column exists
            if "rule" in cols and rule_name:
                fields.append("rule")
                params["rule"] = rule_name
            
            # Add reason
            if "reason_note" in cols:
                fields.append("reason_note")
                params["reason_note"] = reason
            
            placeholders = ", ".join(f":{f}" for f in fields)
            result = conn.execute(
                sa_text(f"INSERT INTO approvals({', '.join(fields)}) VALUES({placeholders})"),
                params
            )
            
            return result.lastrowid
    
    def approve(
        self,
        approval_id: int,
        approver_email: str,
        decision_note: str = "",
        apply_change_callback: Optional[callable] = None,
    ) -> None:
        """
        Approve an approval request and optionally apply the change.
        
        Args:
            approval_id: ID of the approval to approve
            approver_email: Email of the approver
            decision_note: Optional note about the decision
            apply_change_callback: Optional function to call to apply the change
        """
        with self.engine.begin() as conn:
            # Get the approval
            row = conn.execute(sa_text("""
                SELECT * FROM approvals WHERE id = :id
            """), {"id": approval_id}).fetchone()
            
            if not row:
                raise ValueError(f"Approval {approval_id} not found")
            
            approval_dict = dict(row._mapping)
            
            # Verify approver has permission
            from core.policy import user_roles as get_user_roles
            user_roles = get_user_roles(self.engine, approver_email)
            
            if not can_user_approve(
                self.engine,
                approver_email,
                user_roles,
                approval_dict['object_type'],
                approval_dict['action'],
                degree=self.degree_code,
                program=self.program_code,
                branch=self.branch_code
            ):
                raise PermissionError(f"User {approver_email} is not authorized to approve this request")
            
            # Apply the change if callback provided
            if apply_change_callback:
                apply_change_callback(conn, approval_dict)
            
            # Update approval status
            conn.execute(sa_text("""
                UPDATE approvals 
                SET status = 'approved',
                    approver = :approver,
                    decided_at = CURRENT_TIMESTAMP,
                    decision_note = :note
                WHERE id = :id
            """), {
                "id": approval_id,
                "approver": approver_email,
                "note": decision_note
            })
    
    def reject(
        self,
        approval_id: int,
        approver_email: str,
        decision_note: str = "",
    ) -> None:
        """Reject an approval request."""
        with self.engine.begin() as conn:
            # Get the approval
            row = conn.execute(sa_text("""
                SELECT * FROM approvals WHERE id = :id
            """), {"id": approval_id}).fetchone()
            
            if not row:
                raise ValueError(f"Approval {approval_id} not found")
            
            approval_dict = dict(row._mapping)
            
            # Verify approver has permission
            from core.policy import user_roles as get_user_roles
            user_roles = get_user_roles(self.engine, approver_email)
            
            if not can_user_approve(
                self.engine,
                approver_email,
                user_roles,
                approval_dict['object_type'],
                approval_dict['action'],
                degree=self.degree_code,
                program=self.program_code,
                branch=self.branch_code
            ):
                raise PermissionError(f"User {approver_email} is not authorized to reject this request")
            
            conn.execute(sa_text("""
                UPDATE approvals 
                SET status = 'rejected',
                    approver = :approver,
                    decided_at = CURRENT_TIMESTAMP,
                    decision_note = :note
                WHERE id = :id
            """), {
                "id": approval_id,
                "approver": approver_email,
                "note": decision_note
            })
    
    # ==================== UI SECTIONS ====================
    
    def show_pending_section(
        self,
        title: str = "Pending Approvals",
        show_empty_message: bool = True,
        user_email: Optional[str] = None,
        user_roles: Optional[set] = None,
    ) -> None:
        """Display a section showing pending approvals for this object type."""
        approvals = self.get_pending_approvals(user_email, user_roles)
        
        if not approvals:
            if show_empty_message:
                if user_email:
                    st.info(f"No pending approvals that you can act on for {self.object_type}.")
                else:
                    st.info(f"No pending approvals for {self.object_type}.")
            return
        
        st.subheader(title)
        
        # Convert to dataframe for display
        df = pd.DataFrame(approvals)
        display_cols = ["id", "object_id", "action", "status", "requester", "created_at"]
        display_cols = [c for c in display_cols if c in df.columns]
        
        st.dataframe(
            df[display_cols],
            use_container_width=True,
            hide_index=True,
            column_config={
                "created_at": st.column_config.DatetimeColumn(
                    "Created",
                    format="YYYY-MM-DD hh:mm A"
                )
            }
        )
        
        # Show details
        with st.expander("View Details"):
            for approval in approvals:
                st.markdown(f"**Approval #{approval['id']}**")
                st.json(approval)
    
    def show_history_section(
        self,
        title: str = "Approval History",
        show_empty_message: bool = True,
    ) -> None:
        """Display a section showing approval history."""
        df = self.get_all_completed()
        
        if df.empty:
            if show_empty_message:
                st.info(f"No approval history for {self.object_type}.")
            return
        
        st.subheader(title)
        
        display_cols = ["id", "object_id", "action", "status", "requester", "approver", "decided_at"]
        display_cols = [c for c in display_cols if c in df.columns]
        
        st.dataframe(
            df[display_cols],
            use_container_width=True,
            hide_index=True,
            column_config={
                "decided_at": st.column_config.DatetimeColumn(
                    "Decided",
                    format="YYYY-MM-DD hh:mm A"
                ),
                "status": st.column_config.TextColumn("Status")
            }
        )
    
    def show_recent_approvals_alert(self, days: int = 1) -> None:
        """Show an info box with recently approved changes."""
        recent = self.get_approved_changes(days=days)
        
        if recent:
            st.success(f"**{len(recent)} change(s) approved in the last {days} day(s)**")
            for change in recent:
                st.caption(
                    f"✓ {change['action']} on {change['object_id']} "
                    f"by {change.get('approver', 'unknown')} "
                    f"at {change.get('decided_at', 'unknown time')}"
                )
    
    def show_inline_approval_actions(
        self,
        approval_id: int,
        user_email: str,
        apply_change_callback: Optional[callable] = None,
    ) -> None:
        """Show inline approve/reject buttons for a specific approval."""
        # Get user roles
        from core.policy import user_roles as get_user_roles
        roles = get_user_roles(self.engine, user_email)
        
        # Get approval details
        with self.engine.begin() as conn:
            row = conn.execute(sa_text("""
                SELECT * FROM approvals WHERE id = :id
            """), {"id": approval_id}).fetchone()
        
        if not row:
            st.error(f"Approval {approval_id} not found")
            return
        
        approval_dict = dict(row._mapping)
        
        # Check if user can approve
        if not can_user_approve(
            self.engine,
            user_email,
            roles,
            approval_dict['object_type'],
            approval_dict['action'],
            degree=self.degree_code,
            program=self.program_code,
            branch=self.branch_code
        ):
            st.warning("You are not authorized to approve this request.")
            return
        
        # Show action buttons
        col1, col2 = st.columns(2)
        
        decision_note = st.text_area(
            "Decision Note (optional)",
            key=f"decision_note_{approval_id}"
        )
        
        with col1:
            if st.button("✓ Approve", key=f"approve_{approval_id}", type="primary"):
                try:
                    self.approve(
                        approval_id,
                        user_email,
                        decision_note,
                        apply_change_callback
                    )
                    st.success(f"Approved #{approval_id}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error approving: {e}")
        
        with col2:
            if st.button("✗ Reject", key=f"reject_{approval_id}"):
                try:
                    self.reject(approval_id, user_email, decision_note)
                    st.success(f"Rejected #{approval_id}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error rejecting: {e}")


# ==================== CONVENIENCE FUNCTIONS ====================

def create_approval_request(
    engine: Engine,
    object_type: str,
    object_id: str,
    action: str,
    requester_email: str,
    reason: str = "",
    payload: Optional[Dict[str, Any]] = None,
) -> int:
    """Convenience function to create an approval request."""
    handler = ApprovalHandler(engine, object_type)
    return handler.request_approval(object_id, action, requester_email, reason, payload)
