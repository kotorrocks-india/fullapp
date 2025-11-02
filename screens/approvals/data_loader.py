# In screens/approvals/data_loader.py

import pandas as pd
from sqlalchemy import text as sa_text
from .schema_helpers import _has_col, _cols # <-- 1. MODIFIED IMPORT (added _cols)

def _fetch_open_approvals(engine) -> pd.DataFrame: #
    with engine.begin() as conn: #
        has_payload = _has_col(conn, "approvals", "payload") #
        cols = "id, object_type, object_id, action, status, requester, note, created_at" #
        if has_payload: #
            cols += ", payload" #
        rows = conn.execute(sa_text(f"""
            SELECT {cols}
              FROM approvals
             WHERE status IN ('pending','under_review')
             ORDER BY created_at DESC, id DESC
        """)).fetchall() #
    
    if not rows: #
        base = ["id","object_type","object_id","action","status","requester","note","created_at"] #
        if has_payload: base.append("payload") #
        return pd.DataFrame(columns=base) #
    
    return pd.DataFrame([dict(r._mapping) for r in rows]) #

# --- 2. NEW FUNCTION TO ADD ---
def _fetch_completed_approvals(engine) -> pd.DataFrame:
    """Fetch all 'approved' and 'rejected' approvals for the audit log."""
    with engine.begin() as conn:
        # We need _cols for this, which must be imported from schema_helpers
        db_cols = _cols(conn, "approvals")
        
        # Define the columns we want for the audit log
        select_cols = ["id", "object_type", "object_id", "action", "status", "requester", "note", "created_at"]
        
        # Add optional columns if they exist in the database
        if "approver" in db_cols:
            select_cols.append("approver")
        if "decided_at" in db_cols:
            select_cols.append("decided_at")
            
        sql = f"""
            SELECT {', '.join(select_cols)}
              FROM approvals
             WHERE status IN ('approved', 'rejected')
             ORDER BY decided_at DESC, id DESC
        """
        rows = conn.execute(sa_text(sql)).fetchall()
    
    if not rows:
        return pd.DataFrame(columns=select_cols)
    
    return pd.DataFrame([dict(r._mapping) for r in rows])
# --- END OF NEW FUNCTION ---

def get_affiliation_details(engine, affiliation_id: int) -> dict: #
    """Get details about an affiliation for display in approval UI.""" #
    with engine.begin() as conn: #
        row = conn.execute(sa_text("""
            SELECT fa.email, fa.degree_code, fa.branch_code, fa.designation, fa.type,
                   fp.name as faculty_name, d.name as degree_name
            FROM faculty_affiliations fa
            LEFT JOIN faculty_profiles fp ON fp.email = fa.email
            LEFT JOIN degrees d ON d.code = fa.degree_code
            WHERE fa.id = :aff_id
        """), {"aff_id": affiliation_id}).fetchone() #
        
        if row: #
            return dict(row._mapping) #
        return {} #
