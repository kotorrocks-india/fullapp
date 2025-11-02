# schemas/program_outcomes_schema.py
from datetime import datetime

DDL = r"""
PRAGMA foreign_keys=ON;

-- Program Outcomes (POs), versioned and scoped like subjects
CREATE TABLE IF NOT EXISTS program_outcomes (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    po_code             TEXT NOT NULL,            -- e.g., PO-1, PO-ENV-3
    version             INTEGER NOT NULL,

    description         TEXT NOT NULL,
    short_title         TEXT DEFAULT '',

    cg_degree           INTEGER NOT NULL DEFAULT 0,
    cg_program          INTEGER NOT NULL DEFAULT 0,
    cg_branch           INTEGER NOT NULL DEFAULT 0,

    degree_code         TEXT NOT NULL DEFAULT '',
    program_code        TEXT NOT NULL DEFAULT '',
    branch_code         TEXT NOT NULL DEFAULT '',

    effective_ay_from   TEXT NOT NULL,
    effective_ay_to     TEXT,

    status              TEXT NOT NULL DEFAULT 'draft',  -- draft|active|sunset|archived
    notes               TEXT DEFAULT '',

    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now')),

    UNIQUE(po_code, version, degree_code, program_code, branch_code),

    CHECK ((cg_degree IN (0,1)) AND (cg_program IN (0,1)) AND (cg_branch IN (0,1))),
    CHECK ((cg_degree = 1 AND degree_code <> '') OR (cg_degree = 0 AND degree_code = '')),
    CHECK ((cg_program = 0 AND program_code = '') OR (cg_program = 1 AND program_code <> '')),
    CHECK ((cg_branch = 0 AND branch_code = '') OR (cg_branch = 1 AND branch_code <> '')),
    CHECK ( (cg_program = 0 OR cg_degree = 1) ),
    CHECK ( (cg_branch = 0 OR (cg_program = 1 AND cg_degree = 1)) )
);

CREATE INDEX IF NOT EXISTS idx_pos_code_ver ON program_outcomes(po_code, version);
CREATE INDEX IF NOT EXISTS idx_pos_scope ON program_outcomes(degree_code, program_code, branch_code);
CREATE INDEX IF NOT EXISTS idx_pos_status ON program_outcomes(status);
CREATE INDEX IF NOT EXISTS idx_pos_ay ON program_outcomes(effective_ay_from, effective_ay_to);

-- Approval log mirroring subjects (rollover/activate/close/reopen)
CREATE TABLE IF NOT EXISTS program_outcome_approvals (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    po_id            INTEGER NOT NULL,
    action           TEXT NOT NULL,
    from_ay_code     TEXT,
    to_ay_code       TEXT,
    approver_user_id INTEGER NOT NULL,
    approver_role    TEXT NOT NULL,
    note             TEXT DEFAULT '',
    approved_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY(po_id) REFERENCES program_outcomes(id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_pos_approvals ON program_outcome_approvals(po_id, action, approved_at);

-- Keep updated_at fresh
CREATE TRIGGER IF NOT EXISTS trg_pos_touch_upd
AFTER UPDATE ON program_outcomes
BEGIN
  UPDATE program_outcomes SET updated_at = datetime('now') WHERE id = NEW.id;
END;
"""

VIEW_LATEST = r"""
CREATE VIEW IF NOT EXISTS v_program_outcomes_latest_per_scope AS
SELECT *
FROM (
  SELECT p.*,
         ROW_NUMBER() OVER (
           PARTITION BY p.po_code, p.degree_code, p.program_code, p.branch_code
           ORDER BY p.version DESC, p.id DESC
         ) AS rn
  FROM program_outcomes p
)
WHERE rn = 1;
"""

def install_program_outcomes(engine):
    with engine.begin() as conn:
        conn.exec_driver_sql(DDL)
        conn.exec_driver_sql(VIEW_LATEST)
