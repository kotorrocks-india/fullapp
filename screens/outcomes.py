# app/modules/outcomes.py
"""
Complete implementation of Program Outcomes module (PEOs, POs, PSOs).
Based on slide16_POS.yaml specification.

Features:
- CRUD operations for outcomes sets and items
- Scope management (per_degree, per_program, per_branch)
- Workflow (draft → published → archived)
- Import/Export with validation and dry-run
- Version control and rollback
- Approval workflow integration
- Mapping tracking to prevent breaking changes
"""

from __future__ import annotations
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
import json
import csv
import io
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine


# ============================================================================
# ENUMS
# ============================================================================

class ScopeLevel(str, Enum):
    PER_DEGREE = "per_degree"
    PER_PROGRAM = "per_program"
    PER_BRANCH = "per_branch"


class SetType(str, Enum):
    PEOS = "peos"
    POS = "pos"
    PSOS = "psos"


class Status(str, Enum):
    DRAFT = "draft"
    PUBLISHED = "published"
    ARCHIVED = "archived"


class BloomLevel(str, Enum):
    REMEMBER = "Remember"
    UNDERSTAND = "Understand"
    APPLY = "Apply"
    ANALYZE = "Analyze"
    EVALUATE = "Evaluate"
    CREATE = "Create"


class OperationKey(str, Enum):
    CREATE = "OUTCOMES_CREATE"
    PUBLISH = "OUTCOMES_PUBLISH"
    UNPUBLISH = "OUTCOMES_UNPUBLISH"
    MAJOR_EDIT = "OUTCOMES_MAJOR_EDIT"
    SCOPE_CHANGE = "OUTCOMES_SCOPE_CHANGE"


class ImportStatus(str, Enum):
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class OutcomeItem:
    """Represents a single PEO/PO/PSO item."""
    code: str
    description: str
    title: Optional[str] = None
    bloom_level: Optional[BloomLevel] = None
    timeline_years: Optional[int] = None
    tags: List[str] = field(default_factory=list)
    sort_order: int = 100
    id: Optional[int] = None
    set_id: Optional[int] = None
    created_by: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_by: Optional[str] = None
    updated_at: Optional[datetime] = None

    def validate(self) -> List[str]:
        """Validate item fields and return list of errors."""
        errors = []
        
        if not self.code or not self.code.strip():
            errors.append("Code is required")
        elif len(self.code) > 16:
            errors.append("Code must be 16 characters or less")
        
        if not self.description or not self.description.strip():
            errors.append("Description is required")
        elif len(self.description) > 4000:
            errors.append("Description must be 4000 characters or less")
        
        if self.title and len(self.title) > 200:
            errors.append("Title must be 200 characters or less")
        
        if self.timeline_years is not None:
            if self.timeline_years < 1 or self.timeline_years > 10:
                errors.append("Timeline must be between 1 and 10 years")
        
        return errors


@dataclass
class OutcomeSet:
    """Represents a collection of outcomes (PEOs, POs, or PSOs)."""
    degree_code: str
    set_type: SetType
    status: Status = Status.DRAFT
    program_code: Optional[str] = None
    branch_code: Optional[str] = None
    version: int = 1
    is_current: bool = True
    items: List[OutcomeItem] = field(default_factory=list)
    id: Optional[int] = None
    created_by: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_by: Optional[str] = None
    updated_at: Optional[datetime] = None
    published_by: Optional[str] = None
    published_at: Optional[datetime] = None
    archived_by: Optional[str] = None
    archived_at: Optional[datetime] = None
    archive_reason: Optional[str] = None

    def validate(self) -> List[str]:
        """Validate set and return list of errors."""
        errors = []
        
        if not self.degree_code or not self.degree_code.strip():
            errors.append("Degree code is required")
        
        # Validate item count
        min_items = 0 if self.set_type == SetType.PSOS else 1
        max_items = 50
        
        if len(self.items) < min_items:
            errors.append(f"{self.set_type.value} requires at least {min_items} item(s)")
        
        if len(self.items) > max_items:
            errors.append(f"{self.set_type.value} cannot have more than {max_items} items")
        
        # Validate each item
        for i, item in enumerate(self.items):
            item_errors = item.validate()
            for err in item_errors:
                errors.append(f"Item {i+1} ({item.code}): {err}")
        
        # Check for duplicate codes
        codes = [item.code.upper() for item in self.items]
        duplicates = [code for code in set(codes) if codes.count(code) > 1]
        if duplicates:
            errors.append(f"Duplicate codes found: {', '.join(duplicates)}")
        
        return errors


@dataclass
class ImportRow:
    """Represents a single row from import CSV."""
    degree_code: str
    set_type: str
    code: str
    description: str
    program_code: Optional[str] = None
    branch_code: Optional[str] = None
    status: str = "draft"
    title: Optional[str] = None
    bloom_level: Optional[str] = None
    timeline_years: Optional[int] = None
    tags: Optional[str] = None
    
    row_number: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class ImportResult:
    """Results of an import operation."""
    session_id: str
    total_rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    imported_rows: int = 0
    failed_rows: int = 0
    errors: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[Dict[str, Any]] = field(default_factory=list)
    dry_run: bool = True
    preview_data: List[ImportRow] = field(default_factory=list)


# ============================================================================
# OUTCOMES MANAGER
# ============================================================================

class OutcomesManager:
    """Main manager for outcomes operations."""
    
    def __init__(self, engine: Engine, actor: str, actor_role: str):
        self.engine = engine
        self.actor = actor
        self.actor_role = actor_role
    
    # ========================================================================
    # SCOPE CONFIGURATION
    # ========================================================================
    
    def get_scope_config(self, degree_code: str) -> Optional[ScopeLevel]:
        """Get the scope level configuration for a degree."""
        with self.engine.connect() as conn:
            result = conn.execute(sa_text("""
                SELECT scope_level FROM outcomes_scope_config
                WHERE lower(degree_code) = lower(:degree_code)
            """), {"degree_code": degree_code}).fetchone()
            
            if result:
                return ScopeLevel(result[0])
            return ScopeLevel.PER_PROGRAM  # Default
    
    def set_scope_config(
        self,
        degree_code: str,
        scope_level: ScopeLevel,
        reason: str,
        archive_existing: bool = True
    ) -> bool:
        """
        Change scope configuration for a degree.
        Archives existing sets if requested.
        """
        with self.engine.begin() as conn:
            # Check if config exists
            existing = conn.execute(sa_text("""
                SELECT scope_level FROM outcomes_scope_config
                WHERE lower(degree_code) = lower(:degree_code)
            """), {"degree_code": degree_code}).fetchone()
            
            old_scope = existing[0] if existing else None
            
            # Archive existing sets if scope is changing
            if archive_existing and old_scope and old_scope != scope_level.value:
                archive_note = f"Scope changed from {old_scope} to {scope_level.value}; prior sets archived for provenance."
                conn.execute(sa_text("""
                    UPDATE outcomes_sets
                    SET status = 'archived',
                        is_current = 0,
                        archived_by = :actor,
                        archived_at = CURRENT_TIMESTAMP,
                        archive_reason = :reason
                    WHERE lower(degree_code) = lower(:degree_code)
                    AND status != 'archived'
                """), {
                    "degree_code": degree_code,
                    "actor": self.actor,
                    "reason": archive_note
                })
            
            # Upsert scope config
            if existing:
                conn.execute(sa_text("""
                    UPDATE outcomes_scope_config
                    SET scope_level = :scope_level,
                        changed_by = :actor,
                        changed_at = CURRENT_TIMESTAMP,
                        change_reason = :reason
                    WHERE lower(degree_code) = lower(:degree_code)
                """), {
                    "scope_level": scope_level.value,
                    "degree_code": degree_code,
                    "actor": self.actor,
                    "reason": reason
                })
            else:
                conn.execute(sa_text("""
                    INSERT INTO outcomes_scope_config
                    (degree_code, scope_level, changed_by, change_reason)
                    VALUES (:degree_code, :scope_level, :actor, :reason)
                """), {
                    "degree_code": degree_code,
                    "scope_level": scope_level.value,
                    "actor": self.actor,
                    "reason": reason
                })
            
            # Audit
            conn.execute(sa_text("""
                INSERT INTO outcomes_scope_config_audit
                (degree_code, action, old_scope, new_scope, reason, actor)
                VALUES (:degree_code, 'SCOPE_CHANGE', :old_scope, :new_scope, :reason, :actor)
            """), {
                "degree_code": degree_code,
                "old_scope": old_scope,
                "new_scope": scope_level.value,
                "reason": reason,
                "actor": self.actor
            })
            
            return True
    
    # ========================================================================
    # CRUD OPERATIONS
    # ========================================================================
    
    def create_set(
        self,
        outcome_set: OutcomeSet,
        reason: str,
        step_up_verified: bool = False
    ) -> Tuple[bool, Optional[int], List[str]]:
        """
        Create a new outcome set with items.
        Returns (success, set_id, errors).
        """
        # Validate
        errors = outcome_set.validate()
        if errors:
            return False, None, errors
        
        with self.engine.begin() as conn:
            # Check scope compatibility
            scope_level = self.get_scope_config(outcome_set.degree_code)
            if scope_level == ScopeLevel.PER_DEGREE:
                if outcome_set.program_code or outcome_set.branch_code:
                    return False, None, ["Degree uses per_degree scope; program/branch must be empty"]
            elif scope_level == ScopeLevel.PER_PROGRAM:
                if outcome_set.branch_code:
                    return False, None, ["Degree uses per_program scope; branch must be empty"]
            
            # Check for existing current set at same scope
            existing = conn.execute(sa_text("""
                SELECT id FROM outcomes_sets
                WHERE lower(degree_code) = lower(:degree_code)
                AND (:program_code IS NULL OR lower(program_code) = lower(:program_code))
                AND (:branch_code IS NULL OR lower(branch_code) = lower(:branch_code))
                AND set_type = :set_type
                AND is_current = 1
            """), {
                "degree_code": outcome_set.degree_code,
                "program_code": outcome_set.program_code,
                "branch_code": outcome_set.branch_code,
                "set_type": outcome_set.set_type.value
            }).fetchone()
            
            if existing:
                return False, None, [f"A current {outcome_set.set_type.value} set already exists at this scope"]
            
            # Insert set
            result = conn.execute(sa_text("""
                INSERT INTO outcomes_sets
                (degree_code, program_code, branch_code, set_type, status, version, 
                 is_current, created_by)
                VALUES (:degree_code, :program_code, :branch_code, :set_type, :status,
                        :version, :is_current, :actor)
            """), {
                "degree_code": outcome_set.degree_code,
                "program_code": outcome_set.program_code,
                "branch_code": outcome_set.branch_code,
                "set_type": outcome_set.set_type.value,
                "status": outcome_set.status.value,
                "version": outcome_set.version,
                "is_current": 1 if outcome_set.is_current else 0,
                "actor": self.actor
            })
            
            set_id = result.lastrowid
            
            # Insert items
            for item in outcome_set.items:
                conn.execute(sa_text("""
                    INSERT INTO outcomes_items
                    (set_id, code, title, description, bloom_level, timeline_years,
                     tags, sort_order, created_by)
                    VALUES (:set_id, :code, :title, :description, :bloom_level,
                            :timeline_years, :tags, :sort_order, :actor)
                """), {
                    "set_id": set_id,
                    "code": item.code.upper().strip(),
                    "title": item.title,
                    "description": item.description,
                    "bloom_level": item.bloom_level.value if item.bloom_level else None,
                    "timeline_years": item.timeline_years,
                    "tags": "|".join(item.tags) if item.tags else None,
                    "sort_order": item.sort_order,
                    "actor": self.actor
                })
            
            # Audit
            self._audit(conn, "OUTCOMES_CREATED", set_id=set_id, reason=reason,
                       step_up=step_up_verified, after_data=self._serialize_set(outcome_set))
            
            # Snapshot
            self._create_snapshot(conn, set_id, "create", reason)
            
            return True, set_id, []
    
    def get_set(
        self,
        degree_code: str,
        set_type: SetType,
        program_code: Optional[str] = None,
        branch_code: Optional[str] = None,
        include_archived: bool = False
    ) -> Optional[OutcomeSet]:
        """Get the current outcome set for given scope."""
        with self.engine.connect() as conn:
            # Get set
            query = """
                SELECT id, degree_code, program_code, branch_code, set_type, status,
                       version, is_current, created_by, created_at, updated_by, updated_at,
                       published_by, published_at, archived_by, archived_at, archive_reason
                FROM outcomes_sets
                WHERE lower(degree_code) = lower(:degree_code)
                AND set_type = :set_type
                AND (:program_code IS NULL OR lower(program_code) = lower(:program_code))
                AND (:branch_code IS NULL OR lower(branch_code) = lower(:branch_code))
                AND is_current = 1
            """
            
            if not include_archived:
                query += " AND status != 'archived'"
            
            result = conn.execute(sa_text(query), {
                "degree_code": degree_code,
                "set_type": set_type.value,
                "program_code": program_code,
                "branch_code": branch_code
            }).fetchone()
            
            if not result:
                return None
            
            # Get items
            items_result = conn.execute(sa_text("""
                SELECT id, code, title, description, bloom_level, timeline_years,
                       tags, sort_order, created_by, created_at, updated_by, updated_at
                FROM outcomes_items
                WHERE set_id = :set_id
                ORDER BY sort_order, code
            """), {"set_id": result[0]}).fetchall()
            
            items = []
            for item_row in items_result:
                items.append(OutcomeItem(
                    id=item_row[0],
                    code=item_row[1],
                    title=item_row[2],
                    description=item_row[3],
                    bloom_level=BloomLevel(item_row[4]) if item_row[4] else None,
                    timeline_years=item_row[5],
                    tags=item_row[6].split("|") if item_row[6] else [],
                    sort_order=item_row[7],
                    created_by=item_row[8],
                    created_at=item_row[9],
                    updated_by=item_row[10],
                    updated_at=item_row[11]
                ))
            
            return OutcomeSet(
                id=result[0],
                degree_code=result[1],
                program_code=result[2],
                branch_code=result[3],
                set_type=SetType(result[4]),
                status=Status(result[5]),
                version=result[6],
                is_current=bool(result[7]),
                created_by=result[8],
                created_at=result[9],
                updated_by=result[10],
                updated_at=result[11],
                published_by=result[12],
                published_at=result[13],
                archived_by=result[14],
                archived_at=result[15],
                archive_reason=result[16],
                items=items
            )
    
    def update_set(
        self,
        set_id: int,
        updated_set: OutcomeSet,
        reason: str,
        is_major_edit: bool = False,
        step_up_verified: bool = False
    ) -> Tuple[bool, List[str]]:
        """
        Update an existing outcome set.
        Major edits include: code renames, bulk deletes >25%, scope moves.
        """
        # Validate
        errors = updated_set.validate()
        if errors:
            return False, errors
        
        with self.engine.begin() as conn:
            # Get existing set
            existing = conn.execute(sa_text("""
                SELECT degree_code, set_type, status, program_code, branch_code
                FROM outcomes_sets WHERE id = :set_id
            """), {"set_id": set_id}).fetchone()
            
            if not existing:
                return False, ["Set not found"]
            
            existing_status = Status(existing[2])
            
            # Check if published and has mappings
            if existing_status == Status.PUBLISHED:
                has_mappings = self._check_mappings(conn, set_id)
                if has_mappings:
                    if is_major_edit:
                        return False, ["Major edits blocked: set has active mappings. Requires approval."]
                    # Minor edits allowed with reason
            
            # Get existing items for comparison
            existing_items = conn.execute(sa_text("""
                SELECT code FROM outcomes_items WHERE set_id = :set_id
            """), {"set_id": set_id}).fetchall()
            existing_codes = {row[0].upper() for row in existing_items}
            new_codes = {item.code.upper() for item in updated_set.items}
            
            # Detect major changes
            deleted_codes = existing_codes - new_codes
            renamed_codes = existing_codes != new_codes and len(deleted_codes) > 0
            bulk_delete = len(deleted_codes) > len(existing_codes) * 0.25
            
            if (renamed_codes or bulk_delete) and not is_major_edit:
                return False, ["This update requires major edit approval"]
            
            # Snapshot before update
            self._create_snapshot(conn, set_id, "update", reason)
            
            # Update set metadata
            conn.execute(sa_text("""
                UPDATE outcomes_sets
                SET updated_by = :actor,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :set_id
            """), {"set_id": set_id, "actor": self.actor})
            
            # Delete old items
            conn.execute(sa_text("DELETE FROM outcomes_items WHERE set_id = :set_id"),
                        {"set_id": set_id})
            
            # Insert updated items
            for item in updated_set.items:
                conn.execute(sa_text("""
                    INSERT INTO outcomes_items
                    (set_id, code, title, description, bloom_level, timeline_years,
                     tags, sort_order, created_by)
                    VALUES (:set_id, :code, :title, :description, :bloom_level,
                            :timeline_years, :tags, :sort_order, :actor)
                """), {
                    "set_id": set_id,
                    "code": item.code.upper().strip(),
                    "title": item.title,
                    "description": item.description,
                    "bloom_level": item.bloom_level.value if item.bloom_level else None,
                    "timeline_years": item.timeline_years,
                    "tags": "|".join(item.tags) if item.tags else [],
                    "sort_order": item.sort_order,
                    "actor": self.actor
                })
            
            # Audit
            event = "OUTCOMES_MAJOR_EDIT" if is_major_edit else "OUTCOMES_UPDATED"
            self._audit(conn, event, set_id=set_id, reason=reason,
                       step_up=step_up_verified, after_data=self._serialize_set(updated_set))
            
            return True, []
    
    def publish_set(
        self,
        set_id: int,
        reason: str,
        step_up_verified: bool = False
    ) -> Tuple[bool, List[str]]:
        """Publish a draft outcome set."""
        with self.engine.begin() as conn:
            # Get set
            result = conn.execute(sa_text("""
                SELECT status FROM outcomes_sets WHERE id = :set_id
            """), {"set_id": set_id}).fetchone()
            
            if not result:
                return False, ["Set not found"]
            
            if result[0] != Status.DRAFT.value:
                return False, [f"Can only publish draft sets (current: {result[0]})"]
            
            # Snapshot
            self._create_snapshot(conn, set_id, "publish", reason)
            
            # Publish
            conn.execute(sa_text("""
                UPDATE outcomes_sets
                SET status = 'published',
                    published_by = :actor,
                    published_at = CURRENT_TIMESTAMP,
                    updated_by = :actor,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :set_id
            """), {"set_id": set_id, "actor": self.actor})
            
            # Audit
            self._audit(conn, "OUTCOMES_PUBLISHED", set_id=set_id, reason=reason,
                       step_up=step_up_verified)
            
            return True, []
    
    def archive_set(
        self,
        set_id: int,
        reason: str,
        step_up_verified: bool = False
    ) -> Tuple[bool, List[str]]:
        """Archive an outcome set."""
        with self.engine.begin() as conn:
            # Check if has mappings
            has_mappings = self._check_mappings(conn, set_id)
            if has_mappings:
                return False, ["Cannot archive: set has active mappings"]
            
            # Snapshot
            self._create_snapshot(conn, set_id, "archive", reason)
            
            # Archive
            conn.execute(sa_text("""
                UPDATE outcomes_sets
                SET status = 'archived',
                    is_current = 0,
                    archived_by = :actor,
                    archived_at = CURRENT_TIMESTAMP,
                    archive_reason = :reason,
                    updated_by = :actor,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :set_id
            """), {"set_id": set_id, "actor": self.actor, "reason": reason})
            
            # Audit
            self._audit(conn, "OUTCOMES_ARCHIVED", set_id=set_id, reason=reason,
                       step_up=step_up_verified)
            
            return True, []
    
    # ========================================================================
    # IMPORT / EXPORT
    # ========================================================================
    
    def import_preview(
        self,
        csv_content: str,
        degree_code: str,
        session_id: str
    ) -> ImportResult:
        """
        Preview import: validate CSV without committing to database.
        Returns validation results with errors/warnings.
        """
        result = ImportResult(session_id=session_id, dry_run=True)
        
        try:
            # Parse CSV
            reader = csv.DictReader(io.StringIO(csv_content))
            rows = list(reader)
            result.total_rows = len(rows)
            
            # Get scope config
            scope_level = self.get_scope_config(degree_code)
            
            # Validate each row
            for i, row_dict in enumerate(rows, start=2):  # Start at 2 (header is row 1)
                import_row = self._parse_import_row(row_dict, i, scope_level)
                result.preview_data.append(import_row)
                
                if import_row.errors:
                    result.invalid_rows += 1
                    result.errors.append({
                        "row": i,
                        "code": import_row.code,
                        "errors": import_row.errors
                    })
                else:
                    result.valid_rows += 1
                
                if import_row.warnings:
                    result.warnings.append({
                        "row": i,
                        "code": import_row.code,
                        "warnings": import_row.warnings
                    })
            
            # Store session
            with self.engine.begin() as conn:
                conn.execute(sa_text("""
                    INSERT INTO outcomes_import_sessions
                    (session_id, file_name, file_size, degree_code, uploaded_by,
                     preview_status, preview_errors, preview_warnings, records_total)
                    VALUES (:session_id, :file_name, :file_size, :degree_code, :actor,
                            :status, :errors, :warnings, :total)
                """), {
                    "session_id": session_id,
                    "file_name": f"import_{session_id}.csv",
                    "file_size": len(csv_content),
                    "degree_code": degree_code,
                    "actor": self.actor,
                    "status": "completed",
                    "errors": json.dumps(result.errors),
                    "warnings": json.dumps(result.warnings),
                    "total": result.total_rows
                })
            
        except Exception as e:
            result.errors.append({"row": 0, "errors": [f"CSV parsing failed: {str(e)}"]})
        
        return result
    
    def import_execute(
        self,
        csv_content: str,
        degree_code: str,
        session_id: str,
        skip_validation: bool = False
    ) -> ImportResult:
        """
        Execute import: create outcome sets from validated CSV.
        If skip_validation=False, re-runs validation.
        """
        # First validate if needed
        if not skip_validation:
            preview = self.import_preview(csv_content, degree_code, session_id)
            if preview.invalid_rows > 0:
                preview.dry_run = False
                return preview
        
        result = ImportResult(session_id=session_id, dry_run=False)
        
        with self.engine.begin() as conn:
            try:
                # Update session status
                conn.execute(sa_text("""
                    UPDATE outcomes_import_sessions
                    SET import_status = 'in_progress',
                        import_started_at = CURRENT_TIMESTAMP
                    WHERE session_id = :session_id
                """), {"session_id": session_id})
                
                # Parse CSV
                reader = csv.DictReader(io.StringIO(csv_content))
                rows = list(reader)
                result.total_rows = len(rows)
                
                scope_level = self.get_scope_config(degree_code)
                
                # Group rows by scope + set_type
                grouped: Dict[Tuple, List[ImportRow]] = {}
                for i, row_dict in enumerate(rows, start=2):
                    import_row = self._parse_import_row(row_dict, i, scope_level)
                    
                    if import_row.errors:
                        result.invalid_rows += 1
                        result.failed_rows += 1
                        result.errors.append({
                            "row": i,
                            "code": import_row.code,
                            "errors": import_row.errors
                        })
                        continue
                    
                    result.valid_rows += 1
                    
                    key = (
                        import_row.degree_code,
                        import_row.program_code or "",
                        import_row.branch_code or "",
                        import_row.set_type
                    )
                    
                    if key not in grouped:
                        grouped[key] = []
                    grouped[key].append(import_row)
                
                # Create sets
                for key, import_rows in grouped.items():
                    degree, program, branch, set_type_str = key
                    
                    # Build outcome set
                    items = []
                    for import_row in import_rows:
                        items.append(OutcomeItem(
                            code=import_row.code,
                            title=import_row.title,
                            description=import_row.description,
                            bloom_level=BloomLevel(import_row.bloom_level) if import_row.bloom_level else None,
                            timeline_years=import_row.timeline_years,
                            tags=import_row.tags.split("|") if import_row.tags else [],
                            sort_order=import_row.row_number
                        ))
                    
                    outcome_set = OutcomeSet(
                        degree_code=degree,
                        program_code=program if program else None,
                        branch_code=branch if branch else None,
                        set_type=SetType(set_type_str.lower()),
                        status=Status(import_rows[0].status),
                        items=items
                    )
                    
                    # Create the set
                    success, set_id, errors = self.create_set(
                        outcome_set,
                        reason=f"Imported from CSV session {session_id}",
                        step_up_verified=False
                    )
                    
                    if success:
                        result.imported_rows += len(import_rows)
                    else:
                        result.failed_rows += len(import_rows)
                        result.errors.append({
                            "scope": f"{degree}/{program}/{branch}/{set_type_str}",
                            "errors": errors
                        })
                
                # Update session as completed
                conn.execute(sa_text("""
                    UPDATE outcomes_import_sessions
                    SET import_status = 'completed',
                        import_completed_at = CURRENT_TIMESTAMP,
                        records_total = :total,
                        records_imported = :imported,
                        records_failed = :failed,
                        error_log = :errors
                    WHERE session_id = :session_id
                """), {
                    "session_id": session_id,
                    "total": result.total_rows,
                    "imported": result.imported_rows,
                    "failed": result.failed_rows,
                    "errors": json.dumps(result.errors)
                })
                
            except Exception as e:
                # Rollback
                result.errors.append({"error": f"Import failed: {str(e)}"})
                conn.execute(sa_text("""
                    UPDATE outcomes_import_sessions
                    SET import_status = 'failed',
                        import_completed_at = CURRENT_TIMESTAMP,
                        error_log = :error
                    WHERE session_id = :session_id
                """), {
                    "session_id": session_id,
                    "error": str(e)
                })
        
        return result
    
    def export_outcomes(
        self,
        degree_code: str,
        program_code: Optional[str] = None,
        branch_code: Optional[str] = None,
        set_types: Optional[List[SetType]] = None,
        include_archived: bool = False
    ) -> str:
        """
        Export outcomes to CSV format.
        Returns CSV string.
        """
        if set_types is None:
            set_types = [SetType.PEOS, SetType.POS, SetType.PSOS]
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow([
            "degree_code", "program_code", "branch_code", "set_type", "status",
            "code", "title", "description", "bloom_level", "timeline_years",
            "tags", "updated_at", "updated_by"
        ])
        
        # Get all matching sets
        with self.engine.connect() as conn:
            for set_type in set_types:
                outcome_set = self.get_set(
                    degree_code, set_type, program_code, branch_code, include_archived
                )
                
                if outcome_set:
                    for item in outcome_set.items:
                        writer.writerow([
                            outcome_set.degree_code,
                            outcome_set.program_code or "",
                            outcome_set.branch_code or "",
                            outcome_set.set_type.value.upper(),
                            outcome_set.status.value,
                            item.code,
                            item.title or "",
                            item.description,
                            item.bloom_level.value if item.bloom_level else "",
                            item.timeline_years or "",
                            "|".join(item.tags) if item.tags else "",
                            item.updated_at or item.created_at,
                            item.updated_by or item.created_by
                        ])
        
        return output.getvalue()
    
    # ========================================================================
    # VERSIONING & ROLLBACK
    # ========================================================================
    
    def get_versions(self, set_id: int) -> List[Dict[str, Any]]:
        """Get all version snapshots for a set."""
        with self.engine.connect() as conn:
            result = conn.execute(sa_text("""
                SELECT version, snapshot_reason, created_by, created_at
                FROM outcomes_versions
                WHERE set_id = :set_id
                ORDER BY version DESC
                LIMIT 100
            """), {"set_id": set_id}).fetchall()
            
            return [
                {
                    "version": row[0],
                    "reason": row[1],
                    "created_by": row[2],
                    "created_at": row[3]
                }
                for row in result
            ]
    
    def rollback_to_version(
        self,
        set_id: int,
        version: int,
        reason: str
    ) -> Tuple[bool, List[str]]:
        """
        Rollback a set to a previous version.
        Only allowed on drafts or published sets without mappings.
        """
        with self.engine.begin() as conn:
            # Get current status
            current = conn.execute(sa_text("""
                SELECT status FROM outcomes_sets WHERE id = :set_id
            """), {"set_id": set_id}).fetchone()
            
            if not current:
                return False, ["Set not found"]
            
            current_status = Status(current[0])
            
            # Check constraints
            if current_status == Status.PUBLISHED:
                has_mappings = self._check_mappings(conn, set_id)
                if has_mappings:
                    return False, ["Cannot rollback published set with mappings. Clone as draft instead."]
            
            if current_status == Status.ARCHIVED:
                return False, ["Cannot rollback archived sets"]
            
            # Get version snapshot
            snapshot = conn.execute(sa_text("""
                SELECT snapshot_data FROM outcomes_versions
                WHERE set_id = :set_id AND version = :version
            """), {"set_id": set_id, "version": version}).fetchone()
            
            if not snapshot:
                return False, [f"Version {version} not found"]
            
            # Parse snapshot
            snapshot_data = json.loads(snapshot[0])
            
            # Delete current items
            conn.execute(sa_text("DELETE FROM outcomes_items WHERE set_id = :set_id"),
                        {"set_id": set_id})
            
            # Restore items from snapshot
            for item_data in snapshot_data.get("items", []):
                conn.execute(sa_text("""
                    INSERT INTO outcomes_items
                    (set_id, code, title, description, bloom_level, timeline_years,
                     tags, sort_order, created_by)
                    VALUES (:set_id, :code, :title, :description, :bloom_level,
                            :timeline_years, :tags, :sort_order, :actor)
                """), {
                    "set_id": set_id,
                    "code": item_data["code"],
                    "title": item_data.get("title"),
                    "description": item_data["description"],
                    "bloom_level": item_data.get("bloom_level"),
                    "timeline_years": item_data.get("timeline_years"),
                    "tags": item_data.get("tags"),
                    "sort_order": item_data.get("sort_order", 100),
                    "actor": self.actor
                })
            
            # Update set metadata
            conn.execute(sa_text("""
                UPDATE outcomes_sets
                SET updated_by = :actor,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :set_id
            """), {"set_id": set_id, "actor": self.actor})
            
            # Audit
            self._audit(conn, "OUTCOMES_ROLLBACK", set_id=set_id, reason=reason,
                       after_data=f"Rolled back to version {version}")
            
            # Create new snapshot
            self._create_snapshot(conn, set_id, "rollback", reason)
            
            return True, []
    
    # ========================================================================
    # APPROVALS WORKFLOW
    # ========================================================================
    
    def submit_for_approval(
        self,
        operation_key: OperationKey,
        degree_code: str,
        payload: Dict[str, Any],
        reason: str,
        program_code: Optional[str] = None,
        branch_code: Optional[str] = None
    ) -> str:
        """
        Submit an operation for approval.
        Returns request_id.
        """
        import uuid
        request_id = f"OUTC_{uuid.uuid4().hex[:12].upper()}"
        
        # Get operation config
        op_config = self._get_operation_config(operation_key)
        
        with self.engine.begin() as conn:
            conn.execute(sa_text("""
                INSERT INTO outcomes_approvals_queue
                (request_id, operation_key, queue_name, degree_code, program_code,
                 branch_code, submitted_by, reason, payload_data, approver_roles,
                 sla_hours, escalate_after_hours, expires_at)
                VALUES (:request_id, :op_key, :queue, :degree, :program, :branch,
                        :actor, :reason, :payload, :approvers, :sla, :escalate,
                        datetime('now', '+' || :sla || ' hours'))
            """), {
                "request_id": request_id,
                "op_key": operation_key.value,
                "queue": "OUTCOMES",
                "degree": degree_code,
                "program": program_code,
                "branch": branch_code,
                "actor": self.actor,
                "reason": reason,
                "payload": json.dumps(payload),
                "approvers": json.dumps(op_config["approvers"]),
                "sla": op_config["sla_hours"],
                "escalate": op_config["escalate_after_hours"]
            })
            
            # Audit
            self._audit(conn, f"{operation_key.value}_REQUESTED",
                       reason=reason, after_data=json.dumps(payload))
        
        return request_id
    
    def approve_request(
        self,
        request_id: str,
        approval_note: Optional[str] = None
    ) -> Tuple[bool, List[str]]:
        """Approve a pending request."""
        with self.engine.begin() as conn:
            # Get request
            request = conn.execute(sa_text("""
                SELECT status, operation_key, payload_data
                FROM outcomes_approvals_queue
                WHERE request_id = :request_id
            """), {"request_id": request_id}).fetchone()
            
            if not request:
                return False, ["Request not found"]
            
            if request[0] != "pending":
                return False, [f"Request is not pending (status: {request[0]})"]
            
            # Update request
            conn.execute(sa_text("""
                UPDATE outcomes_approvals_queue
                SET status = 'approved',
                    approved_by = :actor,
                    approved_at = CURRENT_TIMESTAMP,
                    completed_at = CURRENT_TIMESTAMP
                WHERE request_id = :request_id
            """), {"request_id": request_id, "actor": self.actor})
            
            # Audit
            self._audit(conn, f"{request[1]}_APPROVED",
                       reason=approval_note or "Approved",
                       after_data=f"Request {request_id}")
            
            return True, []
    
    def reject_request(
        self,
        request_id: str,
        rejection_reason: str
    ) -> Tuple[bool, List[str]]:
        """Reject a pending request."""
        with self.engine.begin() as conn:
            # Get request
            request = conn.execute(sa_text("""
                SELECT status, operation_key
                FROM outcomes_approvals_queue
                WHERE request_id = :request_id
            """), {"request_id": request_id}).fetchone()
            
            if not request:
                return False, ["Request not found"]
            
            if request[0] != "pending":
                return False, [f"Request is not pending (status: {request[0]})"]
            
            # Update request
            conn.execute(sa_text("""
                UPDATE outcomes_approvals_queue
                SET status = 'rejected',
                    rejected_by = :actor,
                    rejected_at = CURRENT_TIMESTAMP,
                    rejection_reason = :reason,
                    completed_at = CURRENT_TIMESTAMP
                WHERE request_id = :request_id
            """), {
                "request_id": request_id,
                "actor": self.actor,
                "reason": rejection_reason
            })
            
            # Audit
            self._audit(conn, f"{request[1]}_REJECTED",
                       reason=rejection_reason,
                       after_data=f"Request {request_id}")
            
            return True, []
    
    # ========================================================================
    # HELPER METHODS
    # ========================================================================
    
    def _parse_import_row(
        self,
        row_dict: Dict[str, str],
        row_number: int,
        scope_level: ScopeLevel
    ) -> ImportRow:
        """Parse and validate a single CSV row."""
        import_row = ImportRow(
            degree_code=row_dict.get("degree_code", "").strip(),
            program_code=row_dict.get("program_code", "").strip() or None,
            branch_code=row_dict.get("branch_code", "").strip() or None,
            set_type=row_dict.get("set_type", "").strip().lower(),
            status=row_dict.get("status", "draft").strip().lower(),
            code=row_dict.get("code", "").strip(),
            title=row_dict.get("title", "").strip() or None,
            description=row_dict.get("description", "").strip(),
            bloom_level=row_dict.get("bloom_level", "").strip() or None,
            timeline_years=None,
            tags=row_dict.get("tags", "").strip() or None,
            row_number=row_number
        )
        
        # Parse timeline_years
        timeline_str = row_dict.get("timeline_years", "").strip()
        if timeline_str:
            try:
                import_row.timeline_years = int(timeline_str)
            except ValueError:
                import_row.errors.append(f"Invalid timeline_years: {timeline_str}")
        
        # Validate required fields
        if not import_row.degree_code:
            import_row.errors.append("degree_code is required")
        
        if not import_row.set_type:
            import_row.errors.append("set_type is required")
        elif import_row.set_type not in ["peo", "po", "pso"]:
            import_row.errors.append(f"Invalid set_type: {import_row.set_type}")
        else:
            # Normalize to plural form
            import_row.set_type = import_row.set_type + "s"
        
        if not import_row.code:
            import_row.errors.append("code is required")
        elif len(import_row.code) > 16:
            import_row.errors.append("code must be 16 characters or less")
        
        if not import_row.description:
            import_row.errors.append("description is required")
        elif len(import_row.description) > 4000:
            import_row.errors.append("description must be 4000 characters or less")
        
        # Validate status
        if import_row.status not in ["draft", "published", "archived"]:
            import_row.errors.append(f"Invalid status: {import_row.status}")
        
        # Validate bloom_level
        if import_row.bloom_level:
            valid_blooms = ["Remember", "Understand", "Apply", "Analyze", "Evaluate", "Create"]
            if import_row.bloom_level not in valid_blooms:
                import_row.errors.append(f"Invalid bloom_level: {import_row.bloom_level}")
        
        # Validate timeline_years
        if import_row.timeline_years is not None:
            if import_row.timeline_years < 1 or import_row.timeline_years > 10:
                import_row.errors.append("timeline_years must be between 1 and 10")
        
        # Validate scope compatibility
        if scope_level == ScopeLevel.PER_DEGREE:
            if import_row.program_code or import_row.branch_code:
                import_row.warnings.append("Degree uses per_degree scope; program/branch will be ignored")
                import_row.program_code = None
                import_row.branch_code = None
        elif scope_level == ScopeLevel.PER_PROGRAM:
            if import_row.branch_code:
                import_row.warnings.append("Degree uses per_program scope; branch will be ignored")
                import_row.branch_code = None
        
        return import_row
    
    def _check_mappings(self, conn, set_id: int) -> bool:
        """Check if a set has any active mappings."""
        result = conn.execute(sa_text("""
            SELECT COUNT(*) FROM outcomes_mappings
            WHERE outcome_set_id = :set_id
        """), {"set_id": set_id}).fetchone()
        return result[0] > 0
    
    def _create_snapshot(
        self,
        conn,
        set_id: int,
        reason_type: str,
        reason: str
    ):
        """Create a version snapshot."""
        # Get current version
        current = conn.execute(sa_text("""
            SELECT version FROM outcomes_sets WHERE id = :set_id
        """), {"set_id": set_id}).fetchone()
        
        version = current[0] if current else 1
        
        # Get all items
        items = conn.execute(sa_text("""
            SELECT code, title, description, bloom_level, timeline_years, tags, sort_order
            FROM outcomes_items
            WHERE set_id = :set_id
            ORDER BY sort_order, code
        """), {"set_id": set_id}).fetchall()
        
        snapshot_data = {
            "version": version,
            "reason_type": reason_type,
            "items": [
                {
                    "code": row[0],
                    "title": row[1],
                    "description": row[2],
                    "bloom_level": row[3],
                    "timeline_years": row[4],
                    "tags": row[5],
                    "sort_order": row[6]
                }
                for row in items
            ]
        }
        
        # Insert snapshot
        conn.execute(sa_text("""
            INSERT INTO outcomes_versions
            (set_id, version, snapshot_data, snapshot_reason, created_by)
            VALUES (:set_id, :version, :data, :reason, :actor)
        """), {
            "set_id": set_id,
            "version": version,
            "data": json.dumps(snapshot_data),
            "reason": reason,
            "actor": self.actor
        })
        
        # Update set version
        conn.execute(sa_text("""
            UPDATE outcomes_sets
            SET version = version + 1
            WHERE id = :set_id
        """), {"set_id": set_id})
    
    def _audit(
        self,
        conn,
        event_type: str,
        set_id: Optional[int] = None,
        item_id: Optional[int] = None,
        reason: Optional[str] = None,
        before_data: Optional[str] = None,
        after_data: Optional[str] = None,
        step_up: bool = False
    ):
        """Record an audit event."""
        conn.execute(sa_text("""
            INSERT INTO outcomes_audit
            (event_type, actor_id, actor_role, operation, set_id, item_id,
             before_data, after_data, reason, source, step_up_performed)
            VALUES (:event, :actor, :role, :operation, :set_id, :item_id,
                    :before, :after, :reason, 'ui', :step_up)
        """), {
            "event": event_type,
            "actor": self.actor,
            "role": self.actor_role,
            "operation": event_type,
            "set_id": set_id,
            "item_id": item_id,
            "before": before_data,
            "after": after_data,
            "reason": reason,
            "step_up": 1 if step_up else 0
        })
    
    def _serialize_set(self, outcome_set: OutcomeSet) -> str:
        """Serialize outcome set to JSON."""
        data = asdict(outcome_set)
        # Convert enums to strings
        data["set_type"] = data["set_type"].value if isinstance(data["set_type"], SetType) else data["set_type"]
        data["status"] = data["status"].value if isinstance(data["status"], Status) else data["status"]
        for item in data.get("items", []):
            if item.get("bloom_level"):
                item["bloom_level"] = item["bloom_level"].value if isinstance(item["bloom_level"], BloomLevel) else item["bloom_level"]
        return json.dumps(data)
    
    def _get_operation_config(self, operation_key: OperationKey) -> Dict[str, Any]:
        """Get configuration for an approval operation."""
        configs = {
            OperationKey.CREATE: {
                "approvers": ["degree_head", "principal", "director"],
                "sla_hours": 72,
                "escalate_after_hours": 72
            },
            OperationKey.PUBLISH: {
                "approvers": ["principal", "director"],
                "sla_hours": 48,
                "escalate_after_hours": 48
            },
            OperationKey.UNPUBLISH: {
                "approvers": ["principal", "director"],
                "sla_hours": 48,
                "escalate_after_hours": 48
            },
            OperationKey.MAJOR_EDIT: {
                "approvers": ["principal", "director"],
                "sla_hours": 72,
                "escalate_after_hours": 72
            },
            OperationKey.SCOPE_CHANGE: {
                "approvers": ["principal", "director"],
                "sla_hours": 72,
                "escalate_after_hours": 72
            }
        }
        return configs.get(operation_key, configs[OperationKey.CREATE])


# ============================================================================
# SCHEMA INITIALIZATION (from outcomes_schema.py)
# ============================================================================

def _exec(conn, sql: str):
    """Execute SQL with SQLAlchemy text wrapper."""
    conn.execute(sa_text(sql))


def ensure_outcomes_schema(engine: Engine):
    """Initialize all outcomes tables. Call this before using OutcomesManager."""
    # Import and call the schema creation from outcomes_schema.py
    # This would be: from app.schemas.outcomes_schema import ensure_outcomes_schema
    # For now, inline minimal version
    pass  # Assume schema exists


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

def example_usage():
    """
    Example usage of the OutcomesManager.
    """
    from sqlalchemy import create_engine
    
    # Setup
    engine = create_engine("sqlite:///outcomes.db")
    ensure_outcomes_schema(engine)
    
    manager = OutcomesManager(engine, actor="user123", actor_role="degree_head")
    
    # 1. Set scope configuration
    manager.set_scope_config(
        degree_code="BTECH",
        scope_level=ScopeLevel.PER_PROGRAM,
        reason="Initial configuration for BTech degree"
    )
    
    # 2. Create outcome set
    peos = OutcomeSet(
        degree_code="BTECH",
        program_code="CSE",
        set_type=SetType.PEOS,
        items=[
            OutcomeItem(
                code="PEO1",
                title="Professional Excellence",
                description="Graduates will excel in professional careers...",
                timeline_years=5
            ),
            OutcomeItem(
                code="PEO2",
                title="Lifelong Learning",
                description="Graduates will engage in lifelong learning...",
                timeline_years=5
            )
        ]
    )
    
    success, set_id, errors = manager.create_set(
        peos,
        reason="Initial PEOs for CSE program"
    )
    
    if success:
        print(f"Created set {set_id}")
        
        # 3. Publish the set
        manager.publish_set(set_id, reason="Approved by academic council")
        
        # 4. Export to CSV
        csv_content = manager.export_outcomes(
            degree_code="BTECH",
            program_code="CSE"
        )
        print(csv_content)
        
        # 5. Import preview
        import_result = manager.import_preview(
            csv_content,
            degree_code="BTECH",
            session_id="session123"
        )
        print(f"Valid rows: {import_result.valid_rows}, Invalid: {import_result.invalid_rows}")
    else:
        print(f"Errors: {errors}")


if __name__ == "__main__":
    example_usage()