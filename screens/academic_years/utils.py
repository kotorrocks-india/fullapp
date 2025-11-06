# screens/academic_years/utils.py
from __future__ import annotations

import re
import datetime
import logging
import json
import streamlit as st

logger = logging.getLogger(__name__)

# --- Fallback error handler (if faculty utils not available) ---
try:
    # Reuse central toast / error UI if present
    from screens.faculty.utils import _handle_error  # type: ignore
except Exception:  # pragma: no cover - fallback
    def _handle_error(e: Exception, user_message: str = "An error occurred.") -> None:
        logger.error(user_message, exc_info=True)
        st.error(user_message)


# --------------------------------------------------------------------
# Academic year helpers
# --------------------------------------------------------------------

# Allows optional "AY" prefix (any case) and "/" or "-" as separator,
# e.g. "2025-26", "2025/26", "AY2025-26", "ay2025/26"
AY_CODE_PATTERN = re.compile(r"^(?:[Aa][Yy])?\d{4}[-/]\d{2}$")


def is_valid_ay_code(ay_code: str) -> bool:
    """Validate the basic AY code shape."""
    return bool(ay_code and AY_CODE_PATTERN.match(ay_code))


def validate_date_format(date_str: str) -> bool:
    """True if date_str is ISO-8601 (YYYY-MM-DD)."""
    try:
        datetime.date.fromisoformat(date_str)
        return True
    except Exception:
        return False


def _get_year_from_ay_code(ay_code: str) -> int | None:
    """
    Extract the 4-digit "start year" from an AY code.

    Examples
    --------
    - "2025-26"      -> 2025
    - "AY2025/26"    -> 2025
    - "ay2024-25"    -> 2024
    """
    if not ay_code:
        return None
    match = re.search(r"(\d{4})", ay_code)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def get_next_ay_code(current_ay_code: str) -> str | None:
    """
    Generates the next AY code (e.g., "2026-27") from a given code.
    Always returns the standard "YYYY-YY" format.
    """
    if not is_valid_ay_code(current_ay_code):
        return None

    base = _get_year_from_ay_code(current_ay_code)
    if base is None:
        return None

    nxt = base + 1
    yy = (nxt + 1) % 100
    return f"{nxt}-{yy:02d}"


def validate_ay_code_dates(ay_code: str, start_date: datetime.date) -> bool:
    """
    Check if the AY code's start year aligns logically with the
    start date's calendar year.

    We allow either the AY start year or the previous calendar year
    (to support AYs that "start" a bit earlier, e.g. June vs July).
    """
    if not is_valid_ay_code(ay_code):
        return False

    ay_start_year = _get_year_from_ay_code(ay_code)
    if ay_start_year is None:
        return False

    date_year = start_date.year
    return date_year in (ay_start_year, ay_start_year - 1)


def generate_ay_range(start_ay: str, num_years: int) -> list[str]:
    """
    Generate a list of consecutive AY codes starting from `start_ay`.

    Example
    -------
    generate_ay_range("2024-25", 3) -> ["2024-25", "2025-26", "2026-27"]
    """
    if not is_valid_ay_code(start_ay):
        return []
    out: list[str] = []
    cur = start_ay
    for _ in range(num_years):
        out.append(cur)
        cur = get_next_ay_code(cur)
        if not cur:
            break
    return out


# --------------------------------------------------------------------
# Calendar profile helpers
# --------------------------------------------------------------------


def _mmdd_to_date(
    ay_start_year: int,
    mmdd: str,
    anchor_mmdd: str | None = None,
) -> datetime.date:
    """
    Map "MM-DD" to a concrete date within the AY span.

    If `anchor_mmdd` is provided, we treat that month/day as the boundary
    between "this AY's calendar year" and "next AY's calendar year":

        - Dates >= anchor_mmdd belong to `ay_start_year`
        - Dates <  anchor_mmdd belong to `ay_start_year + 1`

    This lets a profile whose anchor is "06-15" (June 15) represent an
    AY that runs June -> next April, for example.

    If no anchor is provided, we fall back to the previous behaviour
    where July (month=7) is the year boundary.
    """
    mm, dd = map(int, mmdd.split("-"))

    if anchor_mmdd:
        a_mm, a_dd = map(int, anchor_mmdd.split("-"))
        if (mm, dd) >= (a_mm, a_dd):
            year = ay_start_year
        else:
            year = ay_start_year + 1
    else:
        # Legacy behaviour: everything from July onwards is in ay_start_year
        year = ay_start_year if mm >= 7 else ay_start_year + 1

    return datetime.date(year, mm, dd)


def compute_term_windows_for_ay(
    profile: dict,
    ay_code: str,
    shift_days: int = 0,
) -> list[dict]:
    """
    Given a stored calendar profile (with JSON spec) and AY code, produce
    concrete term windows:

        [{ "label", "start_date", "end_date" }, ...]

    The profile is expected to contain:
        - term_spec_json: JSON list of {label, start_mmdd, end_mmdd}
        - anchor_mmdd:    string "MM-DD" used as year boundary (optional)

    The same logic applies to *all* terms in the profile, not just a
    specific semester (e.g. 9 or 10).
    """
    if not is_valid_ay_code(ay_code):
        raise ValueError("Invalid AY code.")
    if shift_days < -30 or shift_days > 30:
        raise ValueError("shift_days must be between -30 and +30.")

    # May raise if malformed, intentionally.
    spec = json.loads(profile.get("term_spec_json") or "[]")

    ay_start_year = _get_year_from_ay_code(ay_code)
    if ay_start_year is None:
        raise ValueError("Invalid AY code format for year extraction.")

    anchor_mmdd = profile.get("anchor_mmdd")

    results: list[dict] = []
    for idx, term in enumerate(spec):
        label = term.get("label") or f"Term {idx + 1}"
        start_mmdd = term["start_mmdd"]
        end_mmdd = term["end_mmdd"]

        start_dt = _mmdd_to_date(ay_start_year, start_mmdd, anchor_mmdd)
        end_dt = _mmdd_to_date(ay_start_year, end_mmdd, anchor_mmdd)

        # If computed end < start (e.g. a wrap over New Year), bump end one year.
        if end_dt < start_dt:
            end_dt = datetime.date(end_dt.year + 1, end_dt.month, end_dt.day)

        if shift_days:
            delta = datetime.timedelta(days=shift_days)
            start_dt += delta
            end_dt += delta

        results.append(
            {
                "label": label,
                "start_date": start_dt.isoformat(),
                "end_date": end_dt.isoformat(),
            }
        )

    return results
