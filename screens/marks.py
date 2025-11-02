from __future__ import annotations
import streamlit as st
from sqlalchemy import text
from core.settings import load_settings
from core.db import get_engine, init_db, SessionLocal
from core.forms import tagline, success, warn

def render():
    st.title("Marks Entry (Demo)")
    tagline()
    settings = load_settings()
    engine = get_engine(settings.db.url)
    init_db(engine)
    SessionLocal.configure(bind=engine)

    with engine.begin() as conn:
        degree_rows = conn.execute(text("SELECT code FROM degrees WHERE status='active' ORDER BY code"))
        degree_codes = [r[0] for r in degree_rows]

    if not degree_codes:
        warn("No active degrees found. Add a degree first.")
        return
    degree_code = st.selectbox("Degree", degree_codes, index=0)

    with engine.begin() as conn:
        assgn_rows = conn.execute(text("""        SELECT assignment_code, title, max_marks FROM assignments
        WHERE degree_code=:d ORDER BY updated_at DESC
        """), dict(d=degree_code))
        assignments = [dict(code=r[0], title=r[1], max=r[2]) for r in assgn_rows]

    if not assignments:
        warn("No assignments found for this degree. Create one first.")
        return

    label = [f"{a['code']} — {a['title']} (/{a['max']})" for a in assignments]
    pick = st.selectbox("Assignment", label, index=0)
    assgn = assignments[label.index(pick)]

    with st.form("marks_form"):
        student_id = st.text_input("Student ID", value="STU-0001")
        mark = st.number_input("Marks obtained", min_value=0.0, max_value=float(assgn['max']), value=0.0, step=1.0)
        submitted = st.form_submit_button("Save mark")
        if submitted:
            with engine.begin() as conn:
                conn.execute(text("""                INSERT INTO marks (degree_code, assignment_code, student_id, marks_obtained)
                VALUES (:d, :a, :s, :m)
                ON CONFLICT(degree_code, assignment_code, student_id) DO UPDATE
                SET marks_obtained=excluded.marks_obtained
                """), dict(d=degree_code, a=assgn['code'], s=student_id, m=float(mark)))
            success("Saved!")
            st.rerun()

    st.subheader("Marks for this assignment")
    with engine.begin() as conn:
        rows = conn.execute(text("""        SELECT student_id, marks_obtained, updated_at
        FROM marks WHERE degree_code=:d AND assignment_code=:a ORDER BY updated_at DESC
        """), dict(d=degree_code, a=assgn['code']))
        st.dataframe([dict(r._mapping) for r in rows])

render()      
