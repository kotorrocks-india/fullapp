# screens/office_admin/importer.py
from __future__ import annotations
import pandas as pd
import streamlit as st
from sqlalchemy import text as sa_text

def render_import_export(engine):
    st.subheader("Import / Export (CSV) — Locations, Rooms, Assets")
    t = st.selectbox("Entity", ["office_locations","office_rooms","office_assets"], index=0)

    col1, col2 = st.columns(2)

    # Export
    with col1:
        st.write("**Export CSV**")
        if st.button("Download"):
            with engine.begin() as conn:
                rows = conn.execute(sa_text(f"SELECT * FROM {t}")).fetchall()
            df = pd.DataFrame([dict(r) for r in rows])
            if df.empty:
                st.info("No records.")
            else:
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button("Save CSV", data=csv, file_name=f"{t}.csv", mime="text/csv")

    # Import
    with col2:
        st.write("**Import CSV**")
        file = st.file_uploader("Upload CSV", type=["csv"])
        if file is not None:
            df = pd.read_csv(file)
            st.dataframe(df.head(20))
            if st.button("Import Now"):
                with engine.begin() as conn:
                    cols = ",".join([f'"{c}"' for c in df.columns])
                    ph = ",".join([":"+c for c in df.columns])
                    sql = sa_text(f"INSERT INTO {t} ({cols}) VALUES ({ph}) ON CONFLICT DO NOTHING")
                    for _, row in df.iterrows():
                        conn.execute(sql, row.to_dict())
                st.success("Import completed.")
