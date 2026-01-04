import streamlit as st
import pandas as pd
import json
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("🛡️ WorkSafe AI – Workforce Safety Intelligence")

st.markdown("""
Upload work-hour data and detect unsafe scheduling patterns.
This tool adapts to different industries and focuses on wellbeing and safety.
""")

industry = st.selectbox(
    "Select Industry",
    ["Healthcare", "IT"]
)

st.subheader("📤 Upload Shift Data (CSV)")
uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)

    st.write("Raw columns detected:", list(df.columns))

    if len(df.columns) == 1:
        df = df[df.columns[0]].str.split(",", expand=True)
        df.columns = ["staff_id", "role", "shift_start", "shift_end"]
    else:
        df.columns = df.columns.str.strip().str.lower()

    st.write("Final columns used:", list(df.columns))
    st.write("Preview", df.head())

    for _, row in df.iterrows():
        row_dict = {
            "staff_id": str(row["staff_id"]),
            "role": str(row["role"]),
            "shift_start": str(row["shift_start"]),
            "shift_end": str(row["shift_end"])
        }

        session.sql(
            f"""
            INSERT INTO RAW_WORK_DATA (ORG_NAME, INDUSTRY, DEPARTMENT, RAW_PAYLOAD)
            SELECT
              'DemoOrg',
              '{industry}',
              'General',
              PARSE_JSON('{json.dumps(row_dict)}')
            """
        ).collect()

    st.success("Data uploaded successfully!")

st.subheader("⚠️ Unsafe Work-Hour Patterns Detected")

risk_df = session.sql("""
    SELECT INDUSTRY, STAFF_ID, RISK_FLAG, AI_EXPLANATION
    FROM AI_RISK_SUMMARY
""").to_pandas()

st.dataframe(risk_df)
