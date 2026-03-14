import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

DB_DEFAULT = "/Users/yegor/Documents/Agency & Security Stuff/BIZ/leadgen/data/leads.db"


def load_df(db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        """
        SELECT b.id, b.name, b.category, b.rating, b.address, b.phone, b.website, b.maps_url, b.query,
               w.about_text, w.services_text,
               e.classification, e.pain_points, e.outreach_message
        FROM businesses b
        LEFT JOIN website_data w ON w.business_id = b.id
        LEFT JOIN enrichment e ON e.business_id = b.id
        ORDER BY b.id DESC
        """,
        conn,
    )
    conn.close()
    return df


def save_edits(db_path: str, edited: pd.DataFrame) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    for _, row in edited.iterrows():
        cur.execute(
            """
            UPDATE businesses
            SET name=?, category=?, rating=?, address=?, phone=?, website=?, maps_url=?, query=?
            WHERE id=?
            """,
            (
                row["name"],
                row["category"],
                row["rating"],
                row["address"],
                row["phone"],
                row["website"],
                row["maps_url"],
                row["query"],
                int(row["id"]),
            ),
        )
    conn.commit()
    conn.close()


def main() -> None:
    st.set_page_config(page_title="Lead Review", layout="wide")
    st.title("Lead Review and Edit")

    db_path = st.text_input("DB Path", value=DB_DEFAULT)
    if not Path(db_path).exists():
        st.error("DB not found")
        return

    df = load_df(db_path)
    st.write(f"Loaded {len(df)} leads")

    editable_cols = [
        "id",
        "name",
        "category",
        "rating",
        "address",
        "phone",
        "website",
        "maps_url",
        "query",
    ]

    edited = st.data_editor(df[editable_cols], num_rows="dynamic", use_container_width=True)

    if st.button("Save edits"):
        save_edits(db_path, edited)
        st.success("Saved")

    st.subheader("Outreach Drafts")
    st.dataframe(df[["id", "classification", "pain_points", "outreach_message"]], use_container_width=True)


if __name__ == "__main__":
    main()
