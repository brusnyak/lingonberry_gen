import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st
from datetime import datetime

_HERE = Path(__file__).parent
DB_DEFAULT = str(_HERE / "data/leads.db")


def load_df(db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        """
        SELECT b.id, b.name, b.category, b.rating, b.address, b.phone, b.website, b.maps_url, b.query,
               b.approved, b.approved_at, b.score, b.score_reason,
               w.about_text, w.services_text,
               e.industry, e.role, e.icp_fit, e.pain_points, e.outreach_message
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
        approved_val = int(row["approved"]) if row["approved"] in [0, 1] else int(bool(row["approved"]))
        approved_at = row.get("approved_at")
        if approved_val == 1 and (approved_at is None or str(approved_at).strip() == ""):
            approved_at = datetime.utcnow().isoformat()
        cur.execute(
            """
            UPDATE businesses
            SET name=?, category=?, rating=?, address=?, phone=?, website=?, maps_url=?, query=?, approved=?, approved_at=?
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
                approved_val,
                approved_at,
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

    st.subheader("Review Queue (Approve before enrichment)")
    min_score = st.slider("Min score", 0, 100, 20)
    unapproved = df[df["approved"] != 1].copy()
    unapproved = unapproved[unapproved["score"].fillna(0) >= min_score]
    st.write(f"Unapproved leads: {len(unapproved)}")
    queue_cols = [
        "id",
        "name",
        "category",
        "rating",
        "address",
        "phone",
        "website",
        "maps_url",
        "query",
        "approved",
        "approved_at",
        "score",
        "score_reason",
    ]
    edited_queue = st.data_editor(unapproved[queue_cols], num_rows="dynamic", use_container_width=True)
    if st.button("Save approvals"):
        save_edits(db_path, edited_queue)
        st.success("Saved approvals")

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
        "approved",
        "approved_at",
    ]

    edited = st.data_editor(df[editable_cols], num_rows="dynamic", use_container_width=True)

    if st.button("Save edits"):
        save_edits(db_path, edited)
        st.success("Saved")

    st.subheader("Outreach Drafts")
    st.dataframe(df[["id", "industry", "role", "icp_fit", "pain_points", "outreach_message"]], use_container_width=True)


if __name__ == "__main__":
    main()
