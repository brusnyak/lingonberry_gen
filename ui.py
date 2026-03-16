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
               b.validation_status, b.validation_notes,
               b.site_reachable, b.has_email, b.has_phone, b.has_socials,
               b.brand_summary, b.apparent_size, b.digital_maturity,
               b.pain_point_guess, b.outreach_angle,
               b.site_qualification, b.site_qual_reason,
               w.about_text, w.services_text, w.emails, w.socials, w.tech_stack,
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


def save_validation(db_path: str, rows: list[dict]) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    for row in rows:
        cur.execute(
            "UPDATE businesses SET validation_status=?, validation_notes=? WHERE id=?",
            (row["validation_status"], row.get("validation_notes", ""), int(row["id"])),
        )
    conn.commit()
    conn.close()


def main() -> None:
    st.set_page_config(page_title="Lead Review", layout="wide")
    st.title("Lead Review")

    db_path = st.text_input("DB Path", value=DB_DEFAULT)
    if not Path(db_path).exists():
        st.error("DB not found")
        return

    df = load_df(db_path)
    st.write(f"Loaded {len(df)} leads")

    tab_approve, tab_intel, tab_validate, tab_qualified, tab_outreach = st.tabs(
        ["Approve", "Site Intel", "Validation", "Qualified", "Outreach Drafts"]
    )

    # ---- Tab 1: Approve ----
    with tab_approve:
        st.subheader("Review Queue")
        min_score = st.slider("Min score", 0, 100, 20)
        unapproved = df[df["approved"] != 1].copy()
        unapproved = unapproved[unapproved["score"].fillna(0) >= min_score]
        st.write(f"Unapproved: {len(unapproved)}")
        queue_cols = ["id", "name", "category", "rating", "address", "phone", "website",
                      "maps_url", "query", "approved", "approved_at", "score", "score_reason"]
        edited_queue = st.data_editor(unapproved[queue_cols], num_rows="dynamic", use_container_width=True)
        if st.button("Save approvals"):
            save_edits(db_path, edited_queue)
            st.success("Saved")

        st.divider()
        editable_cols = ["id", "name", "category", "rating", "address", "phone", "website",
                         "maps_url", "query", "approved", "approved_at"]
        edited = st.data_editor(df[editable_cols], num_rows="dynamic", use_container_width=True)
        if st.button("Save edits"):
            save_edits(db_path, edited)
            st.success("Saved")

    # ---- Tab 2: Site Intel ----
    with tab_intel:
        st.subheader("Website Intelligence")

        has_website = df[df["website"].notna() & (df["website"] != "")]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Has website", len(has_website))
        col2.metric("Reachable", int(df["site_reachable"].fillna(0).sum()) if "site_reachable" in df.columns else "—")
        col3.metric("Has email", int(df["has_email"].fillna(0).sum()) if "has_email" in df.columns else "—")
        col4.metric("Has socials", int(df["has_socials"].fillna(0).sum()) if "has_socials" in df.columns else "—")

        st.divider()
        intel_filter = st.selectbox("Filter by site qualification", ["all", "strong", "moderate", "weak", "skip", "unknown"])
        intel_df = df.copy()
        if intel_filter != "all":
            intel_df = intel_df[intel_df["site_qualification"] == intel_filter]

        intel_cols = ["id", "name", "category", "website", "site_reachable", "has_email",
                      "has_socials", "tech_stack", "apparent_size", "digital_maturity",
                      "site_qualification", "brand_summary", "pain_point_guess", "outreach_angle", "site_qual_reason"]
        # only show cols that exist
        show_cols = [c for c in intel_cols if c in intel_df.columns]
        st.dataframe(intel_df[show_cols], use_container_width=True)

        st.divider()
        st.subheader("Run Site Intel")
        use_ai_intel = st.checkbox("Use AI brand evaluation", value=True, key="intel_ai")
        only_missing_intel = st.checkbox("Only leads without intel yet", value=True, key="intel_missing")
        if st.button("Run Site Intel Now"):
            import sys, os
            _leadgen_dir = str(Path(db_path).parent.parent)
            if _leadgen_dir not in sys.path:
                sys.path.insert(0, _leadgen_dir)
            _env = Path(db_path).parent.parent.parent / ".env"
            if _env.exists():
                for line in _env.read_text().splitlines():
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, _, v = line.partition("=")
                        os.environ.setdefault(k.strip(), v.strip())
            from validation.website_intel import run_website_intel, _ensure_columns
            import sqlite3 as _sq
            _conn = _sq.connect(db_path)
            _conn.row_factory = _sq.Row
            _ensure_columns(_conn)
            with st.spinner("Running site intel..."):
                counts = run_website_intel(_conn, use_ai=use_ai_intel, only_missing=only_missing_intel)
            _conn.close()
            st.success(
                f"Done — reachable: {counts['reachable']} | unreachable: {counts['unreachable']} | "
                f"strong: {counts.get('strong',0)} | moderate: {counts.get('moderate',0)} | "
                f"weak: {counts.get('weak',0)} | skip: {counts.get('skip',0)} | total: {counts['total']}"
            )
            st.rerun()

    # ---- Tab 3: Validation ----
    with tab_validate:
        st.subheader("Validation Status")
        status_counts = df["validation_status"].value_counts()
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Qualified", int(status_counts.get("qualified", 0)))
        col2.metric("Needs Review", int(status_counts.get("needs_review", 0)))
        col3.metric("Skip", int(status_counts.get("skip", 0)))
        col4.metric("Pending", int(status_counts.get("pending", 0)) + int(df["validation_status"].isna().sum()))

        st.divider()
        filter_status = st.selectbox("Filter by status", ["all", "needs_review", "pending", "qualified", "skip"])
        val_df = df.copy()
        if filter_status != "all":
            if filter_status == "pending":
                val_df = val_df[val_df["validation_status"].isin(["pending"]) | val_df["validation_status"].isna()]
            else:
                val_df = val_df[val_df["validation_status"] == filter_status]

        val_cols = ["id", "name", "category", "phone", "website", "emails",
                    "score", "validation_status", "validation_notes"]
        edited_val = st.data_editor(
            val_df[val_cols],
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "validation_status": st.column_config.SelectboxColumn(
                    "Status", options=["qualified", "skip", "needs_review", "pending"]
                )
            },
        )
        if st.button("Save validation changes"):
            rows = edited_val[["id", "validation_status", "validation_notes"]].to_dict("records")
            save_validation(db_path, rows)
            st.success("Saved")

        st.divider()
        st.subheader("Run Validation")
        use_ai = st.checkbox("Use AI classification", value=True)
        only_pending = st.checkbox("Only pending leads", value=True)
        if st.button("Run Validation Now"):
            import sys, os
            _leadgen_dir = str(Path(db_path).parent.parent)
            if _leadgen_dir not in sys.path:
                sys.path.insert(0, _leadgen_dir)
            _env = Path(db_path).parent.parent.parent / ".env"
            if _env.exists():
                for line in _env.read_text().splitlines():
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, _, v = line.partition("=")
                        os.environ.setdefault(k.strip(), v.strip())
            from validation.validator import run_validation
            import sqlite3 as _sq
            _conn = _sq.connect(db_path)
            _conn.row_factory = _sq.Row
            with st.spinner("Validating..."):
                counts = run_validation(_conn, use_ai=use_ai, only_pending=only_pending)
            _conn.close()
            st.success(f"Done — qualified: {counts['qualified']} | skip: {counts['skip']} | needs_review: {counts['needs_review']} | total: {counts['total']}")
            st.rerun()

    # ---- Tab 4: Qualified ----
    with tab_qualified:
        st.subheader("Qualified Leads")
        qualified = df[df["validation_status"] == "qualified"].copy()
        st.write(f"{len(qualified)} qualified leads ready for outreach")
        q_cols = ["id", "name", "category", "phone", "website", "emails",
                  "brand_summary", "apparent_size", "outreach_angle",
                  "industry", "icp_fit", "pain_points", "validation_notes"]
        show_q = [c for c in q_cols if c in qualified.columns]
        st.dataframe(qualified[show_q], use_container_width=True)
        if st.button("Export qualified CSV"):
            import io
            buf = io.StringIO()
            qualified.to_csv(buf, index=False)
            st.download_button("Download", buf.getvalue(), "qualified_leads.csv", "text/csv")

    # ---- Tab 5: Outreach Drafts ----
    with tab_outreach:
        st.subheader("Outreach Drafts")
        out_cols = ["id", "name", "outreach_angle", "industry", "role", "icp_fit", "pain_points", "outreach_message"]
        show_out = [c for c in out_cols if c in df.columns]
        st.dataframe(df[show_out], use_container_width=True)


if __name__ == "__main__":
    main()
