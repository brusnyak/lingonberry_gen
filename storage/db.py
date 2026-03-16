import sqlite3
from pathlib import Path
from typing import Any, Dict

SCHEMA = """
CREATE TABLE IF NOT EXISTS businesses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    place_id TEXT UNIQUE,
    name TEXT,
    category TEXT,
    rating REAL,
    reviews_count INTEGER,
    address TEXT,
    phone TEXT,
    website TEXT,
    email_maps TEXT,
    hours TEXT,
    maps_url TEXT,
    query TEXT,
    collected_at TEXT,
    approved INTEGER DEFAULT 0,
    approved_at TEXT,
    score REAL,
    score_reason TEXT,
    validation_status TEXT DEFAULT 'pending',
    validation_notes TEXT
);

CREATE TABLE IF NOT EXISTS website_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id INTEGER,
    site_url TEXT,
    about_text TEXT,
    services_text TEXT,
    emails TEXT,
    phones TEXT,
    collected_at TEXT,
    FOREIGN KEY (business_id) REFERENCES businesses(id)
);

CREATE TABLE IF NOT EXISTS enrichment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id INTEGER,
    industry TEXT,
    role TEXT,
    icp_fit TEXT,
    pain_points TEXT,
    outreach_message TEXT,
    model TEXT,
    created_at TEXT,
    FOREIGN KEY (business_id) REFERENCES businesses(id)
);

CREATE TABLE IF NOT EXISTS query_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query TEXT,
    run_date TEXT,
    max_results INTEGER,
    collected_count INTEGER,
    created_at TEXT
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    # Lightweight migration for new columns
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(businesses)").fetchall()}
    for col, typedef in [
        ("approved", "INTEGER DEFAULT 0"),
        ("approved_at", "TEXT"),
        ("score", "REAL"),
        ("score_reason", "TEXT"),
        ("email_maps", "TEXT"),
        ("hours", "TEXT"),
        ("validation_status", "TEXT DEFAULT 'pending'"),
        ("validation_notes", "TEXT"),
    ]:
        if col not in cols:
            conn.execute(f"ALTER TABLE businesses ADD COLUMN {col} {typedef}")

    ecols = {row["name"] for row in conn.execute("PRAGMA table_info(enrichment)").fetchall()}
    if "industry" not in ecols:
        conn.execute("ALTER TABLE enrichment ADD COLUMN industry TEXT")
    if "role" not in ecols:
        conn.execute("ALTER TABLE enrichment ADD COLUMN role TEXT")
    if "icp_fit" not in ecols:
        conn.execute("ALTER TABLE enrichment ADD COLUMN icp_fit TEXT")
    conn.commit()


def upsert_business(conn: sqlite3.Connection, data: Dict[str, Any]) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO businesses (
            place_id, name, category, rating, reviews_count,
            address, phone, website, email_maps, hours,
            maps_url, query, collected_at, approved, approved_at
        )
        VALUES (
            :place_id, :name, :category, :rating, :reviews_count,
            :address, :phone, :website, :email_maps, :hours,
            :maps_url, :query, :collected_at, :approved, :approved_at
        )
        ON CONFLICT(place_id) DO UPDATE SET
            name=excluded.name,
            category=excluded.category,
            rating=excluded.rating,
            reviews_count=excluded.reviews_count,
            address=excluded.address,
            phone=excluded.phone,
            website=excluded.website,
            email_maps=excluded.email_maps,
            hours=excluded.hours,
            maps_url=excluded.maps_url,
            query=excluded.query,
            collected_at=excluded.collected_at
        """,
        {**{"email_maps": "", "hours": "", "approved": 0, "approved_at": None}, **data},
    )
    conn.commit()
    if data.get("place_id"):
        row = cur.execute("SELECT id FROM businesses WHERE place_id=?", (data["place_id"],)).fetchone()
        return int(row["id"])
    return int(cur.lastrowid)


def update_business(conn: sqlite3.Connection, business_id: int, data: Dict[str, Any]) -> None:
    if not data:
        return
    keys = ", ".join([f"{k}=?" for k in data.keys()])
    values = list(data.values()) + [business_id]
    conn.execute(f"UPDATE businesses SET {keys} WHERE id=?", values)
    conn.commit()


def insert_website_data(conn: sqlite3.Connection, business_id: int, data: Dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO website_data (business_id, site_url, about_text, services_text, emails, phones, collected_at)
        VALUES (:business_id, :site_url, :about_text, :services_text, :emails, :phones, :collected_at)
        """,
        {"business_id": business_id, **data},
    )
    conn.commit()


def insert_enrichment(conn: sqlite3.Connection, business_id: int, data: Dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO enrichment (business_id, industry, role, icp_fit, pain_points, outreach_message, model, created_at)
        VALUES (:business_id, :industry, :role, :icp_fit, :pain_points, :outreach_message, :model, :created_at)
        """,
        {"business_id": business_id, **data},
    )
    conn.commit()


def approve_business(conn: sqlite3.Connection, business_id: int, approved: int, approved_at: str) -> None:
    conn.execute(
        "UPDATE businesses SET approved=?, approved_at=? WHERE id=?",
        (approved, approved_at, business_id),
    )
    conn.commit()


def list_approved_without_enrichment(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT b.*, w.about_text, w.services_text, w.emails, w.phones
        FROM businesses b
        LEFT JOIN website_data w ON w.business_id = b.id
        LEFT JOIN enrichment e ON e.business_id = b.id
        WHERE b.approved = 1 AND e.id IS NULL
        ORDER BY b.id DESC
        """
    ).fetchall()


def log_query_run(conn: sqlite3.Connection, query: str, run_date: str, max_results: int, collected_count: int, created_at: str) -> None:
    conn.execute(
        """
        INSERT INTO query_runs (query, run_date, max_results, collected_count, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (query, run_date, max_results, collected_count, created_at),
    )
    conn.commit()


def export_csv(conn: sqlite3.Connection, out_path: str) -> None:
    import csv

    rows = conn.execute(
        """
        SELECT b.id, b.place_id, b.name, b.category, b.rating, b.reviews_count, b.address, b.phone, b.website, b.maps_url, b.query,
               b.score, b.score_reason,
               w.about_text, w.services_text, w.emails, w.phones,
               e.industry, e.role, e.icp_fit, e.pain_points, e.outreach_message
        FROM businesses b
        LEFT JOIN website_data w ON w.business_id = b.id
        LEFT JOIN enrichment e ON e.business_id = b.id
        ORDER BY b.id DESC
        """
    ).fetchall()

    fieldnames = rows[0].keys() if rows else [
        "id","place_id","name","category","rating","reviews_count","address","phone","website","maps_url","query",
        "score","score_reason","about_text","services_text","emails","phones","industry","role","icp_fit","pain_points","outreach_message"
    ]

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        for row in rows:
            writer.writerow([row[k] for k in fieldnames])
