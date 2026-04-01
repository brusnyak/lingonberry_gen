import sqlite3
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from leadgen.validation.website_intel import run_website_intel

def main():
    db_path = Path(__file__).resolve().parent.parent / "data" / "leads.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    # We want to re-run intel for all qualified leads where top_gap is empty
    # To do this safely, we temporarily set site_intel_done = 0 for them.
    cur = conn.cursor()
    cur.execute(
        "UPDATE businesses SET site_intel_done = 0 "
        "WHERE validation_status = 'qualified' AND (top_gap IS NULL OR top_gap = '')"
    )
    rows_affected = cur.rowcount
    conn.commit()
    
    print(f"Set site_intel_done=0 for {rows_affected} qualified leads missing top_gap.")
    
    if rows_affected > 0:
        print("Running site intel backfill...")
        counts = run_website_intel(conn, use_ai=True, only_missing=True)
        print(f"Finished. Counts: {counts}")
        
    conn.close()

if __name__ == "__main__":
    main()
