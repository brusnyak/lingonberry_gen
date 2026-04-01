"""
leadgen/kpi_tracker.py
Simple KPI tracking for agent performance.
Integrates into existing leads.db for easy dashboard addition later.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional


@dataclass
class DailyMetrics:
    """Single day of agent activity."""
    date: str
    niche: str
    emails_sent: int
    emails_opened: int  # if tracking available
    replies_received: int
    replies_positive: int
    calls_booked: int
    deals_closed: int
    revenue: int
    hours_worked: float
    notes: str


@dataclass
class NichePerformance:
    """Aggregated performance by niche."""
    niche: str
    leads_total: int
    leads_qualified: int
    contacted: int
    replies: int
    interested: int
    calls: int
    closes: int
    revenue: int
    reply_rate: float
    conversion_rate: float


def init_kpi_tables(conn: sqlite3.Connection) -> None:
    """Create KPI tracking tables."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_metrics (
            date TEXT NOT NULL,
            niche TEXT NOT NULL,
            emails_sent INTEGER DEFAULT 0,
            emails_opened INTEGER DEFAULT 0,
            replies_received INTEGER DEFAULT 0,
            replies_positive INTEGER DEFAULT 0,
            calls_booked INTEGER DEFAULT 0,
            deals_closed INTEGER DEFAULT 0,
            revenue INTEGER DEFAULT 0,
            hours_worked REAL DEFAULT 0,
            notes TEXT DEFAULT '',
            PRIMARY KEY (date, niche)
        )
        """
    )
    conn.commit()


def log_daily_metrics(
    conn: sqlite3.Connection,
    date: str,
    niche: str,
    emails_sent: int = 0,
    emails_opened: int = 0,
    replies: int = 0,
    replies_positive: int = 0,
    calls: int = 0,
    closes: int = 0,
    revenue: int = 0,
    hours: float = 0,
    notes: str = "",
) -> None:
    """Log or update daily metrics."""
    conn.execute(
        """
        INSERT INTO daily_metrics (
            date, niche, emails_sent, emails_opened, replies_received,
            replies_positive, calls_booked, deals_closed, revenue, hours_worked, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date, niche) DO UPDATE SET
            emails_sent = emails_sent + excluded.emails_sent,
            emails_opened = emails_opened + excluded.emails_opened,
            replies_received = replies_received + excluded.replies_received,
            replies_positive = replies_positive + excluded.replies_positive,
            calls_booked = calls_booked + excluded.calls_booked,
            deals_closed = deals_closed + excluded.deals_closed,
            revenue = revenue + excluded.revenue,
            hours_worked = hours_worked + excluded.hours_worked,
            notes = COALESCE(notes, '') || ' ' || excluded.notes
        """,
        (date, niche, emails_sent, emails_opened, replies, replies_positive,
         calls, closes, revenue, hours, notes),
    )
    conn.commit()


def get_weekly_summary(conn: sqlite3.Connection) -> dict:
    """Get last 7 days summary across all niches."""
    row = conn.execute(
        """
        SELECT
            SUM(emails_sent) as sent,
            SUM(emails_opened) as opened,
            SUM(replies_received) as replies,
            SUM(replies_positive) as positive,
            SUM(calls_booked) as calls,
            SUM(deals_closed) as closes,
            SUM(revenue) as revenue,
            SUM(hours_worked) as hours
        FROM daily_metrics
        WHERE date >= date('now', '-7 days')
        """
    ).fetchone()
    
    sent = row["sent"] or 0
    opened = row["opened"] or 0
    replies = row["replies"] or 0
    
    return {
        "emails_sent": sent,
        "emails_opened": opened,
        "open_rate": round((opened / sent * 100), 1) if sent else 0,
        "replies": replies,
        "reply_rate": round((replies / sent * 100), 1) if sent else 0,
        "positive_replies": row["positive"] or 0,
        "calls": row["calls"] or 0,
        "closes": row["closes"] or 0,
        "revenue": row["revenue"] or 0,
        "hours_worked": round(row["hours"] or 0, 1),
    }


def get_niche_performance(conn: sqlite3.Connection, days: int = 30) -> list[NichePerformance]:
    """Get performance breakdown by niche."""
    rows = conn.execute(
        f"""
        SELECT
            niche,
            SUM(emails_sent) as contacted,
            SUM(replies_received) as replies,
            SUM(replies_positive) as interested,
            SUM(calls_booked) as calls,
            SUM(deals_closed) as closes,
            SUM(revenue) as revenue
        FROM daily_metrics
        WHERE date >= date('now', '-{days} days')
        GROUP BY niche
        """
    ).fetchall()
    
    results = []
    for r in rows:
        contacted = r["contacted"] or 0
        replies = r["replies"] or 0
        closes = r["closes"] or 0
        
        results.append(NichePerformance(
            niche=r["niche"],
            leads_total=0,  # Would come from businesses table
            leads_qualified=0,
            contacted=contacted,
            replies=replies,
            interested=r["interested"] or 0,
            calls=r["calls"] or 0,
            closes=closes,
            revenue=r["revenue"] or 0,
            reply_rate=round((replies / contacted * 100), 1) if contacted else 0,
            conversion_rate=round((closes / contacted * 100), 1) if contacted else 0,
        ))
    
    return results


def print_daily_checklist() -> None:
    """Print daily agent checklist for manual tracking."""
    print("""
=== DAILY AGENT CHECKLIST ===

MORNING (30 min):
[ ] Check overnight replies
[ ] Classify new replies (interested/not interested/needs followup)
[ ] Book calls for warm leads
[ ] Queue follow-ups for previous contacts

MID-DAY (60 min):
[ ] Run lead scraper (50–100 new leads)
[ ] Validate/enrich new leads
[ ] Load outreach queue

AFTERNOON (30 min):
[ ] Review outreach performance
[ ] A/B test subject lines
[ ] Log learnings

TODAY'S TARGETS:
  Emails sent: ___/75
  Replies: ___/5
  Calls booked: ___/2
  Closes: ___/1

=== LOG TODAY ===
python kpi_tracker.py log uk_trades --emails 50 --replies 3 --calls 1
""")


def print_weekly_report(conn: sqlite3.Connection) -> None:
    """Print formatted weekly report."""
    summary = get_weekly_summary(conn)
    niches = get_niche_performance(conn, days=7)
    
    print("\n" + "="*50)
    print("WEEKLY KPI REPORT (Last 7 Days)")
    print("="*50)
    
    print(f"\n📧 OUTREACH")
    print(f"  Emails sent: {summary['emails_sent']}")
    print(f"  Open rate: {summary['open_rate']}%")
    print(f"  Reply rate: {summary['reply_rate']}%")
    
    print(f"\n🎯 CONVERSION")
    print(f"  Positive replies: {summary['positive_replies']}")
    print(f"  Calls booked: {summary['calls']}")
    print(f"  Deals closed: {summary['closes']}")
    
    print(f"\n💰 REVENUE")
    print(f"  Total: €{summary['revenue']}")
    print(f"  Hours worked: {summary['hours_worked']}h")
    
    if summary['closes'] > 0:
        print(f"  Revenue per hour: €{round(summary['revenue'] / summary['hours_worked'], 0)}/h")
    
    print(f"\n📊 BY NICHE")
    for n in niches:
        print(f"  {n.niche}: {n.contact} contacted → {n.replies} replies ({n.reply_rate}%) → {n.closes} closes")
    
    print("\n" + "="*50)


# CLI entry point
if __name__ == "__main__":
    import sys
    
    db_path = "leads.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    init_kpi_tables(conn)
    
    if len(sys.argv) < 2:
        print_daily_checklist()
        sys.exit(0)
    
    cmd = sys.argv[1]
    
    if cmd == "log":
        if len(sys.argv) < 3:
            print("Usage: python kpi_tracker.py log <niche> [--emails N] [--replies N] ...")
            sys.exit(1)
        
        niche = sys.argv[2]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        # Parse args
        kwargs = {"date": today, "niche": niche}
        i = 3
        while i < len(sys.argv):
            if sys.argv[i] == "--emails" and i + 1 < len(sys.argv):
                kwargs["emails_sent"] = int(sys.argv[i+1])
                i += 2
            elif sys.argv[i] == "--replies" and i + 1 < len(sys.argv):
                kwargs["replies"] = int(sys.argv[i+1])
                i += 2
            elif sys.argv[i] == "--positive" and i + 1 < len(sys.argv):
                kwargs["replies_positive"] = int(sys.argv[i+1])
                i += 2
            elif sys.argv[i] == "--calls" and i + 1 < len(sys.argv):
                kwargs["calls"] = int(sys.argv[i+1])
                i += 2
            elif sys.argv[i] == "--closes" and i + 1 < len(sys.argv):
                kwargs["closes"] = int(sys.argv[i+1])
                i += 2
            elif sys.argv[i] == "--revenue" and i + 1 < len(sys.argv):
                kwargs["revenue"] = int(sys.argv[i+1])
                i += 2
            elif sys.argv[i] == "--hours" and i + 1 < len(sys.argv):
                kwargs["hours"] = float(sys.argv[i+1])
                i += 2
            else:
                i += 1
        
        log_daily_metrics(conn, **kwargs)
        print(f"Logged metrics for {niche} on {today}")
    
    elif cmd == "report":
        print_weekly_report(conn)
    
    elif cmd == "niches":
        niches = get_niche_performance(conn)
        print("\nNiche Performance (30 days):")
        for n in niches:
            print(f"  {n.niche}: {n.reply_rate}% reply → {n.conversion_rate}% close")
    
    conn.close()
