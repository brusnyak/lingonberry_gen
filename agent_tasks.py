"""
leadgen/agent_tasks.py
Structured task templates for agent execution and KPI tracking.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional


@dataclass
class AgentTask:
    """Single task for agent execution."""
    task_id: str
    niche: str
    priority: int  # 1 = highest
    task_type: str  # "outreach", "delivery", "followup", "research"
    status: str  # "pending", "in_progress", "completed", "blocked"
    
    # Target specs
    target_location: str
    target_professions: list[str]
    lead_count_target: int
    
    # Offer details
    copy_template: str
    setup_fee: int
    monthly_fee: int
    offer_terms: str  # "intro_testimonial", "standard", "rev_share"
    
    # KPI targets
    emails_sent_target: int
    reply_target: int
    call_target: int
    close_target: int
    
    # Timeline
    created_at: str
    due_date: str
    completed_at: Optional[str] = None
    
    # Results (filled during/after execution)
    emails_sent_actual: int = 0
    replies_actual: int = 0
    calls_booked: int = 0
    closes_actual: int = 0
    revenue: int = 0
    notes: str = ""


# Predefined task templates by niche
TASK_TEMPLATES = {
    "uk_trades": {
        "niche": "home_services",
        "priority": 1,
        "target_location": "UK",
        "target_professions": ["plumber", "electrician", "hvac", "roofer"],
        "lead_count_target": 100,
        "copy_template": "uk_trades_v1",
        "setup_fee": 497,
        "monthly_fee": 197,
        "offer_terms": "intro_testimonial",
        "emails_sent_target": 100,
        "reply_target": 5,
        "call_target": 2,
        "close_target": 1,
        "timeline_days": 7,
    },
    "real_estate": {
        "niche": "real_estate",
        "priority": 2,
        "target_location": "UK",
        "target_professions": ["real estate agent", "property consultant"],
        "lead_count_target": 75,
        "copy_template": "real_estate_v1",
        "setup_fee": 697,
        "monthly_fee": 397,
        "offer_terms": "intro_testimonial",
        "emails_sent_target": 75,
        "reply_target": 4,
        "call_target": 2,
        "close_target": 1,
        "timeline_days": 10,
    },
    "accounting": {
        "niche": "accounting_tax",
        "priority": 3,
        "target_location": "UK",
        "target_professions": ["accountant", "tax advisor"],
        "lead_count_target": 50,
        "copy_template": "accounting_v1",
        "setup_fee": 797,
        "monthly_fee": 497,
        "offer_terms": "intro_testimonial",
        "emails_sent_target": 50,
        "reply_target": 3,
        "call_target": 1,
        "close_target": 1,
        "timeline_days": 14,
    },
}


def init_agent_tasks_table(conn: sqlite3.Connection) -> None:
    """Create agent_tasks table if not exists."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT UNIQUE NOT NULL,
            niche TEXT NOT NULL,
            priority INTEGER DEFAULT 1,
            task_type TEXT DEFAULT 'outreach',
            status TEXT DEFAULT 'pending',
            target_location TEXT,
            target_professions TEXT,  -- JSON list
            lead_count_target INTEGER,
            copy_template TEXT,
            setup_fee INTEGER,
            monthly_fee INTEGER,
            offer_terms TEXT,
            emails_sent_target INTEGER,
            reply_target INTEGER,
            call_target INTEGER,
            close_target INTEGER,
            created_at TEXT,
            due_date TEXT,
            completed_at TEXT,
            emails_sent_actual INTEGER DEFAULT 0,
            replies_actual INTEGER DEFAULT 0,
            calls_booked INTEGER DEFAULT 0,
            closes_actual INTEGER DEFAULT 0,
            revenue INTEGER DEFAULT 0,
            notes TEXT DEFAULT ''
        )
        """
    )
    conn.commit()


def create_task_from_template(
    conn: sqlite3.Connection,
    template_key: str,
    task_suffix: str = "",
) -> str:
    """Create a new task from template. Returns task_id."""
    if template_key not in TASK_TEMPLATES:
        raise ValueError(f"Unknown template: {template_key}")
    
    tpl = TASK_TEMPLATES[template_key]
    now = datetime.now(timezone.utc)
    task_id = f"{template_key}-{now.strftime('%Y%m%d')}-{task_suffix}"
    due = now + timedelta(days=tpl["timeline_days"])
    
    task = AgentTask(
        task_id=task_id,
        niche=tpl["niche"],
        priority=tpl["priority"],
        task_type="outreach",
        status="pending",
        target_location=tpl["target_location"],
        target_professions=tpl["target_professions"],
        lead_count_target=tpl["lead_count_target"],
        copy_template=tpl["copy_template"],
        setup_fee=tpl["setup_fee"],
        monthly_fee=tpl["monthly_fee"],
        offer_terms=tpl["offer_terms"],
        emails_sent_target=tpl["emails_sent_target"],
        reply_target=tpl["reply_target"],
        call_target=tpl["call_target"],
        close_target=tpl["close_target"],
        created_at=now.isoformat(),
        due_date=due.isoformat(),
    )
    
    conn.execute(
        """
        INSERT INTO agent_tasks (
            task_id, niche, priority, task_type, status, target_location,
            target_professions, lead_count_target, copy_template, setup_fee,
            monthly_fee, offer_terms, emails_sent_target, reply_target,
            call_target, close_target, created_at, due_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            task.task_id, task.niche, task.priority, task.task_type, task.status,
            task.target_location, json.dumps(task.target_professions),
            task.lead_count_target, task.copy_template, task.setup_fee,
            task.monthly_fee, task.offer_terms, task.emails_sent_target,
            task.reply_target, task.call_target, task.close_target,
            task.created_at, task.due_date,
        ),
    )
    conn.commit()
    return task_id


def get_active_tasks(conn: sqlite3.Connection) -> list[dict]:
    """Get all non-completed tasks ordered by priority."""
    rows = conn.execute(
        """
        SELECT * FROM agent_tasks 
        WHERE status IN ('pending', 'in_progress', 'blocked')
        ORDER BY priority ASC, created_at ASC
        """
    ).fetchall()
    return [dict(r) for r in rows]


def update_task_progress(
    conn: sqlite3.Connection,
    task_id: str,
    emails_sent: Optional[int] = None,
    replies: Optional[int] = None,
    calls: Optional[int] = None,
    closes: Optional[int] = None,
    revenue: Optional[int] = None,
    notes: Optional[str] = None,
    status: Optional[str] = None,
) -> None:
    """Update task metrics during execution."""
    updates = []
    params = []
    
    if emails_sent is not None:
        updates.append("emails_sent_actual = ?")
        params.append(emails_sent)
    if replies is not None:
        updates.append("replies_actual = ?")
        params.append(replies)
    if calls is not None:
        updates.append("calls_booked = ?")
        params.append(calls)
    if closes is not None:
        updates.append("closes_actual = ?")
        params.append(closes)
    if revenue is not None:
        updates.append("revenue = ?")
        params.append(revenue)
    if notes is not None:
        updates.append("notes = ?")
        params.append(notes)
    if status is not None:
        updates.append("status = ?")
        params.append(status)
        if status == "completed":
            updates.append("completed_at = ?")
            params.append(datetime.now(timezone.utc).isoformat())
    
    if updates:
        sql = f"UPDATE agent_tasks SET {', '.join(updates)} WHERE task_id = ?"
        params.append(task_id)
        conn.execute(sql, params)
        conn.commit()


def get_weekly_kpi_summary(conn: sqlite3.Connection) -> dict:
    """Get KPI summary for the current week."""
    row = conn.execute(
        """
        SELECT 
            COUNT(*) as total_tasks,
            SUM(emails_sent_actual) as emails_sent,
            SUM(replies_actual) as replies,
            SUM(calls_booked) as calls,
            SUM(closes_actual) as closes,
            SUM(revenue) as revenue
        FROM agent_tasks
        WHERE created_at >= date('now', '-7 days')
        """
    ).fetchone()
    
    targets = conn.execute(
        """
        SELECT 
            SUM(emails_sent_target) as email_target,
            SUM(reply_target) as reply_target,
            SUM(call_target) as call_target,
            SUM(close_target) as close_target
        FROM agent_tasks
        WHERE created_at >= date('now', '-7 days')
        """
    ).fetchone()
    
    return {
        "tasks_active": row["total_tasks"] or 0,
        "emails_sent": row["emails_sent"] or 0,
        "emails_target": targets["email_target"] or 0,
        "replies": row["replies"] or 0,
        "replies_target": targets["reply_target"] or 0,
        "calls": row["calls"] or 0,
        "calls_target": targets["call_target"] or 0,
        "closes": row["closes"] or 0,
        "closes_target": targets["close_target"] or 0,
        "revenue": row["revenue"] or 0,
    }


# CLI helpers
if __name__ == "__main__":
    import sys
    from datetime import timedelta
    
    db_path = "leads.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    init_agent_tasks_table(conn)
    
    if len(sys.argv) < 2:
        print("Usage: python agent_tasks.py [create|list|progress|kpi]")
        sys.exit(1)
    
    cmd = sys.argv[1]
    
    if cmd == "create" and len(sys.argv) >= 3:
        template = sys.argv[2]
        suffix = sys.argv[3] if len(sys.argv) > 3 else "01"
        task_id = create_task_from_template(conn, template, suffix)
        print(f"Created task: {task_id}")
    
    elif cmd == "list":
        tasks = get_active_tasks(conn)
        for t in tasks:
            print(f"[{t['status']}] P{t['priority']} | {t['task_id']} | {t['niche']}")
    
    elif cmd == "kpi":
        kpi = get_weekly_kpi_summary(conn)
        print(f"Weekly KPIs:")
        print(f"  Emails: {kpi['emails_sent']}/{kpi['emails_target']}")
        print(f"  Replies: {kpi['replies']}/{kpi['replies_target']}")
        print(f"  Calls: {kpi['calls']}/{kpi['calls_target']}")
        print(f"  Closes: {kpi['closes']}/{kpi['closes_target']}")
        print(f"  Revenue: €{kpi['revenue']}")
    
    conn.close()
