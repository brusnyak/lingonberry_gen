import argparse
import time
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

from storage.db import (
    connect,
    init_db,
    upsert_business,
    insert_website_data,
    insert_enrichment,
    export_csv,
    log_query_run,
    list_approved_without_enrichment,
)
from scrapers.google_maps import scrape_google_maps
from scrapers.website import scrape_site
from enrichment.ollama import enrich_business
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback


def _load_queries(path: str) -> list[str]:
    queries = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        q = line.strip()
        if q and not q.startswith("#"):
            queries.append(q)
    return queries


def main() -> None:
    parser = argparse.ArgumentParser(description="Hybrid lead research tool (Google Maps + websites + local LLM)")
    parser.add_argument("--query", help="Search query, e.g. 'dentists in Bratislava'")
    parser.add_argument("--queries-file", help="File with one query per line")
    parser.add_argument("--daily", action="store_true", help="Append today's date to each query to reduce duplicates")
    parser.add_argument("--max", type=int, default=50, help="Max results per query")
    parser.add_argument("--db", default=str(_HERE / "data/leads.db"), help="SQLite DB path")
    parser.add_argument("--csv", default=str(_HERE / "data/leads.csv"), help="CSV export path")
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    parser.add_argument("--slow-mo", type=int, default=0, help="Playwright slow_mo in ms")
    parser.add_argument("--min-delay", type=float, default=0.8, help="Min human delay between actions")
    parser.add_argument("--max-delay", type=float, default=2.0, help="Max human delay between actions")
    parser.add_argument("--profile", help="Playwright user data dir for session persistence")
    parser.add_argument("--model", default="llama3.1", help="Ollama model name")
    parser.add_argument("--skip-llm", action="store_true", help="Skip LLM enrichment")
    parser.add_argument("--enrich-approved", action="store_true", help="Only enrich approved leads (no scraping)")
    parser.add_argument("--auto-approve", action="store_true", help="Mark scraped leads as approved immediately")
    parser.add_argument("--workers", type=int, default=6, help="Worker threads for website/LLM steps")
    parser.add_argument("--retries", type=int, default=2, help="Retries for website/LLM steps")
    parser.add_argument("--review", action="store_true", help="Review mode: confirm each listing before capture")
    args = parser.parse_args()

    if not args.enrich_approved and not args.query and not args.queries_file:
        raise SystemExit("Provide --query or --queries-file")

    queries = []
    if args.query:
        queries.append(args.query)
    if args.queries_file:
        queries.extend(_load_queries(args.queries_file))

    today_tag = datetime.utcnow().strftime("%Y-%m-%d")
    conn = connect(args.db)
    init_db(conn)

    def score_lead(lead: dict) -> tuple[float, str]:
        score = 0.0
        reasons = []
        if lead.get("website"):
            score += 20
            reasons.append("has_website")
        if lead.get("phone"):
            score += 10
            reasons.append("has_phone")
        rating = lead.get("rating") or 0
        reviews = lead.get("reviews_count") or 0
        if rating and rating >= 4.0:
            score += 10
            reasons.append("good_rating")
        if reviews and reviews >= 25:
            score += 10
            reasons.append("many_reviews")
        if lead.get("category"):
            score += 5
            reasons.append("has_category")
        return score, ",".join(reasons)

    def scrape_and_enrich(lead: dict) -> dict:
        result = {"lead": lead, "site": None, "enriched": None, "error": None}
        website = lead.get("website")
        if website:
            for _ in range(args.retries + 1):
                try:
                    site_data = scrape_site(website)
                    result["site"] = site_data
                    break
                except Exception as e:
                    result["error"] = f"site: {e}"
                    time.sleep(1.0)
        if not args.skip_llm:
            for _ in range(args.retries + 1):
                try:
                    enriched = enrich_business(args.model, {**lead, **(result["site"] or {})})
                    result["enriched"] = enriched
                    break
                except Exception as e:
                    result["error"] = f"llm: {e}"
                    time.sleep(1.0)
        return result

    if args.enrich_approved:
        rows = list_approved_without_enrichment(conn)
        print(f"[enrich] Approved leads without enrichment: {len(rows)}")
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = [ex.submit(scrape_and_enrich, dict(row)) for row in rows]
            for i, fut in enumerate(as_completed(futures), start=1):
                res = fut.result()
                lead = res["lead"]
                if res["enriched"]:
                    res["enriched"]["model"] = args.model
                    res["enriched"]["created_at"] = datetime.utcnow().isoformat()
                    insert_enrichment(conn, int(lead["id"]), res["enriched"])
                if i % 10 == 0:
                    print(f"[enrich] {i}/{len(futures)} done")
    else:
        for q in queries:
            run_query = f"{q} {today_tag}" if args.daily else q
            print(f"[maps] Searching: {run_query}")
            leads = scrape_google_maps(
                run_query,
                max_results=args.max,
                headless=args.headless,
                slow_mo=args.slow_mo,
                min_delay_s=args.min_delay,
                max_delay_s=args.max_delay,
                user_data_dir=args.profile,
                review_mode=args.review,
            )
            print(f"[maps] Collected {len(leads)} leads")

            for lead in leads:
                if args.auto_approve:
                    lead["approved"] = 1
                    lead["approved_at"] = datetime.utcnow().isoformat()
                else:
                    lead["approved"] = 0
                    lead["approved_at"] = None
                score, reason = score_lead(lead)
                lead["score"] = score
                lead["score_reason"] = reason
                business_id = upsert_business(conn, lead)

            with ThreadPoolExecutor(max_workers=args.workers) as ex:
                futures = [ex.submit(scrape_and_enrich, lead) for lead in leads]
                for i, fut in enumerate(as_completed(futures), start=1):
                    res = fut.result()
                    lead = res["lead"]
                    business_id = upsert_business(conn, lead)
                    if res["site"]:
                        res["site"]["collected_at"] = datetime.utcnow().isoformat()
                        insert_website_data(conn, business_id, res["site"])
                    if res["enriched"]:
                        res["enriched"]["model"] = args.model
                        res["enriched"]["created_at"] = datetime.utcnow().isoformat()
                        insert_enrichment(conn, business_id, res["enriched"])
                    if i % 10 == 0:
                        print(f"[pipeline] {i}/{len(futures)} done")

            log_query_run(conn, run_query, today_tag, args.max, len(leads), datetime.utcnow().isoformat())

    export_csv(conn, args.csv)
    print(f"[done] Exported CSV to {args.csv}")


if __name__ == "__main__":
    main()
