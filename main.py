import argparse
from datetime import datetime
from pathlib import Path

from storage.db import (
    connect,
    init_db,
    upsert_business,
    insert_website_data,
    insert_enrichment,
    export_csv,
    log_query_run,
)
from scrapers.google_maps import scrape_google_maps
from scrapers.website import scrape_site
from enrichment.ollama import enrich_business


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
    parser.add_argument("--db", default="/Users/yegor/Documents/Agency & Security Stuff/BIZ/leadgen/data/leads.db", help="SQLite DB path")
    parser.add_argument("--csv", default="/Users/yegor/Documents/Agency & Security Stuff/BIZ/leadgen/data/leads.csv", help="CSV export path")
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    parser.add_argument("--slow-mo", type=int, default=0, help="Playwright slow_mo in ms")
    parser.add_argument("--min-delay", type=float, default=0.8, help="Min human delay between actions")
    parser.add_argument("--max-delay", type=float, default=2.0, help="Max human delay between actions")
    parser.add_argument("--profile", help="Playwright user data dir for session persistence")
    parser.add_argument("--model", default="llama3.1-4k", help="Ollama model name")
    parser.add_argument("--skip-llm", action="store_true", help="Skip LLM enrichment")
    args = parser.parse_args()

    if not args.query and not args.queries_file:
        raise SystemExit("Provide --query or --queries-file")

    queries = []
    if args.query:
        queries.append(args.query)
    if args.queries_file:
        queries.extend(_load_queries(args.queries_file))

    today_tag = datetime.utcnow().strftime("%Y-%m-%d")
    conn = connect(args.db)
    init_db(conn)

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
        )
        print(f"[maps] Collected {len(leads)} leads")

        for lead in leads:
            business_id = upsert_business(conn, lead)

            if lead.get("website"):
                print(f"[site] {lead['name']} -> {lead['website']}")
                site_data = scrape_site(lead["website"])
                site_data["collected_at"] = datetime.utcnow().isoformat()
                insert_website_data(conn, business_id, site_data)

                if not args.skip_llm:
                    enriched = enrich_business(args.model, {**lead, **site_data})
                    enriched["model"] = args.model
                    enriched["created_at"] = datetime.utcnow().isoformat()
                    insert_enrichment(conn, business_id, enriched)
            else:
                if not args.skip_llm:
                    enriched = enrich_business(args.model, lead)
                    enriched["model"] = args.model
                    enriched["created_at"] = datetime.utcnow().isoformat()
                    insert_enrichment(conn, business_id, enriched)

        log_query_run(conn, run_query, today_tag, args.max, len(leads), datetime.utcnow().isoformat())

    export_csv(conn, args.csv)
    print(f"[done] Exported CSV to {args.csv}")


if __name__ == "__main__":
    main()
