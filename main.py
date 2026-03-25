import argparse
import os
import time
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

# load ../.env so OLLAMA_BASE_URL, OPENROUTER_API_KEY etc. are available
_env_file = _HERE.parent / ".env"
if _env_file.exists():
    for _line in _env_file.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

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
from scrapers.web_search import search_leads as ddg_search_leads, batch_search as ddg_batch_search
from scrapers.hipages import scrape_hipages
from scrapers.facebook import scrape_facebook
from enrichment.ollama import enrich_business
from enrichment.contact_enrichment import run_contact_enrichment
from validation.validator import run_validation
from validation.website_intel import run_website_intel, _ensure_columns as _ensure_intel_cols
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
    parser.add_argument("--validate", action="store_true", help="Run validation layer on pending leads")
    parser.add_argument("--validate-all", action="store_true", help="Re-validate all leads (not just pending)")
    parser.add_argument("--no-ai-validate", action="store_true", help="Use rule-based validation only (no LLM)")
    parser.add_argument("--site-intel", action="store_true", help="Run website intelligence layer (reachability + brand profile)")
    parser.add_argument("--site-intel-all", action="store_true", help="Re-run site intel on all leads (not just missing)")
    # New source flags
    parser.add_argument("--source", default="google_maps", choices=["google_maps", "hipages", "web_search", "facebook"], help="Lead source to scrape")
    parser.add_argument("--trade", default="plumber", help="Trade key for hipages (plumber, electrician, hvac, locksmith...)")
    parser.add_argument("--location", default="sydney", help="Location for hipages/web_search scraping")
    parser.add_argument("--contact-enrich", action="store_true", help="Run contact+pain enrichment on qualified leads missing outreach_angle")
    parser.add_argument("--contact-enrich-all", action="store_true", help="Re-run contact enrichment on all qualified leads")
    args = parser.parse_args()

    if not args.enrich_approved and not args.query and not args.queries_file \
            and not args.validate and not args.validate_all \
            and not args.site_intel and not args.site_intel_all:
        raise SystemExit("Provide --query, --queries-file, --validate, --validate-all, --site-intel, --site-intel-all, --source hipages/web_search/facebook, or --contact-enrich")

    queries = []
    if args.query:
        queries.append(args.query)
    if args.queries_file:
        queries.extend(_load_queries(args.queries_file))

    today_tag = datetime.utcnow().strftime("%Y-%m-%d")
    conn = connect(args.db)
    init_db(conn)
    _ensure_intel_cols(conn)

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

    # ── New source scrapers ────────────────────────────────────────────────────
    if args.source == "hipages" and not args.enrich_approved:
        from scrapers.queries import AU_HIPAGES_MATRIX
        if args.query:
            # --query "plumber sydney" style
            parts = args.query.strip().split(None, 1)
            trade, location = (parts[0], parts[1]) if len(parts) == 2 else (parts[0], "sydney")
            hipages_jobs = [(trade, location)]
        else:
            hipages_jobs = [(args.trade, args.location)]

        all_leads = []
        for trade, location in hipages_jobs:
            leads = scrape_hipages(trade, location, max_results=args.max, headless=args.headless)
            all_leads.extend(leads)

        print(f"[hipages] Total collected: {len(all_leads)}")
        for lead in all_leads:
            score, reason = score_lead(lead)
            lead["score"] = score
            lead["score_reason"] = reason
            if args.auto_approve:
                lead["approved"] = 1
                lead["approved_at"] = datetime.utcnow().isoformat()
            business_id = upsert_business(conn, lead)
            # Store about_text in website_data if present
            if lead.get("about_text"):
                insert_website_data(conn, business_id, {
                    "site_url": lead.get("website", ""),
                    "site_status": "ok" if lead.get("website") else "",
                    "about_text": lead.get("about_text", ""),
                    "collected_at": lead["collected_at"],
                })
            # Store contact_name directly
            if lead.get("contact_name"):
                update_business(conn, business_id, {"contact_name": lead["contact_name"]})

    elif args.source == "web_search" and not args.enrich_approved:
        from scrapers.queries import WEB_SEARCH_MATRIX
        if args.query:
            web_queries = [(args.query, args.trade or "business")]
        else:
            from scrapers.queries import get_daily_queries
            web_queries = get_daily_queries(n=args.max // 10 or 6, source="web_search")

        all_leads = []
        for query_str, category in web_queries:
            leads = ddg_search_leads(query_str, max_results=15, category=category)
            all_leads.extend(leads)

        print(f"[web_search] Total collected: {len(all_leads)}")
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = [ex.submit(scrape_and_enrich, lead) for lead in all_leads]
            for i, fut in enumerate(as_completed(futures), start=1):
                res = fut.result()
                lead = res["lead"]
                score, reason = score_lead(lead)
                lead["score"] = score
                lead["score_reason"] = reason
                if args.auto_approve:
                    lead["approved"] = 1
                    lead["approved_at"] = datetime.utcnow().isoformat()
                business_id = upsert_business(conn, lead)
                if res["site"]:
                    res["site"]["collected_at"] = datetime.utcnow().isoformat()
                    insert_website_data(conn, business_id, res["site"])
                if res["enriched"]:
                    res["enriched"]["model"] = args.model
                    res["enriched"]["created_at"] = datetime.utcnow().isoformat()
                    insert_enrichment(conn, business_id, res["enriched"])
                if i % 10 == 0:
                    print(f"[web_search] {i}/{len(futures)} done")

    elif args.source == "facebook" and not args.enrich_approved:
        if args.query:
            fb_jobs = [(args.query, args.trade or "business")]
        else:
            from scrapers.queries import get_daily_queries
            fb_jobs = get_daily_queries(n=6, source="facebook")

        all_leads = []
        for query_str, category in fb_jobs:
            leads = scrape_facebook(query_str, max_results=args.max, headless=args.headless, category=category)
            all_leads.extend(leads)

        print(f"[facebook] Total collected: {len(all_leads)}")
        for lead in all_leads:
            score, reason = score_lead(lead)
            lead["score"] = score
            lead["score_reason"] = reason
            if args.auto_approve:
                lead["approved"] = 1
                lead["approved_at"] = datetime.utcnow().isoformat()
            business_id = upsert_business(conn, lead)
            if lead.get("about_text"):
                insert_website_data(conn, business_id, {
                    "site_url": lead.get("website", ""),
                    "site_status": "ok" if lead.get("website") else "",
                    "about_text": lead.get("about_text", ""),
                    "collected_at": lead["collected_at"],
                })

    if args.enrich_approved:
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

    if args.site_intel or args.site_intel_all:
        only_missing = not args.site_intel_all
        use_ai = not args.no_ai_validate
        print(f"[site-intel] Running website intelligence (only_missing={only_missing}, use_ai={use_ai})")
        counts = run_website_intel(conn, use_ai=use_ai, only_missing=only_missing)
        print(f"[site-intel] Done — reachable={counts['reachable']} unreachable={counts['unreachable']} "
              f"strong={counts.get('strong',0)} moderate={counts.get('moderate',0)} "
              f"weak={counts.get('weak',0)} skip={counts.get('skip',0)} total={counts['total']}")

    if args.validate or args.validate_all:
        only_pending = not args.validate_all
        use_ai = not args.no_ai_validate
        print(f"[validate] Running validation (only_pending={only_pending}, use_ai={use_ai})")
        counts = run_validation(conn, use_ai=use_ai, only_pending=only_pending)
        print(f"[validate] Done — qualified={counts['qualified']} skip={counts['skip']} needs_review={counts['needs_review']} total={counts['total']}")

    if args.contact_enrich or args.contact_enrich_all:
        only_missing = not args.contact_enrich_all
        print(f"[contact-enrich] Running contact+pain enrichment (only_missing={only_missing})")
        counts = run_contact_enrichment(conn, limit=200, only_missing=only_missing)
        print(f"[contact-enrich] Done — enriched={counts['enriched']} skipped={counts['skipped']} total={counts['total']}")

    export_csv(conn, args.csv)
    print(f"[done] Exported CSV to {args.csv}")


if __name__ == "__main__":
    main()
