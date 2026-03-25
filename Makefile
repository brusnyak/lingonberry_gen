.PHONY: install browser run batch ui enrich validate contact-enrich hipages web-search facebook daily-au

install:
	pip install -r requirements.txt

browser:
	python -m playwright install chromium

run:
	bash scripts/run.sh "$(q)"

batch:
	bash scripts/run_batch.sh $(qf)

ui:
	bash scripts/ui.sh

enrich:
	bash scripts/enrich.sh $(m)

validate:
	python main.py --validate --db data/leads.db

validate-all:
	python main.py --validate-all --db data/leads.db

contact-enrich:
	python main.py --contact-enrich --db data/leads.db

hipages:
	python main.py --source hipages --trade $(trade) --location $(loc) --max 20 --headless --auto-approve --db data/leads.db

web-search:
	python main.py --source web_search --max 60 --db data/leads.db

facebook:
	python main.py --source facebook --max 15 --headless --db data/leads.db

daily-au:
	python main.py --source hipages --trade plumber --location sydney --max 20 --headless --auto-approve --db data/leads.db
	python main.py --source hipages --trade electrician --location melbourne --max 20 --headless --auto-approve --db data/leads.db
	python main.py --source web_search --max 60 --db data/leads.db
	python main.py --contact-enrich --db data/leads.db
