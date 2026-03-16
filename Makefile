.PHONY: install browser run batch ui enrich

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
