.PHONY: install browser run batch ui

install:
	pip install -r /Users/yegor/Documents/Agency\ \&\ Security\ Stuff/BIZ/leadgen/requirements.txt

browser:
	python -m playwright install chromium

run:
	/Users/yegor/Documents/Agency\ \&\ Security\ Stuff/BIZ/leadgen/scripts/run.sh "$(q)"

batch:
	/Users/yegor/Documents/Agency\ \&\ Security\ Stuff/BIZ/leadgen/scripts/run_batch.sh $(qf)

ui:
	/Users/yegor/Documents/Agency\ \&\ Security\ Stuff/BIZ/leadgen/scripts/ui.sh
