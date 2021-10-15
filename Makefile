# For updating the requirements files using pip-tools

BASE = "requirements"

all: base dev docs tests

base:
	rm -f requirements.txt
	pip-compile --output-file requirements.txt

dev:
	pip-compile ${BASE}/dev.in --output-file ${BASE}/${BASE}-dev.txt

docs:
	pip-compile ${BASE}/docs.in --output-file ${BASE}/${BASE}-docs.txt

tests:
	pip-compile ${BASE}/tests.in --output-file ${BASE}/${BASE}-tests.txt
