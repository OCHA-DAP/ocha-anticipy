# For updating the requirements files using pip-tools

BASE = "requirements"

all: base dev docs tests

base:
	pip-compile ../setup.cfg --output-file ${BASE}.txt

dev:
	pip-compile dev.in --output-file ${BASE}-dev.txt

docs:
	pip-compile docs.in --output-file ${BASE}-docs.txt

tests:
	pip-compile tests.in --output-file ${BASE}-tests.txt
