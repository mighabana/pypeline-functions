.PHONY: init build publish build-docker publish-docker

VERSION := $(shell python3 -c "from _version import __version__; print(__version__)")

help:
	@echo "make"
	@echo "		init"
	@echo "			initializes the repository"
	@echo "		build"
	@echo "			builds the python distribution"
	@echo "		publish"
	@echo "			publishes the python distribution"
	@echo "		build-docker"
	@echo "			builds the docker image"
	@echo "		publish-docker"
	@echo "			publishes the docker image"

init:
	python3 -m venv venv && \
	source venv/bin/activate && \
	pip install .[dev]

build:
	hatch build

publish:
	python3 -m twine upload dist/*

build-docker:
	docker buildx build --platform linux/amd64,linux/arm64 --tag mighabana/pypeline-functions:latest --tag mighabana/pypeline-functions:v$(VERSION) .

publish-docker:
	docker push mighabana/pypeline-functions:latest && \
	docker push mighabana/pypeline-functions:v$(VERSION)