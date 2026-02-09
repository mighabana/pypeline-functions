.PHONY: init build publish build-docker publish-docker

VERSION := $(shell grep -m 1 version pyproject.toml | tr -s ' ' | tr -d '"' | tr -d "'" | cut -d' ' -f3)

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
	python3 -m venv .venv && \
	source .venv/bin/activate && \
	pip install .[dev]

build:
	hatch build

publish:
	python3 -m twine upload dist/*

build-docker:
	docker buildx build \
	--platform linux/amd64,linux/arm64 \
	--tag mighabana/infolio:latest \
	--tag mighabana/infolio:v$(VERSION) \
	.

build-docker-dev:
	docker buildx build \
	--platform linux/amd64,linux/arm64 \
	--tag mighabana/infolio:dev \
	.

publish-docker:
	docker push mighabana/infolio:latest && \
	docker push mighabana/infolio:v$(VERSION)

publish-docker-dev:
	docker push mighabana/infolio:dev
