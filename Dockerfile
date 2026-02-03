FROM python:3.13-slim

WORKDIR /pipelines/

COPY . /pipelines/

RUN python3 -m pip install -e .

