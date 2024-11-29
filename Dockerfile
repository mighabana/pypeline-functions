FROM python:3.12-slim

WORKDIR /home/user/

COPY . /home/user/

RUN  python3 -m pip install -r requirements.txt