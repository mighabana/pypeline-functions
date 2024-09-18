FROM python:3.12-slim

# TODO: Review secret injection strategies

# ARG private_ssh_key
# ARG public_ssh_key

RUN apt-get update \
    && apt-get install -y \
    g++ \
    unixodbc \
    unixodbc-dev \
    freetds-dev \
    freetds-bin \
    tdsodbc 

WORKDIR /home/user/

COPY . /home/user/

COPY ./config/odbcinst.ini /etc/odbcinst.ini
COPY ./config/freetds.conf /etc/freetds/freetds.conf

# RUN --mount=type=secret,id=ssh_key mkdir -p /home/user/.ssh \
#     && cp /run/secrets/ssh_key /home/user/.ssh/id_rsa

# RUN --mount=type=secret,id=service_account_key mkdir -p /home/user/secrets \
#     && cp /run/secrets/service_account_key /home/user/secrets/service_account_key.json

RUN  python3 -m pip install -r requirements.txt