FROM alpine

RUN apk add --update ca-certificates postgresql openssl libffi python3 && \
    rm -rf /var/cache/apk/*

WORKDIR /usr/src/app
COPY requirements.txt /usr/src/app/

RUN apk add --update build-base python3-dev libffi-dev openssl-dev postgresql-dev && \
    pip3 install -r requirements.txt
    rm -rf /var/cache/apk/*

ENV PGDATA=/pg.data
RUN mkdir -p "$PGDATA" && \
    chown postgres:postgres "$PGDATA" && \
    chmod 0700 "$PGDATA"
