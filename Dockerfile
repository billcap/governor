FROM alpine

RUN apk add --update ca-certificates postgresql python3 && \
    rm -rf /var/cache/apk/*

WORKDIR /usr/src/app
COPY requirements.txt /usr/src/app/

RUN apk add --update build-base python3-dev
RUN pip3 install -r requirements.txt

ENV PGDATA=/pg.data
RUN mkdir -p "$PGDATA" && \
    chown postgres:postgres "$PGDATA" && \
    chmod 0700 "$PGDATA"
