FROM postgres:9.4

RUN localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8
ENV LANG en_US.utf8

# install python
RUN apt-get purge -y python python-minimal python2.7-minimal \
    && apt-get update \
    && apt-get install -y python3 python3-pip

# install deps
RUN apt-get install -y libpq-dev

# make some symlinks
RUN cd /usr/bin \
    && ln -s pip-3.2 pip \
    && ln -s python3.2 python

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/
RUN pip install -r requirements.txt

RUN mkdir -p /data /wal_archive

COPY governor.py /usr/src/app/
COPY helpers /usr/src/app/helpers
COPY postgres.yml /usr/src/app/
ENV PYTHONPATH=/usr/src/app/

ENTRYPOINT ["python", "governor.py"]
CMD ["postgres.yml"]

