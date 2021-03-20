FROM mdillon/postgis:9.5

ENV DB_NAME=postgres
RUN apt-get update
RUN apt-get install -y pgbadger pg-activity
RUN apt-get clean
COPY postgresql.conf .
COPY help.sh /usr/local/bin/help.sh
COPY setup.sh /docker-entrypoint-initdb.d/
RUN echo "help.sh" >> /root/.bashrc



