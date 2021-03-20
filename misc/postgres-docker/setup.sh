#!/bin/sh

set -e

CONFIG_FILE="$(psql -t -P format=unaligned -U postgres -c 'show config_file')"
echo $CONFIG_FILE

cp /postgresql.conf ${CONFIG_FILE}

psql $DB_NAME << EOF
create role fraudmanager login password 'officer' CREATEDB;
EOF

psql $DB_NAME << EOF
create database fraudmanager WITH ENCODING='UTF8' owner=fraudmanager;
EOF

psql $DB_NAME << EOF
\connect fraudmanager
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_topology;
CREATE EXTENSION pg_stat_statements;
CREATE EXTENSION "uuid-ossp";
GRANT SELECT ON geometry_columns TO fraudmanager;
GRANT SELECT ON geography_columns TO fraudmanager;
GRANT SELECT ON spatial_ref_sys TO fraudmanager;
EOF

psql $DB_NAME << EOF
\connect fraudmanager
CREATE EXTENSION pg_trgm;
create database "fm-test" WITH ENCODING='UTF8' owner=fraudmanager;
EOF

psql $DB_NAME << EOF
\connect fm-test
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_topology;
CREATE EXTENSION "uuid-ossp";
GRANT SELECT ON geometry_columns TO fraudmanager;
GRANT SELECT ON geography_columns TO fraudmanager;
GRANT SELECT ON spatial_ref_sys TO fraudmanager;
CREATE EXTENSION pg_trgm;
EOF

psql $DB_NAME << EOF
create role geocode_proxy login password 'officer';
EOF

psql $DB_NAME << EOF
create database geocode_proxy WITH ENCODING='UTF8' owner=geocode_proxy;
EOF

psql $DB_NAME << EOF
\connect geocode_proxy;
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_topology;
GRANT SELECT ON geometry_columns TO geocode_proxy;
GRANT SELECT ON geography_columns TO geocode_proxy;
GRANT SELECT ON spatial_ref_sys TO geocode_proxy;
CREATE EXTENSION pg_trgm;
EOF

psql $DB_NAME << EOF
create database "geocode_proxy-test" WITH ENCODING='UTF8' owner=geocode_proxy;
\connect geocode_proxy-test;
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_topology;
GRANT SELECT ON geometry_columns TO geocode_proxy;
GRANT SELECT ON geography_columns TO geocode_proxy;
GRANT SELECT ON spatial_ref_sys TO geocode_proxy;
CREATE EXTENSION pg_trgm;
EOF