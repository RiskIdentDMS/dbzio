# DBZIO
DBZIO is a wrapper to combine ZIO and DBIO actions in one for-comprehension. Unlike other libraries,
DBZIO provides possibility to run the resulting action in a context of database transaction.

This skeleton project reflects very closely what we have in FRIDA 1.

## Prerequisites

- [AdoptOpenJDK JDK 11](https://adoptopenjdk.net/installation.html#)
- [Ammonite](https://ammonite.io/#Ammonite-REPL)
- [Docker](https://docs.docker.com/engine/install/)
- [Docker-compose](https://docs.docker.com/compose/install/)
- [sbt](https://www.scala-sbt.org/1.x/docs/Setup.html)

## How to run tests

From the folder `misc` run script `run.sc`. This will start a docker with postgresql.
After the PGSql is started, you may run the tests.
