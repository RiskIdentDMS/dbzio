# DBZIO
DBZIO is a wrapper to combine ZIO and DBIO actions in one for-comprehension. Unlike other libraries,
DBZIO provides possibility to run the resulting action in a context of database transaction.

[![Build & Tests](https://github.com/RiskIdentDMS/dbzio/actions/workflows/tests.yaml/badge.svg?branch=master)](https://github.com/RiskIdentDMS/dbzio/actions/workflows/tests.yaml)

## Prerequisites

- [AdoptOpenJDK JDK 11](https://adoptopenjdk.net/installation.html#)
- [sbt](https://www.scala-sbt.org/1.x/docs/Setup.html)

## How to use the library

Add dependency to the project
```sbt
libraryDependencies += "com.riskident" %% "dbzio" % <Version>
```

In the code:
```scala
import com.riskident.dbzio._
import slick.jdbc.<SomeProfile>.api._
import zio._

...
// Read data from DB
val readFromDb: DBIO[Vector[Data]] = sql"SELECT * FROM table".as[Data]
// Get relevant data from a remote service
val transformData: Vector[Data] => Task[List[OtherData]] = ...
// Save updated data in DB
val saveDataInDb: List[OtherData] => DBIO[Int] = data => sqlu"INSERT INTO table2 VALUES ($data)"

// The combined action
val action = for {
  data <- DBZIO(readFromDb)
  newData <- DBZIO(transformData(data))
  res <- DBZIO(saveDataInDb(newData))
} yield res

// The overall action should run within a db-transaction
val dbzioAction: DBZIO[Any, Int] = action.transactionally

// Back to ZIO world
val zioAction: RIO[DbDependency, Int] = dbzioAction.result

```

In the example above:
1. some data is read from DB
2. this data is sent to some remote service, which returns a new data relevant to the one being sent
3. received data is saved in the DB

The whole operation is done inside db-transaction

## Using DBZIO in tests

DBZIO provides a way to easily provide `DbDependency` for tests. In order to achieve it, add a dependency to the project

```sbt
libraryDependencies += "com.riskident" %% "dbzio-test" % <Version> % Test
```

And in tests add the following:

```scala
import com.riskident.dbzio
import com.typesafe.config.{Config, ConfigFactory}
import slick.jdbc.H2Profile.api.{Database => _, _}
import slick.jdbc.JdbcBackend.Database
import zio.{Tag => _, _}
import zio.test._
import zio.blocking.Blocking
import zio.console.Console
import zio.test.TestFailure
import zio.test.environment.TestEnvironment

object SomeTest extends DefaultRunnableSpec with TestLayers[Config] {
  override def produceConfig(string: String): Task[Config] = Task {
    ConfigFactory
      .parseString(string)
      .resolve()
  }

  override def makeDb(config: Config): Task[Database] =
    Task(Db.forConfig(path = "db", config = config, classLoader = this.getClass.getClassLoader))


  val testLayer: ZLayer[TestEnvironment, TestFailure[Throwable], DbDependency] =
    (testDbLayer ++ ZLayer.identity[Blocking] ++ ZLayer.identity[Console]).mapError(TestFailure.fail)

  override def spec: ZSpec[TestEnvironment, Any] = suite("Some tests using db")(...).provideCustomLayer(testLayer)
}

```

This will allow to use [H2](https://www.h2database.com/html/main.html) in-memory database with random name in each test,
effectively running tests in parallel with separate databases.
