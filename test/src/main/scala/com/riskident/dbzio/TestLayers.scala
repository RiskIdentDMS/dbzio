package com.riskident.dbzio

import slick.jdbc.JdbcBackend.Database
import zio.Console.printLineError
import zio.Random.nextUUID
import zio._

/**
  * Provides a bunch of methods for creating a db-environment for tests
  * @tparam T type of the DB config
  */
abstract class TestLayers[T](implicit tag: Tag[T]) {

  /**
    * Creates a string with DB config
    * @param dbName name of the test database
    * @return string - part of the config, with configuration for H2 in-memory database
    */
  def makeDbConfig(dbName: String): String = {
    s"""
       |db = {
       |  connectionPool = "HikariCP"
       |  jdbcUrl = "jdbc:h2:mem:$dbName"
       |  maximumPoolSize = 1
       |  numThreads = 1
       |  connectionTimeout = 1s
       |}
       |""".stripMargin
  }

  /** Creates a db config string with random db name */
  val randomDbConfig: UIO[String] = nextUUID.map(_.toString).map(makeDbConfig)

  /**
    * Creates a db config object from a config string
    * @param string a configuration string
    * @return a db config object
    */
  def produceConfig(string: String): Task[T]

  /**
    * Creates a [[zio.Layer]] with db config object from a [[ZIO]] that produces config string
    */
  def makeConfigLayer[R](makeConfigString: RIO[R, String]): RLayer[R, T] = ZLayer {
    makeConfigString
      .flatMap(produceConfig)
  }

  /**
    * Creates a [[zio.Layer]] with db config object from a [[zio.Layer]] that provides a db config string
    */
  def makeConfigLayer[R](stringLayer: RLayer[R, String]): RLayer[R, T] = ZLayer {
    ZIO.serviceWithZIO[String](produceConfig).provideSome[R](stringLayer)
  }

  /** Creates a [[zio.Layer]] with db config object for an in-memory database with random name */
  val randomConfigLayer: TaskLayer[T] = makeConfigLayer(randomDbConfig)

  /**
    * Produces a [[Database]] from a db config object
    */
  def makeDb(config: T): Task[Database]

  /** Creates a scoped [[ZIO]] of a [[Database]] from a db config object dependency */
  val testDbManaged: RIO[T with Scope, Database] =
    ZIO.acquireRelease {
      ZIO.service[T].flatMap(makeDb)
    } { db =>
      ZIO
        .attempt(db.close())
        .tapError(t => printLineError(s"Failed to close connections due to: $t"))
        .ignore
    }

  /** Creates a [[zio.Layer]] of a [[Database]] from a db config object dependency */
  val testDbLayerDep: RLayer[T, HasDb] = ZLayer.scoped[T](testDbManaged)

  /** Creates a [[zio.Layer]] of a [[Database]] with random name */
  val testDbLayer: TaskLayer[HasDb] =
    randomConfigLayer.to(testDbLayerDep)
}
