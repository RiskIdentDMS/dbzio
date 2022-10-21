package com.riskident.dbzio

import slick.jdbc.JdbcBackend.Database
import zio._
import zio.console.{putStrLnErr, Console}
import zio.random.{nextUUID, Random}

/**
  * Provides a bunch of methods for creating a db-environment for tests
  * @tparam T type of the DB config
  */
trait TestLayers[T] {

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
  val randomDbConfig: URIO[Random, String] = nextUUID.map(_.toString).map(makeDbConfig)

  /**
    * Creates a db config object from a config string
    * @param string a configuration string
    * @return a db config object
    */
  def produceConfig(string: String): Task[T]

  /**
    * Creates a [[zio.Layer]] with db config object from a [[ZIO]] that produces config string
    */
  def makeConfigLayer[R](makeConfigString: RIO[R, String])(implicit ev: Tag[T]): RLayer[R, Has[T]] =
    makeConfigString
      .flatMap(produceConfig)
      .toLayer

  /**
    * Creates a [[zio.Layer]] with db config object from a [[zio.Layer]] that provides a db config string
    */
  def makeConfigLayer[R](stringLayer: RLayer[R, Has[String]])(implicit ev: Tag[T]): RLayer[R, Has[T]] =
    stringLayer >>> ZLayer.fromFunctionM(h => produceConfig(h.get))

  /** Creates a [[zio.Layer]] with db config object for an in-memory database with random name */
  def randomConfigLayer(implicit ev: Tag[T]): RLayer[Random, Has[T]] = makeConfigLayer(randomDbConfig)

  /**
    * Produces a [[Database]] from a db config object
    */
  def makeDb(config: T): Task[Database]

  /** Creates a [[zio.Managed]] of a [[Database]] from a db config object dependency */
  def testDbManaged(implicit ev: Tag[T]): RManaged[Console with Has[T], Database] =
    Managed.make {
      ZIO.service[T] >>= makeDb
    } { db => Task(db.close()).catchAll { t => putStrLnErr(s"Failed to close connections due to: $t") }.ignore }

  /** Creates a [[zio.Layer]] of a [[Database]] from a db config object dependency */
  def testDbLayerDep(implicit ev: Tag[T]): RLayer[Has[T] with Console, HasDb] = testDbManaged.toLayer

  /** Creates a [[zio.Layer]] of a [[Database]] with random name */
  def testDbLayer(implicit ev: Tag[T]): RLayer[Console with Random, HasDb] =
    (ZLayer.identity[Console] ++ randomConfigLayer) >>> testDbLayerDep
}
