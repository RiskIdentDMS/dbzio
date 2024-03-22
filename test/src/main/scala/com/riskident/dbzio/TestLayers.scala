package com.riskident.dbzio

import slick.jdbc.JdbcBackend.Database
import zio.Console.printLineError
import zio.Random.nextUUID
import zio._

/**  Provides a bunch of methods for creating a db-environment for tests */
trait TestLayers {

  /** Type of the DB config object*/
  type Config

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
  def produceConfig(string: String): Task[Config]

  /**
    * Creates a [[zio.Layer]] with db config object from a [[ZIO]] that produces config string
    */
  def makeConfigLayer[R](makeConfigString: RIO[R, String])(implicit ev: zio.Tag[Config]): RLayer[R, Config] = ZLayer {
    makeConfigString
      .flatMap(produceConfig)
  }

  /**
    * Creates a [[zio.Layer]] with db config object from a [[zio.Layer]] that provides a db config string
    */
  def makeConfigLayer[R](stringLayer: RLayer[R, String])(implicit ev: zio.Tag[Config]): RLayer[R, Config] = ZLayer {
    ZIO.serviceWithZIO[String](produceConfig).provideSome[R](stringLayer)
  }

  /** Creates a [[zio.Layer]] with db config object for an in-memory database with random name */
  def randomConfigLayer(implicit ev: zio.Tag[Config]): TaskLayer[Config] = makeConfigLayer(randomDbConfig)

  /**
    * Produces a [[Database]] from a db config object
    */
  def makeDb(config: Config): Task[Database]

  /** Creates a scoped [[ZIO]] of a [[Database]] from a db config object dependency */
  def testDbManaged(implicit ev: zio.Tag[Config]): RIO[Config with Scope, Database] =
    ZIO.acquireRelease {
      ZIO.serviceWithZIO[Config](makeDb)
    } { db =>
      ZIO
        .attempt(db.close())
        .tapError(t => printLineError(s"Failed to close connections due to: $t"))
        .ignore
    }

  /** Creates a [[zio.Layer]] of a [[Database]] from a db config object dependency */
  def testDbLayerDep(implicit ev: zio.Tag[Config]): RLayer[Config, HasDb] = ZLayer.scoped[Config](testDbManaged)

  /** Creates a [[zio.Layer]] of a [[Database]] with random name */
  def testDbLayer(implicit ev: zio.Tag[Config]): TaskLayer[HasDb] =
    randomConfigLayer
      .to(testDbLayerDep)
}
