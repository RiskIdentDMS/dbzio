package com.riskident.dbzio

import slick.jdbc.JdbcBackend.Database
import zio._
import zio.console.{Console, putStrLnErr}
import zio.random.{Random, nextUUID}

trait TestLayers[T] {

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

  val randomDbConfig: URIO[Random, String] = nextUUID.map(_.toString).map(makeDbConfig)

  def produceConfig(string: String): Task[T]

  def makeConfigLayer[R](makeConfigString: RIO[R, String])(implicit ev: Tag[T]): RLayer[R, Has[T]] =
    makeConfigString
      .flatMap(produceConfig)
      .toLayer

  def makeConfigLayer[R](stringLayer: RLayer[R, Has[String]])(implicit ev: Tag[T]): RLayer[R, Has[T]] =
    stringLayer >>> ZLayer.fromFunctionM(h => produceConfig(h.get))

  def randomConfigLayer(implicit ev: Tag[T]): RLayer[Random, Has[T]] = makeConfigLayer(randomDbConfig)

  def makeDb(config: T): Task[Database]

  def testDbManaged(implicit ev: Tag[T]): RManaged[Console with Has[T], Database] =
    Managed.make {
      ZIO.service[T] >>= makeDb
    } { db => Task(db.close()).catchAll { t => putStrLnErr(s"Failed to close connections due to: $t") }.ignore }

  def testDbLayerDep(implicit ev: Tag[T]): RLayer[Has[T] with Console, HasDb] = testDbManaged.toLayer

  def testDbLayer(implicit ev: Tag[T]): RLayer[Console with Random, HasDb] =
    (ZLayer.identity[Console] ++ randomConfigLayer) >>> testDbLayerDep
}
