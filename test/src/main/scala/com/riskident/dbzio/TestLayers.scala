package com.riskident.dbzio

import slick.jdbc.JdbcBackend.Database
import zio.Console.printLineError
import zio.Random.nextUUID
import zio._

abstract class TestLayers[T](implicit tag: Tag[T]) {

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

  val randomDbConfig: UIO[String] = nextUUID.map(_.toString).map(makeDbConfig)

  def produceConfig(string: String): Task[T]

  def makeConfigLayer[R](makeConfigString: RIO[R, String]): RLayer[R, T] = ZLayer {
    makeConfigString
      .flatMap(produceConfig)
  }

  def makeConfigLayer[R](stringLayer: RLayer[R, String]): RLayer[R, T] = ZLayer {
    ZIO.serviceWithZIO[String](produceConfig).provideSome[R](stringLayer)
  }

  val randomConfigLayer: TaskLayer[T] = makeConfigLayer(randomDbConfig)

  def makeDb(config: T): Task[Database]

  val testDbManaged: RIO[T with Scope, Database] =
    ZIO.acquireRelease {
      ZIO.service[T].flatMap(makeDb)
    } { db =>
      ZIO.attempt(db.close()).catchAll { t => printLineError(s"Failed to close connections due to: $t") }.ignore
    }

  val testDbLayerDep: RLayer[T, HasDb] = ZLayer.scoped[T](testDbManaged)

  val testDbLayer: TaskLayer[HasDb] =
    randomConfigLayer.to(testDbLayerDep)
}
