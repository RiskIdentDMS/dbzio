package com.riskident.dbzio

import com.typesafe.config.{Config, ConfigFactory}
import slick.jdbc.JdbcBackend.Database
import zio._
import zio.console.{putStrLnErr, Console}
import zio.random._

package object test {

  private val defaultDbConfig = nextUUID.map(_.toString).map { name =>
    s"""
       |db = {
       |  connectionPool = "HikariCP"
       |  jdbcUrl = "jdbc:h2:mem:$name"
       |  maximumPoolSize = 1
       |  numThreads = 1
       |  connectionTimeout = 1s
       |}
       |""".stripMargin
  }

  val configLayer: RLayer[Random, Has[Config]] = defaultDbConfig
    .map(config =>
      ConfigFactory
        .parseString(config)
        .resolve()
    )
    .toLayer

  val testDbManaged: RManaged[Console with Has[Config], Database] = Managed.make {
    for {
      config <- ZIO.service[Config]
      db     <- ZIO.effect(Db.forConfig(path = "db", config = config, classLoader = this.getClass.getClassLoader))
    } yield db
  } { db => Task(db.close()).catchAll { t => putStrLnErr(s"Failed to close connections due to: $t") }.ignore }

  val testDbLayerDep: RLayer[Has[Config] with Console, HasDb] = testDbManaged.toLayer

  val testDbLayer: RLayer[Console with Random, HasDb] = (ZLayer.identity[Console] ++ configLayer) >>> testDbLayerDep
}
