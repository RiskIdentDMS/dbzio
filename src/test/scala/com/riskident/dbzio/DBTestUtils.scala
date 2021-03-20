package com.riskident.dbzio

import com.riskident.dbzio.DBZIO.HasDb
import com.typesafe.config.ConfigFactory
import slick.dbio.DBIO
import slick.jdbc.JdbcDataSource
import slick.jdbc.PostgresProfile.api.{Database => Db, _}
import zio._
import zio.console.{Console, putStrLnErr}
import zio.test.TestFailure

import java.sql.Connection

object DBTestUtils {
  object TestDb {
    val databaseName        = "fm-test"
    val user                = "fraudmanager"
    val password            = "officer"
    val serverName          = "localhost"
    val dataSourceClassName = "org.postgresql.ds.PGSimpleDataSource"
  }
  class HikariCPJdbcDataSource(val ds: com.zaxxer.hikari.HikariDataSource, val hconf: com.zaxxer.hikari.HikariConfig)
    extends JdbcDataSource {
    def createConnection(): Connection = ds.getConnection()
    def close(): Unit                  = ds.close()

    override val maxConnections: Option[Int] = Some(200)
  }

  /**
   * Executes [[DBIO]] in a naive way. We don't want [[DBZIO]] here, since this is what we're testing.
   */
  def execDBIO[T](dbio: DBIO[T]): RIO[Db, T] = ZIO.accessM[Db] { db => ZIO.fromFuture(_ => db.run(dbio)) }

  private val createDb: String => RIO[Db, Boolean] = name => {
    val test: RIO[Db, Boolean] = {
      val sql = sql"""SELECT COUNT(*) > 0 as f FROM pg_database WHERE datname = '#$name'"""
      execDBIO(sql.as[Boolean].head)
    }

    val create: RIO[Db, Any] = {
      val sql = sqlu"""CREATE DATABASE #$name"""
      execDBIO(sql)
    }
    for {
      exists <- test
      _      <- create.unless(exists)
    } yield !exists
  }

  val defaultDbConfig = s"""
                           |db = {
                           |  connectionPool = "HikariCP"
                           |  dataSourceClass = "org.postgresql.ds.PGSimpleDataSource"
                           |  properties {
                           |    serverName = "${TestDb.serverName}"
                           |    user = "${TestDb.user}"
                           |    password = "${TestDb.password}"
                           |    databaseName = "${TestDb.databaseName}"
                           |    portNumber = "5432"
                           |  }
                           |  maximumPoolSize = 1
                           |  numThreads = 1
                           |  connectionTimeout = 10s
                           |}
                           |""".stripMargin
  val testDb: (String, String) => RManaged[Console, Db] = (name, configStr) => {

    val res: RManaged[Console, (Db, Boolean)] = ZManaged.make {
      for {
        config <- ZIO.effect(
          ConfigFactory
            .parseString(configStr)
            .resolve()
        )
        db      <- ZIO.effect(Db.forConfig(path = "db", config = config, classLoader = DBTestUtils.getClass.getClassLoader))
        created <- createDb(name).provide(db)

      } yield (db, created)
    } {
      case (db, true) =>
        val zio = for {
          _ <- execDBIO(sqlu"DROP DATABASE #$name")
            .provide(db)
            .catchAll { t => putStrLnErr(s"Failed to drop database $name due to: $t") }
          _ <- ZIO.effect(db.close()).catchAll { t => putStrLnErr(s"Failed to close connections due to: $t") }
        } yield ()
        zio.ignore
      case (db, false) =>
        ZIO
          .effect(db.close())
          .catchAll { t => putStrLnErr(s"Failed to close connections due to: $t") }
          .ignore
    }
    res.map(_._1)
  }

  val dbLayer: RLayer[Console, HasDb]                         = ZLayer.fromManaged(testDb(TestDb.databaseName, defaultDbConfig))
  val testDbLayer: ZLayer[Console, TestFailure[Throwable], HasDb] = dbLayer.mapError(TestFailure.fail)


}
