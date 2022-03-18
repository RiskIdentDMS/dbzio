package com.riskident.dbzio

import com.typesafe.config.ConfigFactory
import slick.jdbc.H2Profile.api.{Database => _, _}
import slick.jdbc.JdbcBackend.{Database => Db}
import slick.lifted.Tag
import zio.blocking.Blocking
import zio.{Tag => _, _}
import zio.console.{putStrLnErr, Console}
import zio.test.TestFailure
import zio.test.environment.TestEnvironment

object DBTestUtils {

  case class Data(id: Int, name: String)

  class DataTable(tag: Tag) extends BaseTable[Data](tag, "data") {
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("name", O.SqlType("varchar"))

    override def * = ((id, name)).<>((Data.apply _).tupled, Data.unapply)

    def nameIndex = indexWithDefaultName(name, unique = true)
  }

  object DataDaoZio extends BaseTableQuery[Data, DataTable](new DataTable(_)) {

    def doInsert(data: Data): DBZIO[Any, Data] = DBZIO { implicit ex =>
      for {
        id <- this.returning(this.map(_.id)).+=(data)
      } yield data.copy(id = id)
    }

    def loadById(id: Int): DBIO[Data] = this.filter(_.id === id).take(1).result.head

    def delete(id: Int): DBZIO[Any, Int] = DBZIO {
      this.filter(_.id === id).delete
    }

    val load: DBZIO[Any, Seq[Data]] = DBZIO {
      this.result
    }

    val count: DBZIO[Any, Int] = DBZIO {
      this.length.result
    }

  }

  class FailedTable(tag: Tag) extends BaseTable[Data](tag, "fail") {
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("name", O.SqlType("varchar"))

    override def * = ((id, name)).<>((Data.apply _).tupled, Data.unapply)

    def nameIndex = indexWithDefaultName(name, unique = true)
  }

  object FailedDaoZio extends BaseTableQuery[Data, FailedTable](new FailedTable(_)) {
    def doInsert(data: Data): DBZIO[Any, Data] = DBZIO { implicit ex =>
      for {
        id <- this.returning(this.map(_.id)).+=(data)
      } yield data.copy(id = id)
    }

  }

  val defaultDbConfig =
    s"""
       |db = {
       |  connectionPool = "HikariCP"
       |  jdbcUrl = "jdbc:h2:mem:"
       |  maximumPoolSize = 1
       |  numThreads = 1
       |  connectionTimeout = 1s
       |}
       |""".stripMargin
  val testDb: RManaged[Console, Db] = Managed.make {
    for {
      config <- ZIO.effect(
        ConfigFactory
          .parseString(defaultDbConfig)
          .resolve()
      )
      db <- ZIO.effect(Db.forConfig(path = "db", config = config, classLoader = DBTestUtils.getClass.getClassLoader))
    } yield db
  } { db =>
    ZIO
      .effect(db.close())
      .catchAll { t => putStrLnErr(s"Failed to close connections due to: $t") }
      .ignore
  }

  val dbLayer: RLayer[Blocking with Console, DbDependency] = ZLayer.fromManaged {
    for {
      db <- testDb
      _ <- ZManaged.make {
        Task.fromFuture { implicit ec =>
          db.run {
            for {
              exists <- DataDaoZio.createTableDBIO(ec)
              _      <- if (exists) DataDaoZio.dropTableDBIO else DBIO.successful(())
              res    <- DataDaoZio.createTableDBIO(ec)
            } yield res
          }
        }
      } { _ => DataDaoZio.dropTable.provide(Has(db)).ignore }
    } yield db
  } ++ ZLayer.identity[Blocking]

  val testLayer: ZLayer[TestEnvironment, TestFailure[Throwable], DbDependency] = dbLayer.mapError(TestFailure.fail)

}
