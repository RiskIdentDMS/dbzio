package com.riskident.dbzio

import com.riskident.dbzio.test.testDbLayer
import slick.jdbc.H2Profile.api.{Database => _, _}
import slick.jdbc.JdbcBackend.Database
import slick.lifted.Tag
import zio.blocking.Blocking
import zio.console.Console
import zio.test.TestFailure
import zio.test.environment.TestEnvironment
import zio.{Tag => _, _}

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

  val dbLayer: RLayer[Blocking with Console with HasDb, DbDependency] = ZLayer.fromManaged {
    for {
      db <- ZManaged.service[Database]
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

  val testLayer: ZLayer[TestEnvironment, TestFailure[Throwable], DbDependency] =
    ((testDbLayer ++ ZLayer.identity[Blocking] ++ ZLayer.identity[Console]) >>> dbLayer).mapError(TestFailure.fail)

}
