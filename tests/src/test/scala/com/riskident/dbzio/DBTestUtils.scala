package com.riskident.dbzio

import com.riskident.dbzio
import com.typesafe.config.ConfigFactory
import slick.jdbc.H2Profile.api.{Database => _, _}
import slick.jdbc.JdbcBackend.Database
import slick.lifted.Tag
import zio.test.TestFailure
import zio.{Tag => _, _}

object DBTestUtils extends dbzio.TestLayers {
  override type Config = com.typesafe.config.Config

  case class Data(id: Int, name: String)

  class DataTable(tag: Tag) extends BaseTable[Data](tag, "data") {
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("name", O.SqlType("varchar"))

    override def * = (id, name).<>((Data.apply _).tupled, Data.unapply)

    def nameIndex = indexWithDefaultName(name, unique = true)
  }

  object DataDaoZio extends BaseTableQuery[Data, DataTable](new DataTable(_)) {

    def doInsert(data: Data): DBZIO[Any, Data] = DBZIO { implicit ex =>
      for {
        id <- this.returning(this.map(_.id)).+=(data)
      } yield data.copy(id = id)
    }

    def find(name: String): DBAction[Option[Data]] = DBZIO {
      this.filter(_.name === name).result.headOption
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

    override def * = (id, name).<>((Data.apply _).tupled, Data.unapply)

    def nameIndex = indexWithDefaultName(name, unique = true)
  }

  object FailedDaoZio extends BaseTableQuery[Data, FailedTable](new FailedTable(_)) {
    def doInsert(data: Data): DBZIO[Any, Data] = DBZIO { implicit ex =>
      for {
        id <- this.returning(this.map(_.id)).+=(data)
      } yield data.copy(id = id)
    }

  }

  val dbLayer: RLayer[DbDependency, DbDependency] = ZLayer.scoped[DbDependency] {
    for {
      db <- ZIO.service[Database]
      _ <- ZIO.acquireRelease {
        ZIO.fromFuture { implicit ec =>
          db.run {
            for {
              exists <- DataDaoZio.createTableDBIO(ec)
              _      <- if (exists) DataDaoZio.dropTableDBIO else DBIO.successful(())
              res    <- DataDaoZio.createTableDBIO(ec)
            } yield res
          }
        }
      } { _ => DataDaoZio.dropTable.ignore }
    } yield db
  }

  val testLayer: ZLayer[Any, TestFailure[Throwable], DbDependency] =
    (testDbLayer >>> dbLayer).mapError(TestFailure.fail)

  override def produceConfig(string: String): Task[Config] = ZIO.attempt {
    ConfigFactory
      .parseString(string)
      .resolve()
  }

  override def makeDb(config: Config): Task[Database] =
    ZIO.attempt(Db.forConfig(path = "db", config = config, classLoader = this.getClass.getClassLoader))
}
