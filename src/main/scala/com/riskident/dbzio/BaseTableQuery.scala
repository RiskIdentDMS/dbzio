package com.riskident.dbzio


import com.riskident.dbzio.DBZIO.HasDb
import slick.ast.{BaseTypedType, ColumnOption, OptionTypedType, Type}
import slick.dbio.DBIO
import slick.jdbc.JdbcType
import slick.jdbc.PostgresProfile.api._
import slick.lifted.ProvenShape
import slick.sql.SqlProfile.ColumnOption.SqlType
import zio.{RIO, ZIO}

import scala.concurrent.ExecutionContext
import scala.util.Try

abstract class BaseTableQuery[T, E <: BaseTable[T]](cons: Tag => E)
  extends TableQuery(cons) {

  val cols: E = this.baseTableRow

  val tableName: String = this.baseTableRow.tableName

  def name(col: Rep[_]): String           = cols.name(col)
  def name(col: E => Rep[_]): String      = Try { cols.name(col(cols)) }.toOption.getOrElse("")
  def oldCol[C](col: E => Rep[C]): Rep[C] = col(cols)

  def extract(col: E => Rep[_]): Rep[_] = col(this.baseTableRow)

  val createTable: ExecutionContext => DBIO[Boolean] = implicit ec =>
    {
      for {
        exists <- sql"SELECT COUNT(*) > 0 as f FROM information_schema.tables where table_name = '#$tableName'"
          .as[Boolean]
          .head
        _ <- if (exists) {
          DBIO.successful(())
        } else {
          DBIO.sequence(this.schema.create +: allAdditionalDDLStatements.map(s => sqlu"#$s"))
        }
      } yield exists
    }.withPinnedSession

  /**
   * Should clear all caches that could be used in subclasses. Default: Empty.
   */
  val clearCaches: DBZIO[Any, Unit] = DBZIO.unit

  lazy val allAdditionalDDLStatements: Seq[String] =
    cols.additionalDDLStatements ++ cols.statementsForIndexesWithLikeSupport

  val dropTableDBIO: DBIO[Int] = sqlu"""drop table if exists #$tableName cascade"""
  val dropTable: RIO[HasDb, Unit] = ZIO
    .accessM[HasDb] { env => ZIO.fromFuture(_ => env.get.run(dropTableDBIO)) }
    .unit

  def allCols(tableAlias: String): String = cols(cols.*, tableAlias)

  val allCols: String = allCols(tableName)

  def nameWithTable(col: Rep[_]): String = tableName + "." + name(col)

  def cols(projection: ProvenShape[_], tableAlias: String): String = {
    BaseTable.cols(projection, tableAlias)
  }

  def colTypeMapper(col: Rep[_]): BaseColumnType[_] with BaseTypedType[_] = {
    col.toNode.nodeType.asInstanceOf[BaseColumnType[_] with BaseTypedType[_]]
  }

  val countRows: ExecutionContext => DBIO[Int] = implicit ec => {
    sql"select count(*) from #$tableName".as[Int].headOption.map(_.getOrElse(0))
  }

  def countEstimatedRows: DBZIO[HasDb, Int] = {
    DBZIO(sql"SELECT reltuples AS approximate_row_count FROM pg_class WHERE relname = '#$tableName'".as[Int])
      .map(_.headOption.getOrElse(0))
  }

  val truncate: ExecutionContext => DBIO[Int] = implicit ec => {
    for {
      count <- this.countRows(ec)
      res <- if (count > 0) {
        sqlu"truncate table #$tableName cascade"
      } else {
        DBIO.successful(0)
      }
    } yield res
  }
}

object BaseTableQuery {

  type Generic                         = BaseTableQuery[_, _ <: BaseTable[_]]
  type GenericTable[T]                 = BaseTableQuery[T, _ <: BaseTable[T]]
  type GenericQuery[T <: BaseTable[_]] = BaseTableQuery[_, T]

  private val typesToIgnore = Seq("serial")

  def sqlTypeName(tpe: Type, options: Seq[ColumnOption[_]] = Seq.empty): String = {
    extractType(options) match {
      case Some(t) => t
      case _ =>
        tpe match {
          case o: OptionTypedType[_] => o.elementType.asInstanceOf[JdbcType[_]].sqlTypeName(None)
          case j: BaseColumnType[_]  => j.sqlTypeName(None)
        }
    }
  }

  private def extractType(options: Seq[ColumnOption[_]]): Option[String] = {
    options.collectFirst {
      case SqlType(t) if !typesToIgnore.exists(_.equalsIgnoreCase(t)) => t
    }
  }

  def allCols(tables: BaseTableQuery[_, _]*): String = {
    tables.map(_.allCols).mkString(", ")
  }

  trait Insert[T, E <: BaseTable[T]] extends BaseTableQuery[T, E] {
    def insertMany(el: Iterable[T]): DBAction[Unit] = {
      DBZIO(this ++= el).unit
    }

    def insertElement(el: T): DBAction[T] = {
      DBZIO(this.returning(this) += el)
    }

    def insertElement[I, R](el: T, f: E => R)(
      implicit shape: Shape[_ <: FlatShapeLevel, R, I, R]
    ): DBZIO[Any, I] = {
      val q: Query[R, I, Seq] = this.map(f)
      DBZIO {
        this.returning(q) += el
      }
    }

  }

}
