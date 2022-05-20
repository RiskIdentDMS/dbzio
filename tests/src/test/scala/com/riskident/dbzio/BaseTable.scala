package com.riskident.dbzio

import slick.ast._
import slick.jdbc.H2Profile.api._
import slick.lifted.{Index, ProvenShape, RepShape}
import slick.util.ConstArray

abstract class BaseTable[T](tag: Tag, tableName: String) extends Table[T](tag, tableName) {

  def geoLocationIndexWithDefaultName(col: Rep[_]): String =
    "CREATE INDEX " + tableName + "_" + name(col) + "_gix ON " +
      tableName + " USING GIST ( " + name(col) + " );"

  def indexWithDefaultName(col: Rep[_], unique: Boolean = false): Index =
    this.index(tableName + "_" + name(col) + "_idx", col, unique)(RepShape)

  def name(col: Rep[_]): String = {
    val node = col.toNode match {
      case x: OptionApply => x.child
      case x              => x
    }
    node.asInstanceOf[Select].field.name
  }

  protected def createTrigramIndexStatement(indexName: String, column: Rep[_]): String = {
    "CREATE INDEX " + indexName + " ON " + tableName + " USING GIST(" + name(column) + " gist_trgm_ops);"
  }

  def additionalDDLStatements: Seq[String] = Nil

  def indexedColumnsWithLikeSupport: Seq[Rep[_]] = Nil

  private[dbzio] def statementsForIndexesWithLikeSupport: Seq[String] =
    indexedColumnsWithLikeSupport.map { column =>
      s"""CREATE INDEX "${this.tableName}_${name(column)}_with_like_support_idx" ON "${this.tableName}" """ +
        s"""USING btree (lower("${name(column)}") COLLATE pg_catalog.default text_pattern_ops);""" // text_pattern_ops seems to be an alias for varchar_pattern_ops
    }

}

object BaseTable {

  def selectsFromProjection(proj: ProvenShape[_]): Seq[Select] = {
    val selects = extractSelects(proj.toNode)
    selects.toSeq
  }

  private def extractSelects(node: Node): ConstArray[Select] = {
    val selects = node.children.filter {
      case _: Select => true
      case _         => false
    }
    if (selects.isEmpty) {
      node.children.flatMap(extractSelects)
    } else {
      selects.map(_.asInstanceOf[Select])
    }
  }

  def cols(projection: ProvenShape[_], tableAlias: String): String = {
    selectsFromProjection(projection).map(tableAlias + "." + _.field.name).mkString(", ")
  }

  def colNamesWithTypesAsList(projection: ProvenShape[_]): Seq[(String, String)] = {
    selectsFromProjection(projection).map { s: Select =>
      val fieldSymbol = s.field.asInstanceOf[FieldSymbol]
      val options     = fieldSymbol.options
      (s.field.name, BaseTableQuery.sqlTypeName(fieldSymbol.tpe, options))
    }
  }

}
