package com.riskident

import slick.jdbc
import slick.jdbc.JdbcBackend.Database
import zio.{Has, RIO, URIO, ZIO}
import zio.blocking.Blocking

import scala.annotation.tailrec

package object dbzio {
  type HasDb = Has[Database]

  type DbDependency = HasDb with Blocking
  type DBAction[T]  = DBZIO[DbDependency, T]

  type DbRIO[-R, +T]        = RIO[R with DbDependency, T]
  type DbURIO[-R, +T]       = URIO[R with DbDependency, T]
  type DbUIO[+T]            = URIO[DbDependency, T]
  type DbIO[+E, +T]         = ZIO[DbDependency, E, T]
  type DbZIO[-R, +E, +T]    = ZIO[DbDependency with R, E, T]
  type DbTask[+T]           = RIO[DbDependency, T]
  type JdbcProfile          = jdbc.JdbcProfile
  type TransactionIsolation = jdbc.TransactionIsolation

  val Db = Database

  implicit private[dbzio] def ordering[T]: Ordering[(Int, DBZIO.ZioResult[T])] =
    (x: (Int, DBZIO.ZioResult[T]), y: (Int, DBZIO.ZioResult[T])) => x._1.compareTo(y._1)

  /** It is allowed only to increase the isolation level in nested `DBZIO`, hence the [[Ordering]]. */
  implicit private[dbzio] val isolationOrd: Ordering[TransactionIsolation] = {
    import slick.jdbc.TransactionIsolation._
    val order: List[TransactionIsolation] = List(ReadUncommitted, ReadCommitted, RepeatableRead, Serializable)
    (x: TransactionIsolation, y: TransactionIsolation) => {
      @tailrec
      def cmp(check: List[TransactionIsolation]): Int = {
        if (check.isEmpty) {
          0
        } else {
          val head = check.head
          if (x == head && y == head) {
            0
          } else if (x == head) {
            -1
          } else if (y == head) {
            1
          } else {
            cmp(check.tail)
          }
        }
      }

      cmp(order)
    }
  }
}
