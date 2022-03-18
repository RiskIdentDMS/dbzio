package com.riskident

import slick.jdbc.JdbcBackend.Database
import zio.{Has, RIO, URIO, ZIO}
import zio.blocking.Blocking

package object dbzio {
  type HasDb = Has[Database]

  type DbDependency = HasDb with Blocking
  type DBAction[T]  = DBZIO[DbDependency, T]

  type DbRIO[-R, +T]     = RIO[R with DbDependency, T]
  type DbURIO[-R, +T]    = URIO[R with DbDependency, T]
  type DbUIO[+T]         = URIO[DbDependency, T]
  type DbIO[+E, +T]      = ZIO[DbDependency, E, T]
  type DbZIO[-R, +E, +T] = ZIO[DbDependency with R, E, T]
  type DbTask[+T]        = RIO[DbDependency, T]

  implicit private[dbzio] def ordering[T]: Ordering[(Int, DBZIO.ZioResult[T])] =
    (x: (Int, DBZIO.ZioResult[T]), y: (Int, DBZIO.ZioResult[T])) => x._1.compareTo(y._1)
}
