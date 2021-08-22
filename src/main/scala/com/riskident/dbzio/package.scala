package com.riskident

import slick.jdbc.JdbcBackend.Database
import zio.Has

package object dbzio {
  type HasDb = Has[Database]
  type DBAction[T] = DBZIO[HasDb, T]
}
