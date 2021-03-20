package com.riskident

import com.riskident.dbzio.DBZIO.HasDb

package object dbzio {
  type DBAction[T] = DBZIO[HasDb, T]
}
