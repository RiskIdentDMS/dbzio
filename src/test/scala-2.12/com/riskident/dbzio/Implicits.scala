package com.riskident.dbzio

import cats.laws.discipline.SemigroupalTests.Isomorphisms

object Implicits extends LowPrioImplicits {
  implicit val isomorphism: Isomorphisms[DBAction] = Isomorphisms.invariant[DBAction]
}
