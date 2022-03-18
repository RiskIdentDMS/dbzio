package com.riskident.dbzio

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.language.higherKinds

trait CanCollect[Elem, To[_]] extends CanBuildFrom[To[Any], Elem, To[Elem]] {
  def from(from: To[Any]): mutable.Builder[Elem, To[Elem]]
}

object CanCollect {
  implicit def canCollect[Elem, To[_]](
      implicit builder: CanBuildFrom[To[Any], Elem, To[Elem]]
  ): CanCollect[Elem, To] = {
    new CanCollect[Elem, To] {
      override def apply(from: To[Any]): mutable.Builder[Elem, To[Elem]]     = builder(from)
      override def apply(): scala.collection.mutable.Builder[Elem, To[Elem]] = builder()
      override def from(from: To[Any]): mutable.Builder[Elem, To[Elem]]      = builder(from)
    }
  }
}
