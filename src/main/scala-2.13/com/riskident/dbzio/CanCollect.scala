package com.riskident.dbzio

import scala.collection.{mutable, BuildFrom}

trait CanCollect[Elem, To[_]] extends BuildFrom[To[Any], Elem, To[Elem]] {
  def from(from: To[Any]): mutable.Builder[Elem, To[Elem]]
}

object CanCollect {
  implicit def canCollect[Elem, To[_]](implicit builder: BuildFrom[To[Any], Elem, To[Elem]]): CanCollect[Elem, To] =
    new CanCollect[Elem, To] {
      override def fromSpecific(from: To[Any])(it: IterableOnce[Elem]): To[Elem] = builder.fromSpecific(from)(it)
      override def newBuilder(from: To[Any]): mutable.Builder[Elem, To[Elem]]    = builder.newBuilder(from)
      override def from(from: To[Any]): mutable.Builder[Elem, To[Elem]]          = builder.newBuilder(from)
    }
}
