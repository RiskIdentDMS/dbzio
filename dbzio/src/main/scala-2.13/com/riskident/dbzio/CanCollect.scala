package com.riskident.dbzio

import scala.collection.{mutable, BuildFrom, IterableOps}

trait CanCollect[Elem, To[+Element] <: Iterable[Element]] extends BuildFrom[To[Any], Elem, To[Elem]] {
  def from(from: To[Any]): mutable.Builder[Elem, To[Elem]]
}
object CanCollect {
  implicit def canCollect[Elem, To[+E] <: Iterable[E] with IterableOps[E, To, _]]: CanCollect[Elem, To] = {
    val builder: BuildFrom[To[Any], Elem, To[Elem]] = BuildFrom.buildFromIterableOps[To, Any, Elem]
    new CanCollect[Elem, To] {
      override def fromSpecific(from: To[Any])(it: IterableOnce[Elem]): To[Elem] = builder.fromSpecific(from)(it)
      override def newBuilder(from: To[Any]): mutable.Builder[Elem, To[Elem]]    = builder.newBuilder(from)
      override def from(from: To[Any]): mutable.Builder[Elem, To[Elem]]          = builder.newBuilder(from)
    }
  }
}
