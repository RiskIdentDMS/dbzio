package com.riskident.dbzio

import cats.Monad
import cats.kernel.Eq
import com.riskident.dbzio.DBTestUtils.{Data, dbLayer}
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalacheck.rng.Seed
import slick.dbio.DBIO
import zio.UIO

trait LowPrioImplicits {

  implicit val monad: Monad[DBAction] = new Monad[DBAction] {
    override def flatMap[A, B](fa: DBAction[A])(f: A => DBAction[B]): DBAction[B] = fa.flatMap(f)

    override def tailRecM[A, B](a: A)(f: A => DBAction[Either[A, B]]): DBAction[B] = {
      f(a).flatMap {
        case Left(value) => tailRecM(value)(f)
        case Right(value) => DBZIO.success(value)
      }
    }

    override def pure[A](x: A): DBAction[A] = DBZIO.success(x)
  }

  implicit val arbData: Arbitrary[Data] = Arbitrary(for {
    id <- Arbitrary.arbitrary[Int]
    name <- Arbitrary.arbitrary[String]
  } yield Data(id, name))

  implicit def arbDBAction[A: Arbitrary]: Arbitrary[DBAction[A]] = Arbitrary(Gen.oneOf(
    Arbitrary.arbitrary[A].map(a => DBZIO(UIO(a))),
    Arbitrary.arbitrary[A].map(a => DBZIO(DBIO.successful(a))),
    Arbitrary.arbitrary[A].map(a => DBZIO(implicit ec => DBIO.successful(()).map(_ => a))),
    Arbitrary.arbitrary[A].map(a => DBZIO(UIO(DBIO.successful(a))))
  ))

  implicit val eqData: Eq[Data] = new Eq[Data] {
    override def eqv(x: Data, y: Data): Boolean = x == y
  }
  implicit def eqDBAction[A]: Eq[DBAction[A]] = new Eq[DBAction[A]] {
    override def eqv(x: DBAction[A], y: DBAction[A]): Boolean = zio.Runtime.default.unsafeRun((for {
      a <- x
      b <- y
    } yield a == b).result.provideLayer(dbLayer))
  }

  implicit val cogenData: Cogen[Data] = Cogen((seed, t) => Seed.random())

}
