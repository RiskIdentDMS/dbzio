package com.riskident.dbzio

import cats.Monad
import cats.kernel.Eq
import com.riskident.dbzio.DBTestUtils.Data
import org.scalacheck.rng.Seed
import org.scalacheck.{Arbitrary, Cogen, Gen}
import slick.dbio.DBIO
import zio.{TaskLayer, Unsafe, ZIO}

trait LowPrioImplicits {

  implicit val monad: Monad[DBAction] = new Monad[DBAction] {
    override def flatMap[A, B](fa: DBAction[A])(f: A => DBAction[B]): DBAction[B] = fa.flatMap(f)

    override def tailRecM[A, B](a: A)(f: A => DBAction[Either[A, B]]): DBAction[B] = {
      f(a).flatMap {
        case Left(value)  => tailRecM(value)(f)
        case Right(value) => DBZIO.success(value)
      }
    }

    override def pure[A](x: A): DBAction[A] = DBZIO.success(x)
  }

  implicit def arbDBAction[A: Arbitrary]: Arbitrary[DBAction[A]] =
    Arbitrary(
      Gen.oneOf(
        Arbitrary.arbitrary[A].map(a => DBZIO(ZIO.succeed(a))),
        Arbitrary.arbitrary[A].map(a => DBZIO(DBIO.successful(a))),
        Arbitrary.arbitrary[A].map(a => DBZIO(implicit ec => DBIO.successful(()).map(_ => a))),
        Arbitrary.arbitrary[A].map(a => DBZIO(ZIO.succeed(DBIO.successful(a))))
      )
    )

  implicit val eqData: Eq[Data]              = _ == _
  private val layer: TaskLayer[DbDependency] = DBTestUtils.testDbLayer
  implicit def eqDBAction[A]: Eq[DBAction[A]] =
    (x: DBAction[A], y: DBAction[A]) =>
      Unsafe.unsafe { implicit u =>
        zio.Runtime.default.unsafe
          .run {
            (for {
              a <- x
              b <- y
            } yield a == b).result
              .provideLayer(layer)
          }
          .getOrThrow()
      }

  implicit val cogenData: Cogen[Data] = Cogen((seed, t) => Seed.random())

}
