package com.riskident.dbzio

import cats.implicits._
import cats.laws.discipline._
import com.riskident.dbzio.DBTestUtils._
import com.riskident.dbzio.Implicits._
import org.scalacheck.Test.{Parameters, TestCallback}
import org.scalacheck.util.ConsoleReporter
import org.scalacheck.{Arbitrary, Gen, Test}
import zio._
import zio.test.Assertion._
import zio.test.{Gen => _, _}

object DBZIOTestLaws extends ZIOSpecDefault {

  case class PromiseCallback(p: Promise[Throwable, Unit], runtime: zio.Runtime[Any]) extends TestCallback {
    override def onTestResult(name: String, result: Test.Result): Unit = {
      Unsafe.unsafe { implicit u =>
        runtime.unsafe.run {
          p.succeed(()).when(result.passed) *>
            p.fail(new RuntimeException(s"Test $name didn't pass"))
              .unless(result.passed)
        }
      }
    }
  }

  implicit val abData: Arbitrary[Data] = Arbitrary(
    for {
      id   <- Gen.long.map(_.toInt)
      name <- Gen.alphaNumStr
    } yield Data(id, name)
  )

  val laws: MonadTests[DBAction]#RuleSet = MonadTests[DBAction].monad[Int, String, Data]

  private val tests = laws.all.properties
    .map {
      case (name, prop) =>
        test(name) {
          for {
            p       <- Promise.make[Throwable, Unit]
            runtime <- ZIO.runtime[Any]
            callback = PromiseCallback(p, runtime)
            params   = Parameters.default.withTestCallback(ConsoleReporter(0)).withTestCallback(callback)
            res1 <- ZIO.attempt(prop.check(params)).exit
            res2 <- p.await.exit
          } yield assert(res1)(succeeds(anything)) && assert(res2)(succeeds(anything))
        } @@ TestAspect.timed @@ TestAspect.timeout(30.seconds)
    }
    .reduceLeft(_ + _)

  def spec: Spec[Any, Any] = suite("DBZIO monad")(tests)
}
