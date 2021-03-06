package com.riskident.dbzio

import cats.implicits._
import cats.laws.discipline._
import com.riskident.dbzio.DBTestUtils._
import com.riskident.dbzio.Implicits._
import org.scalacheck.Test.{Parameters, TestCallback}
import org.scalacheck.util.ConsoleReporter
import org.scalacheck.{Arbitrary, Gen, Test}
import zio._
import zio.duration._
import zio.test.Assertion._
import zio.test.environment.TestEnvironment
import zio.test.{Gen => _, _}

object DBZIOTestLaws extends DefaultRunnableSpec {

  case class PromiseCallback(p: Promise[Throwable, Unit], runtime: zio.Runtime[Any]) extends TestCallback {
    override def onTestResult(name: String, result: Test.Result): Unit = {
      runtime.unsafeRun {
        p.succeed(()).when(result.passed) *>
          p.fail(new RuntimeException(s"Test $name didn't pass"))
            .unless(result.passed)
      }
    }
  }

  implicit val abData: Arbitrary[Data] = Arbitrary(
    for {
      id   <- Gen.long.map(_.toInt)
      name <- Gen.alphaNumStr
    } yield Data(id, name)
  )

  val laws = MonadTests[DBAction].monad[Int, String, Data]

  val tests: Seq[ZSpec[TestEnvironment, Throwable]] = laws.all.properties.map {
    case (name, prop) =>
      val t: ZSpec[TestEnvironment, Throwable] = testM(name) {
        for {
          p       <- Promise.make[Throwable, Unit]
          runtime <- ZIO.runtime[Any]
          callback = PromiseCallback(p, runtime)
          params   = Parameters.default.withTestCallback(ConsoleReporter(0)).withTestCallback(callback)
          run      = Task.effect(prop.check(params)).run
          res1 <- assertM(run)(succeeds(anything))
          res2 <- assertM(p.await.run)(succeeds(anything))
        } yield res1 && res2
      } @@ TestAspect.timed @@ TestAspect.timeout(30.seconds)

      t
  }.toSeq

  def spec: ZSpec[TestEnvironment, Any] = suite("DBZIO monad")(tests: _*)
}
