package com.riskident.dbzio
import cats.implicits._
import cats.laws.discipline._
import com.riskident.dbzio.DBTestUtils._
import com.riskident.dbzio.Implicits._
import org.scalacheck.Test.Parameters
import org.scalacheck.util.ConsoleReporter
import zio.Task
import zio.duration._
import zio.test.Assertion._
import zio.test._
import zio.test.environment.TestEnvironment

object DBZIOTestLaws extends DefaultRunnableSpec {

  val laws = MonadTests[DBAction].stackUnsafeMonad[Int, String, Data]

  val tests: List[ZSpec[TestEnvironment, Any]] = laws.all.properties.map{
    case (name, prop) =>
      val params = Parameters.default.withTestCallback(ConsoleReporter(0))
      val run = Task.effect(prop.check(params)).run
      testM(name)(assertM(run)(succeeds(anything)))
  }.map(_ @@ TestAspect.timed @@ TestAspect.timeout(10.seconds)).toList

  def spec: ZSpec[TestEnvironment, Any] = suite("DBZIO stack unsafe monad")(tests: _*)
}
