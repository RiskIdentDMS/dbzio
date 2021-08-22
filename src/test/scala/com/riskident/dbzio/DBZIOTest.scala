package com.riskident.dbzio


import com.riskident.dbzio.DBTestUtils._
import com.riskident.dbzio.DBZIO.DBZIOException
import slick.jdbc.H2Profile.api.{Database => _, _}
import slick.jdbc.JdbcBackend.Database
import slick.sql.SqlStreamingAction
import zio.clock.Clock
import zio.console.Console
import zio.duration._
import zio.test.Assertion._
import zio.test._
import zio.test.environment.{TestClock, TestEnvironment}
import zio.{Tag => _, _}

object DBZIOTest extends DefaultRunnableSpec {

  val countDBIO: DBIO[Int] = DataDaoZio.length.result
  val countFailDBIO: DBIO[Int] = FailedDaoZio.length.result


  type TestEnv = TestEnvironment with HasDb

  val testExec: ZSpec[TestEnv, Throwable] = testM("execute mixed actions") {
    val timeout = 5.seconds

    val insert: RIO[HasDb with Clock with TestClock, (Int, Int)] = (for {
      d1 <- DataDaoZio.doInsert(Data(0, "foo"))
      _ <- DBZIO(for {
        fiber <- ZIO.sleep(timeout).fork
        _ <- TestClock.adjust(timeout)
        _ <- fiber.join
      } yield ())
      d2 <- DataDaoZio.doInsert(Data(0, "bar"))
    } yield (d1.id, d2.id)).result

    for {
      (id1, id2) <- insert
    } yield assert(id1)(not(equalTo(0))) &&
      assert(id2)(not(equalTo(0)))
  }

  val testFailWithoutTx: ZSpec[TestEnv, Throwable] = testM("leave inconsistent data after fail without transaction") {
    val insert: RIO[HasDb, Unit] = (for {
      d1 <- DataDaoZio.doInsert(Data(0, "foo"))
      _  <- DBZIO.fail(Ex())
      _  <- DataDaoZio.delete(d1.id)
    } yield ()).result

    for {
      ins <- assertM(insert.run)(fails(anything))
      one <- assertM(DataDaoZio.load.result)(hasSize(equalTo(1)))
    } yield ins && one
  }

  case class Ex() extends Throwable

  val assertCauseIsEx: Assertion[Exit[Throwable, Any]] =
    failsCause(containsCause(Cause.fail(DBZIOException[Ex](Cause.fail(Ex())))))

  val transaction: ZSpec[TestEnvironment, Throwable] = {
    def testRollback[T](name: String, fail: DBZIO[HasDb, T]): ZSpec[TestEnv, Throwable] =
      testM(s"rollback when fail inside $name") {
        val insert: RIO[HasDb, (Int, Int)] = (for {
          d1 <- DataDaoZio.doInsert(Data(0, "foo"))
          d2 <- DataDaoZio.doInsert(Data(0, "bar"))
          _  <- fail
        } yield (d1.id, d2.id)).transactionally.result

        for {
          ins  <- assertM(insert.run)(assertCauseIsEx)
          zero <- assertM(DataDaoZio.load.result)(hasSize(equalTo(0)))
        } yield zero && ins
      }
    suite("transaction")(
      Seq(
        testRollback("ZIO", DBZIO(ZIO.fail(Ex()).unit)),
      testRollback("DBIO", DBZIO(DBIO.failed(Ex()))),
      testRollback("DBZIO", DBZIO.fail(Ex())),
      testFailWithoutTx
      ): _*
    ).provideSomeLayer(testLayer) @@ TestAspect.sequential
  }

  val session: ZSpec[TestEnv, Throwable] = {

    def pinnedSession(
                       name: String,
                       f: DBZIO[HasDb with Clock, Int] => DBZIO[HasDb with Clock, Int],
                       assertion: Assertion[Int]
                     ): ZSpec[TestEnv, Throwable] = testM(name) {
      val timeout = 1.seconds

      val createWriteAction: Promise[Nothing, Unit] => DBZIO[HasDb with Clock, Int] = p => {
        val wait: URIO[Clock, Unit] = ZIO.sleep(timeout * 2).race(p.await)
        for {
          d <- DataDaoZio.doInsert(Data(0, "foo"))
          res <- DataDaoZio.load.map(_.size)
          _ <- DBZIO(wait)
          _ <- DataDaoZio.delete(d.id)
        } yield res
      }

      val createCountAction: Promise[Nothing, Unit] => DBZIO[Any, Int] = p => {
        for {
          res <- DataDaoZio.load.map(_.size)
          _ <- DBZIO(p.succeed(()))
        } yield res
      }
      for {
        p <- Promise.make[Nothing, Unit]
        write = createWriteAction(p)
        count = createCountAction(p)
        writeFiber <- f(write).result.fork
        countFiber <- count.result.fork
        _ <- TestClock.adjust(timeout).repeatN(2)
        cnt <- writeFiber.join
        res <- countFiber.join
      } yield assert(cnt)(not(equalTo(0))) && assert(res)(assertion)
    }

    suite("Session")(
      pinnedSession("pinned session should queue tasks", _.withPinnedSession, equalTo(0)),
      // Failing in tests due to limitations of the test environment. Does work in stand-alone app.
      pinnedSession("not pinned session allows switch of tasks", identity, not(equalTo(0))) @@ TestAspect.ignore
    )
  }

  val failedActions: Seq[DBZIO[Any, Unit]] = Seq(
    FailedDaoZio.doInsert(Data(1, "foo")).unit,
    DBZIO(countFailDBIO).unit,
    DBZIO(ZIO.fail(new RuntimeException).unit),
    DBZIO(UIO(countFailDBIO)).unit
  )
  val errorProc: ZSpec[TestEnv, Throwable] = {

    val onError: ZSpec[TestEnv, Throwable] = testM("onError correctly works") {
      def combine(ref: Ref[Boolean], action: DBZIO[Any, Unit]): DBZIO[HasDb, Unit] = {
        action.onError(_ => DBZIO(ref.set(true)))
      }

      def prepare(action: DBZIO[Any, Unit]): URIO[HasDb, TestResult] =
        for {
          ref <- Ref.make(false)
          run <- assertM(combine(ref, action).result.run)(fails(isSubtype[Throwable](anything)))
          res <- assertM(ref.get)(equalTo(true))
        } yield res && run

      ZIO.foreach(failedActions)(prepare)
        .map(r => r.tail.foldLeft(r.head)(_ && _))
    }

    def testErrors[R](name: String, mapError: DBZIO[Any, Unit] => DBZIO[R, Unit]): ZSpec[TestEnv with R, Throwable] =
      testM(name) {
        val actions = ZIO.foreach {
          failedActions
            .map(mapError)
            .map(_.result.run)
        }(assertM(_)(assertCauseIsEx))
        actions.map(r => r.tail.foldLeft(r.head)(_ && _))
      }

    suite("Error processing")(
      onError,
      testErrors("map Throwable to another Throwable", _.mapError(_ => Ex())),
      testErrors("flatMap Throwable to DBZIO[_, Throwable]", _.flatMapError(_ => DBZIO(ZIO.effectTotal(Ex()))))
    ) @@ TestAspect.sequential
  }


  val foldSuccess = Seq(
    "PureZio"     -> DBZIO(UIO(0)),
    "PureDBIO"    -> DBZIO(countDBIO),
    "DBIOInZio"   -> DataDaoZio.load,
    "ZioOverDBIO" -> DBZIO(UIO(countDBIO))
  )

  val foldFail = Seq(
    "PureZio"     -> DBZIO(ZIO.fail(Ex()).unit),
    "PureDBIO"    -> DBZIO(countFailDBIO),
    "DBIOInZio"   -> FailedDaoZio.doInsert(Data(0, "foo")),
    "ZioOverDBIO" -> DBZIO(UIO(countFailDBIO))
  )


  val foldM: ZSpec[TestEnv, Throwable] = {
    def testFoldM[T](prefix: String, expected: Boolean): (String, DBZIO[Any, T]) => ZSpec[TestEnv, Throwable] = {
      (name, action) =>
        testM(s"$prefix $name to DBZIO[HasDb, Boolean]") {
          val tested: DBZIO[HasDb, Boolean] = action.foldM(_ => DBZIO.success(false), _ => DBZIO.success(true))
          assertM(tested.result)(equalTo(expected))
        }
    }

    val success = foldSuccess.map(testFoldM("successful", true).tupled)
    val fail    = foldFail.map(testFoldM("failed", false).tupled)

    suite("FoldM DBZIO")(
      success ++ fail: _*
    )
  }


  val fold: ZSpec[TestEnv, Throwable] = {
    def testFold[T](prefix: String, expected: Boolean): (String, DBZIO[Any, T]) => ZSpec[TestEnv, Throwable] = {
      (name, action) =>
        testM(s"$prefix $name to DBZIO[HasDb, Boolean]") {
          assertM(action.fold(_ => false, _ => true).result)(equalTo(expected))
        }
    }

    val success = foldSuccess.map(testFold("successful", true).tupled)
    val fail    = foldFail.map(testFold("failed", false).tupled)

    suite("Fold DBZIO")(
      success ++ fail: _*
    )
  }

  def spec: ZSpec[TestEnvironment, Any] = suite("DBZIO")(
    Seq(
      testExec,
      session,
      transaction,
      errorProc,
      foldM,
      fold
    ).map(_ @@ TestAspect.timeout(30.seconds) @@ TestAspect.timed): _*
  ).provideSomeLayer(testLayer) @@ TestAspect.sequential
}

