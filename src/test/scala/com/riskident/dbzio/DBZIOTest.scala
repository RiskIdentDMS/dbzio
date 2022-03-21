package com.riskident.dbzio

import com.riskident.dbzio.DBTestUtils._
import com.riskident.dbzio.DBZIO.DBZIOException
import slick.jdbc.H2Profile.api.{Database => _, _}
import zio.clock.Clock
import zio.duration._
import zio.random.Random
import zio.test.Assertion._
import zio.test._
import zio.test.environment.{TestClock, TestEnvironment}
import zio.{Tag => _, _}

import scala.annotation.tailrec

object DBZIOTest extends DefaultRunnableSpec {

  override val aspects: List[TestAspect[Nothing, TestEnvironment, Nothing, Any]] =
    super.aspects :+ TestAspect.sequential

  private def withAspects(s: ZSpec[TestEnv, Throwable]): ZSpec[TestEnv, Throwable] = {
    aspects.foldLeft(s)(_ @@ _)
  }

  lazy val genStr: URIO[Sized, String] = {
    Gen
      .alphaNumericStringBounded(5, 10)
      .runHead
      .flatMap {
        case Some(str) if str.nonEmpty && str(0).isLetter => UIO.succeed(str)
        case _                                            => genStr
      }
      .provideSomeLayer[Sized](Random.live)
  }
  val countDBIO: DBIO[Int]     = DataDaoZio.length.result
  val countFailDBIO: DBIO[Int] = FailedDaoZio.length.result

  type TestEnv = TestEnvironment with HasDb

  val testExec: ZSpec[TestEnv, Throwable] = withAspects(testM("execute mixed actions") {
    val timeout = 5.seconds

    val insert: DbRIO[Clock with TestClock, (Int, Int)] = (for {
      d1 <- DataDaoZio.doInsert(Data(0, "foo"))
      _ <- DBZIO(for {
        fiber <- ZIO.sleep(timeout).fork
        _     <- TestClock.adjust(timeout)
        _     <- fiber.join
      } yield ())
      d2 <- DataDaoZio.doInsert(Data(0, "bar"))
    } yield (d1.id, d2.id)).result

    for {
      (id1, id2) <- insert
    } yield assert(id1)(not(equalTo(0))) &&
      assert(id2)(not(equalTo(0)))
  })

  val testFailWithoutTx: ZSpec[TestEnv, Throwable] = withAspects(
    testM("leave inconsistent data after fail without transaction") {
      val insert: DbTask[Unit] = (for {
        d1 <- DataDaoZio.doInsert(Data(0, "foo"))
        _  <- DBZIO.fail(Ex())
        _  <- DataDaoZio.delete(d1.id)
      } yield ()).result

      for {
        ins <- assertM(insert.run)(fails(anything))
        one <- assertM(DataDaoZio.count.result)(equalTo(1))
      } yield ins && one
    }
  )

  case class Ex() extends Throwable

  val assertCauseIsEx: Assertion[Exit[Throwable, Any]] =
    failsCause(containsCause(Cause.fail(DBZIOException[Ex](Cause.fail(Ex())))))

  val transaction: ZSpec[TestEnv, Throwable] = withAspects {
    def testRollback[T](name: String, fail: DBAction[T]): ZSpec[TestEnv, Throwable] =
      withAspects(testM(s"rollback when fail inside $name") {
        val insert: DbTask[(Int, Int)] = (for {
          d1 <- DataDaoZio.doInsert(Data(0, "foo"))
          d2 <- DataDaoZio.doInsert(Data(0, "bar"))
          _  <- fail
        } yield (d1.id, d2.id)).transactionally.result

        for {
          ins  <- assertM(insert.run)(assertCauseIsEx)
          zero <- assertM(DataDaoZio.count.result)(equalTo(0))
        } yield zero && ins
      })

    suite("transaction")(
      Seq(
        testRollback("ZIO", DBZIO(ZIO.fail(Ex()).unit)),
        testRollback("DBIO", DBZIO(DBIO.failed(Ex()))),
        testRollback("DBZIO", DBZIO.fail(Ex())),
        testFailWithoutTx
      ): _*
    )
  }

  val session: ZSpec[TestEnv, Throwable] = {

    def pinnedSession(
        name: String,
        f: DBZIO[Clock, Unit] => DBZIO[Clock, Unit],
        assertion: Assertion[Int]
    ): ZSpec[TestEnv, Throwable] = testM(name) {
      val timeout = 30.seconds

      def createWriteAction(
          wait: Promise[Nothing, Unit],
          release: Promise[Nothing, Unit]
      ): DBZIO[Clock, Unit] = {
        val await: URIO[Clock, Unit] = wait.await.timeout(timeout * 2).unit
        for {
          d <- DataDaoZio.doInsert(Data(0, "foo"))
          _ <- DBZIO(release.succeed(()))
          _ <- DBZIO(await)
          _ <- DataDaoZio.delete(d.id)
        } yield ()
      }

      def createCountAction(release: Promise[Nothing, Unit], wait: Promise[Nothing, Unit]): DBAction[Int] = {
        for {
          _   <- DBZIO(wait.await)
          res <- DataDaoZio.count
          _   <- DBZIO(release.succeed(()))
        } yield res
      }
      for {
        release    <- Promise.make[Nothing, Unit]
        wait       <- Promise.make[Nothing, Unit]
        writeFiber <- f(createWriteAction(release, wait)).result.fork
        countFiber <- createCountAction(release, wait).result.fork
        _          <- TestClock.adjust(timeout)
        _          <- TestClock.adjust(timeout)
        _          <- writeFiber.join
        res        <- countFiber.join
      } yield assert(res)(assertion)
    }

    withAspects(
      suite("Session")(
        pinnedSession("pinned session should queue tasks", _.withPinnedSession, equalTo(0)),
        // Failing in tests due to limitations of the test environment. Does work in stand-alone app.
        pinnedSession("not pinned session allows switch of tasks", identity, not(equalTo(0)))
      ) @@ TestAspect.timeout(10.seconds)
    )
  }

  val failedActions: Seq[DBAction[Unit]] = Seq(
    FailedDaoZio.doInsert(Data(1, "foo")).unit,
    DBZIO(countFailDBIO).unit,
    DBZIO(ZIO.fail(new RuntimeException).unit),
    DBZIO(UIO(countFailDBIO)).unit
  )

  val errorProc: ZSpec[TestEnv, Throwable] = withAspects(suite("Error processing")({

    def testErrors[R](
        name: String,
        mapError: DBAction[Unit] => DBZIO[R with DbDependency, Unit]
    ): ZSpec[TestEnv with R, Throwable] =
      testM(name) {
        val actions = ZIO.foreach {
          failedActions
            .map(mapError)
            .map(_.result.run)
        }(assertM(_)(assertCauseIsEx))
        actions.map(_.reduceLeft(_ && _))
      }

    val onError: ZSpec[TestEnv, Throwable] = testM("onError correctly works") {
      def combine(ref: Ref[Boolean], action: DBAction[Unit]): DBAction[Unit] = {
        action.onError(_ => DBZIO(ref.set(true)))
      }

      def prepare(action: DBAction[Unit]): DbUIO[TestResult] =
        for {
          ref <- Ref.make(false)
          run <- assertM(combine(ref, action).result.run)(fails(isSubtype[Throwable](anything)))
          res <- assertM(ref.get)(equalTo(true))
        } yield res && run

      ZIO
        .foreach(failedActions)(prepare)
        .map(_.reduceLeft(_ && _))
    }

    Seq(
      onError,
      testErrors("map Throwable to another Throwable", _.mapError(_ => Ex())),
      testErrors("flatMap Throwable to DBZIO[_, Throwable]", _.flatMapError(_ => DBZIO.success(Ex())))
    )
  }: _*))

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

  val foldM: ZSpec[TestEnv, Throwable] = withAspects {
    def testFoldM[T](prefix: String, expected: Boolean): (String, DBZIO[Any, T]) => ZSpec[TestEnv, Throwable] =
      (name, action) =>
        testM(s"$prefix $name to DBAction[Boolean]") {
          assertM(action.foldM(_ => DBZIO.success(false), _ => DBZIO.success(true)).result)(equalTo(expected))
        }

    val success = foldSuccess.map(testFoldM("successful", true).tupled)
    val fail    = foldFail.map(testFoldM("failed", false).tupled)

    val pureValue = testFoldM("successful", true)("PureValue", DBZIO.success(()))
    val failure   = testFoldM("failed", false)("Failure", DBZIO.fail(new RuntimeException))
    suite("FoldM DBZIO")(
      (pureValue +: success) ++ (failure +: fail): _*
    )
  }

  val fold: ZSpec[TestEnv, Throwable] = withAspects {
    def testFold[T](prefix: String, expected: Boolean): (String, DBZIO[Any, T]) => ZSpec[TestEnv, Throwable] =
      (name, action) =>
        testM(s"$prefix $name to DBAction[Boolean]") {
          assertM(action.fold(_ => false, _ => true).result)(equalTo(expected))
        }

    val success: Seq[ZSpec[TestEnv, Throwable]] = foldSuccess.map(testFold("successful", true).tupled)
    val fail                                    = foldFail.map(testFold("failed", false).tupled)

    suite("Fold DBZIO")(
      (success ++ fail): _*
    )
  }

  val collection: ZSpec[TestEnv, Throwable] = withAspects(
    suite("collectAll")(
      testM("collection") {
        val count = 100
        DBZIO
          .collectAll {
            (0 to count).toList.map { _ =>
              for {
                name <- DBZIO(genStr)
                data <- DataDaoZio.doInsert(Data(0, name))
              } yield assert(data.id)(isGreaterThan(0))
            }
          }
          .result
          .map(_.reduceLeft(_ && _))
      },
      testM("Option[DBZIO[_, _]]") {
        for {
          name <- genStr
          _    <- DBZIO.collectAll(None.map(_ => DataDaoZio.doInsert(Data(0, name)))).result
          a1   <- assertM(DataDaoZio.load.result)(isEmpty)
          _    <- DBZIO.collectAll(Some(1).map(_ => DataDaoZio.doInsert(Data(0, name)))).result
          a2   <- assertM(DataDaoZio.load.result)(not(isEmpty))
        } yield a1 && a2
      }
    )
  )

  val dbzioIf: ZSpec[TestEnv, Throwable] = withAspects(
    suite("DBZIO.if*")(
      testM("ifF") {
        (for {
          _  <- DBZIO.ifF(false, onTrue = DataDaoZio.doInsert(Data(0, "foo")).unit, onFalse = DBZIO.unit)
          a1 <- DBZIO(assertM(DataDaoZio.load.result)(isEmpty))
          _  <- DBZIO.ifF(true, onTrue = DataDaoZio.doInsert(Data(0, "foo")).unit, onFalse = DBZIO.unit)
          a2 <- DBZIO(assertM(DataDaoZio.load.result)(not(isEmpty)))
        } yield a1 && a2).result
      },
      testM("ifM") {
        (for {
          _  <- DBZIO.ifM(DBZIO.success(false), onTrue = DataDaoZio.doInsert(Data(0, "foo")).unit, onFalse = DBZIO.unit)
          a1 <- DBZIO(assertM(DataDaoZio.load.result)(isEmpty))
          _  <- DBZIO.ifM(DBZIO.success(true), onTrue = DataDaoZio.doInsert(Data(0, "foo")).unit, onFalse = DBZIO.unit)
          a2 <- DBZIO(assertM(DataDaoZio.load.result)(not(isEmpty)))
        } yield a1 && a2).result
      }
    )
  )

  val nested: ZSpec[TestEnv, Throwable] = withAspects {
    val count = 1000
    testM(s"Nested (dbio > zio > dbio > zio... $count times)") {
      def recur(cnt: Int, ref: Ref[Int], action: DBAction[Int]): DBAction[Int] = {
        if (cnt <= 0) {
          action
        } else {
          recur(cnt - 1, ref, action.flatMap(_ => DBZIO(ref.updateAndGet(_ + 1).map(DBIO.successful(_)))))
        }
      }
      for {
        ref <- Ref.make(0)
        action = recur(count, ref, DBZIO.success(0))
        a1 <- assertM(action.result)(equalTo(count))
        a2 <- assertM(ref.get)(equalTo(count))
      } yield a1 && a2
    }
  }

  val combined: ZSpec[TestEnv, Throwable] = withAspects(testM("combined for-comprehension") {
    val action = for {
      name          <- DBZIO(genStr)
      data          <- DataDaoZio.doInsert(Data(0, name))
      loaded        <- DBZIO(DataDaoZio.loadById(data.id))
      loadedFromZio <- DBZIO(UIO.unit.as(DataDaoZio.loadById(data.id)))
      newName = data.name
      finalLoaded <- DataDaoZio.load
    } yield assert(finalLoaded)(not(isEmpty)) &&
      assert(data.id)(not(equalTo(0))) &&
      assert(newName)(equalTo(name)) &&
      assert(loaded)(equalTo(loadedFromZio))

    action.result
  })

  val stackSafeResult: ZSpec[TestEnv, Throwable] = withAspects {
    @tailrec
    def chain(next: DBZIO[Any, Int], action: DBZIO[Any, Int], left: Int): DBZIO[Any, Int] = {
      if (left == 0) action
      else chain(next, action.flatMap(_ => next), left - 1)
    }

    val cases: Seq[(DBZIO[Any, Int], String)] = Seq(
      DataDaoZio.count                     -> "DBIO",
      DBZIO(UIO(0))                        -> "ZIO",
      DBZIO.success(0)                     -> "pure values",
      DBZIO(UIO(DataDaoZio.length.result)) -> "ZIO over DBIO"
    )

    val tests = cases.map {
      case (action, name) =>
        testM(s"chain of $name") {
          chain(action, action, 16384).result.as(assertCompletes)
        }
    }

    suite("Computing result is stack-safe")(tests: _*)
  }

  val zip = testM("zip") {
    (for {
      p1                     <- DBZIO(Promise.make[Any, Unit])
      p2                     <- DBZIO(Promise.make[Any, Unit])
      result                 <- DBZIO(p1.succeed(())).map(_ => 1) <*> DBZIO(p2.succeed(())).map(_ => 2)
      bothEffectsAreExecuted <- DBZIO(assertM(p1.isDone && p2.isDone)(isTrue))
      correctCombinedResult  <- DBZIO.success(assertTrue(result == (1 -> 2)))
    } yield bothEffectsAreExecuted && correctCombinedResult).result
  }

  val zipRight = testM("zipRight") {
    (for {
      p1                     <- DBZIO(Promise.make[Any, Unit])
      p2                     <- DBZIO(Promise.make[Any, Unit])
      result                 <- DBZIO(p1.succeed(())).map(_ => 1) *> DBZIO(p2.succeed(())).map(_ => 2)
      bothEffectsAreExecuted <- DBZIO(assertM(p1.isDone && p2.isDone)(isTrue))
      correctRightResult     <- DBZIO.success(assertTrue(result == 2))
    } yield bothEffectsAreExecuted && correctRightResult).result
  }

  val zipLeft = testM("zipLeft") {
    (for {
      p1                     <- DBZIO(Promise.make[Any, Unit])
      p2                     <- DBZIO(Promise.make[Any, Unit])
      result                 <- DBZIO(p1.succeed(())).map(_ => 1) <* DBZIO(p2.succeed(())).map(_ => 2)
      bothEffectsAreExecuted <- DBZIO(assertM(p1.isDone && p2.isDone)(isTrue))
      correctLeftResult      <- DBZIO.success(assertTrue(result == 1))
    } yield bothEffectsAreExecuted && correctLeftResult).result
  }

  val zipSuite = suite("All zips")(zip, zipRight, zipLeft)

  override def spec: ZSpec[TestEnvironment, Any] =
    withAspects {
      suite("DBZIO")(
        Seq(
          testExec,
          session,
          transaction,
          collection,
          dbzioIf,
          nested,
          errorProc,
          foldM,
          fold,
          combined,
          zipSuite,
          stackSafeResult
        ).map(_ @@ TestAspect.timeout(30.seconds)): _*
      )
    }.provideCustomLayer(testLayer) @@ TestAspect.timed
}
