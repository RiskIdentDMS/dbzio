package com.riskident.dbzio

import com.riskident.dbzio.DBTestUtils._
import com.riskident.dbzio.DBZIO.DBZIOException
import slick.jdbc.H2Profile.api.{Database => _, _}
import zio.test.Assertion._
import zio.test._
import zio.{Tag => _, _}

import scala.annotation.tailrec

object DBZIOTest extends ZIOSpecDefault {
  type TestEnv   = TestEnvironment with Clock with Scope
  type DbTestEnv = HasDb with TestEnv
  type DbSpec    = Spec[DbTestEnv, Throwable]

  override val aspects: Chunk[TestAspectAtLeastR[TestEnvironment]] =
    super.aspects :+ TestAspect.sequential

  private def withAspects(s: DbSpec): DbSpec = {
    aspects.foldLeft(s)(_ @@ _)
  }

  lazy val genStr: UIO[String] = {
    Gen
      .alphaNumericStringBounded(5, 10)
      .runHead
      .flatMap {
        case Some(str) if str.nonEmpty && str(0).isLetter => ZIO.succeed(str)
        case _                                            => genStr
      }
      .withRandom(Random.RandomLive)
  }
  val countDBIO: DBIO[Int]     = DataDaoZio.length.result
  val countFailDBIO: DBIO[Int] = FailedDaoZio.length.result

  val testExec: DbSpec = withAspects(test("execute mixed actions") {
    val timeout = 5.seconds

    (for {
      d1 <- DataDaoZio.doInsert(Data(0, "foo"))
      _ <- DBZIO(for {
        fiber <- ZIO.sleep(timeout).fork
        _     <- TestClock.adjust(timeout)
        _     <- fiber.join
      } yield ())
      d2 <- DataDaoZio.doInsert(Data(0, "bar"))
    } yield assertTrue(d1.id != 0) && assertTrue(d2.id != 0)).result

  })

  val testFailWithoutTx: DbSpec = withAspects(
    test("leave inconsistent data after fail without transaction") {
      val insert: DbTask[Unit] = (for {
        d1 <- DataDaoZio.doInsert(Data(0, "foo"))
        _  <- DBZIO.fail(Ex())
        _  <- DataDaoZio.delete(d1.id)
      } yield ()).result

      for {
        ins <- insert.exit
        one <- DataDaoZio.count.result
      } yield assert(ins)(fails(anything)) && assertTrue(one == 1)
    }
  )

  case class Ex() extends Throwable

  val assertCauseIsEx: Assertion[Exit[Throwable, Any]] =
    failsCause(containsCause(Cause.fail(DBZIOException[Ex](Cause.fail(Ex())))))

  val transaction: DbSpec = withAspects {
    def testRollback[T](name: String, fail: DBAction[T]): DbSpec =
      withAspects(test(s"rollback when fail inside $name") {
        val insert: DbTask[(Int, Int)] = (for {
          d1 <- DataDaoZio.doInsert(Data(0, "foo"))
          d2 <- DataDaoZio.doInsert(Data(0, "bar"))
          _  <- fail
        } yield (d1.id, d2.id)).transactionally.result

        for {
          ins  <- insert.exit
          zero <- DataDaoZio.count.result
        } yield assert(ins)(assertCauseIsEx) && assertTrue(zero == 0)
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

  val session: DbSpec = {

    def pinnedSession(
        name: String,
        f: DBZIO[Clock, Unit] => DBZIO[Clock, Unit],
        assertion: Assertion[Int]
    )(implicit trace: Trace): DbSpec = test(name) {
      val timeout = 30.seconds

      def createWriteAction(
          wait: Promise[Nothing, Unit],
          release: Promise[Nothing, Unit]
      ): DBZIO[Any, Unit] = {
        val await: UIO[Unit] = wait.await.timeout(timeout * 2).unit
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
    DBZIO(ZIO.succeed(countFailDBIO)).unit
  )

  val errorProc: DbSpec = withAspects(suite("Error processing") {

    def testErrors[R](
        name: String,
        mapError: DBAction[Unit] => DBZIO[R with HasDb, Unit]
    ): Spec[HasDb with R, Throwable] =
      test(name) {
        val actions = ZIO.foreach {
          failedActions
            .map(mapError)
            .map(_.result.exit)
        }(_.map(assert(_)(assertCauseIsEx)))
        actions.map(_.reduceLeft(_ && _))
      }

    val onError: DbSpec = test("onError correctly works") {
      def combine(ref: Ref[Boolean], action: DBAction[Unit]): DBAction[Unit] = {
        action.onError(_ => DBZIO(ref.set(true)))
      }

      def prepare(action: DBAction[Unit]): DbUIO[TestResult] =
        for {
          ref <- Ref.make(false)
          run <- combine(ref, action).result.exit
          res <- ref.get
        } yield assertTrue(res) && assert(run)(fails(isSubtype[Throwable](anything)))

      ZIO
        .foreach(failedActions)(prepare)
        .map(_.reduceLeft(_ && _))
    }

    onError +
      testErrors("map Throwable to another Throwable", _.mapError(_ => Ex())) +
      testErrors("flatMap Throwable to DBZIO[_, Throwable]", _.flatMapError(_ => DBZIO.success(Ex())))
  })

  private val foldSuccess = Seq(
    "PureZio"     -> DBZIO(ZIO.succeed(0)),
    "PureDBIO"    -> DBZIO(countDBIO),
    "DBIOInZio"   -> DataDaoZio.load,
    "ZioOverDBIO" -> DBZIO(ZIO.succeed(countDBIO))
  )

  private val foldFail = Seq(
    "PureZio"     -> DBZIO(ZIO.fail(Ex()).unit),
    "PureDBIO"    -> DBZIO(countFailDBIO),
    "DBIOInZio"   -> FailedDaoZio.doInsert(Data(0, "foo")),
    "ZioOverDBIO" -> DBZIO(ZIO.succeed(countFailDBIO))
  )

  val foldM: DbSpec = withAspects {
    def testFoldM[T](prefix: String, expected: Boolean)(name: String, action: DBZIO[Any, T])(
        implicit trace: Trace
    ): DbSpec =
      test(s"$prefix $name to DBAction[Boolean]") {
        action.foldM(_ => DBZIO.success(false), _ => DBZIO.success(true)).result.map { r => assertTrue(r == expected) }
      }

    val success = foldSuccess.map((testFoldM("successful", true) _).tupled)
    val fail    = foldFail.map((testFoldM("failed", false) _).tupled)

    val pureValue = testFoldM("successful", true)("PureValue", DBZIO.success(()))
    val failure   = testFoldM("failed", false)("Failure", DBZIO.fail(new RuntimeException))
    suite("FoldM DBZIO") {
      ((pureValue +: success) ++ (failure +: fail)).reduceLeft(_ + _)
    }
  }

  val fold: DbSpec = withAspects {
    def testFold[T](prefix: String, expected: Boolean): (String, DBZIO[Any, T]) => DbSpec =
      (name, action) =>
        test(s"$prefix $name to DBAction[Boolean]") {
          action.fold(_ => false, _ => true).result.map(r => assertTrue(r == expected))
        }

    val success = foldSuccess.map(testFold("successful", true).tupled).reduceLeft(_ + _)
    val fail    = foldFail.map(testFold("failed", false).tupled).reduceLeft(_ + _)

    suite("Fold DBZIO")(success + fail)
  }

  val collection: DbSpec = withAspects(
    suite("collectAll")(
      suite("collection")(
        test("List[DBAction[Data]]") {
          val count = 100
          DBZIO
            .collectAll {
              (0 to count).toList.map { _ =>
                for {
                  name <- DBZIO(genStr)
                  data <- DataDaoZio.doInsert(Data(0, name))
                } yield assertTrue(data.id > 0)
              }
            }
            .result
            .map(_.reduceLeft(_ && _))
        },
        test("List[DBAction[Option[Data]]") {
          val count = 100
          for {
            names <- ZIO.foreach((0 to count).toList)(_ => genStr)
            _     <- DBZIO.collectAll(names.map(DataDaoZio.find)).result
          } yield assertCompletes
        }
      ),
      test("Option[DBZIO[_, _]]") {
        for {
          name <- genStr
          _    <- DBZIO.collectAll(None.map(_ => DataDaoZio.doInsert(Data(0, name)))).result
          a1   <- DataDaoZio.load.result
          _    <- DBZIO.collectAll(Some(1).map(_ => DataDaoZio.doInsert(Data(0, name)))).result
          a2   <- DataDaoZio.load.result
        } yield assertTrue(a1.isEmpty) && assertTrue(a2.nonEmpty)
      }
    )
  )

  val dbzioIf: DbSpec = withAspects(
    suite("DBZIO.if*")(
      test("ifF") {
        (for {
          _  <- DBZIO.ifF(false, onTrue = DataDaoZio.doInsert(Data(0, "foo")).unit, onFalse = DBZIO.unit)
          a1 <- DataDaoZio.load
          _  <- DBZIO.ifF(true, onTrue = DataDaoZio.doInsert(Data(0, "foo")).unit, onFalse = DBZIO.unit)
          a2 <- DataDaoZio.load
        } yield assertTrue(a1.isEmpty) && assertTrue(a2.nonEmpty)).result
      },
      test("ifM") {
        (for {
          _  <- DBZIO.ifM(DBZIO.success(false), onTrue = DataDaoZio.doInsert(Data(0, "foo")).unit, onFalse = DBZIO.unit)
          a1 <- DataDaoZio.load
          _  <- DBZIO.ifM(DBZIO.success(true), onTrue = DataDaoZio.doInsert(Data(0, "foo")).unit, onFalse = DBZIO.unit)
          a2 <- DataDaoZio.load
        } yield assertTrue(a1.isEmpty) && assertTrue(a2.nonEmpty)).result
      }
    )
  )

  val nested: DbSpec = withAspects {
    val count = 1000
    test(s"Nested (dbio > zio > dbio > zio... $count times)") {
      @tailrec
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
        a1 <- action.result
        a2 <- ref.get
      } yield assertTrue(a1 == count) && assertTrue(a2 == count)
    }
  }

  val combined: DbSpec = withAspects(test("combined for-comprehension") {
    val action = for {
      name          <- DBZIO(genStr)
      data          <- DataDaoZio.doInsert(Data(0, name))
      loaded        <- DBZIO(DataDaoZio.loadById(data.id))
      loadedFromZio <- DBZIO(ZIO.succeed(DataDaoZio.loadById(data.id)))
      newName = data.name
      finalLoaded <- DataDaoZio.load
    } yield assertTrue(finalLoaded.nonEmpty) &&
      assertTrue(data.id != 0) &&
      assertTrue(newName == name) &&
      assertTrue(loaded == loadedFromZio)

    action.result
  })

  val stackSafeResult: DbSpec = withAspects {
    @tailrec
    def chain(next: DBZIO[Any, Int], action: DBZIO[Any, Int], left: Int): DBZIO[Any, Int] = {
      if (left == 0) action
      else chain(next, action.flatMap(_ => next), left - 1)
    }

    val cases: Seq[(DBZIO[Any, Int], String)] = Seq(
      DataDaoZio.count                             -> "DBIO",
      DBZIO(ZIO.succeed(0))                        -> "ZIO",
      DBZIO.success(0)                             -> "pure values",
      DBZIO(ZIO.succeed(DataDaoZio.length.result)) -> "ZIO over DBIO"
    )

    val tests = cases.map {
      case (action, name) =>
        test(s"chain of $name") {
          chain(action, action, 16384).result.as(assertCompletes)
        }
    }

    withAspects(suite("Computing result is stack-safe")(tests: _*))
  }

  val zip: DbSpec = test("zip") {
    (for {
      p1      <- DBZIO(Promise.make[Any, Unit])
      p2      <- DBZIO(Promise.make[Any, Unit])
      result  <- DBZIO(p1.succeed(())).map(_ => 1) <*> DBZIO(p2.succeed(())).map(_ => 2)
      allDone <- DBZIO(p1.isDone) <*> DBZIO(p2.isDone)
    } yield assertTrue(allDone._1 && allDone._2) && assertTrue(result == (1 -> 2))).result
  }

  val zipRight: DbSpec = test("zipRight") {
    (for {
      p1      <- DBZIO(Promise.make[Any, Unit])
      p2      <- DBZIO(Promise.make[Any, Unit])
      result  <- DBZIO(p1.succeed(())).map(_ => 1) *> DBZIO(p2.succeed(())).map(_ => 2)
      allDone <- DBZIO(p1.isDone) <*> DBZIO(p2.isDone)
    } yield assertTrue(allDone._1 && allDone._2) && assertTrue(result == 2)).result
  }

  val zipLeft: DbSpec = test("zipLeft") {
    (for {
      p1      <- DBZIO(Promise.make[Any, Unit])
      p2      <- DBZIO(Promise.make[Any, Unit])
      result  <- DBZIO(p1.succeed(())).map(_ => 1) <* DBZIO(p2.succeed(())).map(_ => 2)
      allDone <- DBZIO(p1.isDone) <*> DBZIO(p2.isDone)
    } yield assertTrue(allDone._1 && allDone._2) && assertTrue(result == 1)).result
  }

  val zipSuite: DbSpec = suite("All zips")(zip, zipRight, zipLeft)

  override def spec: Spec[TestEnvironment with Scope, Any] =
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
    }.provideSomeLayer[TestEnvironment with Scope](testLayer ++ TestClock.default) @@ TestAspect.timed
}
