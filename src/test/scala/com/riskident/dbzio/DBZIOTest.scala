package com.riskident.dbzio


import com.riskident.dbzio.DBTestUtils.{defaultDbConfig, testDb}
import com.riskident.dbzio.DBZIO.{DBZIOException, HasDb}
import zio.{Tag => _, _}
import zio.clock.Clock
import zio.console.Console
import zio.duration._
import zio.random.Random
import zio.test._
import zio.test.environment.{TestClock, TestEnvironment}
import zio.test.Assertion._
import slick.jdbc.PostgresProfile.api._

object DBZIOTest extends DefaultRunnableSpec {

  case class Data(id: Int, name: String)

  class DataTable(tag: Tag) extends BaseTable[Data](tag, "data") {
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("name", O.SqlType("text"))

    override def * = (id, name) <> ((Data.apply _).tupled, Data.unapply)

    def nameIndex = indexWithDefaultName(name, unique = true)
  }

  object DataDaoZio extends BaseTableQuery[Data, DataTable](new DataTable(_)) {

    def doInsert(data: Data): DBZIO[Any, Data] = DBZIO { implicit ex =>
      for {
        id <- this.returning(this.map(_.id)).+=(data)
      } yield data.copy(id = id)
    }

    def loadById(id: Int): DBZIO[HasDb, Data] = DBZIO { implicit ex => this.filter(_.id === id).take(1).result.head }

    def delete(id: Int): DBZIO[Any, Int] = DBZIO {
      this.filter(_.id === id).delete
    }

    val load: DBZIO[Any, Seq[Data]] = DBZIO {
      this.result
    }

  }

  class FailedTable(tag: Tag) extends BaseTable[Data](tag, "fail") {
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("name", O.SqlType("text"))

    override def * = (id, name) <> ((Data.apply _).tupled, Data.unapply)

    def nameIndex = indexWithDefaultName(name, unique = true)
  }

  object FailedDaoZio extends BaseTableQuery[Data, FailedTable](new FailedTable(_)) {
    def doInsert(data: Data): DBZIO[Any, Data] = DBZIO { implicit ex =>
      for {
        id <- this.returning(this.map(_.id)).+=(data)
      } yield data.copy(id = id)
    }

  }

  lazy val genStr: URIO[Random with Sized, String] = {
    Gen.alphaNumericStringBounded(5, 10).runHead.flatMap {
      case Some(str) if str.nonEmpty && str(0).isLetter => IO.succeed(str)
      case _                                            => genStr
    }
  }

  val dbLayer: RLayer[Sized with Console, HasDb] = ZLayer.fromManaged {
    for {
      name <- ZManaged.fromEffect(genStr).provideSomeLayer[Sized](Random.live)
      db   <- testDb(name, defaultDbConfig)
      _ <- ZManaged.make {
        ZIO.fromFuture { implicit ec =>
          db.run {
            for {
              exists <- DataDaoZio.createTable(ec)
              _      <- if (exists) DataDaoZio.dropTableDBIO else DBIO.successful(())
              _      <- DataDaoZio.createTable(ec)
            } yield ()
          }
        }
      } { _ => DataDaoZio.dropTable.provide(Has(db)).ignore }
    } yield db
  }

  val testLayer: ZLayer[TestEnvironment, TestFailure[Throwable], HasDb] = dbLayer.mapError(TestFailure.fail)

  val countData: RIO[HasDb, Int] = for {
    db  <- ZIO.access[HasDb](_.get)
    res <- ZIO.fromFuture(_ => db.run(sql"SELECT COUNT(*) FROM data".as[Int].head))
  } yield res

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
      one <- assertM(countData)(equalTo(1))
    } yield ins && one
  }

  case class Ex() extends Throwable

  val assertCauseIsEx: Assertion[Exit[Throwable, Any]] =
    failsCause(containsCause(Cause.fail(DBZIOException[Ex](Cause.fail(Ex())))))

  def testRollback[T](name: String, fail: DBZIO[HasDb, T]): ZSpec[TestEnv, Throwable] =
    testM(s"rollback when fail inside $name") {
      val insert: RIO[HasDb, (Int, Int)] = (for {
        d1 <- DataDaoZio.doInsert(Data(0, "foo"))
        d2 <- DataDaoZio.doInsert(Data(0, "bar"))
        _  <- fail
      } yield (d1.id, d2.id)).transactionally.result

      for {
        ins  <- assertM(insert.run)(assertCauseIsEx)
        zero <- assertM(countData)(equalTo(0))
      } yield zero && ins
    }

  val transaction: ZSpec[TestEnv, Throwable] = suite("transaction")(
    Seq(
      testRollback("ZIO", DBZIO(ZIO.fail(Ex()).unit)),
      testRollback("DBIO", DBZIO(DBIO.failed(Ex()))),
      testRollback("DBZIO", DBZIO.fail(Ex())),
      testFailWithoutTx
    ): _*
  ) @@ TestAspect.sequential

  def pinnedSession(
                     name: String,
                     f: DBZIO[HasDb, Unit] => DBZIO[HasDb, Unit],
                     assertion: Assertion[Int]
                   ): ZSpec[TestEnv, Throwable] = testM(name) {
    val timeout = 1.seconds

    val createWriteAction: Promise[Nothing, Unit] => URIO[Clock, DBZIO[HasDb, Unit]] = p =>
      ZIO.access[Clock] { env =>
        val wait: UIO[Unit] = ZIO.sleep(timeout * 2).race(p.await).provide(env)
        for {
          d <- DataDaoZio.doInsert(Data(0, "foo"))
          _ <- DBZIO(wait)
          _ <- DataDaoZio.delete(d.id)
        } yield ()
      }

    val createCountAction: Promise[Nothing, Unit] => DBZIO[Any, Int] = p => {
      for {
        res <- DataDaoZio.load.map(_.size)
        _   <- DBZIO(p.succeed(()))
      } yield res
    }
    for {
      p     <- Promise.make[Nothing, Unit]
      write <- createWriteAction(p)
      count = createCountAction(p)
      writeFiber <- f(write).result.fork
      countFiber <- count.result.fork
      _          <- TestClock.adjust(timeout)
      _          <- TestClock.adjust(timeout)
      _          <- writeFiber.join
      res        <- countFiber.join
    } yield assert(res)(assertion)
  }

  val session: ZSpec[TestEnv, Throwable] = suite("Session")(
    pinnedSession("pinned session should queue tasks", _.withPinnedSession, equalTo(0)),
    // Failing in tests due to limitations of the test environment. Does work in stand-alone app.
    pinnedSession("not pinned session allows switch of tasks", identity, not(equalTo(0))) @@ TestAspect.ignore
  ) @@ TestAspect.sequential

  val failedActions: Seq[DBZIO[Any, Unit]] = Seq(
    FailedDaoZio.doInsert(Data(1, "foo")).unit,
    DBZIO(sql"SELECT COUNT(*) from foo".as[Int]).unit,
    DBZIO(ZIO.fail(new RuntimeException).unit),
    DBZIO(ZIO.effectTotal(sqlu"SELECT COUNT(*) from foo")).unit
  )

  def testErrors[R](name: String, mapError: DBZIO[Any, Unit] => DBZIO[R, Unit]): ZSpec[TestEnv with R, Throwable] =
    testM(name) {
      val actions = ZIO.foreach {
        failedActions
          .map(mapError)
          .map(_.result.run)
      }(assertM(_)(assertCauseIsEx))
      actions.map(r => r.tail.foldLeft(r.head)(_ && _))
    }

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

  val errorProc: ZSpec[TestEnv, Throwable] = suite("Error processing")(
    onError,
    testErrors("map Throwable to another Throwable", _.mapError(_ => Ex())),
    testErrors("flatMap Throwable to DBZIO[_, Throwable]", _.flatMapError(_ => DBZIO(ZIO.effectTotal(Ex()))))
  ) @@ TestAspect.sequential

  val foldSuccess = Seq(
    "PureZio"     -> DBZIO(ZIO.effectTotal(0)),
    "PureDBIO"    -> DBZIO(sql"SELECT COUNT(*) from data".as[Int]),
    "DBIOInZio"   -> DataDaoZio.load,
    "ZioOverDBIO" -> DBZIO(ZIO.effectTotal(sql"SELECT COUNT(*) from data".as[Int]))
  )

  val foldFail = Seq(
    "PureZio"     -> DBZIO(ZIO.fail(Ex()).unit),
    "PureDBIO"    -> DBZIO(sql"SELECT COUNT(*) from fail".as[Int]),
    "DBIOInZio"   -> FailedDaoZio.doInsert(Data(0, "foo")),
    "ZioOverDBIO" -> DBZIO(ZIO.effectTotal(sql"SELECT COUNT(*) from fail".as[Int]))
  )

  def testFoldM[T](prefix: String, expected: Boolean): (String, DBZIO[Any, T]) => ZSpec[TestEnv, Throwable] =
    (name, action) =>
      testM(s"$prefix $name to DBZIO[HasDb, Boolean]") {
        assertM(action.foldM(_ => DBZIO.success(false), _ => DBZIO.success(true)).result)(equalTo(expected))
      }

  val foldM: ZSpec[TestEnv, Throwable] = {
    val success = foldSuccess.map(testFoldM("successful", true).tupled)
    val fail    = foldFail.map(testFoldM("failed", false).tupled)

    suite("FoldM DBZIO")(
      success ++ fail: _*
    ) @@ TestAspect.sequential
  }
  def testFold[T](prefix: String, expected: Boolean): (String, DBZIO[Any, T]) => ZSpec[TestEnv, Throwable] =
    (name, action) =>
      testM(s"$prefix $name to DBZIO[HasDb, Boolean]") {
        assertM(action.fold(_ => false, _ => true).result)(equalTo(expected))
      }

  val fold: ZSpec[TestEnv, Throwable] = {
    val success = foldSuccess.map(testFold("successful", true).tupled)
    val fail    = foldFail.map(testFold("failed", false).tupled)

    suite("Fold DBZIO")(
      (success ++ fail): _*
    ) @@ TestAspect.sequential
  }

  def spec: ZSpec[TestEnvironment, Any] =
    suite("DBZIO")(
      Seq(
        testExec,
        session,
        transaction,
        errorProc,
        foldM,
        fold
      ).map(_ @@ TestAspect.timeout(30.seconds)): _*
    ).provideSomeLayer(testLayer) @@ TestAspect.sequential
}

