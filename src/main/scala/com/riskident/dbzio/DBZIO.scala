package com.riskident.dbzio

import com.riskident.dbzio.DBZIO._
import shapeless.=:!=
import slick.dbio.DBIO

import zio._
import zio.stm.TReentrantLock
import Aliases._
import scala.collection.compat.Factory
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Success

sealed trait DBZIO[-R, +T] {
  def map[Q](f: T => Q): DBZIO[R, Q]

  def flatMap[Q, R1](f: T => DBZIO[R1, Q]): DBZIO[R1 with R, Q] = ZioOverDBIO {
    for {
      dbio <- toDBIO
      runtime <- ZIO.runtime[R1]
      res <- UIO {
        implicit val ec: ExecutionContext = runtime.platform.executor.asEC
        for {
          t <- dbio
          res <- runToDBIO(runtime, f(t).toDBIO.on(ec))
        } yield res
      }
    } yield res
  }

  def withResource[R1](managed: RManaged[R1, Any]): DBZIO[R with R1, T] = DBZIO {
    managed.reserve.flatMap(r => withResource(r.acquire, r.release(Exit.succeed(()))).toDBIO)
  }

  def withResource[R1, R2](acquire: RIO[R1, Any], release: URIO[R2, Any]): DBZIO[R with R1 with R2, T] = DBZIO {
    for {
      dbio <- toDBIO
      runtime <- ZIO.runtime[R1 with R2]
    } yield {
      implicit val ec: ExecutionContext = runtime.platform.executor.asEC
      val acq = DBIO.successful(() => runtime.unsafeRun(wrapError(acquire).on(ec))).map(_ ())
      val rel = DBIO.successful(() => runtime.unsafeRun(release.on(ec))).map(_ ())
      for {
        _ <- acq
        res <- dbio.cleanUp(_ => rel)
        _ <- rel
      } yield res
    }
  }

  def flatten[S, Q](implicit ev: T <:< DBZIO[S, Q]): DBZIO[S with R, Q] = flatMap(ev(_))

  def mapError(f: Throwable => Throwable): DBZIO[R, T]

  def flatMapError[R1](f: Throwable => DBZIO[R1, Throwable]): DBZIO[R with R1 with HasDb, T]

  def onError[R1 <: R](cleanup: Cause[Throwable] => DBZIO[R1, Any]): DBZIO[R1 with HasDb, T]

  def fold[B](failure: Throwable => B, success: T => B): DBZIO[R, B]

  def foldM[R1, B](failure: Throwable => DBZIO[R1, B], success: T => DBZIO[R1, B]): DBZIO[R with R1 with HasDb, B]

  def orElse[R1, K, Q >: T](other: DBZIO[R1, Q])(implicit ev: T <:< Option[K]): DBZIO[R with R1 with HasDb, Q] = {
    this.flatMap(_.fold(other)(t => DBZIO.success(Some(t).asInstanceOf[Q])))
  }

  def tapM[R1](f: T => DBZIO[R1, Any]): DBZIO[R1 with R, T] = flatMap(t => f(t).map(_ => t))

  def withPinnedSession: DBZIO[R, T]

  def transactionally(implicit profile: JdbcProfile): DBZIO[R, T]

  lazy val unit: DBZIO[R, Unit] = map(_ => ())

  def result: RIO[R with HasDb, T]

  def toDBIO: RIO[R, DBIO[T]]

  def withTransactionIsolation(ti: TransactionIsolation)(implicit profile: JdbcProfile): DBZIO[R, T]
}

object DBZIO {

  case class DBZIOException[+E <: Throwable](cause: Cause[E]) extends Throwable {
    final override def getMessage: String = cause.prettyPrint
  }

  def ifF[R, T](cond: => Boolean, ifTrue: DBZIO[R, T], ifFalse: DBZIO[R, T]): DBZIO[R, T] = {
    if (cond) ifTrue else ifFalse
  }

  def ifM[R, T](cond: DBZIO[R, Boolean], ifTrue: DBZIO[R, T], ifFalse: DBZIO[R, T]): DBZIO[R, T] = {
    cond.flatMap(DBZIO.ifF(_, ifTrue, ifFalse))
  }

  def apply[T](action: DBIO[T]): DBZIO[Any, T] = PureDBIO(action)

  def apply[T](action: ExecutionContext => DBIO[T]): DBZIO[Any, T] = DBIOInZio(action)

  def apply[R, E <: Throwable, T](action: ZIO[R, E, T])(implicit ev: T =:!= DBIO[_]): DBZIO[R, T] = PureZio(action)

  def apply[R, E <: Throwable, T](action: ZIO[R, E, DBIO[T]]): DBZIO[R, T] = ZioOverDBIO(action)

  class Sync(lock: TReentrantLock, ref: Ref[(Boolean, Promise[Nothing, Unit])]) {
    def acquire: UIO[Unit] = lock.writeLock.use { _ =>
      ref.get.flatMap {
        case (f, p) => p.await.when(f) *> Promise.make[Nothing, Unit].flatMap(n => ref.set(true -> n))
      }.unit
    }

    def release: UIO[Unit] = {
      ref.get.flatMap {
        case (_, p) => ref.set(false -> p) *> p.succeed(()).unit
      }
    }
  }

  object Sync {
    def make: UIO[Sync] =
      for {
        promise <- Promise.make[Nothing, Unit]
        ref <- Ref.make((false, promise))
        lock <- TReentrantLock.make.commit
      } yield new Sync(lock, ref)
  }

  def synchronized[R, T](sync: Sync)(action: DBZIO[R, T]): DBZIO[R with HasDb, T] = {
    action.withResource(sync.acquire, sync.release)
  }

  def fail[E <: Throwable, T](error: E): DBZIO[Any, T] =
    Failure(DBZIOException(Cause.fail(error)))


  val unit: DBZIO[Any, Unit] = success(())

  def success[A](v: => A): DBZIO[Any, A] = PureZio[Any, Nothing, A](ZIO.effectTotal(v))

  def collectAll[R, T, Collection[+Element] <: Iterable[Element]](
                                                                   col: Collection[DBZIO[R, T]]
                                                                 )(
                                                                   implicit ev1: BuildFrom[Collection[DBZIO[R, T]], DBIO[T], Collection[DBIO[T]]],
                                                                   ev2: BuildFrom[Collection[DBIO[T]], T, Collection[T]],
                                                                   f: Factory[T, Collection[T]]
                                                                 ): DBZIO[R, Collection[T]] = {
    ZioOverDBIO {
      ZIO.foreach(col)(_.toDBIO).map(DBIO.sequence(_))
    }
  }

  final def collectAll[R, T](col: Set[DBZIO[R, T]]): DBZIO[R, Set[T]] = {
    collectAll[R, T, Iterable](col).map(_.toSet)
  }

  final def collectAll[R, T: ClassTag](col: Array[DBZIO[R, T]]): DBZIO[R, Array[T]] = {
    collectAll[R, T, Iterable](col).map(_.toArray)
  }

  final def collectAll[R, T](col: Option[DBZIO[R, T]]): DBZIO[R, Option[T]] = {
    col.map(_.map(Some(_))).getOrElse(DBZIO.success(None))
  }

  case class Failure[E <: Throwable] private[DBZIO](error: DBZIOException[E]) extends DBZIO[Any, Nothing] {
    override def map[Q](f: Nothing => Q): DBZIO[Any, Q] = this.asInstanceOf[DBZIO[Any, Q]]

    override val withPinnedSession: DBZIO[Any, Nothing] = this

    override def transactionally(implicit profile: JdbcProfile): DBZIO[Any, Nothing] = this

    override val result: RIO[HasDb, Nothing] = wrapError(RIO.fail(error))
    override val toDBIO: RIO[Any, DBIO[Nothing]] = RIO.fail(error)

    override def mapError(f: Throwable => Throwable): DBZIO[Any, Nothing] = DBZIO.fail(f(error.cause.squash))

    override def flatMapError[R](f: Throwable => DBZIO[R, Throwable]): DBZIO[R with HasDb, Nothing] = {
      f(error.cause.squash).flatMap(DBZIO.fail)
    }

    override def onError[R](cleanup: Cause[Throwable] => DBZIO[R, Any]): DBZIO[R with HasDb, Nothing] = {
      cleanup(error.cause)
        .flatMap(_ => this)
    }

    override def fold[B](failure: Throwable => B, success: Nothing => B): DBZIO[Any, B] = DBZIO.success(failure(error))

    override def foldM[R1, B](
                               failure: Throwable => DBZIO[R1, B],
                               success: Nothing => DBZIO[R1, B]
                             ): DBZIO[R1 with HasDb, B] = failure(error)

    override def withTransactionIsolation(ti: TransactionIsolation)(implicit profile: JdbcProfile): DBZIO[Any, Nothing] = this
  }

  case class PureZio[-R, E <: Throwable, +T] private[DBZIO](action: ZIO[R, E, T]) extends DBZIO[R, T] {
    def map[Q](f: T => Q): DBZIO[R, Q] = PureZio(action.map(f))

    override lazy val withPinnedSession: DBZIO[R, T] = this

    override def transactionally(implicit profile: JdbcProfile): DBZIO[R, T] = this

    override lazy val result: RIO[R with HasDb, T] = wrapError(action)

    override lazy val toDBIO: RIO[R, DBIO[T]] = action.map {
      DBIO.successful
    }

    override def mapError(f: Throwable => Throwable): DBZIO[R, T] = copy(action = action.mapError(f))

    override def flatMapError[R1](f: Throwable => DBZIO[R1, Throwable]): DBZIO[R with R1 with HasDb, T] = copy(
      action = action.flatMapError(e => f(e).result.either).mapError(_.fold(identity, identity))
    )

    override def onError[R1 <: R](cleanup: Cause[Throwable] => DBZIO[R1, Any]): DBZIO[R1 with HasDb, T] = PureZio {
      action.onError(cleanup(_).result.ignore)
    }

    override def fold[B](failure: Throwable => B, success: T => B): DBZIO[R, B] = PureZio {
      action.fold(failure, success)
    }

    override def foldM[R1, B](
                               failure: Throwable => DBZIO[R1, B],
                               success: T => DBZIO[R1, B]
                             ): DBZIO[R with R1 with HasDb, B] = ZioOverDBIO {
      action.either
        .map {
          case Left(a) => failure(a)
          case Right(b) => success(b)
        }
        .flatMap(_.toDBIO)
    }

    override def withTransactionIsolation(ti: TransactionIsolation)(implicit profile: JdbcProfile): DBZIO[R, T] = this

    override def withResource[R1, R2](acquire: RIO[R1, Any], release: URIO[R2, Any]): DBZIO[R with R1 with R2, T] =
      copy(action = ZManaged.make(acquire)(_ => release).use(_ => action))
  }

  case class PureDBIO[+T] private[DBZIO](action: DBIO[T]) extends DBZIO[Any, T] {
    override def map[Q](f: T => Q): DBZIO[Any, Q] = DBIOInZio { implicit ex => action.map(f) }

    override lazy val withPinnedSession: DBZIO[Any, T] = copy(action = action.withPinnedSession)

    override def transactionally(implicit profile: JdbcProfile): DBZIO[Any, T] = {
      import profile.api._
      copy(action = action.transactionally)
    }

    override lazy val result: RIO[HasDb, T] = runDBIO(action)

    override lazy val toDBIO: UIO[DBIO[T]] = UIO.succeed(action)

    override def mapError(f: Throwable => Throwable): DBZIO[Any, T] = DBZIO { implicit ex =>
      action.cleanUp(f = _.map(f).map(DBIO.failed).getOrElse(DBIO.successful(())), keepFailure = false)
    }

    override def flatMapError[R1](f: Throwable => DBZIO[R1, Throwable]): DBZIO[R1 with HasDb, T] = ZioOverDBIO {
      ZIO.runtime[R1].map {
        mapErrorDBZIO(action, _, f)
      }
    }

    override def onError[R1](f: Cause[Throwable] => DBZIO[R1, Any]): DBZIO[R1 with HasDb, T] = ZioOverDBIO {
      ZIO.runtime[R1].map {
        cleanup(action)(_, f)
      }
    }

    override def fold[B](failure: Throwable => B, success: T => B): DBZIO[Any, B] = DBZIO { implicit ex =>
      foldDBIO(action, failure, success)
    }

    override def foldM[R1, B](
                               failure: Throwable => DBZIO[R1, B],
                               success: T => DBZIO[R1, B]
                             ): DBZIO[R1 with HasDb, B] = {
      foldMDBIO(action, failure, success)
    }

    override def withTransactionIsolation(ti: TransactionIsolation)(implicit profile: JdbcProfile): DBZIO[Any, T] = {
      import profile.api._
      copy(
        action = action.withTransactionIsolation(ti)
      )
    }
  }

  case class DBIOInZio[+T] private[DBZIO](action: ExecutionContext => DBIO[T]) extends DBZIO[Any, T] {
    override def map[Q](f: T => Q): DBZIO[Any, Q] = DBIOInZio { implicit ex => action(ex).map(f) }

    override lazy val withPinnedSession: DBZIO[Any, T] = copy(action = action(_).withPinnedSession)

    override def transactionally(implicit profile: JdbcProfile): DBZIO[Any, T] = {
      import profile.api._
      copy(action = action(_).transactionally)
    }

    override lazy val result: RIO[HasDb, T] = {
      ZIO
        .runtime[HasDb]
        .map(_.platform.executor.asEC)
        .map(action)
        .flatMap(runDBIO)
    }

    override lazy val toDBIO: Task[DBIO[T]] = ZIO.fromFuture { ex => Future.successful(action(ex)) }

    override def withTransactionIsolation(ti: TransactionIsolation)(implicit profile: JdbcProfile): DBZIO[Any, T] = {
      import profile.api._
      DBIOInZio {
        action(_).withTransactionIsolation(ti)
      }
    }

    override def mapError(f: Throwable => Throwable): DBZIO[Any, T] = DBIOInZio { implicit ex =>
      action(ex).cleanUp(
        f = _.map(f).map(DBIO.failed).getOrElse(DBIO.successful(())),
        keepFailure = false
      )
    }

    override def flatMapError[R](f: Throwable => DBZIO[R, Throwable]): DBZIO[R with HasDb, T] = ZioOverDBIO {
      ZIO.runtime[R].map { runtime =>
        val ec = runtime.platform.executor.asEC
        mapErrorDBZIO(action(ec), runtime, f)
      }
    }

    override def onError[R1 <: Any](f: Cause[Throwable] => DBZIO[R1, Any]): DBZIO[R1 with HasDb, T] =
      ZioOverDBIO {
        ZIO.runtime[R1].map { runtime =>
          val ec = runtime.platform.executor.asEC
          cleanup(action(ec))(runtime, f)
        }
      }

    override def fold[B](failure: Throwable => B, success: T => B): DBZIO[Any, B] = DBZIO { implicit ec =>
      action(ec).asTry.map {
        case Success(value) => success(value)
        case scala.util.Failure(exception) => failure(exception)
      }
    }

    override def foldM[R1, B](
                               failure: Throwable => DBZIO[R1, B],
                               success: T => DBZIO[R1, B]
                             ): DBZIO[R1 with HasDb, B] = ZioOverDBIO {
      for {
        ec <- ZIO.runtime[R1].map(_.platform.executor.asEC)
        dbio <- ZIO.fromFuture(_ => Future.successful(action(ec)))
        res <- foldMDBIO(dbio, failure, success).toDBIO
      } yield res
    }
  }

  case class ZioOverDBIO[-R, +T] private[DBZIO](action: RIO[R, DBIO[T]]) extends DBZIO[R, T] {
    override def map[Q](f: T => Q): DBZIO[R, Q] = ZioOverDBIO(
      for {
        ac <- action
        ec <- ZIO.runtime[R].map(_.platform.executor.asEC)
      } yield ac.map(f)(ec)
    )

    override lazy val withPinnedSession: DBZIO[R, T] = copy(action = action.map(_.withPinnedSession))

    override def transactionally(implicit profile: JdbcProfile): DBZIO[R, T] = {
      import profile.api._
      copy(action = action.map(_.transactionally))
    }

    override lazy val result: RIO[R with HasDb, T] = {
      wrapError(action)
        .flatMap(runDBIO)
    }

    override lazy val toDBIO: RIO[R, DBIO[T]] = action

    override def mapError(f: Throwable => Throwable): DBZIO[R, T] = ZioOverDBIO {
      for {
        dbio <- action.mapError(f)
        ec <- ZIO.runtime[R].map(_.platform.executor.asEC)
      } yield dbio.cleanUp(
        f = _.map(f).map(DBIO.failed).getOrElse(DBIO.successful(())),
        keepFailure = false
      )(ec)
    }

    override def flatMapError[R1](f: Throwable => DBZIO[R1, Throwable]): DBZIO[R with R1 with HasDb, T] = ZioOverDBIO {
      for {
        runtime <- ZIO.runtime[R1]
        dbio <- action.flatMapError(f(_).result.either.map(_.fold(identity, identity)))
      } yield mapErrorDBZIO(dbio, runtime, f)
    }

    override def onError[R1 <: R](f: Cause[Throwable] => DBZIO[R1, Any]): DBZIO[R1 with HasDb, T] = ZioOverDBIO {
      for {
        runtime <- ZIO.runtime[R1]
        dbio <- action.onError(f(_).result.ignore)
      } yield cleanup(dbio)(runtime, f)
    }

    override def fold[B](failure: Throwable => B, success: T => B): DBZIO[R, B] = ZioOverDBIO {
      action.either
        .map {
          case Left(a) => DBZIO.success(failure(a))
          case Right(b) =>
            DBZIO { implicit ec => foldDBIO(b, failure, success) }
        }
        .flatMap(_.toDBIO)
    }

    override def foldM[R1, B](
                               failure: Throwable => DBZIO[R1, B],
                               success: T => DBZIO[R1, B]
                             ): DBZIO[R with R1 with HasDb, B] = ZioOverDBIO {
      for {
        dbio <- action
        res <- foldMDBIO(dbio, failure, success).toDBIO
      } yield res
    }

    override def withTransactionIsolation(ti: TransactionIsolation)(implicit profile: JdbcProfile): DBZIO[R, T] = {
      import profile.api._
      copy(
        action = action.map(_.withTransactionIsolation(ti))
      )
    }
  }

  private def foldDBIO[A, B](dbio: DBIO[A], failure: Throwable => B, success: A => B)(
    implicit ec: ExecutionContext
  ): DBIO[B] = {
    dbio.asTry.map {
      case Success(value) => success(value)
      case scala.util.Failure(exception) => failure(exception)
    }
  }

  private def foldMDBIO[R, A, B](
                                  dbio: DBIO[A],
                                  failure: Throwable => DBZIO[R, B],
                                  success: A => DBZIO[R, B]
                                ): DBZIO[R, B] = DBZIO {
    ZIO.runtime[R].map { runtime =>
      implicit val ec: ExecutionContext = runtime.platform.executor.asEC
      dbio.asTry.flatMap {
        case util.Failure(exception) => runToDBIO(runtime, failure(exception).toDBIO.on(ec))
        case Success(value) => runToDBIO(runtime, success(value).toDBIO.on(ec))
      }
    }
  }

  private def cleanup[R, T](
                             dbio: DBIO[T]
                           )(runtime: Runtime[R], f: Cause[Throwable] => DBZIO[R, Any]): DBIO[T] = {
    implicit val ec: ExecutionContext = runtime.platform.executor.asEC
    dbio.cleanUp(
      f = _.map(Cause.fail)
        .map(f(_).toDBIO.on(ec))
        .map(runToDBIO(runtime, _))
        .getOrElse(DBIO.successful(())),
      keepFailure = true
    )
  }

  private def mapErrorDBZIO[R, T](
                                   dbio: DBIO[T],
                                   runtime: Runtime[R],
                                   f: Throwable => DBZIO[R, Throwable]
                                 ): DBIO[T] = {
    implicit val ec: ExecutionContext = runtime.platform.executor.asEC
    dbio.cleanUp(
      f = _.map(f(_).toDBIO.on(ec))
        .map(runToDBIO(runtime, _).map(DBIO.failed).flatMap(identity))
        .getOrElse(DBIO.successful(())),
      keepFailure = false
    )
  }

  private def runToDBIO[R, T](runtime: Runtime[R], zio: RIO[R, DBIO[T]]): DBIO[T] = {
    runtime
      .unsafeRunSync(wrapError(zio))
      .fold(
        cause => DBIO.failed(wrapCause(cause)),
        identity
      )
  }

  private def runDBIO[T](dbio: DBIO[T]): RIO[HasDb, T] = ZIO.accessM[HasDb] { env =>
    wrapError(Task.fromFuture(_ => env.get.run(dbio)))
  }

  private def findDBZIOException[E <: Throwable](cause: Cause[E]): Option[Throwable] = {
    cause.failures
      .find {
        case _: DBZIOException[_] => true
        case _ => false
      }
  }

  private def wrapError[R, T](zio: RIO[R, T]): RIO[R, T] = {
    zio.mapErrorCause { c =>
      findDBZIOException(c)
        .map(_ => c)
        .getOrElse(Cause.fail(DBZIOException(c)))
    }
  }

  private def wrapCause[E <: Throwable](cause: Cause[E]): DBZIOException[E] = {
    findDBZIOException(cause)
      .map(_.asInstanceOf[DBZIOException[E]])
      .getOrElse(DBZIOException(cause))
  }
}
