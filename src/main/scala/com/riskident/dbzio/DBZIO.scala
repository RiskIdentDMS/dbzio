package com.riskident.dbzio

import com.riskident.dbzio.DBZIO._
import shapeless.=:!=
import slick.dbio.DBIO
import slick.jdbc.TransactionIsolation
import slick.jdbc.PostgresProfile.api._
import zio._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

sealed trait DBZIO[-R, +T] {
  def map[Q](f: T => Q): DBZIO[R, Q]

  def flatMap[Q, R1](f: T => DBZIO[R1, Q]): DBZIO[R1 with R, Q] = ZioOverDBIO {
    for {
      dbio    <- toDBIO
      runtime <- ZIO.runtime[R1]
      res <- Task.fromFuture { implicit ec =>
        Future {
          for {
            t   <- dbio
            res <- DBIO.from(runToDBIO(runtime, f(t).toDBIO.on(ec))).flatten
          } yield res
        }
      }
    } yield res
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

  val withPinnedSession: DBZIO[R, T]
  val transactionally: DBZIO[R, T]
  lazy val unit: DBZIO[R, Unit] = map(_ => ())
  val result: RIO[R with HasDb, T]
  protected val toDBIO: RIO[R, DBIO[T]]
  def withTransactionIsolation(ti: TransactionIsolation): DBZIO[R, T]
}

object DBZIO {

  case class DBZIOException[+E <: Throwable](cause: Cause[E]) extends Throwable {
    final override def getMessage: String = cause.prettyPrint
  }

  type HasDb = Has[Database]

  def apply[T](action: DBIO[T]): DBZIO[Any, T] = PureDBIO(action)

  def apply[T](action: ExecutionContext => DBIO[T]): DBZIO[Any, T] = DBIOInZio(action)

  def apply[R, E <: Throwable, T](action: ZIO[R, E, T])(implicit ev: T =:!= DBIO[_]): DBZIO[R, T] = PureZio(action)

  def apply[R, E <: Throwable, T](action: ZIO[R, E, DBIO[T]]): DBZIO[R, T] = ZioOverDBIO(action)

  def synchronized[R, T](sync: Semaphore)(action: DBZIO[R, T]): DBZIO[R, T] = ZioOverDBIO {
    sync.withPermit(action.toDBIO)
  }

  def fail[E <: Throwable, T](error: E): DBZIO[Any, T] =
    Failure(DBZIOException(Cause.fail(error)))

  @deprecated("Outdated and wasteful. Use DBIO instead", "02/11/2020")
  def withinSession[T](f: Session => T): DBZIO[HasDb, T] = PureZio {
    ZManaged
      .fromAutoCloseable(ZIO.access[HasDb](_.get.createSession()))
      .use(session => ZIO.effect(f(session)))
  }

  val unit: DBZIO[Any, Unit] = success(())

  def success[A](v: => A): DBZIO[Any, A] = PureZio[Any, Nothing, A](ZIO.effectTotal(v))

  def collectAll[R, T](col: Iterable[DBZIO[R, T]]): DBZIO[R, Iterable[T]] = {
    ZioOverDBIO {
      ZIO.foreach(col)(_.toDBIO).map(DBIO.sequence(_))
    }
  }

  case class Failure[E <: Throwable] private[DBZIO] (error: DBZIOException[E]) extends DBZIO[Any, Nothing] {
    override def map[Q](f: Nothing => Q): DBZIO[Any, Q] = this.asInstanceOf[DBZIO[Any, Q]]

    override val withPinnedSession: DBZIO[Any, Nothing]    = this
    override val transactionally: DBZIO[Any, Nothing]      = this
    override val result: RIO[HasDb, Nothing]               = wrapError(RIO.fail(error))
    override protected val toDBIO: RIO[Any, DBIO[Nothing]] = RIO.fail(error)

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
                             ): DBZIO[R1 with HasDb, B]                                                           = failure(error)
    override def withTransactionIsolation(ti: TransactionIsolation): DBZIO[Any, Nothing] = this
  }

  case class PureZio[-R, E <: Throwable, +T] private[DBZIO] (action: ZIO[R, E, T]) extends DBZIO[R, T] {
    def map[Q](f: T => Q): DBZIO[R, Q] = PureZio(action.map(f))

    override lazy val withPinnedSession: DBZIO[R, T] = this

    override lazy val transactionally: DBZIO[R, T] = this

    override lazy val result: RIO[R with HasDb, T] = wrapError(action)

    override protected lazy val toDBIO: RIO[R, DBIO[T]] = action.map(DBIO.successful)

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
          case Left(a)  => failure(a)
          case Right(b) => success(b)
        }
        .flatMap(_.toDBIO)
    }
    override def withTransactionIsolation(ti: TransactionIsolation): DBZIO[R, T] = this
  }

  case class PureDBIO[+T] private[DBZIO] (action: DBIO[T]) extends DBZIO[Any, T] {
    override def map[Q](f: T => Q): DBZIO[Any, Q] = DBIOInZio { implicit ex => action.map(f) }

    override lazy val withPinnedSession: DBZIO[Any, T] = copy(action = action.withPinnedSession)

    override lazy val transactionally: DBZIO[Any, T] = copy(action = action.transactionally)

    override lazy val result: RIO[HasDb, T] = runDBIO(action)

    override protected lazy val toDBIO: UIO[DBIO[T]] = UIO.succeed(action)

    override def mapError(f: Throwable => Throwable): DBZIO[Any, T] = DBZIO { implicit ex =>
      action.cleanUp(f = _.map(f).map(DBIO.failed).getOrElse(DBIO.successful(())), keepFailure = false)
    }

    override def flatMapError[R1](f: Throwable => DBZIO[R1, Throwable]): DBZIO[R1 with HasDb, T] = ZioOverDBIO {
      for {
        runtime <- ZIO.runtime[R1]
        res     <- ZIO.fromFuture { mapErrorDBZIO(action, runtime, f)(_) }
      } yield res
    }

    override def onError[R1](f: Cause[Throwable] => DBZIO[R1, Any]): DBZIO[R1 with HasDb, T] = ZioOverDBIO {
      for {
        runtime <- ZIO.runtime[R1]
        res     <- ZIO.fromFuture(implicit ex => Future.successful(cleanup(action)(runtime, f)))
      } yield res
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

    override def withTransactionIsolation(ti: TransactionIsolation): DBZIO[Any, T] = copy(
      action = action.withTransactionIsolation(ti)
    )
  }

  case class DBIOInZio[+T] private[DBZIO] (action: ExecutionContext => DBIO[T]) extends DBZIO[Any, T] {
    override def map[Q](f: T => Q): DBZIO[Any, Q] = DBIOInZio { implicit ex => action(ex).map(f) }

    override lazy val withPinnedSession: DBZIO[Any, T] = copy(action = action(_).withPinnedSession)

    override lazy val transactionally: DBZIO[Any, T] = copy(action = action(_).transactionally)

    override lazy val result: RIO[HasDb, T] = ZIO.fromFuture(ex => Future.successful(action(ex))).flatMap(runDBIO)

    override protected lazy val toDBIO: Task[DBIO[T]] = ZIO.fromFuture { ex => Future.successful(action(ex)) }

    override def withTransactionIsolation(ti: TransactionIsolation): DBZIO[Any, T] =
      DBIOInZio { ec => action(ec).withTransactionIsolation(ti) }

    override def mapError(f: Throwable => Throwable): DBZIO[Any, T] = DBIOInZio { implicit ex =>
      action(ex).cleanUp(
        f = _.map(f).map(DBIO.failed).getOrElse(DBIO.successful(())),
        keepFailure = false
      )
    }

    override def flatMapError[R](f: Throwable => DBZIO[R, Throwable]): DBZIO[R with HasDb, T] = ZioOverDBIO {
      for {
        runtime <- ZIO.runtime[R]
        res     <- ZIO.fromFuture { ec => mapErrorDBZIO(action(ec), runtime, f)(ec) }
      } yield res
    }

    override def onError[R1 <: Any](f: Cause[Throwable] => DBZIO[R1, Any]): DBZIO[R1 with HasDb, T] = ZioOverDBIO {
      for {
        runtime <- ZIO.runtime[R1]
        res     <- ZIO.fromFuture { implicit ex => Future.successful(cleanup(action(ex))(runtime, f)) }
      } yield res
    }

    override def fold[B](failure: Throwable => B, success: T => B): DBZIO[Any, B] = DBZIO { implicit ec =>
      action(ec).asTry.map {
        case Success(value)                => success(value)
        case scala.util.Failure(exception) => failure(exception)
      }
    }

    override def foldM[R1, B](
                               failure: Throwable => DBZIO[R1, B],
                               success: T => DBZIO[R1, B]
                             ): DBZIO[Any with R1 with HasDb, B] = ZioOverDBIO {
      for {
        dbio <- ZIO.fromFuture(ec => Future.successful(action(ec)))
        res  <- foldMDBIO(dbio, failure, success).toDBIO
      } yield res
    }
  }

  case class ZioOverDBIO[-R, +T] private[DBZIO] (action: RIO[R, DBIO[T]]) extends DBZIO[R, T] {
    override def map[Q](f: T => Q): DBZIO[R, Q] = ZioOverDBIO(
      for {
        ac  <- action
        res <- ZIO.fromFuture(implicit ex => Future(ac.map(f)))
      } yield res
    )

    override lazy val withPinnedSession: DBZIO[R, T] = copy(action = action.map(_.withPinnedSession))

    override lazy val transactionally: DBZIO[R, T] = copy(action = action.map(_.transactionally))

    override lazy val result: RIO[R with HasDb, T] = {
      for {
        ac  <- wrapError(action)
        res <- runDBIO(ac)
      } yield res
    }

    override protected lazy val toDBIO: RIO[R, DBIO[T]] = action

    override def mapError(f: Throwable => Throwable): DBZIO[R, T] = ZioOverDBIO {
      for {
        dbio <- action.mapError(f)
        res <- ZIO.fromFuture { implicit ex =>
          Future.successful {
            dbio.cleanUp(
              f = _.map(f).map(DBIO.failed).getOrElse(DBIO.successful(())),
              keepFailure = false
            )
          }
        }
      } yield res
    }

    override def flatMapError[R1](f: Throwable => DBZIO[R1, Throwable]): DBZIO[R with R1 with HasDb, T] = ZioOverDBIO {
      for {
        runtime <- ZIO.runtime[R1]
        dbio    <- action.flatMapError(f(_).result.either.map(_.fold(identity, identity)))
        res     <- ZIO.fromFuture { mapErrorDBZIO(dbio, runtime, f)(_) }
      } yield res
    }

    override def onError[R1 <: R](f: Cause[Throwable] => DBZIO[R1, Any]): DBZIO[R1 with HasDb, T] = ZioOverDBIO {
      ZIO.fromFuture { implicit ex =>
        Future {
          for {
            runtime <- ZIO.runtime[R1]
            dbio    <- action.onError(f(_).result.ignore)
          } yield cleanup(dbio)(runtime, f)
        }
      }.flatten
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
        res  <- foldMDBIO(dbio, failure, success).toDBIO
      } yield res
    }

    override def withTransactionIsolation(ti: TransactionIsolation): DBZIO[R, T] = copy(
      action = action.map(_.withTransactionIsolation(ti))
    )
  }

  private def foldDBIO[A, B](dbio: DBIO[A], failure: Throwable => B, success: A => B)(
    implicit ec: ExecutionContext
  ): DBIO[B] = {
    dbio.asTry.map {
      case Success(value)                => success(value)
      case scala.util.Failure(exception) => failure(exception)
    }
  }

  private def foldMDBIO[R, A, B](
                                  dbio: DBIO[A],
                                  failure: Throwable => DBZIO[R, B],
                                  success: A => DBZIO[R, B]
                                ): DBZIO[R with HasDb, B] = ZioOverDBIO {
    runDBIO(dbio.asTry)
      .map {
        case util.Failure(exception) => failure(exception)
        case Success(value)          => success(value)
      }
      .flatMap(_.toDBIO)
  }

  private def cleanup[R, T](
                             dbio: DBIO[T]
                           )(runtime: Runtime[R], f: Cause[Throwable] => DBZIO[R, Any])(implicit ex: ExecutionContext): DBIO[T] = {
    dbio.cleanUp(
      f = _.map(Cause.fail)
        .map(f(_).toDBIO)
        .map(runToDBIO(runtime, _))
        .map(DBIO.from(_).flatMap(identity))
        .getOrElse(DBIO.successful(())),
      keepFailure = true
    )
  }

  private def mapErrorDBZIO[R, T](
                                   dbio: DBIO[T],
                                   runtime: Runtime[R],
                                   f: Throwable => DBZIO[R, Throwable]
                                 )(implicit ec: ExecutionContext): Future[DBIO[T]] =
    Future.successful {
      dbio.cleanUp(
        f = _.map(f(_).toDBIO)
          .map(runToDBIO(runtime, _))
          .map(DBIO.from(_).flatMap(identity).map(DBIO.failed).flatMap(identity))
          .getOrElse(DBIO.successful(())),
        keepFailure = false
      )
    }

  private def runToDBIO[R, T](runtime: Runtime[R], zio: RIO[R, DBIO[T]])(
    implicit ex: ExecutionContext
  ): Future[DBIO[T]] =
    Future {
      runtime
        .unsafeRunSync(wrapError(zio))
        .fold(
          cause => DBIO.failed(wrapCause(cause)),
          identity
        )
    }

  private def runDBIO[T](dbio: DBIO[T]): RIO[HasDb, T] = ZIO.accessM[HasDb] { env =>
    wrapError(ZIO.fromFuture(_ => env.get.run(dbio)))
  }

  private def wrapError[R, T](zio: RIO[R, T]): RIO[R, T] = {
    zio.mapErrorCause { c =>
      c.squashTrace match {
        case _: DBZIOException[_] => c
        case t: Throwable         => Cause.fail(DBZIOException(Cause.fail(t)))
      }
    }
  }

  private def wrapCause[E <: Throwable](cause: Cause[E]): DBZIOException[Throwable] = {
    cause.squashTrace match {
      case t: DBZIOException[_] => t
      case _                    => DBZIOException(cause)
    }
  }
}
