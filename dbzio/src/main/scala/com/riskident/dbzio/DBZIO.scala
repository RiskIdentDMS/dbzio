package com.riskident.dbzio

import com.riskident.dbzio.DBZIO._
import shapeless.=:!=
import slick.dbio._
import slick.jdbc.JdbcBackend.Database
import zio.ZIO.blocking
import zio._

import scala.annotation.{nowarn, tailrec}
import scala.concurrent.ExecutionContext
import scala.math.Ordered.orderingToOrdered
import scala.reflect.ClassTag
import scala.util.Success

/**
  * A monadic type to bring together [[DBIO]] and [[ZIO]]. Wrapping either of them into `DBZIO` allows to use them
  * together in one for-comprehension, while preserving DB-specific functionality for managing db-transactions and
  * db-connections.
  *
  * @example {{{
  * val loadFromDb: DBIO[Seq[Data]]
  * def queryWebService(data: Seq[Data]): RIO[R, Seq[Addition]]
  * def combine(data: Seq[Data], addition: Seq[Addition]): Seq[NewData]
  * def saveNewData(data: Seq[NewData]): DBIO[Unit]
  *
  * val action: DBZIO[R, Unit] = (for {
  *   data <- DBZIO(loadFromDb)
  *   addition <- DBZIO(queryWebService(data))
  *   newData = combine(data, addition)
  *   _ <- DBZIO(saveNewData(newData))
  * } yield ()).transactionally
  *
  * val result: ZIO[DbDependency with R, Throwable, Unit] = action.result
  * }}}
  *
  *
  * @todo Make error type
  */
sealed abstract class DBZIO[-R, +T](private[DBZIO] val tag: ActionTag)(implicit trace: Trace) {

  /** Returns a `DBZIO` whose success is mapped by the specified f function. */
  def map[Q](f: T => Q)(implicit trace: Trace): DBZIO[R, Q] = new Map(this, f)

  /**
    * Returns a `DBZIO` that models the execution of this effect, followed by the passing of its value
    * to the specified continuation function `f`, followed by the effect that it returns.
    */
  def flatMap[Q, R1](f: T => DBZIO[R1, Q])(implicit trace: Trace): DBZIO[R1 with R, Q] = new FlatMap(this, f)

  /**
    * Returns a `DBZIO` that first executes the outer `DBZIO`, and then executes the inner `DBZIO`,
    * returning the value from the inner `DBZIO`, and effectively flattening a nested `DBZIO`.
    */
  def flatten[S, Q](implicit ev: T <:< DBZIO[S, Q], trace: Trace): DBZIO[S with R, Q] = flatMap(ev(_))

  /** Returns a `DBZIO` with its error mapped using the specified function. */
  def mapError(f: Throwable => Throwable)(implicit trace: Trace): DBZIO[R, T] = new MapError(this, f)

  /**
    * Creates a composite `DBZIO` that represents this `DBZIO` followed by another
    * one that may depend on the error produced by this one.
    */
  def flatMapError[R1](f: Throwable => DBZIO[R1, Throwable])(implicit trace: Trace): DBZIO[R with R1, T] =
    new FlatMapError(this, f)

  /** Runs the specified `DBZIO` if this `DBZIO` fails, providing the error to the `DBZIO` if it exists. */
  def onError[R1 <: R](cleanup: Cause[Throwable] => DBZIO[R1, Any])(implicit trace: Trace): DBZIO[R1, T] =
    new OnError(this, cleanup)

  /** Recovers from some or all of the error cases. */
  def catchSome[R1, T2 >: T](
      pf: PartialFunction[Throwable, DBZIO[R1, T2]]
  )(implicit trace: Trace): DBZIO[R with R1, T2] =
    new CatchSome(this, pf)

  /** Recovers from all errors. */
  def catchAll[R1, T2 >: T](f: Throwable => DBZIO[R1, T2])(implicit trace: Trace): DBZIO[R with R1, T2] =
    new CatchAll(this, f)

  /**
    * Folds over the failure value or the success value to yield a `DBZIO` that
    * does not fail, but succeeds with the value returned by the left or right
    * function passed to `fold`.
    */
  def fold[B](failure: Throwable => B, success: T => B)(implicit trace: Trace): DBZIO[R, B] =
    new FoldM[R, T, Any, B](this, failure.andThen(DBZIO.success(_)), success.andThen(DBZIO.success(_)))

  /**
    * Recovers from errors by accepting one `DBZIO` to execute for the private of an
    * error, and one `DBZIO` to execute for the private of success.
    */
  def foldM[R1, B](
      failure: Throwable => DBZIO[R1, B],
      success: T => DBZIO[R1, B]
  )(implicit trace: Trace): DBZIO[R with R1, B] = new FoldM(this, failure, success)

  /**
    * Executes this `DBZIO` and returns its value, if the value is defined (given that `T` is `Option[K]`),
    * and executes the specified `DBZIO` if the result value is `None`.
    */
  def orElseOption[R1, K, Q >: T](
      other: DBZIO[R1, Q]
  )(implicit ev: T <:< Option[K], trace: Trace): DBZIO[R with R1, Q] = {
    this.flatMap(_.fold(other)(t => DBZIO.success(Some(t).asInstanceOf[Q])))
  }

  /** Creates `DBZIO` that executes `other` in private current fails. */
  def orElse[R1, Q >: T](other: DBZIO[R1, Q])(implicit trace: Trace): DBZIO[R with R1, Q] = {
    foldM(_ => other, t => DBZIO.success(t.asInstanceOf[Q]))
  }

  /** Returns a `DBZIO` that effectfully "peeks" at the success of this `DBZIO`. */
  def tapM[R1](f: T => DBZIO[R1, Any])(implicit trace: Trace): DBZIO[R1 with R, T] = flatMap(t => f(t).map(_ => t))

  /** Returns the `DBIZO` resulting from mapping the success of this `DBIZO` to unit. */
  def unit(implicit trace: Trace): DBZIO[R, Unit] = map(_ => ())

  /** Returns `DBZIO` that will execute current `DBZIO` if and only if `cond` is `true`. */
  def when(cond: => Boolean): DBZIO[R, Unit] = DBZIO.ifF(cond, onTrue = this.unit, onFalse = DBZIO.unit)

  /**
    * Returns a `DBZIO` which executes current `DBZIO` with pinned db-session, if it makes sense (if current `DBZIO`
    * contains db-action). Otherwise, current `DBZIO` is left unaffected.
    *
    * Behaves exactly as [[DBIOAction.withPinnedSession]]
    */
  def withPinnedSession(implicit trace: Trace): DBZIO[R, T] = new WithPinnedSession(this)

  /**
    * Returns new `DBZIO`, which executes current `DBZIO` in db-transaction,
    * if it makes sense (if current `DBZIO` contains db-action).
    * Otherwise current `DBZIO` is left unaffected
    *
    * Behaves exactly as [[slick.jdbc.JdbcActionComponent.JdbcActionExtensionMethods.transactionally]].
    */
  def transactionally(implicit profile: JdbcProfile, trace: Trace): DBZIO[R, T] = new Transactionally(this, profile)

  /**
    * Returns a `DBZIO` with set transaction isolation level. In order to have any effect, should be called on a
    * `DBZIO` with db-transaction.
    *
    * {{{
    *   DBZIO(...)
    *      .transactionally
    *      .withTransactionIsolation(TransactionIsolation.ReadCommitted)
    * }}}
    *
    * Behaves exactly as
    * [[slick.jdbc.JdbcActionComponent.JdbcActionExtensionMethods.withTransactionIsolation]]
    */
  def withTransactionIsolation(ti: TransactionIsolation)(implicit profile: JdbcProfile, trace: Trace): DBZIO[R, T] =
    new WithTransactionIsolation(this, ti, profile)

  /**
    * Creates the [[ZIO]] effect that executes current 'DBZIO'.
    * @return [[ZIO]] with [[Database]] and `R` dependencies.
    */
  def result(implicit @nowarn trace: Trace): DbRIO[R, T] = DBZIO.result(this)

  /** Maps this `DBZIO` to a DBZIO of another value. */
  def as[A](a: => A)(implicit trace: Trace): DBZIO[R, A] = map(_ => a)

  /** Sequentially zip this `DBZIO` with the `other`, combining the result into a tuple */
  def <*>[R1, B](other: DBZIO[R1, B])(implicit trace: Trace): DBZIO[R1 with R, (T, B)] = flatMap(t => other.map((t, _)))

  /** Alias of <*> */
  def zip[R1, B](other: DBZIO[R1, B])(implicit trace: Trace): DBZIO[R1 with R, (T, B)] = <*>(other)

  /** A variant of flatMap that ignores the value produced by this `DBZIO` */
  def *>[R1, B](other: DBZIO[R1, B])(implicit trace: Trace): DBZIO[R1 with R, B] = flatMap(_ => other)

  /** Alias of *> */
  def zipRight[R1, B](other: DBZIO[R1, B])(implicit trace: Trace): DBZIO[R1 with R, B] = *>(other)

  /** Sequences the other `DBZIO` after this `DBZIO`, but ignores the value produced by `other` */
  def <*[R1, B](other: DBZIO[R1, B])(implicit trace: Trace): DBZIO[R1 with R, T] = flatMap(t => other.as(t))

  /** Alias of <* */
  def zipLeft[R1, B](other: DBZIO[R1, B])(implicit trace: Trace): DBZIO[R1 with R, T] = <*(other)
}

object DBZIO {

  type NotDBIO[T] = T =:!= DBIO[_]
  type NotZIO[T]  = T =:!= ZIO[_, _, _]

  /** Creates `DBZIO` that wraps a pure value. */
  def success[A](v: => A)(implicit trace: Trace): DBZIO[Any, A] = new PureValue[A](() => v)

  /** Creates `DBZIO` that wraps [[DBIO]]. */
  def apply[T](action: => DBIO[T])(implicit trace: Trace): DBZIO[Any, T] = new PureDBIO(() => action)

  /** Creates `DBZIO` that wraps a chain (usually a for-comprehension) of [[DBIO]]. */
  def apply[T](action: ExecutionContext => DBIO[T])(implicit trace: Trace): DBZIO[Any, T] = new DBIOChain(action)

  /** Creates `DBZIO` that wraps [[ZIO]] that returns a pure value. */
  def apply[R, E <: Throwable, T: NotDBIO](action: ZIO[R, E, T])(implicit trace: Trace): DBZIO[R, T] =
    new PureZio(action)

  /** Creates `DBZIO` that wraps [[ZIO]] that produces db-action ([[DBIO]]). */
  def apply[R, E <: Throwable, T](action: ZIO[R, E, DBIO[T]])(implicit trace: Trace): DBZIO[R, T] =
    new ZioOverDBIO(action)

  /** Creates `DBZIO` that wraps a [[Some]] of a pure value. */
  def some[A](v: => A)(implicit trace: Trace): DBZIO[Any, Option[A]] = success(Some(v))

  /** Creates `DBZIO` that wraps a [[None]]. */
  def none[T](implicit trace: Trace): DBZIO[Any, Option[T]] = success(None)

  /** Creates `DBZIO` that fails with provided error. */
  def fail[E <: Throwable, T](error: => E)(implicit trace: Trace): DBZIO[Any, T] =
    new Failure(() => DBZIOException(Cause.fail(error)))

  /** A generic wrapper for all errors produced by `DBZIO`. */
  case class DBZIOException[+E <: Throwable](cause: Cause[E]) extends Throwable {
    final override def getMessage: String = cause.prettyPrint
  }

  /** Creates a `DBZIO` that will execute `onTrue` if `cond` is `true`, and `onFalse` otherwise */
  def ifF[R, T](cond: => Boolean, onTrue: DBZIO[R, T], onFalse: DBZIO[R, T]): DBZIO[R, T] = {
    if (cond) onTrue else onFalse
  }

  /**
    * Creates a `DBZIO` that will execute `cond` and based on result will execute `onTrue`, if the result is `true`,
    * and `onFalse` otherwise.
    */
  def ifM[R, T](cond: DBZIO[R, Boolean], onTrue: DBZIO[R, T], onFalse: DBZIO[R, T])(
      implicit trace: Trace
  ): DBZIO[R, T] = {
    cond.flatMap(DBZIO.ifF(_, onTrue, onFalse))
  }

  /** `DBZIO` that does nothing and returns [[Unit]]. */
  def unit(implicit trace: Trace): DBZIO[Any, Unit] = success(())

  /**
    * Creates `DBZIO` that executes a batch of `DBZIO`, returning collection of results from each one of them.
    *
    * @note Behaves as [[DBIO.seq]], meaning that first error fails the whole batch.
    */
  def collectAll[R, T, C[+Element] <: Iterable[Element]](
      col: C[DBZIO[R, T]]
  )(implicit ev1: CanCollect[T, C], trace: Trace): DBZIO[R, C[T]] = {
    new CollectAll(col)
  }

  /**
    * Creates `DBZIO` that executes a batch of `DBZIO`, returning [[scala.collection.immutable.Seq]] of results from
    * each one of them.
    *
    * @note Behaves as [[DBIO.seq]], meaning that first error fails the whole batch.
    */
  def collectAll[R, T](col: DBZIO[R, T]*)(implicit trace: Trace): DBZIO[R, Seq[T]] = collectAll[R, T, Seq](col)

  /**
    * Creates `DBZIO` that executes a [[scala.collection.immutable.Set]] of `DBZIO`,
    * returning [[scala.collection.immutable.Set]] of results from each one of them.
    *
    * @note Behaves as [[DBIO.seq]], meaning that first error fails the whole batch.
    */
  def collectAll[R, T](col: Set[DBZIO[R, T]])(implicit trace: Trace): DBZIO[R, Set[T]] = {
    collectAll[R, T, Iterable](col).map(_.toSet)
  }

  /**
    * Creates `DBZIO` that executes an [[Array]] of `DBZIO`, returning [[Array]] of results from each one of them.
    *
    * @note Behaves as [[DBIO.seq]], meaning that first error fails the whole batch.
    */
  def collectAll[R, T: ClassTag](col: Array[DBZIO[R, T]])(implicit trace: Trace): DBZIO[R, Array[T]] = {
    collectAll[R, T, Iterable](col).map(_.toArray)
  }

  /**
    * Creates `DBZIO` that executes [[Option]] of `DBZIO`. The produced result will be either [[Some]] of the result of
    * executed `DBZIO`, if it was defined, or [[None]] otherwise.
    */
  def collectAll[R, T](col: Option[DBZIO[R, T]])(implicit trace: Trace): DBZIO[R, Option[T]] = {
    col.map(_.map(Some(_))).getOrElse(DBZIO.success(None))
  }

  /** Represents a failed `DBZIO`. */
  final private class Failure[E <: Throwable](val error: () => DBZIOException[E])(implicit trace: Trace)
      extends DBZIO[Any, Nothing](ActionTag.Failure)

  /** Represents a `DBZIO` producing pure value. */
  final private class PureValue[+T](val value: () => T)(implicit trace: Trace)
      extends DBZIO[Any, T](ActionTag.PureValue)

  /** Represents a `DBZIO` wrapped around [[ZIO]], that produces pure value (not [[DBIO]]). */
  final private class PureZio[-R, E <: Throwable, +T](val action: ZIO[R, E, T])(implicit trace: Trace)
      extends DBZIO[R, T](ActionTag.PureZio)

  /** Represents a `DBZIO` wrapped around single db-action ([[DBIO]]). */
  final private class PureDBIO[+T](val action: () => DBIO[T])(implicit trace: Trace)
      extends DBZIO[Any, T](ActionTag.PureDBIO)

  /** Represents a `DBZIO` wrapped around a chain of [[DBIO]] operations. Usually a for-comprehension. */
  final private class DBIOChain[+T](val action: ExecutionContext => DBIO[T])(implicit trace: Trace)
      extends DBZIO[Any, T](ActionTag.DBIOChain)

  /** Represents a `DBZIO` wrapped around [[ZIO]] effect, that produces a db-action ([[DBIO]]). */
  final private class ZioOverDBIO[-R, +T](val action: RIO[R, DBIO[T]])(implicit trace: Trace)
      extends DBZIO[R, T](ActionTag.ZioOverDBIO)

  /** A trait to uniform postprocessing of lazy computations */
  private trait PostProcessing[-R, A, B] extends Any { this: DBZIO[R, B] =>

    def addPostProcessing[R1 <: R, Res](
        tuple: (ResultProcessor.Aux[R1, Res, B], TransactionInformation)
    )(
        implicit ec: ExecutionContext,
        runtime: Runtime[R1]
    ): (ResultProcessor.Aux[R1, Res, A], TransactionInformation) = {
      addProcessing(tuple._1, tuple._2)
    }

    protected def addProcessing[R1 <: R, Res](
        processor: ResultProcessor.Aux[R1, Res, B],
        ti: TransactionInformation
    )(implicit ec: ExecutionContext, runtime: Runtime[R1]): (ResultProcessor.Aux[R1, Res, A], TransactionInformation)
  }

  /** Represents a [[DBZIO.flatMap]] operation. */
  final private class FlatMap[-R1, -R2, A, B](val self: DBZIO[R1, A], next: A => DBZIO[R2, B])(implicit trace: Trace)
      extends DBZIO[R1 with R2, B](ActionTag.FlatMap)
      with PostProcessing[R1 with R2, A, B] {
    override protected def addProcessing[R <: R1 with R2, Res](
        processor: ResultProcessor.Aux[R, Res, B],
        ti: TransactionInformation
    )(implicit ec: ExecutionContext, runtime: Runtime[R]): (ResultProcessor.Aux[R, Res, A], TransactionInformation) = {
      processor.add[A](_.flatMap(next, ti)) -> ti
    }
  }

  /** Represents a [[DBZIO.mapError]] operation. */
  final private class MapError[-R, T](val self: DBZIO[R, T], errorMap: Throwable => Throwable)(implicit trace: Trace)
      extends DBZIO[R, T](ActionTag.MapError)
      with PostProcessing[R, T, T] {
    override protected def addProcessing[R1 <: R, Res](
        processor: ResultProcessor.Aux[R1, Res, T],
        ti: TransactionInformation
    )(
        implicit ec: ExecutionContext,
        runtime: Runtime[R1]
    ): (ResultProcessor.Aux[R1, Res, T], TransactionInformation) = {
      processor.add[T](_.mapError(errorMap)) -> ti
    }
  }

  /** Represents a [[DBZIO.flatMapError]] operation. */
  final private class FlatMapError[-R, -R1, T](val self: DBZIO[R, T], errorMap: Throwable => DBZIO[R1, Throwable])(
      implicit trace: Trace
  ) extends DBZIO[R with R1, T](ActionTag.FlatMapError)
      with PostProcessing[R with R1, T, T] {
    override protected def addProcessing[R0 <: R with R1, Res](
        processor: ResultProcessor.Aux[R0, Res, T],
        ti: TransactionInformation
    )(
        implicit ec: ExecutionContext,
        runtime: Runtime[R0]
    ): (ResultProcessor.Aux[R0, Res, T], TransactionInformation) = {
      processor.add[T](_.flatMapError(errorMap, ti)) -> ti
    }
  }

  /** Represents a [[DBZIO.onError]] operation. */
  final private class OnError[R, R1 >: R, T](val self: DBZIO[R1, T], errorMap: Cause[Throwable] => DBZIO[R, Any])(
      implicit trace: Trace
  ) extends DBZIO[R, T](ActionTag.OnError)
      with PostProcessing[R, T, T] {
    override protected def addProcessing[R0 <: R, Res](
        processor: ResultProcessor.Aux[R0, Res, T],
        ti: TransactionInformation
    )(
        implicit ec: ExecutionContext,
        runtime: Runtime[R0]
    ): (ResultProcessor.Aux[R0, Res, T], TransactionInformation) = {
      processor.add[T](_.onError(e => errorMap(Cause.fail(e)), ti)) -> ti
    }
  }

  /** Represents a [[DBZIO.foldM]] operation. */
  final private class FoldM[R, T, R1, B](
      val self: DBZIO[R, T],
      failure: Throwable => DBZIO[R1, B],
      success: T => DBZIO[R1, B]
  )(implicit trace: Trace)
      extends DBZIO[R with R1, B](ActionTag.FoldM)
      with PostProcessing[R with R1, T, B] {
    override protected def addProcessing[R0 <: R1 with R, Res](
        processor: ResultProcessor.Aux[R0, Res, B],
        ti: TransactionInformation
    )(
        implicit ec: ExecutionContext,
        runtime: Runtime[R0]
    ): (ResultProcessor.Aux[R0, Res, T], TransactionInformation) = {
      processor.add[T](_.foldM(success, failure, ti)) -> ti
    }
  }

  /** Represents a [[DBZIO.withPinnedSession]] operation. */
  final private class WithPinnedSession[-R, T](val self: DBZIO[R, T])(implicit trace: Trace)
      extends DBZIO[R, T](ActionTag.WithPinnedSession)
      with PostProcessing[R, T, T] {
    override protected def addProcessing[R0 <: R, Res](
        processor: ResultProcessor.Aux[R0, Res, T],
        ti: TransactionInformation
    )(
        implicit ec: ExecutionContext,
        runtime: Runtime[R0]
    ): (ResultProcessor.Aux[R0, Res, T], TransactionInformation) = {
      ti match {
        case TransactionInformation(_, _, true) => processor -> ti
        case TransactionInformation(_, _, false) =>
          processor.add[T](_.mapDBIO(_.withPinnedSession)) -> ti.copy(inPinnedSession = true)
      }
    }
  }

  /** Represents a [[DBZIO.transactionally]] operation. */
  final private class Transactionally[-R, T](val self: DBZIO[R, T], profile: JdbcProfile)(implicit trace: Trace)
      extends DBZIO[R, T](ActionTag.Transactionally)
      with PostProcessing[R, T, T] {
    override protected def addProcessing[R0 <: R, Res](
        processor: ResultProcessor.Aux[R0, Res, T],
        ti: TransactionInformation
    )(
        implicit ec: ExecutionContext,
        runtime: Runtime[R0]
    ): (ResultProcessor.Aux[R0, Res, T], TransactionInformation) = {
      ti match {
        case TransactionInformation(true, _, _) => processor -> ti
        case TransactionInformation(false, _, _) =>
          import profile.api._
          processor.add[T](_.mapDBIO(_.transactionally)) -> ti.copy(inTransaction = true)
      }
    }
  }

  /** Represents a [[DBZIO.catchSome]] operation. */
  final private class CatchSome[-R, T](val self: DBZIO[R, T], pf: PartialFunction[Throwable, DBZIO[R, T]])(
      implicit trace: Trace
  ) extends DBZIO[R, T](ActionTag.CatchSome)
      with PostProcessing[R, T, T] {
    override protected def addProcessing[R0 <: R, Res](
        processor: ResultProcessor.Aux[R0, Res, T],
        ti: TransactionInformation
    )(implicit ec: ExecutionContext, r: Runtime[R0]): (ResultProcessor.Aux[R0, Res, T], TransactionInformation) = {
      processor.add[T](_.catchSome(pf, ti)) -> ti
    }
  }

  /** Represents a [[DBZIO.catchAll]] operation. */
  final private class CatchAll[-R, T](val self: DBZIO[R, T], f: Throwable => DBZIO[R, T])(implicit trace: Trace)
      extends DBZIO[R, T](ActionTag.CatchAll)
      with PostProcessing[R, T, T] {
    override protected def addProcessing[R0 <: R, Res](
        processor: ResultProcessor.Aux[R0, Res, T],
        ti: TransactionInformation
    )(implicit ec: ExecutionContext, r: Runtime[R0]): (ResultProcessor.Aux[R0, Res, T], TransactionInformation) = {
      processor.add[T](_.catchAll(f, ti)) -> ti
    }
  }

  /**
    * Represents a [[DBZIO collectAll]] operation. Also contains some methods for collection manipulations, since it
    * encapsulates collection type information.
    *
    * Behaves exactly as [[DBIO.seq]], in a sense that first error fails the whole batch.
    */
  final private class CollectAll[R, T, C[+Element] <: Iterable[Element]](val col: C[DBZIO[R, T]])(
      implicit ev1: CanCollect[T, C],
      trace: Trace
  ) extends DBZIO[R, C[T]](ActionTag.CollectAll) {

    /**
      * Structure to collect different types of [[Result]] from each element of the batch.
      *
      * @param item   counter, to remember the place for the result
      * @param dbios  collection of [[DBIO]]
      * @param zios   collection of [[ZIO]]
      */
    private case class Collector(
        item: Int,
        dbios: Seq[(Int, DBIO[T])],
        zios: Seq[(Int, Result.Zio[R, T])]
    ) {

      /** Creates new `Collector` instance with `res` added to an appropriate collection. */
      def add(res: Result[R, T]): Collector = {
        if (res.isDbio) {
          copy(dbios = dbios :+ (item -> res.dbio.get(())), item = item + 1)
        } else {
          copy(zios = zios :+ (item -> res.asInstanceOf[Result.Zio[R, T]]), item = item + 1)
        }
      }
    }

    private val empty: Collector = Collector(0, Seq.empty, Seq.empty)

    /**
      * Evaluate the whole collection as [[Result]]
      *
      * @param ti [[TransactionInformation]] for current evaluation
      */
    private[DBZIO] def evalEach[Ctx <: Context](
        ti: TransactionInformation,
        ctx: Ctx
    )(implicit ec: ExecutionContext, runtime: Runtime[R]): Result[R, C[T]] = {

      if (col.isEmpty) {
        ctx.lift(() => ev1.from(col).result())
      } else {
        def addResult(col: Collector, res: DBZIO[R, T]): Collector = {
          col.add(evalDBZIO[R, T, T](res, ctx)(ResultProcessor.empty[R, T] -> ti))
        }

        val collector = col.foldLeft(empty)(addResult)

        val res: Result[R, Seq[(Int, T)]] = if (collector.zios.nonEmpty) {
          Result.zio {
            ZIO
              .foreach(collector.zios) {
                case (n, z) => z.zio.get.map(n -> _)
              }
              .map { r =>
                val (v, d) = r.foldLeft((Seq.empty[(Int, T)], Seq.empty[(Int, DBIO[T])])) {
                  case ((tx, dbios), el) =>
                    el._2.foldTo(x => (tx :+ (el._1 -> x), dbios), x => (tx, dbios :+ (el._1 -> x)))
                }

                val dbios = d ++ collector.dbios

                if (dbios.nonEmpty) {
                  ZioResult(
                    DBIO
                      .sequence(dbios.map {
                        case (n, d) => d.map(n -> _)
                      })
                      .map(_ ++ v)
                  )
                } else {
                  ZioResult(v)
                }
              }
          }
        } else {
          Result.dbio(
            DBIO
              .sequence(collector.dbios.map {
                case (n, d) => d.map(n -> _)
              })
          )
        }

        res.map { _.sortBy(_._1).foldLeft(ev1.from(col))(_ += _._2).result() }
      }
    }
  }

  /** Represents [[DBZIO.withTransactionIsolation]] operation. */
  final private class WithTransactionIsolation[-R, T](
      val self: DBZIO[R, T],
      isolation: TransactionIsolation,
      profile: JdbcProfile
  )(implicit trace: Trace)
      extends DBZIO[R, T](ActionTag.WithTransactionIsolation)
      with PostProcessing[R, T, T] {
    override protected def addProcessing[R0 <: R, Res](
        processor: ResultProcessor.Aux[R0, Res, T],
        ti: TransactionInformation
    )(
        implicit ec: ExecutionContext,
        runtime: Runtime[R0]
    ): (ResultProcessor.Aux[R0, Res, T], TransactionInformation) = {

      def withIsolation: (ResultProcessor.Aux[R0, Res, T], TransactionInformation) = {
        import profile.api._
        val newTi        = ti.copy(isolation = Some(isolation))
        val newProcessor = processor.add[T](_.mapDBIO(_.withTransactionIsolation(isolation)))
        newProcessor -> newTi
      }

      def self: (ResultProcessor.Aux[R0, Res, T], TransactionInformation) = processor -> ti

      (ti: @unchecked) match {
        case TransactionInformation(true, _, _)                      => self
        case TransactionInformation(_, Some(x), _) if x >= isolation => self
        case TransactionInformation(_, Some(x), _) if x < isolation  => withIsolation
        case TransactionInformation(_, None, _)                      => withIsolation
      }

    }
  }

  /** Represents [[DBZIO.map]] operation. */
  final private class Map[-R, T, Q](val self: DBZIO[R, T], val f: T => Q)(implicit trace: Trace)
      extends DBZIO[R, Q](ActionTag.Map)
      with PostProcessing[R, T, Q] {
    override protected def addProcessing[R0 <: R, Res](
        processor: ResultProcessor.Aux[R0, Res, Q],
        ti: TransactionInformation
    )(
        implicit ec: ExecutionContext,
        runtime: Runtime[R0]
    ): (ResultProcessor.Aux[R0, Res, T], TransactionInformation) = {
      processor.add[T](_.map(f)) -> ti
    }
  }

  /** A tag that each `DBZIO` final instance of has to simplify pattern matching. */
  sealed private[DBZIO] trait ActionTag
  private[DBZIO] object ActionTag {
    case object Failure                  extends ActionTag
    case object PureValue                extends ActionTag
    case object PureZio                  extends ActionTag
    case object PureDBIO                 extends ActionTag
    case object DBIOChain                extends ActionTag
    case object ZioOverDBIO              extends ActionTag
    case object FlatMap                  extends ActionTag
    case object MapError                 extends ActionTag
    case object FlatMapError             extends ActionTag
    case object OnError                  extends ActionTag
    case object FoldM                    extends ActionTag
    case object WithPinnedSession        extends ActionTag
    case object Transactionally          extends ActionTag
    case object WithTransactionIsolation extends ActionTag
    case object Map                      extends ActionTag
    case object CollectAll               extends ActionTag
    case object CatchSome                extends ActionTag
    case object CatchAll                 extends ActionTag
  }

  /**
    * Structure to collection information about db specific modificators
    * (see [[DBZIO.transactionally]], [[DBZIO.withTransactionIsolation]], [[DBZIO.withPinnedSession]]).
    * Used to skip unnessessary db-modificators, e.g. [[DBZIO.transactionally]] on some inner `DBZIO`, when there is
    * already an open db-transaction on higher level.
    */
  private case class TransactionInformation(
      inTransaction: Boolean,
      isolation: Option[TransactionIsolation],
      inPinnedSession: Boolean
  )

  private object TransactionInformation {
    val empty: TransactionInformation =
      TransactionInformation(inTransaction = false, isolation = None, inPinnedSession = false)
  }

  /**
    * Represents result of execution of [[ZIO]] inside `DBZIO`.
    *
    * The result can be either pure value, or [[DBIO]].
    */
  sealed private[dbzio] trait ZioResult[+T] {

    /**
      * Creates a new `ZioResult`, that maps the final result
      * (be that a pure value, or the result of the produced [[DBIO]]).
      */
    def map[X](f: T => X)(implicit ec: ExecutionContext): ZioResult[X] = fold(f, _.map(f))

    /**
      * Creates a new `ZioResult`, that applies either `left` to pure value or `right` to [[DBIO]] to the containing
      * value, based on it's type.
      */
    def fold[X](left: T => X, right: DBIO[T] => DBIO[X]): ZioResult[X]

    /**
      * Produces a result of type `X` based on the type of containing value, applying either `left` to pure value
      * or `right` to [[DBIO]].
      */
    def foldTo[X](left: T => X, right: DBIO[T] => X): X

    /**
      * Creates a new `ZioResult` that has transformed [[DBIO]], if the original one contains [[DBIO]]. Otherwise
      * current `ZioResult` is left untouched.
      */
    def mapDBIO[T2 >: T](f: DBIO[T] => DBIO[T2]): ZioResult[T2]

    /** Creates a [[DBIO]] representation of containing value. */
    def toDBIO: DBIO[T]
  }

  private[dbzio] object ZioResult {

    /** `ZioResult` that contains pure value (NOT [[DBIO]]) */
    private case class Value[T](v: T) extends ZioResult[T] {
      override def fold[X](left: T => X, right: DBIO[T] => DBIO[X]): ZioResult[X] = copy(v = left(v))
      override def foldTo[X](left: T => X, right: DBIO[T] => X): X                = left(v)

      override def toDBIO: DBIO[T] = DBIO.successful(v)

      override def mapDBIO[T2 >: T](f: DBIO[T] => DBIO[T2]): ZioResult[T2] = this
    }

    /** `ZioResult` that contains [[DBIO]] */
    private case class Query[T](q: DBIO[T]) extends ZioResult[T] {
      override def fold[X](left: T => X, right: DBIO[T] => DBIO[X]): ZioResult[X] = copy(q = right(q))
      override def foldTo[X](left: T => X, right: DBIO[T] => X): X                = right(q)

      override def toDBIO: DBIO[T] = q

      override def mapDBIO[T2 >: T](f: DBIO[T] => DBIO[T2]): ZioResult[T2] = copy(q = f(q))
    }

    /** Creates pure value `ZioResult` (NOT [[DBIO]]). */
    def apply[T: NotDBIO](value: T): ZioResult[T] = Value(value)

    /** Creates [[DBIO]] `ZioResult`. */
    def apply[T](dbio: DBIO[T]): ZioResult[T] = Query(dbio)
  }

  /**
    * Monadic-like type representing result of evaluation of `DBZIO`.
    *
    * The result may be one of the following:
    *  - `DBIO[T]`
    *  - `ZIO[R, Throwable, ZioResult[T]]`
    *
    * @tparam R [[ZIO]] dependency
    * @tparam T type of the result
    */
  sealed abstract private class Result[-R, +T](implicit trace: Trace) {
    val dbio: Option[Any => DBIO[T]]      = None
    val zio: Option[RIO[R, ZioResult[T]]] = None

    val isDbio: Boolean = false
    val isZio: Boolean  = false

    /**
      * Applies transformation on containing value (based on the value type). All operations on `Result` are expressed
      * either through [[Result.transformAll]] or [[Result.transformAllM]].
      *
      * @note Should be called only on one of the 4 final results:
      *
      * - [[Result.PureDbio]]
      *
      * - [[Result.Zio]]
      */
    protected def transformAll[R2 <: R, T2](
        onZio: RIO[R, ZioResult[T]] => RIO[R2, ZioResult[T2]],
        onDbio: DBIO[T] => DBIO[T2]
    ): Result[R2, T2]

    /**
      * Chains another `Result` based on the value type. All operations on `Result` are expressed
      * either through [[Result.transformAll]] or [[Result.transformAllM]].
      *
      * @note Should be called only on one of the 4 final results:
      *
      * - [[Result.PureDbio]]
      *
      * - [[Result.Zio]]
      */
    protected def transformAllM[R2 <: R, T2](
        onZio: RIO[R, ZioResult[T]] => Result[R2, T2],
        onDbio: DBIO[T] => Result[R2, T2]
    ): Result[R2, T2]

    /** Creates `Result` that transforms final pure value of the current. */
    def map[T2](f: T => T2)(implicit ec: ExecutionContext): Result[R, T2] = transformAll(
      onZio = _.map(_.map(f)),
      onDbio = _.map(f)
    )

    /** Creates `Result` that chains current and the next one return by `f`.  */
    def flatMap[T2, R1 <: R](
        f: T => DBZIO[R1, T2],
        ti: TransactionInformation
    )(implicit ec: ExecutionContext, r: Runtime[R1]): Result[R1, T2] = {

      def waitFor(c: Context, dbzio: DBZIO[R1, T2]): c.F[R1, T2] =
        c.waitFor(evalDBZIO(dbzio, _)(ResultProcessor.empty[R1, T2] -> ti))

      val mapDBIO: DBIO[T] => DBIO[T2] = _.flatMap(t => waitFor(Context.DBIO, f(t)))

      transformAllM(
        onDbio = dbio => Result.dbio(mapDBIO(dbio)),
        onZio = z =>
          Result.zio {
            for {
              t <- z
              res <- t.foldTo(
                left = x => waitFor(Context.ZIO, f(x)),
                right = d => ZIO.succeed(ZioResult(mapDBIO(d)))
              )
            } yield res
          }
      )
    }

    /** Creates `Result` that transforms an error. */
    def mapError(f: Throwable => Throwable)(implicit ec: ExecutionContext): Result[R, T] = {
      val dbioTransform: DBIO[T] => DBIO[T] = _.asTry.flatMap {
        case util.Failure(exception) => DBIO.failed(f(exception))
        case Success(value)          => DBIO.successful(value)
      }

      transformAll(
        onDbio = dbioTransform,
        onZio = _.mapBoth(f, _.mapDBIO(dbioTransform))
      )
    }

    /** Creates `Result` that transforms an error to a new `Result` and then executes it. */
    def flatMapError[R2 <: R](
        f: Throwable => DBZIO[R2, Throwable],
        ti: TransactionInformation
    )(implicit ec: ExecutionContext, r: Runtime[R2]): Result[R2, T] = {

      def waitFor(c: Context, dbzio: DBZIO[R2, Throwable]): c.F[R2, Throwable] =
        c.waitFor(evalDBZIO(dbzio, _)(ResultProcessor.empty[R2, Throwable] -> ti))

      val mapDBIO: DBIO[T] => DBIO[T] = _.asTry.flatMap {
        case util.Failure(exception) =>
          waitFor(Context.DBIO, f(exception)).flatMap(DBIO.failed)
        case Success(value) => DBIO.successful(value)
      }

      val onFailureZ: Throwable => RIO[R2, ZioResult[T]] = a =>
        waitFor(Context.ZIO, f(a)).flatMap {
          _.foldTo(ZIO.fail(_), dbio => ZIO.succeed(ZioResult(dbio.flatMap(DBIO.failed))))
        }

      transformAllM(
        onDbio = d => Result.dbio(mapDBIO(d)),
        onZio = z =>
          Result.zio {
            z.foldZIO(onFailureZ, r => ZIO.succeed(r.mapDBIO(mapDBIO)))
          }
      )
    }

    /** Creates `Result` with cleanup action, which will be executed if execution of the current one fails. */
    def onError[R2 <: R](
        f: Throwable => DBZIO[R2, Any],
        ti: TransactionInformation
    )(implicit ec: ExecutionContext, r: Runtime[R2]): Result[R2, T] = {

      def waitFor(c: Context, dbzio: DBZIO[R2, Any]): c.F[R2, Any] =
        c.waitFor(evalDBZIO(dbzio, _)(ResultProcessor.empty[R2, Any] -> ti))

      val doFail: Throwable => DBIO[T] = e => DBIO.failed(wrapException(e))

      val mapDBIO: DBIO[T] => DBIO[T] = _.asTry.flatMap {
        case util.Failure(exception) =>
          waitFor(Context.DBIO, f(exception))
            .flatMap(_ => doFail(exception))
        case Success(value) => DBIO.successful(value)
      }

      val onFailureZ: Throwable => RIO[R2, ZioResult[T]] = a => {
        val nextZio = waitFor(Context.ZIO, f(a))
        nextZio.map(_.foldTo(_ => doFail(a), _.flatMap(_ => doFail(a)))).map(ZioResult[T])
      }

      transformAllM(
        onZio = z =>
          Result.zio {
            z.foldZIO(onFailureZ, b => ZIO.succeed(b.mapDBIO(mapDBIO)))
          },
        onDbio = d => Result.dbio(mapDBIO(d))
      )
    }

    /** Creates `Result` that will execute current one and then will execute `f` or `e` base of success or failure of
      *  the current one.
      */
    def foldM[T2, R2 <: R](f: T => DBZIO[R2, T2], e: Throwable => DBZIO[R2, T2], ti: TransactionInformation)(
        implicit ec: ExecutionContext,
        r: Runtime[R2]
    ): Result[R2, T2] = {

      def waitFor(c: Context, dbzio: DBZIO[R2, T2]): c.F[R2, T2] =
        c.waitFor(evalDBZIO(dbzio, _)(ResultProcessor.empty[R2, T2] -> ti))

      val foldDBIO: DBIO[T] => DBIO[T2] = _.asTry.flatMap {
        case util.Failure(exception) => waitFor(Context.DBIO, e(exception))
        case Success(value)          => waitFor(Context.DBIO, f(value))
      }

      transformAllM(
        onZio = z =>
          Result.zio(
            z.either.flatMap(
              _.fold(
                x => waitFor(Context.ZIO, e(x)),
                t => {
                  t.foldTo(
                    t => waitFor(Context.ZIO, f(t)),
                    x => ZIO.succeed(ZioResult(foldDBIO(x)))
                  )
                }
              )
            )
          ),
        onDbio = x => Result.dbio(foldDBIO(x))
      )
    }

    /** Creates `Result`, which will apply transformation to the [[DBIO]] if current is [[Result.PureDbio]] or
      * [[Result.Zio]] that produces [[DBIO]]. For other types of `Result` has no effects.
      */
    def mapDBIO[T2 >: T](f: DBIO[T2] => DBIO[T2]): Result[R, T2] = {
      transformAll(
        onDbio = f,
        onZio = _.map(_.mapDBIO(f))
      )
    }

    /** Creates `Result` with fallback for some of the errors that might arise during execution of the current one. */
    def catchSome[R2 <: R, T2 >: T](
        f: PartialFunction[Throwable, DBZIO[R2, T2]],
        ti: TransactionInformation
    )(implicit ec: ExecutionContext, r: Runtime[R2]): Result[R2, T2] = {

      def waitFor(c: Context, dbzio: DBZIO[R2, T2]): c.F[R2, T2] =
        c.waitFor(evalDBZIO(dbzio, _)(ResultProcessor.empty[R2, T2] -> ti))

      val mapDBIO: DBIO[T2] => DBIO[T2] = _.asTry.flatMap {
        case util.Failure(exception) if f.isDefinedAt(exception) =>
          waitFor(Context.DBIO, f(exception))
        case util.Failure(exception) => DBIO.failed(exception)
        case Success(value)          => DBIO.successful(value)
      }

      transformAllM(
        onZio = z =>
          Result.zio {
            z.catchSome(f.andThen(waitFor(Context.ZIO, _)))
              .map(_.mapDBIO(mapDBIO))
          },
        onDbio = x => Result.dbio(mapDBIO(x))
      )
    }

    /** Creates `Result` with fallback for all of the errors that might arise during execution of the current one. */
    def catchAll[T2 >: T, R2 <: R](
        f: Throwable => DBZIO[R2, T2],
        ti: TransactionInformation
    )(implicit ec: ExecutionContext, r: Runtime[R2]): Result[R2, T2] = {

      def waitFor(c: Context, dbzio: DBZIO[R2, T2]): c.F[R2, T2] =
        c.waitFor(evalDBZIO(dbzio, _)(ResultProcessor.empty[R2, T2] -> ti))

      val mapDBIO: DBIO[T2] => DBIO[T2] = _.asTry.flatMap {
        case util.Failure(exception) =>
          waitFor(Context.DBIO, f(exception))
        case Success(value) => DBIO.successful(value)
      }

      transformAllM(
        onDbio = x => Result.dbio(mapDBIO(x)),
        onZio = z =>
          Result.zio {
            z.catchAll(e => waitFor(Context.ZIO, f(e)))
              .map(_.mapDBIO(mapDBIO))
          }
      )
    }
  }

  private object Result {

    /** `Result` containing [[ZIO]] */
    case class Zio[R, T](value: RIO[R, ZioResult[T]])(implicit trace: Trace) extends Result[R, T] {
      override val zio: Option[RIO[R, ZioResult[T]]] = Some(value)
      override val isZio: Boolean                    = true
      override protected def transformAll[R2 <: R, T2](
          onZio: RIO[R, ZioResult[T]] => RIO[R2, ZioResult[T2]],
          onDbio: DBIO[T] => DBIO[T2]
      ): Result[R2, T2] = {
        Result.zio(onZio(value))
      }

      override protected def transformAllM[R2 <: R, T2](
          onZio: RIO[R, ZioResult[T]] => Result[R2, T2],
          onDbio: DBIO[T] => Result[R2, T2]
      ): Result[R2, T2] = {
        onZio(value)
      }
    }

    /** `Result` containing [[DBIO]]. */
    case class PureDbio[T](value: Any => DBIO[T])(implicit trace: Trace) extends Result[Any, T] {
      override val dbio: Option[Any => DBIO[T]] = Some(value)
      override val isDbio: Boolean              = true
      override protected def transformAll[R2 <: Any, T2](
          onZio: RIO[Any, ZioResult[T]] => RIO[R2, ZioResult[T2]],
          onDbio: DBIO[T] => DBIO[T2]
      ): Result[R2, T2] = {
        Result.dbio(onDbio(value(())))
      }

      override protected def transformAllM[R2 <: Any, T2](
          onZio: RIO[Any, ZioResult[T]] => Result[R2, T2],
          onDbio: DBIO[T] => Result[R2, T2]
      ): Result[R2, T2] = {
        onDbio(value(()))
      }

    }

    def dbio[T](dbio: => DBIO[T])(implicit trace: Trace): Result[Any, T] = PureDbio(_ => dbio)

    def zio[R, T: NotDBIO: NotZIO](zio: RIO[R, ZioResult[T]])(implicit trace: Trace): Result[R, T] = Zio(zio)
  }

  /**
    * Collects all the transformations of the `Result`s from the `DBZIO` chain.
    * @tparam R ZIO-like dependency
    * @tparam T Type of the final result
    */
  private trait ResultProcessor[R, T] {

    /** Starting type for the transformation chain */
    type Intermediate

    /** Result type of the first transformation in the chain */
    type Next

    type Transform[A, B, C] = Result[R, A] => ResultProcessor.StepResult[R, B, C]

    /** Chain of transformation functions */
    protected val transform: Transform[Intermediate, Next, ResultProcessor.Aux[R, T, Next]]

    @tailrec
    private def transformChain[Q](t: ResultProcessor.Aux[R, T, Q], in: Result[R, Q]): Result[R, T] = {
      val ResultProcessor.StepResult(next, nextTransformM) = t.transform(in)
      if (nextTransformM.isEmpty) next.asInstanceOf[Result[R, T]]
      else transformChain(nextTransformM.get, next)
    }

    /** Prepends new transformation to the chain of transformations */
    def add[Q](f: Result[R, Q] => Result[R, Intermediate]): ResultProcessor.Aux[R, T, Q] = {
      type N = this.Intermediate
      val self: ResultProcessor.Aux[R, T, N] = this
      new ResultProcessor[R, T] {
        override type Intermediate = Q
        override type Next         = N

        override protected val transform: Transform[Intermediate, Next, ResultProcessor.Aux[R, T, N]] =
          (a: Result[R, Q]) => ResultProcessor.StepResult(f(a), Some(self))
      }
    }

    /** Applies the chain of transformation to the `res` */
    def apply(res: Result[R, Intermediate]): Result[R, T] = {
      transformChain(this.asInstanceOf[ResultProcessor.Aux[R, T, Intermediate]], res)
    }
  }

  private object ResultProcessor {

    type Aux[R, T, I] = ResultProcessor[R, T] { type Intermediate = I }

    def empty[R, T]: ResultProcessor.Aux[R, T, T] = new ResultProcessor[R, T] {
      override type Intermediate = T
      override type Next         = T

      override protected val transform: Transform[T, T, ResultProcessor.Aux[R, T, T]] =
        (a: Result[R, T]) => StepResult(a, None)

    }

    case class StepResult[R, T, L](res: Result[R, T], next: Option[L])
  }

  /** Waits for `DBZIO` to produce result in specific context. */
  private def waitFor[R, T, C <: Context](
      action: DBZIO[R, T],
      ti: TransactionInformation
  )(context: C)(
      implicit runtime: Runtime[R],
      ec: ExecutionContext
  ): C#F[R, T] = {
    context.waitFor(evalDBZIO[R, T, T](action, _)(ResultProcessor.empty[R, T] -> ti))
  }

  /** Context of execution of the `Result`. May be either [[DBZIO.Context.ZIO]] or [[DBZIO.Context.DBIO]]. */
  sealed private trait Context {

    /** Type of the result produced by the specific context. */
    type F[_, _]

    /** Waits for 'result' to produce a value of the [[F]] */
    def waitFor[R, T](
        result: Context => Result[R, T]
    )(implicit runtime: Runtime[R], ec: ExecutionContext, trace: Trace): F[R, T] = {
      waitFor(result(this))
    }
    protected def waitFor[R, T](
        result: => Result[R, T]
    )(implicit runtime: Runtime[R], ec: ExecutionContext, trace: Trace): F[R, T]
    def lift[T](v: () => T)(implicit trace: Trace): Result[Any, T]
    def error[E <: Throwable, T](e: DBZIOException[E])(implicit trace: Trace): Result[Any, T]
  }

  private object Context {

    /** Context inside [[slick.dbio.DBIO]].  */
    object DBIO extends Context {
      override type F[R, T] = slick.dbio.DBIO[T]

      def runZIO[R, T](zio: RIO[R, T])(implicit runtime: Runtime[R], trace: Trace): DBIO[T] = {
        slick.dbio.DBIO
          .from {
            Unsafe.unsafe { implicit u => runtime.unsafe.runToFuture(zio) }
          }
      }

      def waitFor[R, T](
          self: => Result[R, T]
      )(implicit runtime: Runtime[R], ec: ExecutionContext, trace: Trace): F[R, T] = {
        if (self.isDbio) {
          self.dbio.get(())
        } else {
          runZIO(self.zio.get.map(_.toDBIO)).flatMap(identity)
        }
      }

      override def lift[T](v: () => T)(implicit trace: Trace): Result[Any, T] =
        Result.dbio(slick.dbio.DBIO.successful(v()))

      override def error[E <: Throwable, T](e: DBZIOException[E])(implicit trace: Trace): Result[Any, T] =
        Result.dbio(slick.dbio.DBIO.failed(e))
    }

    /** Context inside [[zio.ZIO]]. */
    object ZIO extends Context {
      override type F[R, T] = RIO[R, ZioResult[T]]

      def waitFor[R, T](
          result: => Result[R, T]
      )(implicit runtime: Runtime[R], ec: ExecutionContext, trace: Trace): F[R, T] = {
        if (result.isZio) {
          wrapError(result.zio.get)
        } else {
          zio.ZIO.succeed(ZioResult(result.dbio.get(())))
        }
      }

      override def lift[T](v: () => T)(implicit trace: Trace): Result[Any, T] =
        Result.zio(zio.ZIO.succeed(ZioResult(v())))

      override def error[E <: Throwable, T](e: DBZIOException[E])(implicit trace: Trace): Result[Any, T] = {
        val action: RIO[Any, ZioResult[T]] = zio.ZIO.fail(e)
        Result.zio(action)
      }

    }
  }

  /** Given a `DBZIO` evaluates a `Result` of the action. */
  @tailrec
  private def evalDBZIO[R, T, I](
      dbzio: DBZIO[R, I],
      ctx: Context
  )(
      data: (ResultProcessor.Aux[R, T, I], TransactionInformation)
  )(
      implicit runtime: Runtime[R],
      ec: ExecutionContext
  ): Result[R, T] = {
    dbzio.tag match {
      case ActionTag.Failure =>
        data._1(ctx.error[Throwable, I](dbzio.asInstanceOf[Failure[Throwable]].error()))

      case ActionTag.PureValue =>
        data._1(ctx.lift(dbzio.asInstanceOf[PureValue[data._1.Intermediate]].value))

      case ActionTag.PureZio =>
        val res = Result.zio(
          wrapError(dbzio.asInstanceOf[PureZio[R, Throwable, data._1.Intermediate]].action.map(ZioResult(_)))
        )
        data._1(res)

      case ActionTag.PureDBIO =>
        data._1(Result.dbio(dbzio.asInstanceOf[PureDBIO[data._1.Intermediate]].action()))

      case ActionTag.DBIOChain =>
        data._1(Result.dbio(dbzio.asInstanceOf[DBIOChain[data._1.Intermediate]].action(ec)))

      case ActionTag.ZioOverDBIO =>
        val res =
          Result.zio(wrapError(dbzio.asInstanceOf[ZioOverDBIO[R, data._1.Intermediate]].action.map(ZioResult(_))))
        data._1(res)

      case ActionTag.FlatMap =>
        val flatMap              = dbzio.asInstanceOf[FlatMap[R, R, Any, I]]
        val first: DBZIO[R, Any] = flatMap.self
        evalDBZIO(first, ctx)(flatMap.addPostProcessing(data))

      case ActionTag.MapError =>
        val mapError = dbzio.asInstanceOf[MapError[R, I]]
        evalDBZIO(mapError.self, ctx)(mapError.addPostProcessing(data))

      case ActionTag.FlatMapError =>
        val flatMapError = dbzio.asInstanceOf[FlatMapError[R, R, I]]
        evalDBZIO(flatMapError.self, ctx)(flatMapError.addPostProcessing(data))

      case ActionTag.OnError =>
        val onError = dbzio.asInstanceOf[OnError[R, R, I]]
        evalDBZIO(onError.self, ctx)(onError.addPostProcessing(data))

      case ActionTag.FoldM =>
        val fold = dbzio.asInstanceOf[FoldM[R, Any, R, I]]

        evalDBZIO(fold.self, ctx)(fold.addPostProcessing(data))

      case ActionTag.WithPinnedSession =>
        val withSession = dbzio.asInstanceOf[WithPinnedSession[R, I]]
        evalDBZIO(withSession.self, ctx)(withSession.addPostProcessing(data))

      case ActionTag.Transactionally =>
        val tx = dbzio.asInstanceOf[Transactionally[R, I]]
        evalDBZIO(tx.self, ctx)(tx.addPostProcessing(data))

      case ActionTag.WithTransactionIsolation =>
        val wti = dbzio.asInstanceOf[WithTransactionIsolation[R, I]]
        evalDBZIO(wti.self, ctx)(wti.addPostProcessing(data))

      case ActionTag.Map =>
        val map = dbzio.asInstanceOf[Map[R, Any, I]]
        evalDBZIO(map.self, ctx)(map.addPostProcessing(data))

      case ActionTag.CollectAll =>
        val colAll = dbzio.asInstanceOf[CollectAll[R, Any, Iterable]]
        data._1(colAll.evalEach(data._2, ctx).asInstanceOf[Result[R, I]])

      case ActionTag.CatchSome =>
        val catchSome = dbzio.asInstanceOf[CatchSome[R, I]]
        evalDBZIO(catchSome.self, ctx)(catchSome.addPostProcessing(data))

      case ActionTag.CatchAll =>
        val catchAll = dbzio.asInstanceOf[CatchAll[R, I]]
        evalDBZIO(catchAll.self, ctx)(catchAll.addPostProcessing(data))
    }
  }

  /** Creates a [[ZIO]] effect, that evaluates `DBZIO` and executes the `Result`, effectively executing the `DBZIO`. */
  private def result[R, T](dbzio: DBZIO[R, T]): DbRIO[R, T] = ZIO.scoped[R with DbDependency] {
    for {
      runtime  <- ZIO.runtime[R]
      executor <- ZIO.executor
      dbio <- waitFor(dbzio, TransactionInformation.empty)(Context.ZIO)(
        runtime,
        executor.asExecutionContext
      )
      res <- dbio.foldTo(
        ZIO.succeed(_),
        d =>
          blocking {
            wrapError {
              ZIO.serviceWithZIO[Database] { db => ZIO.fromFuture { _ => db.run(d) } }
            }
          }
      )
    } yield res
  }

  /** Tries to find [[DBZIOException]] in the provided `cause`, so that the error will be wrapped only once. */
  private def findDBZIOException[E <: Throwable](cause: Cause[E]): Option[Throwable] = {
    cause.failures
      .find {
        case _: DBZIOException[_] => true
        case _                    => false
      }
  }

  /** Creates an effect that wraps the error of the provided one in [[DBZIOException]] (if it is not yet wrapped). */
  private def wrapError[R, T](zio: RIO[R, T]): RIO[R, T] = {
    zio.mapErrorCause { c =>
      findDBZIOException(c)
        .map(_ => c)
        .getOrElse(Cause.fail(DBZIOException(c)))
    }
  }

  /** Wraps provided error in [[DBZIOException]] (if it is not yet wrapped). */
  private def wrapException[E <: Throwable](e: E): DBZIOException[E] = {
    val fail = Cause.fail(e)
    findDBZIOException(Cause.fail(e))
      .map(_.asInstanceOf[DBZIOException[E]])
      .getOrElse(DBZIOException(fail))
  }

}
