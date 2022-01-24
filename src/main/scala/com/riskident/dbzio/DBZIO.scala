package com.riskident.dbzio

import com.riskident.dbzio.Aliases._
import shapeless.{::, =:!=, HList, HNil, Poly2}
import shapeless.ops.hlist._
import shapeless.syntax._
import slick.dbio._
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.JdbcProfile
import zio._
import zio.blocking.blocking

import scala.annotation.tailrec
import scala.collection.generic.CanBuild
import scala.concurrent.ExecutionContext
import scala.math.Ordered.orderingToOrdered
import scala.reflect.ClassTag
import scala.util.Success
import DBZIO._
import shapeless.PolyDefns.Case1
import shapeless.ops.hlist.LeftFolder.Aux

import scala.language.higherKinds

/**
  * Stack-unsafe monadic type to bring together [[DBIO]] and [[ZIO]]. Wrapping either of them into `DBZIO` allows to use them
  * together in one for-comprehension, while preserving DB-specific functionality for managing db-transactions and db-connections.
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
sealed abstract class DBZIO[-R, +T](private[DBZIO] val tag: ActionTag) {

  /** Returns a `DBZIO` whose success is mapped by the specified f function. */
  def map[Q](f: T => Q): DBZIO[R, Q] = new Map(this, f)

  /**
    * Returns a `DBZIO` that models the execution of this effect, followed by the passing of its value
    * to the specified continuation function `f`, followed by the effect that it returns.
    */
  def flatMap[Q, R1](f: T => DBZIO[R1, Q]): DBZIO[R1 with R, Q] = new FlatMap(this, f)

  /**
    * Returns a `DBZIO` that first executes the outer `DBZIO`, and then executes the inner `DBZIO`,
    * returning the value from the inner `DBZIO`, and effectively flattening a nested `DBZIO`.
    */
  def flatten[S, Q](implicit ev: T <:< DBZIO[S, Q]): DBZIO[S with R, Q] = flatMap(ev(_))

  /** Returns a `DBZIO` with its error mapped using the specified function. */
  def mapError(f: Throwable => Throwable): DBZIO[R, T] = new MapError(this, f)

  /**
    * Creates a composite `DBZIO` that represents this `DBZIO` followed by another
    * one that may depend on the error produced by this one.
    */
  def flatMapError[R1](f: Throwable => DBZIO[R1, Throwable]): DBZIO[R with R1, T] = new FlatMapError(this, f)

  /** Runs the specified `DBZIO` if this `DBZIO` fails, providing the error to the `DBZIO` if it exists. */
  def onError[R1 <: R](cleanup: Cause[Throwable] => DBZIO[R1, Any]): DBZIO[R1, T] = new OnError(this, cleanup)

  /** Recovers from some or all of the error cases. */
  def catchSome[R1, T2 >: T](pf: PartialFunction[Throwable, DBZIO[R1, T2]]): DBZIO[R with R1, T2] =
    new CatchSome(this, pf)

  /** Recovers from all errors. */
  def catchAll[R1, T2 >: T](f: Throwable => DBZIO[R1, T2]): DBZIO[R with R1, T2] = new CatchAll(this, f)

  /**
    * Folds over the failure value or the success value to yield a `DBZIO` that
    * does not fail, but succeeds with the value returned by the left or right
    * function passed to `fold`.
    */
  def fold[B](failure: Throwable => B, success: T => B): DBZIO[R, B] =
    new FoldM[R, T, Any, B](this, failure.andThen(DBZIO.success(_)), success.andThen(DBZIO.success(_)))

  /**
    * Recovers from errors by accepting one `DBZIO` to execute for the private of an
    * error, and one `DBZIO` to execute for the private of success.
    */
  def foldM[R1, B](
      failure: Throwable => DBZIO[R1, B],
      success: T => DBZIO[R1, B]
  ): DBZIO[R with R1, B] = new FoldM(this, failure, success)

  /**
    * Executes this `DBZIO` and returns its value, if the value is defined (given that `T` is `Option[K]`),
    * and executes the specified `DBZIO` if the result value is `None`.
    */
  def orElseOption[R1, K, Q >: T](
      other: DBZIO[R1, Q]
  )(implicit ev: T <:< Option[K]): DBZIO[R with R1, Q] = {
    this.flatMap(_.fold(other)(t => DBZIO.success(Some(t).asInstanceOf[Q])))
  }

  /** Creates `DBZIO` that executes `other` in private current fails. */
  def orElse[R1, Q >: T](other: DBZIO[R1, Q]): DBZIO[R with R1, Q] = {
    foldM(_ => other, t => DBZIO.success(t.asInstanceOf[Q]))
  }

  /** Returns a `DBZIO` that effectfully "peeks" at the success of this `DBZIO`. */
  def tapM[R1](f: T => DBZIO[R1, Any]): DBZIO[R1 with R, T] = flatMap(t => f(t).map(_ => t))

  /** Returns the `DBIZO` resulting from mapping the success of this `DBIZO` to unit. */
  def unit: DBZIO[R, Unit] = map(_ => ())

  /** Returns `DBZIO` that will execute current `DBZIO` if and only if `cond` is `true`. */
  def when(cond: => Boolean): DBZIO[R, Unit] = DBZIO.ifF(cond, onTrue = this.unit, onFalse = DBZIO.unit)

  /**
    * Returns a `DBZIO` which executes current `DBZIO` with pinned db-session, if it makes sense (if current `DBZIO`
    * contains db-action). Otherwise, current `DBZIO` is left unaffected.
    *
    * Behaves exactly as [[DBIOAction.withPinnedSession]]
    */
  def withPinnedSession: DBZIO[R, T] = new WithPinnedSession(this)

  /**
    * Returns new `DBZIO`, which executes current `DBZIO` in db-transaction,
    * if it makes sense (if current `DBZIO` contains db-action).
    * Otherwise current `DBZIO` is left unaffected
    *
    * Behaves exactly as [[slick.jdbc.JdbcActionComponent.JdbcActionExtensionMethods.transactionally]].
    */
  def transactionally(implicit profile: JdbcProfile): DBZIO[R, T] = new Transactionally(this, profile)

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
  def withTransactionIsolation(ti: TransactionIsolation)(implicit profile: JdbcProfile): DBZIO[R, T] =
    new WithTransactionIsolation(this, ti, profile)

  /**
    * Creates the [[ZIO]] effect that executes current 'DBZIO'.
    * @return [[ZIO]] with [[Database]], [[zio.blocking.Blocking]] and `R` dependencies.
    */
  def result: DbRIO[R, T] = DBZIO.result(this)

}

object DBZIO {

  /** Creates `DBZIO` that wraps a pure value. */
  def success[A](v: => A): DBZIO[Any, A] = new PureValue[A](() => v)

  /** Creates `DBZIO` that wraps [[DBIO]]. */
  def apply[T](action: => DBIO[T]): DBZIO[Any, T] = new PureDBIO(() => action)

  /** Creates `DBZIO` that wraps a chain (usually a for-comprehension) of [[DBIO]]. */
  def apply[T](action: ExecutionContext => DBIO[T]): DBZIO[Any, T] = new DBIOChain(action)

  /** Creates `DBZIO` that wraps [[ZIO]] that returns a pure value. */
  def apply[R, E <: Throwable, T](action: ZIO[R, E, T])(implicit ev: T =:!= DBIO[_]): DBZIO[R, T] = new PureZio(action)

  /** Creates `DBZIO` that wraps [[ZIO]] that produces db-action ([[DBIO]]). */
  def apply[R, E <: Throwable, T](action: ZIO[R, E, DBIO[T]]): DBZIO[R, T] =
    new ZioOverDBIO(action)

  /** Creates `DBZIO` that fails with provided error. */
  def fail[E <: Throwable, T](error: => E): DBZIO[Any, T] =
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
  def ifM[R, T](cond: DBZIO[R, Boolean], onTrue: DBZIO[R, T], onFalse: DBZIO[R, T]): DBZIO[R, T] = {
    cond.flatMap(DBZIO.ifF(_, onTrue, onFalse))
  }

  /** `DBZIO` that does nothing and returns [[Unit]]. */
  val unit: DBZIO[Any, Unit] = success(())

  /**
    * Creates `DBZIO` that executes a batch of `DBZIO`, returning collection of results from each one of them.
    *
    * @note Behaves as [[DBIO.seq]], meaning that first error fails the whole batch.
    */
  def collectAll[R, T, C[+Element] <: Iterable[Element]](
      col: C[DBZIO[R, T]]
  )(implicit ev1: CanBuild[T, C[T]]): DBZIO[R, C[T]] = {
    new CollectAll(col)
  }

  /**
    * Creates `DBZIO` that executes a batch of `DBZIO`, returning [[scala.collection.immutable.Seq]] of results from
    * each one of them.
    *
    * @note Behaves as [[DBIO.seq]], meaning that first error fails the whole batch.
    */
  def collectAll[R, T](col: DBZIO[R, T]*): DBZIO[R, Seq[T]] = collectAll[R, T, Seq](col)

  /**
    * Creates `DBZIO` that executes a [[scala.collection.immutable.Set]] of `DBZIO`,
    * returning [[scala.collection.immutable.Set]] of results from each one of them.
    *
    * @note Behaves as [[DBIO.seq]], meaning that first error fails the whole batch.
    */
  def collectAll[R, T](col: Set[DBZIO[R, T]]): DBZIO[R, Set[T]] = {
    collectAll[R, T, Iterable](col).map(_.toSet)
  }

  /**
    * Creates `DBZIO` that executes an [[Array]] of `DBZIO`, returning [[Array]] of results from each one of them.
    *
    * @note Behaves as [[DBIO.seq]], meaning that first error fails the whole batch.
    */
  def collectAll[R, T: ClassTag](col: Array[DBZIO[R, T]]): DBZIO[R, Array[T]] = {
    collectAll[R, T, Iterable](col).map(_.toArray)
  }

  /**
    * Creates `DBZIO` that executes [[Option]] of `DBZIO`. The produced result will be either [[Some]] of the result of
    * executed `DBZIO`, if it was defined, or [[None]] otherwise.
    */
  def collectAll[R, T](col: Option[DBZIO[R, T]]): DBZIO[R, Option[T]] = {
    col.map(_.map(Some(_))).getOrElse(DBZIO.success(None))
  }

  /** Represents a failed `DBZIO`. */
  final private class Failure[E <: Throwable](val error: () => DBZIOException[E])
      extends DBZIO[Any, Nothing](ActionTag.Failure)

  /** Represents a `DBZIO` producing pure value. */
  final private class PureValue[+T](val value: () => T) extends DBZIO[Any, T](ActionTag.PureValue)

  /** Represents a `DBZIO` wrapped around [[ZIO]], that produces pure value (not [[DBIO]]). */
  final private class PureZio[-R, E <: Throwable, +T](val action: ZIO[R, E, T]) extends DBZIO[R, T](ActionTag.PureZio)

  /** Represents a `DBZIO` wrapped around single db-action ([[DBIO]]). */
  final private class PureDBIO[+T](val action: () => DBIO[T]) extends DBZIO[Any, T](ActionTag.PureDBIO)

  /** Represents a `DBZIO` wrapped around a chain of [[DBIO]] operations. Usually a for-comprehension. */
  final private class DBIOChain[+T](val action: ExecutionContext => DBIO[T]) extends DBZIO[Any, T](ActionTag.DBIOChain)

  /** Represents a `DBZIO` wrapped around [[ZIO]] effect, that produces a db-action ([[DBIO]]). */
  final private class ZioOverDBIO[-R, +T](val action: RIO[R, DBIO[T]]) extends DBZIO[R, T](ActionTag.ZioOverDBIO)

  /** Represents a [[DBZIO.flatMap]] operation. */
  final private class FlatMap[-R1, -R2, A, B](val self: DBZIO[R1, A], next: A => DBZIO[R2, B])
      extends DBZIO[R1 with R2, B](ActionTag.FlatMap) {
    def addProcessing[R <: R1 with R2, Res, C <: Context](
        processor: ResultProcessor.Aux[R, Res, B],
        ctx: C,
        ti: TransactionInformation
    )(implicit ec: ExecutionContext, runtime: Runtime[R]): ResultProcessor.Aux[R, Res, A] = {
      processor.add[A](_.flatMap { t => evalDBZIO[R, B, C, B](next(t), ti, ctx)(ResultProcessor.empty[R, B]) })
    }
  }

  /** Represents a [[DBZIO.mapError]] operation. */
  final private class MapError[R, T](val self: DBZIO[R, T], errorMap: Throwable => Throwable)
      extends DBZIO[R, T](ActionTag.MapError) {
    def addProcessor[Res](
        processor: ResultProcessor.Aux[R, Res, T]
    )(implicit ec: ExecutionContext): ResultProcessor.Aux[R, Res, T] = {
      processor.add[T](_.mapError(errorMap))
    }
  }

  /** Represents a [[DBZIO.flatMapError]] operation. */
  final private class FlatMapError[R, R1, T](val self: DBZIO[R, T], errorMap: Throwable => DBZIO[R1, Throwable])
      extends DBZIO[R with R1, T](ActionTag.FlatMapError) {
    def addProcessor[R0 <: R with R1, Res, C <: Context](
        processor: ResultProcessor.Aux[R0, Res, T],
        ti: TransactionInformation,
        ctx: C
    )(implicit ec: ExecutionContext, runtime: Runtime[R0]): ResultProcessor.Aux[R0, Res, T] = {
      processor.add[T](
        _.flatMapError(e =>
          evalDBZIO[R0, Throwable, C, Throwable](errorMap(e), ti, ctx)(ResultProcessor.empty[R0, Throwable])
        )
      )
    }
  }

  /** Represents a [[DBZIO.onError]] operation. */
  final private class OnError[R, R1 >: R, T](val self: DBZIO[R1, T], errorMap: Cause[Throwable] => DBZIO[R, Any])
      extends DBZIO[R, T](ActionTag.OnError) {
    def addProcessor[Res, C <: Context](
        processor: ResultProcessor.Aux[R, Res, T],
        ti: TransactionInformation,
        ctx: C
    )(implicit ec: ExecutionContext, runtime: Runtime[R]): ResultProcessor.Aux[R, Res, T] = {
      processor.add[T](
        _.onError(e => evalDBZIO[R, Any, C, Any](errorMap(Cause.fail(e)), ti, ctx)(ResultProcessor.empty[R, Any]))
      )
    }
  }

  /** Represents a [[DBZIO.foldM]] operation. */
  final private class FoldM[R, T, R1, B](
      val self: DBZIO[R, T],
      failure: Throwable => DBZIO[R1, B],
      success: T => DBZIO[R1, B]
  ) extends DBZIO[R with R1, B](ActionTag.FoldM) {
    def addProcessing[R0 <: R1 with R, Res, C <: Context](
        processor: ResultProcessor.Aux[R0, Res, B],
        ti: TransactionInformation,
        ctx: C
    )(implicit ec: ExecutionContext, runtime: Runtime[R0]): ResultProcessor.Aux[R0, Res, T] = {
      processor.add[T](
        _.foldM(
          s => evalDBZIO[R0, B, C, B](success(s), ti, ctx)(ResultProcessor.empty[R0, B]),
          f => evalDBZIO[R0, B, C, B](failure(f), ti, ctx)(ResultProcessor.empty[R0, B])
        )
      )
    }
  }

  /** Represents a [[DBZIO.withPinnedSession]] operation. */
  final private class WithPinnedSession[R, T](val self: DBZIO[R, T]) extends DBZIO[R, T](ActionTag.WithPinnedSession) {
    def addProcessing[R0 <: R, Res](
        processor: ResultProcessor.Aux[R0, Res, T],
        ti: TransactionInformation
    ): ResultProcessor.Aux[R0, Res, T] = {
      ti match {
        case TransactionInformation(_, _, true)  => processor
        case TransactionInformation(_, _, false) => processor.add[T](_.mapDBIO(_.withPinnedSession))
      }
    }
  }

  /** Represents a [[DBZIO.transactionally]] operation. */
  final private class Transactionally[R, T](val self: DBZIO[R, T], profile: JdbcProfile)
      extends DBZIO[R, T](ActionTag.Transactionally) {
    def addProcessing[R0 <: R, Res](
        processor: ResultProcessor.Aux[R0, Res, T],
        ti: TransactionInformation
    ): ResultProcessor.Aux[R0, Res, T] = {
      ti match {
        case TransactionInformation(true, _, _) => processor
        case TransactionInformation(false, _, _) =>
          import profile.api._
          processor.add[T](_.mapDBIO(_.transactionally))
      }
    }
  }

  /** Represents a [[DBZIO.catchSome]] operation. */
  final private class CatchSome[R, T](val self: DBZIO[R, T], pf: PartialFunction[Throwable, DBZIO[R, T]])
      extends DBZIO[R, T](ActionTag.CatchSome) {
    def addProcessor[R0 <: R, Res, C <: Context](
        processor: ResultProcessor.Aux[R, Res, T],
        ti: TransactionInformation,
        ctx: C
    )(implicit ec: ExecutionContext, r: Runtime[R0]): ResultProcessor.Aux[R, Res, T] = {
      processor.add[T](_.catchSome {
        case e: Throwable if pf.isDefinedAt(e) => evalDBZIO[R, T, C, T](pf(e), ti, ctx)(ResultProcessor.empty[R, T])
      })
    }
  }

  /** Represents a [[DBZIO.catchAll]] operation. */
  final private class CatchAll[R, T](val self: DBZIO[R, T], f: Throwable => DBZIO[R, T])
      extends DBZIO[R, T](ActionTag.CatchAll) {
    def addProcessor[R0 <: R, Res, C <: Context](
        processor: ResultProcessor.Aux[R0, Res, T],
        ti: TransactionInformation,
        ctx: C
    )(implicit ec: ExecutionContext, r: Runtime[R0]): ResultProcessor.Aux[R0, Res, T] = {
      processor.add[T](_.catchAll(e => evalDBZIO[R, T, C, T](f(e), ti, ctx)(ResultProcessor.empty[R, T])))
    }
  }

  /**
    * Represents a [[DBZIO collectAll]] operation. Also contains some methods for collection manipulations, since it
    * encapsulates collection type information.
    *
    * Behaves exactly as [[DBIO.seq]], in a sense that first error fails the whole batch.
    */
  final private class CollectAll[R, T, C[+Element] <: Iterable[Element]](val col: C[DBZIO[R, T]])(
      implicit ev1: CanBuild[T, C[T]]
  ) extends DBZIO[R, C[T]](ActionTag.CollectAll) {

    /**
      * Structure to collect different types of [[Result]] from each element of the batch.
      *
      * @param item   counter, to remember the place for the result
      * @param values collection of pure values
      * @param dbios  collection of [[DBIO]]
      * @param zios   collection of [[ZIO]]
      * @param errors collection of errors (only first one will be visible)
      */
    private case class Collector(
        item: Int,
        values: Seq[(Int, T)],
        dbios: Seq[(Int, DBIO[T])],
        zios: Seq[(Int, Result.Zio[R, T])],
        errors: Seq[Result.Error]
    ) {

      /** Creates new `Collector` instance with `res` added to an appropriate collection. */
      def add(res: Result[R, T]): Collector = {
        if (res.isError) {
          copy(errors = errors :+ res.asInstanceOf[Result.Error], item = item + 1)
        } else if (res.isPure) {
          copy(values = values :+ (item, res.pureValue.get(())), item = item + 1)
        } else if (res.isDbio) {
          copy(dbios = dbios :+ (item, res.dbio.get(())), item = item + 1)
        } else {
          copy(zios = zios :+ (item, res.asInstanceOf[Result.Zio[R, T]]), item = item + 1)
        }
      }
    }

    private val empty: Collector = Collector(0, Seq.empty, Seq.empty, Seq.empty, Seq.empty)

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
        Result.pure(ev1().result())
      } else {
        def addResult(col: Collector, res: DBZIO[R, T]): Collector = {
          col.add(evalDBZIO[R, T, Ctx, T](res, ti, ctx)(ResultProcessor.empty[R, T]))
        }

        val collector = col.foldLeft(empty)(addResult)

        val res: Result[R, Seq[(Int, T)]] = if (collector.errors.nonEmpty) {
          val reduced = collector.errors.map(_.error.get(()).cause).reduce(_ && _)
          Result.error(DBZIOException(reduced))
        } else if (collector.zios.nonEmpty) {
          Result.zio {
            ZIO
              .foreach(collector.zios) {
                case (n, z) => z.zio.get.map(n -> _)
              }
              .map { r =>
                val (v, d) = r.foldLeft((Seq.empty[(Int, T)], Seq.empty[(Int, DBIO[T])])) {
                  case ((tx, dbios), el) =>
                    el._2.foldTo(x => (tx :+ (el._1, x), dbios), x => (tx, dbios :+ (el._1, x)))
                }

                val dbios = d ++ collector.dbios

                if (dbios.nonEmpty) {
                  ZioResult(
                    DBIO
                      .sequence(dbios.map {
                        case (n, d) => d.map(n -> _)
                      })
                      .map(_ ++ v ++ collector.values)
                  )
                } else {
                  ZioResult(v ++ collector.values)
                }
              }
          }
        } else if (collector.dbios.nonEmpty) {
          Result.dbio(
            DBIO
              .sequence(collector.dbios.map {
                case (n, d) => d.map(n -> _)
              })
              .map(_ ++ collector.values)
          )
        } else {
          Result.pure(collector.values)
        }

        res.map { _.sortBy(_._1).foldLeft(ev1())(_ += _._2).result() }
      }
    }
  }

  /** Represents [[DBZIO.withTransactionIsolation]] operation. */
  final private class WithTransactionIsolation[R, T](
      val self: DBZIO[R, T],
      isolation: TransactionIsolation,
      profile: JdbcProfile
  ) extends DBZIO[R, T](ActionTag.WithTransactionIsolation) {
    def addProcessor[R0 <: R, Res](
        processor: ResultProcessor.Aux[R0, Res, T],
        ti: TransactionInformation
    ): (ResultProcessor.Aux[R0, Res, T], TransactionInformation) = {

      def withIsolation: (ResultProcessor.Aux[R0, Res, T], TransactionInformation) = {
        import profile.api._
        val newTi        = ti.copy(isolation = Some(isolation))
        val newProcessor = processor.add[T](_.mapDBIO(_.withTransactionIsolation(isolation)))
        newProcessor -> newTi
      }

      def self: (ResultProcessor.Aux[R0, Res, T], TransactionInformation) = processor -> ti

      ti match {
        case TransactionInformation(true, _, _)                      => self
        case TransactionInformation(_, Some(x), _) if x >= isolation => self
        case TransactionInformation(_, Some(x), _) if x < isolation  => withIsolation
        case TransactionInformation(_, None, _)                      => withIsolation
      }

    }
  }

  /** Represents [[DBZIO.map]] operation. */
  final private class Map[R, T, Q](val self: DBZIO[R, T], val f: T => Q) extends DBZIO[R, Q](ActionTag.Map) {
    def addProcessing[Res](
        processor: ResultProcessor.Aux[R, Res, Q]
    )(implicit ec: ExecutionContext): ResultProcessor.Aux[R, Res, T] = {
      processor.add[T](_.map(f))
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
  sealed private trait ZioResult[+T] {

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

  private object ZioResult {

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
    def apply[T](value: T)(implicit ev: T =:!= DBIO[_]): ZioResult[T] = Value(value)

    /** Creates [[DBIO]] `ZioResult`. */
    def apply[T](dbio: DBIO[T]): ZioResult[T] = Query(dbio)
  }

  /**
    * Monadic-like type representing result of evaluation of `DBZIO`.
    *
    * The result may be one of the following:
    *  - pure value of type `T`
    *  - `DBIO[T]`
    *  - `ZIO[R, Throwable, ZioResult[T]]`
    *  - `DBZIOException` with an error
    *  - One of the operations on the `Result`
    *
    * @tparam R [[ZIO]] dependency
    * @tparam T type of the result
    */
  sealed private trait Result[-R, +T] {
    val pureValue: Option[Any => T]                     = None
    val dbio: Option[Any => DBIO[T]]                    = None
    val zio: Option[RIO[R, ZioResult[T]]]               = None
    val error: Option[Any => DBZIOException[Throwable]] = None

    val isPure: Boolean  = false
    val isDbio: Boolean  = false
    val isZio: Boolean   = false
    val isError: Boolean = false

    /**
      * Applies transformation on containing value (based on the value type). All operations on `Result` are expressed
      * either through [[Result.transformAll]] or [[Result.transformAllM]].
      *
      * @note Should be called only on one of the 4 final results:
      *
      * - [[Result.Error]]
      *
      * - [[Result.PureResult]]
      *
      * - [[Result.PureDbio]]
      *
      * - [[Result.Zio]]
      */
    protected def transformAll[R2 <: R, T2](
        onError: DBZIOException[Throwable] => DBZIOException[Throwable],
        onZio: RIO[R, ZioResult[T]] => RIO[R2, ZioResult[T2]],
        onDbio: DBIO[T] => DBIO[T2],
        onPure: T => T2
    ): Result[R2, T2]

    /**
      * Chains another `Result` based on the value type. All operations on `Result` are expressed
      * either through [[Result.transformAll]] or [[Result.transformAllM]].
      *
      * @note Should be called only on one of the 4 final results:
      *
      * - [[Result.Error]]
      *
      * - [[Result.PureResult]]
      *
      * - [[Result.PureDbio]]
      *
      * - [[Result.Zio]]
      */
    protected def transformAllM[R2 <: R, T2](
        onError: DBZIOException[Throwable] => Result[R2, T2],
        onZio: RIO[R, ZioResult[T]] => Result[R2, T2],
        onDbio: DBIO[T] => Result[R2, T2],
        onPure: T => Result[R2, T2]
    ): Result[R2, T2]

    /** Creates `Result` that transforms final pure value of the current. */
    def map[T2](f: T => T2)(implicit ec: ExecutionContext): Result[R, T2] = transformAll(
      onError = identity,
      onZio = _.map(_.map(f)),
      onDbio = _.map(f),
      onPure = f(_)
    )

    /** Creates `Result` that chains current and the next one return by `f`.  */
    def flatMap[T2, R1 <: R](f: T => Result[R1, T2])(implicit ec: ExecutionContext, r: Runtime[R1]): Result[R1, T2] = {
      val mapDBIO: DBIO[T] => DBIO[T2] = _.flatMap(t => waitFor(f(t))(Context.DBIO))

      transformAllM(
        onPure = f,
        onError = _ => asInstanceOf[Result[Any, Nothing]],
        onDbio = dbio => Result.dbio(mapDBIO(dbio)),
        onZio = z =>
          Result.zio {
            for {
              t <- z
              res <- t.foldTo(
                left = x => waitFor(f(x))(Context.ZIO),
                right = d => UIO(ZioResult(mapDBIO(d)))
              )
            } yield res
          }
      )
    }

    /** Creates `Result` that transforms an error (if current is [[Result.Error]] or execution of containing value
      * will produce an error).
      */
    def mapError(f: Throwable => Throwable)(implicit ec: ExecutionContext): Result[R, T] = {
      val dbioTransform: DBIO[T] => DBIO[T] = _.asTry.flatMap {
        case util.Failure(exception) => DBIO.failed(f(exception))
        case Success(value)          => DBIO.successful(value)
      }

      transformAll(
        onError = x => wrapException(f(x.cause.squash)),
        onPure = identity,
        onDbio = dbioTransform,
        onZio = _.mapBoth(f, _.mapDBIO(dbioTransform))
      )
    }

    /** Creates `Result` that transforms an error (if current is [[Result.Error]] or execution of containing value
      * will produce an error) to a new `Result` and then executes it.
      */
    def flatMapError[R2 <: R](
        f: Throwable => Result[R2, Throwable]
    )(implicit ec: ExecutionContext, r: Runtime[R2]): Result[R2, T] = {
      val mapDBIO: DBIO[T] => DBIO[T] = _.asTry.flatMap {
        case util.Failure(exception) =>
          waitFor(f(exception))(Context.DBIO).flatMap(DBIO.failed)
        case Success(value) => DBIO.successful(value)
      }

      val onFailureZ: Throwable => RIO[R2, ZioResult[T]] = a =>
        waitFor(f(a))(Context.ZIO).flatMap {
          _.foldTo(Task.fail(_), dbio => UIO(ZioResult(dbio.flatMap(DBIO.failed))))
        }

      transformAllM(
        onPure = _ => this,
        onError = e => f(e.cause.squash).flatMap(t => Result.error(wrapException(t))),
        onDbio = d => Result.dbio(mapDBIO(d)),
        onZio = z =>
          Result.zio {
            z.foldM(onFailureZ, r => UIO(r.mapDBIO(mapDBIO)))
          }
      )
    }

    /** Creates `Result` with cleanup action, which will be executed if execution of the current one fails. */
    def onError[R2 <: R](
        f: Throwable => Result[R2, Any]
    )(implicit ec: ExecutionContext, r: Runtime[R2]): Result[R2, T] = {
      val doFail: Throwable => DBIO[T] = e => DBIO.failed(wrapException(e))

      val mapDBIO: DBIO[T] => DBIO[T] = _.asTry.flatMap {
        case util.Failure(exception) =>
          waitFor(f(exception))(Context.DBIO)
            .flatMap(_ => doFail(exception))
        case Success(value) => DBIO.successful(value)
      }

      val onFailureZ: Throwable => RIO[R2, ZioResult[T]] = a => {
        val nextZio = waitFor(f(a))(Context.ZIO)
        nextZio.map(_.foldTo(_ => doFail(a), _.flatMap(_ => doFail(a)))).map(ZioResult[T])
      }

      val self = this
      transformAllM(
        onError = e => f(e.cause.squash).flatMap(_ => self),
        onPure = _ => self,
        onZio = z =>
          Result.zio {
            z.foldM(onFailureZ, b => UIO(b.mapDBIO(mapDBIO)))
          },
        onDbio = d => Result.dbio(mapDBIO(d))
      )
    }

    /** Creates `Result` that will execute current one and then will execute `f` or `e` base of success or failure of
      *  the current one.
      */
    def foldM[T2, R2 <: R](f: T => Result[R2, T2], e: Throwable => Result[R2, T2])(
        implicit ec: ExecutionContext,
        r: Runtime[R2]
    ): Result[R2, T2] = {
      val foldDBIO: DBIO[T] => DBIO[T2] = _.asTry.flatMap {
        case util.Failure(exception) => waitFor(e(exception))(Context.DBIO)
        case Success(value)          => waitFor(f(value))(Context.DBIO)
      }

      transformAllM(
        onError = x => e(x.cause.squash),
        onPure = f,
        onZio = z =>
          Result.zio(
            z.either.flatMap(
              _.fold(
                x => waitFor(e(x))(Context.ZIO),
                t => {
                  t.foldTo(
                    t => waitFor(f(t))(Context.ZIO),
                    x => UIO(ZioResult(foldDBIO(x)))
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
        onError = identity,
        onPure = identity,
        onDbio = f,
        onZio = _.map(_.mapDBIO(f))
      )
    }

    /** Creates `Result` with fallback for some of the errors that might arise during execution of the current one. */
    def catchSome[R2 <: R, T2 >: T](
        f: PartialFunction[Throwable, Result[R2, T2]]
    )(implicit ec: ExecutionContext, r: Runtime[R2]): Result[R2, T2] = {
      val mapDBIO: DBIO[T2] => DBIO[T2] = _.asTry.flatMap {
        case util.Failure(exception) if f.isDefinedAt(exception) =>
          waitFor(f(exception))(Context.DBIO)
        case util.Failure(exception) => DBIO.failed(exception)
        case Success(value)          => DBIO.successful(value)
      }

      transformAllM(
        onPure = _ => this,
        onError = x => f.lift(x).getOrElse(this),
        onZio = z =>
          Result.zio {
            z.catchSome(f.andThen(waitFor(_)(Context.ZIO)))
              .map(_.mapDBIO(mapDBIO))
          },
        onDbio = x => Result.dbio(mapDBIO(x))
      )
    }

    /** Creates `Result` with fallback for all of the errors that might arise during execution of the current one. */
    def catchAll[T2 >: T, R2 <: R](
        f: Throwable => Result[R2, T2]
    )(implicit ec: ExecutionContext, r: Runtime[R2]): Result[R2, T2] = {
      val mapDBIO: DBIO[T2] => DBIO[T2] = _.asTry.flatMap {
        case util.Failure(exception) => waitFor(f(exception))(Context.DBIO)
        case Success(value)          => DBIO.successful(value)
      }
      transformAllM(
        onPure = _ => this,
        onError = x => f(x.cause.squash),
        onDbio = x => Result.dbio(mapDBIO(x)),
        onZio = z =>
          Result.zio {
            z.catchAll(e => waitFor(f(e))(Context.ZIO))
              .map(_.mapDBIO(mapDBIO))
          }
      )
    }
  }

  private object Result {

    /** Failure of the `DBZIO`. */
    case class Error(value: Any => DBZIOException[Throwable]) extends Result[Any, Nothing] {
      override val error: Option[Any => DBZIOException[Throwable]] = Some(value)
      override val isError: Boolean                                = true

      override protected def transformAll[R2 <: Any, T2](
          onError: DBZIOException[Throwable] => DBZIOException[Throwable],
          onZio: RIO[Any, ZioResult[Nothing]] => RIO[R2, ZioResult[T2]],
          onDbio: DBIO[Nothing] => DBIO[T2],
          onPure: Nothing => T2
      ): Result[R2, T2] = {
        copy(value = _ => onError(value(())))
      }

      override protected def transformAllM[R2 <: Any, T2](
          onError: DBZIOException[Throwable] => Result[R2, T2],
          onZio: RIO[Any, ZioResult[Nothing]] => Result[R2, T2],
          onDbio: DBIO[Nothing] => Result[R2, T2],
          onPure: Nothing => Result[R2, T2]
      ): Result[R2, T2] = {
        onError(value(()))
      }
    }

    /** `Result` containing pure value */
    case class PureResult[T](value: Any => T) extends Result[Any, T] {
      override val pureValue: Option[Any => T] = Some(value)
      override val isPure: Boolean             = true
      override protected def transformAll[R2 <: Any, T2](
          onError: DBZIOException[Throwable] => DBZIOException[Throwable],
          onZio: RIO[Any, ZioResult[T]] => RIO[R2, ZioResult[T2]],
          onDbio: DBIO[T] => DBIO[T2],
          onPure: T => T2
      ): Result[R2, T2] = {
        Result.pure(onPure(value(())))
      }

      override protected def transformAllM[R2 <: Any, T2](
          onError: DBZIOException[Throwable] => Result[R2, T2],
          onZio: RIO[Any, ZioResult[T]] => Result[R2, T2],
          onDbio: DBIO[T] => Result[R2, T2],
          onPure: T => Result[R2, T2]
      ): Result[R2, T2] = {
        onPure(value(()))
      }
    }

    /** `Result` containing [[ZIO]] */
    case class Zio[R, T](value: RIO[R, ZioResult[T]]) extends Result[R, T] {
      override val zio: Option[RIO[R, ZioResult[T]]] = Some(value)
      override val isZio: Boolean                    = true
      override protected def transformAll[R2 <: R, T2](
          onError: DBZIOException[Throwable] => DBZIOException[Throwable],
          onZio: RIO[R, ZioResult[T]] => RIO[R2, ZioResult[T2]],
          onDbio: DBIO[T] => DBIO[T2],
          onPure: T => T2
      ): Result[R2, T2] = {
        Result.zio(onZio(value))
      }

      override protected def transformAllM[R2 <: R, T2](
          onError: DBZIOException[Throwable] => Result[R2, T2],
          onZio: RIO[R, ZioResult[T]] => Result[R2, T2],
          onDbio: DBIO[T] => Result[R2, T2],
          onPure: T => Result[R2, T2]
      ): Result[R2, T2] = {
        onZio(value)
      }
    }

    /** `Result` containing [[DBIO]]. */
    case class PureDbio[T](value: Any => DBIO[T]) extends Result[Any, T] {
      override val dbio: Option[Any => DBIO[T]] = Some(value)
      override val isDbio: Boolean              = true
      override protected def transformAll[R2 <: Any, T2](
          onError: DBZIOException[Throwable] => DBZIOException[Throwable],
          onZio: RIO[Any, ZioResult[T]] => RIO[R2, ZioResult[T2]],
          onDbio: DBIO[T] => DBIO[T2],
          onPure: T => T2
      ): Result[R2, T2] = {
        Result.dbio(onDbio(value(())))
      }

      override protected def transformAllM[R2 <: Any, T2](
          onError: DBZIOException[Throwable] => Result[R2, T2],
          onZio: RIO[Any, ZioResult[T]] => Result[R2, T2],
          onDbio: DBIO[T] => Result[R2, T2],
          onPure: T => Result[R2, T2]
      ): Result[R2, T2] = {
        onDbio(value(()))
      }

    }

    def error(error: => DBZIOException[Throwable]): Result[Any, Nothing] = Error(_ => error)

    def dbio[T](dbio: => DBIO[T]): Result[Any, T] = PureDbio(_ => dbio)

    def zio[R, T](zio: RIO[R, ZioResult[T]])(implicit ev: T =:!= ZIO[_, _, _], ev2: T =:!= DBIO[_]): Result[R, T] =
      Zio(zio)

    def pure[T, R](value: => T): Result[R, T] = PureResult(_ => value)

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

    type Transform[A, B] = Result[R, A] => Result[R, B]

    /** `HList` type of transformation functions (A => B :: B => C :: ... :: S => T :: HNil) */
    type Chain <: HList

    /** Chain of transformation functions */
    protected val transform: Chain

    /** Applicator of the chain of transformation */
    protected val folder: LeftFolder.Aux[Chain, Result[R, Intermediate], Any, Result[R, T]]

    /** Prepends new transformation to the chain of transformations */
    def add[Q](f: Transform[Q, Intermediate]): ResultProcessor.Aux[R, T, Q] = {
      type N = this.Intermediate
      val self: ResultProcessor.Aux[R, T, N] = this
      new ResultProcessor[R, T] {
        override type Intermediate = Q
        override type Next         = N
        override type Chain        = Transform[Intermediate, Next] :: self.Chain

        override protected val folder: Aux[this.Transform[Q, N] :: self.Chain, Result[R, Q], Any, Result[R, T]] =
          new LeftFolder[Transform[Q, N] :: self.Chain, Result[R, Q], Any] {
            type Out = Result[R, T]
            def apply(l: Transform[Q, N] :: self.Chain, in: Result[R, Q]): Out = self.folder(l.tail, f(in))
          }

        override protected val transform: Chain = f :: self.transform
      }
    }

    /** Applies the chain of transformation to the `res` */
    def apply(res: Result[R, Intermediate]): Result[R, T] = {
      folder(transform, res)
    }
  }

  private object ResultProcessor {

    type Aux[R, T, I] = ResultProcessor[R, T] { type Intermediate = I }

    def empty[R, T]: ResultProcessor.Aux[R, T, T] = new ResultProcessor[R, T] {
      override type Intermediate = T
      override type Next         = T
      override type Chain        = HNil
      override protected val transform: Chain = HNil

      override protected val folder: LeftFolder.Aux[HNil, Result[R, T], Any, Result[R, T]] = LeftFolder.hnilLeftFolder

    }
  }

  /** Waits for `result` to produce result in specific context. */
  private def waitFor[R, T, C <: Context](
      result: Result[R, T]
  )(context: C)(implicit runtime: Runtime[R], ec: ExecutionContext): C#F[R, T] = {
    context.waitFor(result)
  }

  /** Waits for `DBZIO` to produce result in specific context. */
  private def waitFor[R, T, C <: Context](
      action: DBZIO[R, T],
      ti: TransactionInformation
  )(context: C)(
      implicit runtime: Runtime[R],
      ec: ExecutionContext
  ): C#F[R, T] = {
    waitFor(evalDBZIO[R, T, C, T](action, ti, context)(ResultProcessor.empty[R, T]))(context)
  }

  /** Context of execution of the `Result`. May be either [[DBZIO.Context.ZIO]] or [[DBZIO.Context.DBIO]]. */
  sealed private trait Context {

    /** Type of the result produced by the specific context. */
    type F[_, _]

    /** Waits for 'result' to produce a value of the [[F]] */
    def waitFor[R, T](result: Result[R, T])(implicit runtime: Runtime[R], ec: ExecutionContext): F[R, T]
    def lift[T](v: () => T): Result[Any, T]
    def error[E <: Throwable, T](e: DBZIOException[E]): Result[Any, T]
  }

  private object Context {

    /** Context inside [[slick.dbio.DBIO]].  */
    object DBIO extends Context {
      override type F[R, T] = slick.dbio.DBIO[T]

      def runZIO[R, T](zio: RIO[R, T])(implicit runtime: Runtime[R], ec: ExecutionContext): DBIO[T] = {
        slick.dbio.DBIO
          .from {
            runtime.unsafeRunToFuture(zio)
          }
      }

      def waitFor[R, T](self: Result[R, T])(implicit runtime: Runtime[R], ec: ExecutionContext): F[R, T] = {
        if (self.isError) {
          slick.dbio.DBIO.failed(self.error.get(()))
        } else if (self.isDbio) {
          self.dbio.get(())
        } else if (self.isPure) {
          slick.dbio.DBIO.successful(self.pureValue.get(()))
        } else {
          runZIO(self.zio.get.map(_.toDBIO)).flatMap(identity)
        }
      }

      override def lift[T](v: () => T): Result[Any, T] = Result.dbio(slick.dbio.DBIO.successful(v()))

      override def error[E <: Throwable, T](e: DBZIOException[E]): Result[Any, T] =
        Result.dbio(slick.dbio.DBIO.failed(e))
    }

    /** Context inside [[zio.ZIO]]. */
    object ZIO extends Context {
      override type F[R, T] = RIO[R, ZioResult[T]]

      def waitFor[R, T](result: Result[R, T])(implicit runtime: Runtime[R], ec: ExecutionContext): F[R, T] = {
        if (result.isError) {
          wrapError(Task.fail(result.error.get(())))
        } else if (result.isPure) {
          UIO(ZioResult.apply(result.pureValue.get(())))
        } else if (result.isZio) {
          wrapError(result.zio.get)
        } else {
          UIO(ZioResult(result.dbio.get(())))
        }
      }

      override def lift[T](v: () => T): Result[Any, T] = Result.zio(UIO(ZioResult(v())))

      override def error[E <: Throwable, T](e: DBZIOException[E]): Result[Any, T] = {
        val zio: RIO[Any, ZioResult[T]] = Task.fail(e)
        Result.zio(zio)
      }
    }
  }

  /** It is allowed only to increase the isolation level in nested `DBZIO`, hence the [[Ordering]]. */
  implicit private val isolationOrd: Ordering[TransactionIsolation] = {
    import slick.jdbc.TransactionIsolation._
    val order: List[TransactionIsolation] = List(ReadUncommitted, ReadCommitted, RepeatableRead, Serializable)
    (x: TransactionIsolation, y: TransactionIsolation) => {
      @tailrec
      def cmp(check: List[TransactionIsolation]): Int = {
        if (check.isEmpty) {
          0
        } else {
          val head = check.head
          if (x == head && y == head) {
            0
          } else if (x == head) {
            -1
          } else if (y == head) {
            1
          } else {
            cmp(check.tail)
          }
        }
      }

      cmp(order)
    }
  }

  /**
    * Given a `DBZIO` evaluates a `Result` of the action.
    *
    * @note Recursive and not stack safe.
    */
  @tailrec
  private def evalDBZIO[R, T, C <: Context, I](
      dbzio: DBZIO[R, I],
      ti: TransactionInformation,
      ctx: C
  )(
      actions: ResultProcessor.Aux[R, T, I]
  )(
      implicit runtime: Runtime[R],
      ec: ExecutionContext
  ): Result[R, T] = {
    dbzio.tag match {
      case ActionTag.Failure =>
        actions(ctx.error[Throwable, I](dbzio.asInstanceOf[Failure[Throwable]].error()))

      case ActionTag.PureValue =>
        actions(ctx.lift(dbzio.asInstanceOf[PureValue[actions.Intermediate]].value))

      case ActionTag.PureZio =>
        val res = Result.zio(
          wrapError(dbzio.asInstanceOf[PureZio[R, Throwable, actions.Intermediate]].action.map(ZioResult(_)))
        )
        actions(res)

      case ActionTag.PureDBIO =>
        actions(Result.dbio(dbzio.asInstanceOf[PureDBIO[actions.Intermediate]].action()))

      case ActionTag.DBIOChain =>
        actions(Result.dbio(dbzio.asInstanceOf[DBIOChain[actions.Intermediate]].action(ec)))

      case ActionTag.ZioOverDBIO =>
        val res =
          Result.zio(wrapError(dbzio.asInstanceOf[ZioOverDBIO[R, actions.Intermediate]].action.map(ZioResult(_))))
        actions(res)

      case ActionTag.FlatMap =>
        val flatMap              = dbzio.asInstanceOf[FlatMap[R, R, Any, I]]
        val first: DBZIO[R, Any] = flatMap.self
        evalDBZIO(first, ti, ctx)(flatMap.addProcessing(actions, ctx, ti))

      case ActionTag.MapError =>
        val mapError = dbzio.asInstanceOf[MapError[R, I]]
        evalDBZIO(mapError.self, ti, ctx)(mapError.addProcessor[T](actions))

      case ActionTag.FlatMapError =>
        val flatMapError = dbzio.asInstanceOf[FlatMapError[R, R, I]]
        evalDBZIO(flatMapError.self, ti, ctx)(flatMapError.addProcessor(actions, ti, ctx))

      case ActionTag.OnError =>
        val onError = dbzio.asInstanceOf[OnError[R, R, I]]
        evalDBZIO(onError.self, ti, ctx)(onError.addProcessor(actions, ti, ctx))

      case ActionTag.FoldM =>
        val fold = dbzio.asInstanceOf[FoldM[R, Any, R, I]]

        evalDBZIO(fold.self, ti, ctx)(fold.addProcessing(actions, ti, ctx))

      case ActionTag.WithPinnedSession =>
        val withSession = dbzio.asInstanceOf[WithPinnedSession[R, I]]
        evalDBZIO(withSession.self, ti.copy(inPinnedSession = true), ctx)(withSession.addProcessing(actions, ti))

      case ActionTag.Transactionally =>
        val tx = dbzio.asInstanceOf[Transactionally[R, I]]
        evalDBZIO(tx.self, ti.copy(inTransaction = true), ctx)(tx.addProcessing(actions, ti))

      case ActionTag.WithTransactionIsolation =>
        val wti                 = dbzio.asInstanceOf[WithTransactionIsolation[R, I]]
        val (newActions, newTi) = wti.addProcessor(actions, ti)
        evalDBZIO(wti.self, newTi, ctx)(newActions)

      case ActionTag.Map =>
        val map = dbzio.asInstanceOf[Map[R, Any, I]]
        evalDBZIO(map.self, ti, ctx)(map.addProcessing(actions))

      case ActionTag.CollectAll =>
        val colAll = dbzio.asInstanceOf[CollectAll[R, Any, Iterable]]
        actions(colAll.evalEach(ti, ctx).asInstanceOf[Result[R, I]])

      case ActionTag.CatchSome =>
        val catchSome = dbzio.asInstanceOf[CatchSome[R, I]]
        evalDBZIO(catchSome.self, ti, ctx)(catchSome.addProcessor(actions, ti, ctx))

      case ActionTag.CatchAll =>
        val catchAll = dbzio.asInstanceOf[CatchAll[R, I]]
        evalDBZIO(catchAll.self, ti, ctx)(catchAll.addProcessor(actions, ti, ctx))
    }
  }

  /** Creates a [[ZIO]] effect, that evaluates `DBZIO` and executes the `Result`, effectively executing the `DBZIO`. */
  private def result[R, T](dbzio: DBZIO[R, T]): DbRIO[R, T] = {
    for {
      runtime  <- ZIO.runtime[R]
      executor <- ZIO.executor
      dbio <- waitFor(dbzio, TransactionInformation.empty)(Context.ZIO)(
        runtime,
        executor.asEC
      )
      res <- dbio.foldTo(
        UIO(_),
        d =>
          blocking {
            wrapError {
              ZIO.serviceWith[Database] { db => Task.fromFuture { _ => db.run(d) } }
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
