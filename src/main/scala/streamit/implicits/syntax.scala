package streamit

import cats.MonadError
import cats.effect.{ ConcurrentEffect, Timer }
import fs2.{ Chunk, Pipe, Pull, Stream }
import io.circe.parser.parse
import io.circe.{ Json, JsonNumber, JsonObject }
import streamit.util.CirceOpticsParser

import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions

package object syntax {

  implicit class StringToJsonOps(val str: String) extends AnyVal {
    def asJson: Json = parse(str) match {
      case Left(parseFail) =>
        // help circe be more helpful
        throw parseFail copy (message = s"""Parse error '${parseFail.message}' for string: "$str"""")
      case Right(expectedJson) => expectedJson
    }

    def asJsonF[F[_]](implicit F: MonadError[F, Throwable]): F[Json] = parse(str) match {
      case Left(parseFail) =>
        // help circe be more helpful
        F.raiseError(
          parseFail copy (message = s"""Parse error '${parseFail.message}' for string: "$str"""")
        )
      case Right(expectedJson) =>
        F.pure(expectedJson)
    }
  }

  implicit class JsonOps(val j: Json) {
    private def canonicalJsonObject(j: JsonObject): JsonObject =
      JsonObject.fromIterable(j.toVector.sortBy(_._1).map { case (k, j) => k -> canonicalJson_(j) })

    private def canonicalJson_(j: Json): Json = j.fold[Json](
      jsonNull = Json.Null,
      jsonBoolean = (x: Boolean) => Json.fromBoolean(x),
      jsonNumber = (x: JsonNumber) => Json.fromJsonNumber(x),
      jsonString = (x: String) => Json.fromString(x),
      jsonArray = (x: Vector[Json]) => Json.fromValues(x.sortBy(_.noSpaces).map(canonicalJson_)),
      jsonObject = (x: JsonObject) => Json.fromJsonObject(canonicalJsonObject(x))
    )

    def canonicalJson: Json = canonicalJson_(j)
  }

  implicit class CirceOpticsDynamicOps(val json: Json) extends AnyVal {
    def valueAtPath[V](path: JsonPathExpr)(implicit parser: CirceOpticsParser[V]): Option[V] =
      parser.parse(json, path)
  }

  implicit class StreamOps[F[_], A](stream: Stream[F, A]) {

    /**
      * Runs the stream for at most `timeout` and short-circuits if `f()` is satisfied.
      */
    def runForAtMost(
      f: A => Boolean,
      timeout: FiniteDuration
    )(implicit T: Timer[F], F: ConcurrentEffect[F]): Stream[F, A] = {
      def p: Pipe[F, A, A] = {
        def go(chunk: Chunk[A], s: Stream[F, A]): Pull[F, A, Unit] =
          s.pull.uncons.flatMap {
            case Some((hd, tl)) if hd.isEmpty =>
              go(chunk, tl)
            case Some((hd, tl)) =>
              hd.find(f)
                .fold {
                  val ns = Chunk.concat(Seq(chunk, hd))
                  go(ns, tl)
                }(a => Pull.output1(a) >> Pull.done)
            case None =>
              if (chunk.isEmpty) Pull.done
              else Pull.output(chunk) >> Pull.done
          }

        in =>
          go(Chunk.empty, in).stream
      }

      stream.interruptWhen(Stream.emit(true).delayBy[F](timeout)).through(p)
    }
  }
}
