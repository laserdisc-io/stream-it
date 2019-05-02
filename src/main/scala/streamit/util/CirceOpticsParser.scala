package streamit.util

import io.circe.optics.JsonPath
import io.circe.optics.JsonPath._
import io.circe.{ Json, JsonNumber, JsonObject }
import streamit.JsonPathExpr

trait CirceOpticsParser[V] {

  /**
    * Locate and return the element at the given JsonPath expression (as understood
    * by https://circe.github.io/circe/optics.html) if found, converting to type V.
    */
  def parse(json: Json, path: JsonPathExpr): Option[V] = {
    val jsonPath = root.selectDynamic(path)
    selectOptional(jsonPath).getOption(json)
  }

  def selectOptional(jp: JsonPath): monocle.Optional[Json, V]
}

object CirceOpticsParser {

  final type JsonPathExpr = String

  // if you need a type that's not here, see what else io.circe.optics.JsonPath offers
  implicit val boolParser: CirceOpticsParser[Boolean]          = (jp: JsonPath) => jp.boolean
  implicit val byteParser: CirceOpticsParser[Byte]             = (jp: JsonPath) => jp.byte
  implicit val shortParser: CirceOpticsParser[Short]           = (jp: JsonPath) => jp.short
  implicit val intParser: CirceOpticsParser[Int]               = (jp: JsonPath) => jp.int
  implicit val longParser: CirceOpticsParser[Long]             = (jp: JsonPath) => jp.long
  implicit val bigintParser: CirceOpticsParser[BigInt]         = (jp: JsonPath) => jp.bigInt
  implicit val doubleParser: CirceOpticsParser[Double]         = (jp: JsonPath) => jp.double
  implicit val bigdecimalParser: CirceOpticsParser[BigDecimal] = (jp: JsonPath) => jp.bigDecimal
  implicit val jsNumberParser: CirceOpticsParser[JsonNumber]   = (jp: JsonPath) => jp.number
  implicit val stringParser: CirceOpticsParser[String]         = (jp: JsonPath) => jp.string
  implicit val jsArrayParser: CirceOpticsParser[Vector[Json]]  = (jp: JsonPath) => jp.arr
  implicit val jsObjecParser: CirceOpticsParser[JsonObject]    = (jp: JsonPath) => jp.obj
}
