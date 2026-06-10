package com.codesolutions.flink.mesh

import spray.json.{DefaultJsonProtocol, RootJsonFormat, deserializationError}

import java.time.Instant

/**
 * JSON formats for the data products. Centralized so producers and
 * consumers (Scala side) share the same serialization rules.
 */
object JsonProtocols extends DefaultJsonProtocol {

  implicit val instantFormat: RootJsonFormat[Instant] = new RootJsonFormat[Instant] {
    def write(i: Instant) = spray.json.JsString(i.toString)
    def read(json: spray.json.JsValue) = json match {
      case spray.json.JsString(s) => Instant.parse(s)
      case other => deserializationError("Instant expected as ISO-8601 string, got " + other)
    }
  }

  implicit val orderFormat: RootJsonFormat[OrderDataProduct] = jsonFormat6(OrderDataProduct.apply _)
  implicit val customerFormat: RootJsonFormat[CustomerDataProduct] = jsonFormat5(CustomerDataProduct.apply _)
  implicit val enrichedFormat: RootJsonFormat[EnrichedOrder] = jsonFormat3(EnrichedOrder.apply _)
}
