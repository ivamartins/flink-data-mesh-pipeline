package com.codesolutions.flink.mesh

import java.time.Instant
import scala.util.Try

/**
 * Pure functions used by the Flink job AND the tests.
 *
 * Keeping them here (no Flink types) means we can unit-test the entire
 * pipeline transformation logic without spinning up a StreamExecutionEnvironment.
 *
 * Pydantic-equivalent validation lives in src/main/python/contracts/.
 * Scala uses these functions for fast-fail; Pydantic enforces at the
 * producer/consumer boundary in Python services.
 */
object EnrichmentLib {

  /** Validate and parse a raw JSON string into an OrderDataProduct. */
  def parseOrder(raw: String): Either[String, OrderDataProduct] = {
    import JsonProtocols.orderFormat
    import spray.json._
    Try(raw.parseJson.convertTo[OrderDataProduct]).toEither.left.map(_.getMessage)
  }

  /** Validate and parse a raw JSON string into a CustomerDataProduct. */
  def parseCustomer(raw: String): Either[String, CustomerDataProduct] = {
    import JsonProtocols.customerFormat
    import spray.json._
    Try(raw.parseJson.convertTo[CustomerDataProduct]).toEither.left.map(_.getMessage)
  }

  /** Validation rules (replicated from Pydantic contracts). */
  def validateOrder(o: OrderDataProduct): Either[String, OrderDataProduct] = {
    if (o.amount <= 0) Left(s"amount must be > 0, got ${o.amount}")
    else if (o.currency == null || o.currency.length != 3) Left(s"currency must be 3-letter ISO, got ${o.currency}")
    else if (o.status == null || o.status.isEmpty) Left("status is required")
    else if (!Set("CREATED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED").contains(o.status))
      Left(s"unknown status: ${o.status}")
    else Right(o)
  }

  def validateCustomer(c: CustomerDataProduct): Either[String, CustomerDataProduct] = {
    if (c.email == null || !c.email.contains("@")) Left(s"invalid email: ${c.email}")
    else if (c.tier == null || !Set("BRONZE", "SILVER", "GOLD", "PLATINUM").contains(c.tier))
      Left(s"invalid tier: ${c.tier}")
    else Right(c)
  }

  /** Apply a business rule based on customer tier (e.g. discount eligibility). */
  def enrich(order: OrderDataProduct, customer: CustomerDataProduct): EnrichedOrder = {
    EnrichedOrder(
      order = order,
      customerTier = customer.tier,
      enrichedAt = Instant.now()
    )
  }

  /** JSON serialize for Kafka sink. */
  def toJson(e: EnrichedOrder): String = {
    import JsonProtocols.enrichedFormat
    import spray.json._
    e.toJson.compactPrint
  }
}
