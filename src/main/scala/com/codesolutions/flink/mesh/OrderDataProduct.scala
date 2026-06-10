package com.codesolutions.flink.mesh

import java.time.Instant

/** Orders data product — owned by the orders domain. */
final case class OrderDataProduct(
    orderId: String,
    customerId: String,
    amount: Double,
    currency: String,
    status: String,
    createdAt: Instant
)

object OrderDataProduct {
  val Owner    = "orders-domain@code-solutions"
  val TopicIn  = "orders.input"
  val TopicOut = "orders.enriched"
  val SLA      = "p99 < 200ms, freshness < 1s"
}
