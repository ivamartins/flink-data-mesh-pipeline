package com.codesolutions.flink.mesh

import java.time.Instant

/** Customers data product — owned by the customers domain. */
final case class CustomerDataProduct(
    customerId: String,
    name: String,
    email: String,
    tier: String,            // BRONZE / SILVER / GOLD / PLATINUM
    createdAt: Instant
)

object CustomerDataProduct {
  val Owner    = "customers-domain@code-solutions"
  val TopicIn  = "customers.input"
  val TopicOut = "customers.enriched"
  val SLA      = "p99 < 200ms, freshness < 1s"
}
