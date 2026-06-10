package com.codesolutions.flink.mesh

/**
 * Data Mesh data product registry.
 *
 * In a real Data Mesh implementation this metadata would be served by a
 * data catalog (e.g. DataHub, Amundsen, Marquez). For this example it
 * lives in code alongside the data product case classes.
 *
 *   - Two INDEPENDENT data products (orders + customers), each owned by
 *     its own domain team.
 *   - A PLATFORM-level join (Flink job) that produces EnrichedOrder.
 *
 * The case classes (and their JSON formats) live in their own files:
 *   - OrderDataProduct.scala
 *   - CustomerDataProduct.scala
 *   - EnrichedOrder.scala
 *
 * Data Contracts (Pydantic) live in src/main/python/contracts/ and validate
 * payloads at the producer/consumer boundary.
 */
object DataProducts {
  val OrdersOwner    = "orders-domain@code-solutions"
  val CustomersOwner = "customers-domain@code-solutions"
  val PlatformOwner  = "platform@code-solutions"
}

final case class DataProductMeta(owner: String, topicIn: String, topicOut: String, sla: String)
