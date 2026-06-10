package com.codesolutions.flink.mesh

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

/**
 * Joins a stream of orders against a broadcast stream of customers.
 * Customers are cached in a MapState keyed by customerId; orders
 * are emitted as EnrichedOrder as soon as the customer is known.
 */
class CustomerEnrichmentFunction
    extends BroadcastProcessFunction[OrderDataProduct, CustomerDataProduct, EnrichedOrder] {

  // Exposed as a static so callers can .broadcast() on the same descriptor instance.
  private[mesh] val customerTypeInfo: TypeInformation[CustomerDataProduct] =
    TypeInformation.of(classOf[CustomerDataProduct])
  val customerDescriptor: MapStateDescriptor[String, CustomerDataProduct] =
    new MapStateDescriptor[String, CustomerDataProduct](
      "customers",
      org.apache.flink.api.common.typeinfo.Types.STRING,
      customerTypeInfo
    )

  override def open(parameters: org.apache.flink.configuration.Configuration): Unit = ()

  override def processElement(
      value: OrderDataProduct,
      ctx: BroadcastProcessFunction[OrderDataProduct, CustomerDataProduct, EnrichedOrder]#ReadOnlyContext,
      out: Collector[EnrichedOrder]
  ): Unit = {
    val customerOpt = Option(ctx.getBroadcastState(customerDescriptor).get(value.customerId))
    customerOpt.foreach(c => out.collect(EnrichmentLib.enrich(value, c)))
  }

  override def processBroadcastElement(
      value: CustomerDataProduct,
      ctx: BroadcastProcessFunction[OrderDataProduct, CustomerDataProduct, EnrichedOrder]#Context,
      out: Collector[EnrichedOrder]
  ): Unit = {
    ctx.getBroadcastState(customerDescriptor).put(value.customerId, value)
  }
}
