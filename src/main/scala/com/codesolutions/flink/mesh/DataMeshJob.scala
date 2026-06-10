package com.codesolutions.flink.mesh

import org.apache.flink.streaming.api.scala._
import com.codesolutions.flink.mesh.EnrichmentLib
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.BroadcastStream

/**
 * Flink Data Mesh Pipeline.
 *
 * Pattern:
 *   - Two INDEPENDENT Kafka topics (one per data product): orders + customers.
 *   - Customers are broadcast and cached in operator state.
 *   - Orders are enriched with the broadcasted customer.
 *   - Resulting EnrichedOrder is published to a third topic.
 *
 * This is the textbook Data Mesh "self-serve platform" job:
 *   - data products (topics) are owned by their domains
 *   - the join is a platform-level capability
 *   - schemas/contracts are independently versioned
 *
 * Mapping to the Data Engineer JD:
 *   - ETL Data Pipelines            ✅
 *   - Apache Flink                  ✅
 *   - Python for Data Engineering   ✅ (Pydantic contracts)
 *   - Data Mesh Architecture        ✅ (two independent data products, joined)
 *   - Pydantic                      ✅ (Python data contracts)
 */
object DataMeshJob {

  /** Local mode: no Kafka needed. */
  def runWithCollections(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.setParallelism(1)

    val orderEvents: Seq[String] = Seq(
      """{"orderId":"o-1","customerId":"c-1","amount":99.9,"currency":"USD","status":"CREATED","createdAt":"2024-01-01T00:00:00Z"}""",
      """{"orderId":"o-2","customerId":"c-2","amount":50.0,"currency":"EUR","status":"PAID","createdAt":"2024-01-01T00:00:00Z"}""",
      """{"orderId":"o-3","customerId":"c-1","amount":250.0,"currency":"USD","status":"CREATED","createdAt":"2024-01-01T00:00:00Z"}"""
    )

    val customerEvents: Seq[String] = Seq(
      """{"customerId":"c-1","name":"Alice","email":"alice@example.com","tier":"GOLD","createdAt":"2023-01-01T00:00:00Z"}""",
      """{"customerId":"c-2","name":"Bob","email":"bob@example.com","tier":"SILVER","createdAt":"2023-02-01T00:00:00Z"}"""
    )

    val orders: DataStream[String] = env.fromCollection(orderEvents)
    val customers: DataStream[String] = env.fromCollection(customerEvents)

    val customerParsed: DataStream[CustomerDataProduct] = customers.map { raw =>
      EnrichmentLib.parseCustomer(raw) match {
        case Right(c) => c
        case Left(err) => throw new RuntimeException(s"Invalid customer payload: $err")
      }
    }

    val parsedOrders: DataStream[OrderDataProduct] = orders.map { raw =>
      EnrichmentLib.parseOrder(raw).flatMap(EnrichmentLib.validateOrder) match {
        case Right(v) => v
        case Left(err) => throw new RuntimeException(s"Invalid order: $err")
      }
    }

    val broadcastCustomers: BroadcastStream[CustomerDataProduct] =
      customerParsed.broadcast((new CustomerEnrichmentFunction()).customerDescriptor)

    parsedOrders
      .connect(broadcastCustomers)
      .process(new CustomerEnrichmentFunction())
      .print()

    env.execute("Flink Data Mesh Pipeline - Local Collection Demo")
  }

  /** Production: Kafka sources. */
  def runWithKafka(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.setParallelism(2)

    val brokers = sys.env.getOrElse("BOOTSTRAP_SERVERS", "localhost:9092")

    import org.apache.flink.connector.kafka.source.KafkaSource
    import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
    import org.apache.flink.api.common.serialization.SimpleStringSchema

    val orderSource = KafkaSource.builder[String]()
      .setBootstrapServers(brokers)
      .setGroupId("data-mesh-orders")
      .setTopics(OrderDataProduct.TopicIn)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val customerSource = KafkaSource.builder[String]()
      .setBootstrapServers(brokers)
      .setGroupId("data-mesh-customers")
      .setTopics(CustomerDataProduct.TopicIn)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val orders: DataStream[String] = env.fromSource(orderSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks[String](), "orders-source")
    val customers: DataStream[String] = env.fromSource(customerSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks[String](), "customers-source")

    val customerParsed: DataStream[CustomerDataProduct] = customers.map { raw =>
      EnrichmentLib.parseCustomer(raw) match {
        case Right(c) => c
        case Left(err) => throw new RuntimeException(s"Invalid customer payload: $err")
      }
    }

    val parsedOrders: DataStream[OrderDataProduct] = orders.map { raw =>
      EnrichmentLib.parseOrder(raw).flatMap(EnrichmentLib.validateOrder) match {
        case Right(v) => v
        case Left(err) => throw new RuntimeException(s"Invalid order: $err")
      }
    }

    val broadcastCustomers: BroadcastStream[CustomerDataProduct] =
      customerParsed.broadcast((new CustomerEnrichmentFunction()).customerDescriptor)

    parsedOrders
      .connect(broadcastCustomers)
      .process(new CustomerEnrichmentFunction())
      .map(EnrichmentLib.toJson _)
      .print()

    env.execute("Flink Data Mesh Pipeline - Kafka")
  }

  def main(args: Array[String]): Unit = {
    args.headOption match {
      case Some("kafka") => runWithKafka()
      case _             => runWithCollections()
    }
  }
}
