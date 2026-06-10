package com.codesolutions.flink.mesh

import com.codesolutions.flink.mesh.EnrichmentLib
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class EnrichmentSpec extends AnyWordSpec with Matchers {

  "parseOrder" should {
    "parse a valid order JSON" in {
      val raw = """{"orderId":"o-1","customerId":"c-1","amount":99.9,"currency":"USD","status":"CREATED","createdAt":"2024-01-01T00:00:00Z"}"""
      val parsed = EnrichmentLib.parseOrder(raw)
      parsed.isRight shouldEqual true
      parsed.toOption.get.amount shouldEqual 99.9
    }

    "fail on invalid JSON" in {
      val parsed = EnrichmentLib.parseOrder("not-json")
      parsed.isLeft shouldEqual true
    }
  }

  "validateOrder" should {
    val good = OrderDataProduct("o-1", "c-1", 99.9, "USD", "CREATED", Instant.now())

    "accept a valid order" in {
      EnrichmentLib.validateOrder(good).isRight shouldEqual true
    }

    "reject amount <= 0" in {
      EnrichmentLib.validateOrder(good.copy(amount = 0)).isLeft shouldEqual true
      EnrichmentLib.validateOrder(good.copy(amount = -1)).isLeft shouldEqual true
    }

    "reject non-3-letter currency" in {
      EnrichmentLib.validateOrder(good.copy(currency = "US")).isLeft shouldEqual true
    }

    "reject unknown status" in {
      EnrichmentLib.validateOrder(good.copy(status = "FOO")).isLeft shouldEqual true
    }
  }

  "validateCustomer" should {
    val good = CustomerDataProduct("c-1", "Alice", "alice@example.com", "GOLD", Instant.now())

    "accept a valid customer" in {
      EnrichmentLib.validateCustomer(good).isRight shouldEqual true
    }

    "reject invalid email" in {
      EnrichmentLib.validateCustomer(good.copy(email = "no-at-sign")).isLeft shouldEqual true
    }

    "reject invalid tier" in {
      EnrichmentLib.validateCustomer(good.copy(tier = "DIAMOND")).isLeft shouldEqual true
    }
  }

  "enrich" should {
    "combine order with customer tier" in {
      val order = OrderDataProduct("o-1", "c-1", 99.9, "USD", "CREATED", Instant.now())
      val customer = CustomerDataProduct("c-1", "Alice", "alice@example.com", "GOLD", Instant.now())
      val result = EnrichmentLib.enrich(order, customer)
      result.customerTier shouldEqual "GOLD"
      result.order.orderId shouldEqual "o-1"
    }
  }
}
