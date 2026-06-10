"""Tests for the Pydantic data contracts."""
import pytest
from datetime import datetime
from contracts.data_contracts import (
    OrderDataProduct, CustomerDataProduct, EnrichedOrder
)


class TestOrderDataProduct:
    def test_valid_order(self):
        o = OrderDataProduct(
            orderId="o-1", customerId="c-1", amount=99.9,
            currency="USD", status="CREATED",
            createdAt=datetime(2024, 1, 1)
        )
        assert o.amount == 99.9
        assert o.currency == "USD"

    def test_currency_normalized_to_uppercase(self):
        o = OrderDataProduct(
            orderId="o-1", customerId="c-1", amount=1.0,
            currency="usd", status="CREATED",
            createdAt=datetime(2024, 1, 1)
        )
        assert o.currency == "USD"

    def test_rejects_non_positive_amount(self):
        with pytest.raises(Exception):
            OrderDataProduct(
                orderId="o-1", customerId="c-1", amount=0,
                currency="USD", status="CREATED",
                createdAt=datetime(2024, 1, 1)
            )

    def test_rejects_invalid_status(self):
        with pytest.raises(Exception):
            OrderDataProduct(
                orderId="o-1", customerId="c-1", amount=1.0,
                currency="USD", status="UNKNOWN",
                createdAt=datetime(2024, 1, 1)
            )

    def test_rejects_wrong_currency_length(self):
        with pytest.raises(Exception):
            OrderDataProduct(
                orderId="o-1", customerId="c-1", amount=1.0,
                currency="US", status="CREATED",
                createdAt=datetime(2024, 1, 1)
            )


class TestCustomerDataProduct:
    def test_valid_customer(self):
        c = CustomerDataProduct(
            customerId="c-1", name="Alice",
            email="alice@example.com", tier="GOLD",
            createdAt=datetime(2023, 1, 1)
        )
        assert c.tier == "GOLD"

    def test_rejects_invalid_email(self):
        with pytest.raises(Exception):
            CustomerDataProduct(
                customerId="c-1", name="Alice",
                email="not-an-email", tier="GOLD",
                createdAt=datetime(2023, 1, 1)
            )

    def test_rejects_invalid_tier(self):
        with pytest.raises(Exception):
            CustomerDataProduct(
                customerId="c-1", name="Alice",
                email="alice@example.com", tier="DIAMOND",
                createdAt=datetime(2023, 1, 1)
            )


class TestEnrichedOrder:
    def test_valid_enrichment(self):
        o = OrderDataProduct(
            orderId="o-1", customerId="c-1", amount=99.9,
            currency="USD", status="CREATED",
            createdAt=datetime(2024, 1, 1)
        )
        e = EnrichedOrder(order=o, customerTier="GOLD", enrichedAt=datetime(2024, 1, 2))
        assert e.customerTier == "GOLD"
