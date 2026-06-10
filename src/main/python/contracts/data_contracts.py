"""
Pydantic data contracts for the Flink Data Mesh Pipeline.

These contracts are the single source of truth for the shape of the data
products that travel through the Kafka topics. Producers validate against
them before publishing; consumers validate at the start of their pipeline.

The same rules are mirrored in:
  - src/main/scala/com/codesolutions/flink/mesh/Enrichment.scala (Scala validators)
  - Pydantic here (Python producers/consumers)
"""
from pydantic import BaseModel, Field, field_validator, EmailStr
from typing import Literal
from datetime import datetime


class OrderDataProduct(BaseModel):
    """Orders data product — owned by the orders domain."""
    orderId: str
    customerId: str
    amount: float = Field(gt=0, description="Must be strictly positive")
    currency: str = Field(min_length=3, max_length=3, description="ISO-4217 3-letter code")
    status: Literal["CREATED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"]
    createdAt: datetime

    @field_validator("currency")
    @classmethod
    def _upper(cls, v: str) -> str:
        return v.upper()


class CustomerDataProduct(BaseModel):
    """Customers data product — owned by the customers domain."""
    customerId: str
    name: str
    email: EmailStr
    tier: Literal["BRONZE", "SILVER", "GOLD", "PLATINUM"]
    createdAt: datetime


class EnrichedOrder(BaseModel):
    """Result of joining an order with its customer — owned by the platform team."""
    order: OrderDataProduct
    customerTier: str
    enrichedAt: datetime


# === Data Product registry — what a real Data Catalog would expose ===

DATA_PRODUCTS = {
    "orders": {
        "owner": "orders-domain@code-solutions",
        "topic_in": "orders.input",
        "topic_out": "orders.enriched",
        "sla": "p99 < 200ms, freshness < 1s",
        "schema": OrderDataProduct.model_json_schema(),
    },
    "customers": {
        "owner": "customers-domain@code-solutions",
        "topic_in": "customers.input",
        "topic_out": "customers.enriched",
        "sla": "p99 < 200ms, freshness < 1s",
        "schema": CustomerDataProduct.model_json_schema(),
    },
}
