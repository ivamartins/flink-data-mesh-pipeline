package com.codesolutions.flink.mesh

import java.time.Instant

/**
 * Enriched join — order enriched with its customer's tier.
 * Owned by the platform team (Flink job).
 */
final case class EnrichedOrder(
    order: OrderDataProduct,
    customerTier: String,
    enrichedAt: Instant
)
