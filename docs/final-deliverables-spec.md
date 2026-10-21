# Final Deliverables Specification

## Objective

Turn the current Kafka/Redis/Flask vertical slice into a reproducible course
deliverable with three integrated capabilities:

1. a polished, usable grocery marketplace with product detail and simulated
   checkout;
2. a real Spark historical bootstrap that produces validated, versioned
   analytical artifacts from the Instacart data; and
3. a live adaptive decision layer that uses client feedback after cold start
   instead of treating a static rule table as the permanent decision engine.

Historical Instacart data is the initial prior. Once a shopper interacts with
the marketplace, later recommendations must incorporate the live event stream.

## Status language

- **Implemented** means runnable code with tests or a reproducible command.
- **Measured** means a saved run artifact records inputs, configuration, and
  results.
- **Designed** means the interface exists on paper but has not run.

Documentation must not label designed work as implemented or measured.

## Deliverable A: marketplace

The marketplace must:

- expose the full local product catalog with pagination, search, aisle and
  department filtering;
- provide a deep-linkable product detail page with product metadata,
  related-product recommendations, real product-view dwell events, and an
  add-to-basket action;
- preserve a shopper and session identity in the browser;
- provide a persistent basket with quantity controls, illustrative INR totals,
  coupon application, and clear disclosure that prices are simulated;
- provide a simulated checkout flow, validation, order confirmation, and a
  locally persisted order record without claiming real payment processing;
- record recommendation impressions/clicks, searches, cart changes, coupon
  lifecycle events, and checkout completion through the event API;
- retain the Signal Room as the auditable operator surface;
- work at 375 px and desktop widths, support keyboard navigation, visible focus,
  reduced motion, and at least 44 px interactive targets.

The design should feel like a modern grocery marketplace, not an analytics
dashboard. Its signature interaction is a product detail page that visibly
connects shopper intent, the basket, and a live recommendation without exposing
backend jargon to the customer.

## Deliverable B: Spark historical bootstrap

The batch CLI must:

- read the six local Instacart CSV tables with explicit schemas;
- validate identifiers, foreign-key coverage, duplicate facts, nulls, and
  allowed order/evaluation values;
- build order baskets while preserving item arrays;
- run Spark MLlib FP-Growth and publish one-item directional rules without
  flattening multi-item rules;
- deduplicate directed pairs deterministically;
- train or export an implicit-feedback ALS recommendation artifact when the
  configured data slice supports it;
- write versioned Parquet/JSON outputs plus a machine-readable manifest
  containing source paths/checksums or sizes, parameters, counts, duration, and
  output locations;
- support a bounded local/sample mode for CI and a full-data mode for the final
  measured run.

Redis may serve the output but must not be described as the analysis engine.

## Deliverable C: live adaptive analytics

The live layer must:

- accept versioned recommendation candidates from the historical bootstrap;
- derive session, shopper, product, and product-pair features from client events;
- rank a policy-safe candidate set using an explainable contextual-bandit
  implementation (LinUCB or Thompson sampling);
- record the recommendation impression before feedback can update the policy;
- accept click, cart, removal, coupon, and checkout feedback and update later
  decisions;
- preserve hard coupon guardrails outside the learning algorithm;
- include a deterministic replay demonstrating that new feedback changes a
  later ranking;
- provide a Spark Structured Streaming entry point with Kafka schema,
  event-time/watermark handling, checkpoint configuration, and a tested
  `foreachBatch` boundary for feature updates;
- fall back to validated historical candidates if adaptive state is absent or
  unavailable.

## Stable public seams

Tests should exercise behavior only through these seams:

1. **Marketplace HTTP seam** — Flask routes and JSON APIs, including catalog,
   product detail, event publication, basket recommendations, checkout, and
   order lookup.
2. **Batch artifact seam** — the Spark CLI and the files/manifest it produces
   from a fixed miniature dataset.
3. **Adaptive decision seam** — a ranking service that accepts shopper/session
   context and candidates, emits a versioned impression/decision, and consumes
   feedback.
4. **Replay seam** — a deterministic event fixture whose externally observed
   second ranking differs after rewarded feedback.

Tests must not couple to private helpers or internal storage layouts.

## Integration acceptance

- `uv sync` installs the project in a clean worktree.
- `uv run pytest -q` passes all unit and integration tests.
- A bounded Spark bootstrap run writes a manifest and validated rule output.
- A deterministic adaptive replay shows a feedback-driven ranking change.
- The marketplace can be opened, searched, navigated to a product, added to a
  basket, checked out in demo mode, and observed through emitted events.
- Existing deterministic and live demonstration commands remain usable.
- README and architecture documentation describe exact commands, evidence, and
  limitations.

## Out of scope

- real payment processing, real discount redemption, inventory reservation, or
  shipment fulfillment;
- causal conversion or revenue claims without a randomized experiment;
- collection of protected attributes or sensitive personalization features;
- presenting illustrative prices as Instacart source data.
