# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

- Install deps: `npm install`
- Run product sync: `node fluidra-syncProductsFromEpicor.js`
- Run customer sync: `node fluidra-syncCustomersFromEpicor.js`
- Dry-run (no Shopify writes, still reads Epicor + writes CSV): prefix with `DRY_RUN=1`
- Smoke test (first Epicor page only): `FIRST_PAGE_ONLY=1`
- Cap processed rows: `MAX_ITEMS=50`
- Verbose tracing: `DEBUG=1`
- Disable ANSI color: `NO_COLOR=1`

No tests, no linter, no build step. These are standalone CLI scripts.

Credentials live in `.env` (gitignored) — copy from `.env.example`. `load-env.js` loads `.env` and has `requireEnv(name)` which exits with a clear message on missing vars; always route new secrets through it rather than touching `process.env` directly.

## Architecture

Two long-lived Node scripts that ETL Epicor → Shopify. They are intentionally independent — no shared library beyond `load-env.js` — but share the same internal shape, so fixes in one usually have a mirror in the other.

**Data sources:**
- Epicor OData API. Authoritative schemas: `openapi-specs/PartSvc.json` (products) and `openapi-specs/CustomersSvc.json` (customers + ShipToes). Consult these when changing `$select`/`$expand` fields or when Epicor responds with unexpected field names — treat the spec as ground truth over intuition.
- Shopify Admin GraphQL API (version pinned at `2026-01` in code, overridable via `SHOPIFY_API_VERSION`).

**Two-phase flow (both scripts):**
1. Fetch *all* Epicor rows into memory via keyset pagination (`PartNum gt …` for products, `CustNum gt …` for customers). `@odata.count` is known-unreliable — stop on a short page, never on a row count.
2. For each row: look up the Shopify counterpart, apply a `decideAction` rule, mutate Shopify (or skip), append a row to the CSV in `fluidra_sync_reports/`. CSV is flushed every 25 rows and on SIGINT/SIGTERM so interrupted runs still produce a report.

**Business rules:**
- **Products** — match Epicor `PartNum` → Shopify variant SKU (exact, case-sensitive post-filter; Shopify's SKU index is case-insensitive and tokenizes punctuation). Create DRAFT + publish to Online Store when `InActive=false` AND `PriceListYN_c='Y'` AND SKU not in Shopify. Update price when it already exists. Archive when `InActive=true` AND it exists. Inventory is sourced from the `EPICOR_MAIN_WAREHOUSE_CODE` (default `MAIN`) warehouse row on `PartWhses`.
- **Customers** — match Epicor `CustNum` → Shopify `Company.externalId`. ShipTo → `CompanyLocation.externalId`, preferring Epicor `ShipTo.ExternalID` and falling back to a synthetic `epicor:<CustNum>:<ShipToNum>` written back on create so subsequent runs re-match deterministically. A `custom.epicor_customer_id` metafield is ensured on every Company. When an Epicor customer flips to inactive but a Shopify Company still exists, the script emits `MANUAL_REVIEW` in the CSV rather than mutating — Shopify has no real B2B Company archive state.
- **Payment terms** — Epicor `TermsCode` is mapped to a Shopify `PaymentTermsTemplate` GID via `PAYMENT_TERMS_MAP`. Unknown codes are non-fatal: location is created/updated without terms and the CSV records a warning. Add new codes there.

**Shared plumbing (duplicated in both files — keep in sync):**
- `withRetry(label, fn)` — exponential backoff + jitter. Retries on network errors, 408/425/429, and 5xx. Shopify responses with `extensions.code === "THROTTLED"` are rewritten into synthetic 429s so they flow through the same path.
- `odataEscapeString` — OData v4 escapes single quotes by doubling them. Use this for every string literal interpolated into `$filter`.
- Customers script only: `asyncPool(concurrency, items, worker)` bounds intra-customer ShipTo parallelism (tuned via `LOCATION_CONCURRENCY`). Worker errors are captured as `{ __workerError }` entries so one bad ship-to cannot abort the whole run.
- Customers script only: customers whose `ShipToes` inline-expand hits `SHIPTO_EXPAND_TOP` are topped up via the dedicated child endpoint (`Customers('COMPANY',CustNum)/ShipToes`) — the compound key segment needs URL-encoded apostrophes **and** OData-escaped company value.

**CSV reports** are written under `fluidra_sync_reports/` (gitignored — may contain business data). Each run creates a new timestamped file; the exact header set differs per script. The `action` column is the operator-facing summary of what actually happened (`created`, `updated_price`, `archived`, `skipped_*`, `manual_review`, `failed`).
