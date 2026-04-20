/**
 * Fluidra — Sync products from Epicor into Shopify
 *
 * Reads all Part records from Epicor (OpenAPI spec: Fluidra/openapi-specs/PartSvc.json),
 * matches each PartNum against the Shopify variant SKU, and applies:
 *
 *   - CREATE  (DRAFT) when the Epicor part is active + web-enabled and does not exist in Shopify
 *   - UPDATE  (price) when the Epicor part is active + web-enabled and exists in Shopify
 *   - ARCHIVE        when the Epicor part is inactive and exists in Shopify
 *   - SKIP           for everything else (inactive + not in Shopify, not web, etc.)
 *
 * Run:
 *   cd Fluidra && npm install && node fluidra-syncProductsFromEpicor.js
 *
 * Required secrets in Fluidra/.env (copy from .env.example):
 *   EPICOR_BASE_URL, EPICOR_COMPANY, EPICOR_USERNAME, EPICOR_PASSWORD, EPICOR_API_KEY
 *   SHOPIFY_STORE_NAME, SHOPIFY_ACCESS_TOKEN
 * Optional: SHOPIFY_API_VERSION, SHOPIFY_LOCATION_ID, EPICOR_MAIN_WAREHOUSE_CODE,
 *   EPICOR_PAGE_SIZE, EPICOR_TIMEOUT_MS, SHOPIFY_TIMEOUT_MS, MAX_RETRIES, DRY_RUN, …
 */

const { requireEnv } = require("./load-env");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { createObjectCsvWriter } = require("csv-writer");

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

// Epicor — OpenAPI spec (PartSvc.json): /{currentCompany}/Erp.BO.PartSvc/Parts
// Auth is Basic + X-API-Key header (see spec components.parameters.X-API-Key).
const epicorConfig = {
  baseUrl: requireEnv("EPICOR_BASE_URL"),
  company: requireEnv("EPICOR_COMPANY"),
  username: requireEnv("EPICOR_USERNAME"),
  password: requireEnv("EPICOR_PASSWORD"),
  apiKey: requireEnv("EPICOR_API_KEY"),
};

const shopifyConfig = {
  storeName: requireEnv("SHOPIFY_STORE_NAME"),
  accessToken: requireEnv("SHOPIFY_ACCESS_TOKEN"),
  apiVersion: process.env.SHOPIFY_API_VERSION || "2026-01",
  // Optional explicit location GID to set inventory at. If unset, the script
  // picks the first Shopify location it finds (usually the shop's primary).
  locationId: process.env.SHOPIFY_LOCATION_ID || null,
};

// Epicor warehouse whose OnHandQty drives Shopify inventory.
const EPICOR_MAIN_WAREHOUSE_CODE = process.env.EPICOR_MAIN_WAREHOUSE_CODE || "MAIN";

// Runtime tuning
const RUN_CONFIG = {
  pageSize: parseInt(process.env.EPICOR_PAGE_SIZE || "200", 10),
  maxRetries: parseInt(process.env.MAX_RETRIES || "5", 10),
  retryBaseDelayMs: parseInt(process.env.RETRY_BASE_DELAY_MS || "2000", 10),
  epicorRequestTimeoutMs: parseInt(
    process.env.EPICOR_TIMEOUT_MS || "60000",
    10
  ),
  shopifyRequestTimeoutMs: parseInt(
    process.env.SHOPIFY_TIMEOUT_MS || "60000",
    10
  ),
  // Throttle between per-item operations to stay under Shopify REST/GraphQL limits.
  perItemDelayMs: parseInt(process.env.PER_ITEM_DELAY_MS || "250", 10),
  // Delay between Epicor pages.
  perPageDelayMs: parseInt(process.env.PER_PAGE_DELAY_MS || "500", 10),
  // Safety cap (0 = no limit). Useful for dry-runs.
  maxItems: parseInt(process.env.MAX_ITEMS || "0", 10),
  // When true, no writes are performed on Shopify; every item is evaluated and
  // recorded in the CSV with a "[DRY RUN]" prefix in the error_message column.
  dryRun: /^(1|true|yes)$/i.test(process.env.DRY_RUN || ""),
  // When true, only the first page is fetched — convenient for smoke tests.
  firstPageOnly: /^(1|true|yes)$/i.test(process.env.FIRST_PAGE_ONLY || ""),
  csvDir:
    process.env.REPORT_DIR ||
    path.join(__dirname, "fluidra_sync_reports"),
};

// ─────────────────────────────────────────────────────────────────────────────
// Logging — ANSI colors + readable structure. Respects NO_COLOR env var and
// falls back to plain text when stdout isn't a TTY (e.g. piped to a file).
// ─────────────────────────────────────────────────────────────────────────────

const COLOR_ENABLED =
  !process.env.NO_COLOR &&
  process.stdout.isTTY !== false; // treat undefined as enabled

const c = (code) => (s) =>
  COLOR_ENABLED ? `\x1b[${code}m${s}\x1b[0m` : String(s);

const color = {
  reset: (s) => s,
  bold: c("1"),
  dim: c("2"),
  red: c("31"),
  green: c("32"),
  yellow: c("33"),
  blue: c("34"),
  magenta: c("35"),
  cyan: c("36"),
  white: c("37"),
  gray: c("90"),
  boldCyan: c("1;36"),
  boldGreen: c("1;32"),
  boldYellow: c("1;33"),
  boldRed: c("1;31"),
  boldMagenta: c("1;35"),
};

function ts() {
  return new Date().toISOString();
}

function formatExtra(extra) {
  if (extra === undefined) return "";
  if (typeof extra === "string") return "\n  " + color.dim(extra);
  try {
    const pretty = JSON.stringify(extra, null, 2)
      .split("\n")
      .map((line) => "  " + color.dim(line))
      .join("\n");
    return "\n" + pretty;
  } catch {
    return "\n  " + color.dim(String(extra));
  }
}

function log(level, msg, extra) {
  const levelColored = {
    INFO: color.cyan("INFO "),
    WARN: color.yellow("WARN "),
    ERROR: color.boldRed("ERROR"),
    DEBUG: color.gray("DEBUG"),
  }[level] || level;
  const timestamp = color.gray(`[${ts()}]`);
  const body = `${timestamp} ${levelColored} ${msg}${formatExtra(extra)}`;
  // Separation rule:
  //   - single-line entries → 1 blank line after (append "\n")
  //   - multi-line entries (extra payload, embedded newlines) → 2 blank lines
  //     after (append "\n\n")
  // `console.log` already writes a trailing newline, so appending N newlines
  // produces N blank lines between entries.
  const suffix = body.includes("\n") ? "\n\n" : "\n";
  console.log(body + suffix);
}

const logger = {
  info: (m, e) => log("INFO", m, e),
  warn: (m, e) => log("WARN", m, e),
  error: (m, e) => log("ERROR", m, e),
  debug: (m, e) => {
    if (process.env.DEBUG) log("DEBUG", m, e);
  },
  /** Blank line for visual breathing room between sections. */
  blank: () => console.log(""),
  /** Heavy section banner. */
  banner: (msg) => {
    const bar = color.boldCyan("═".repeat(78));
    // Multi-line → 2 blank lines after.
    console.log(`\n${bar}\n${color.boldCyan("  " + msg)}\n${bar}\n\n`);
  },
  /** Lighter sub-section header. */
  section: (msg) => {
    // Multi-line (leading blank) → 2 blank lines after.
    console.log(`\n${color.boldMagenta("─── " + msg + " ───")}\n\n`);
  },
};

// Small helpers used by the main loop to highlight key data points.
const hl = {
  sku: (s) => color.boldYellow(s),
  num: (n) => color.boldCyan(String(n)),
  ok: (s) => color.boldGreen(s),
  warn: (s) => color.boldYellow(s),
  bad: (s) => color.boldRed(s),
  key: (s) => color.bold(s),
};

// ─────────────────────────────────────────────────────────────────────────────
// Utilities: sleep + generic retry with exponential backoff + jitter
// ─────────────────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isRetryableAxiosError(err) {
  if (!err) return false;
  if (err.code === "ECONNABORTED") return true;
  if (err.code === "ETIMEDOUT") return true;
  if (err.code === "ECONNRESET") return true;
  if (err.code === "ENOTFOUND") return true;
  if (err.code === "EAI_AGAIN") return true;
  const status = err.response?.status;
  if (!status) return true; // no response → network layer issue
  if (status === 408 || status === 425 || status === 429) return true;
  if (status >= 500 && status <= 599) return true;
  return false;
}

async function withRetry(label, fn, opts = {}) {
  const max = opts.maxRetries ?? RUN_CONFIG.maxRetries;
  const base = opts.baseDelayMs ?? RUN_CONFIG.retryBaseDelayMs;
  let attempt = 0;
  while (true) {
    attempt++;
    try {
      const result = await fn(attempt);
      if (attempt > 1) {
        logger.info(`✅ ${label} succeeded on attempt ${attempt}`);
      }
      return { result, attempts: attempt };
    } catch (err) {
      const retryable = isRetryableAxiosError(err);
      const status = err.response?.status;
      const body = err.response?.data;
      logger.warn(
        `⚠️  ${label} failed (attempt ${attempt}/${max}) status=${status ?? "n/a"} retryable=${retryable}`,
        typeof body === "object" ? body : err.message
      );
      if (!retryable || attempt >= max) {
        throw Object.assign(err, { attempts: attempt });
      }
      const delay = base * Math.pow(2, attempt - 1) + Math.floor(Math.random() * 500);
      logger.info(`   ↻ retrying ${label} in ${delay}ms…`);
      await sleep(delay);
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Epicor client
// ─────────────────────────────────────────────────────────────────────────────

function epicorAuthHeaders() {
  const basic = Buffer.from(
    `${epicorConfig.username}:${epicorConfig.password}`
  ).toString("base64");
  return {
    Accept: "application/json",
    Authorization: `Basic ${basic}`,
    "X-API-Key": epicorConfig.apiKey,
  };
}

/**
 * Verify credentials early by hitting the service document endpoint.
 * Spec: GET /{currentCompany}/Erp.BO.PartSvc/
 */
async function epicorHealthCheck() {
  const url = `${epicorConfig.baseUrl}/${encodeURIComponent(
    epicorConfig.company
  )}/Erp.BO.PartSvc/`;
  logger.info("🔐 Authenticating against Epicor…", { url });
  await withRetry("epicor:auth", async () => {
    const res = await axios.get(url, {
      headers: epicorAuthHeaders(),
      timeout: RUN_CONFIG.epicorRequestTimeoutMs,
    });
    return res.data;
  });
  logger.info("✅ Epicor authentication succeeded.");
}

/**
 * Fetch one Parts page.
 * Spec: GET /{currentCompany}/Erp.BO.PartSvc/Parts
 * OData query params (case-sensitive): $top, $skip, $count, $select, $orderby.
 * Only the fields required by the business rules are selected to keep payloads small.
 */
/**
 * Escape a string for safe inclusion inside an OData single-quoted literal.
 * OData v4 escapes single quotes by doubling them: O'Brien → 'O''Brien'.
 */
function odataEscapeString(v) {
  return String(v).replace(/'/g, "''");
}

async function fetchEpicorPartsPage({ top, includeCount, afterPartNum }) {
  const url = `${epicorConfig.baseUrl}/${encodeURIComponent(
    epicorConfig.company
  )}/Erp.BO.PartSvc/Parts`;

  // Fields needed for the Epicor → Shopify mapping shown in the spec screenshots:
  //   Shopify product.title         ← Epicor LitDesc_c
  //   Shopify product.descriptionHtml ← Epicor LitDesc_c
  //   Shopify product.vendor        ← Epicor Company
  //   Shopify variant.sku           ← Epicor PartNum
  //   Shopify variant.barcode       ← Epicor ProdCode
  //   Shopify variant.price         ← Epicor UnitPrice
  //   Shopify inventoryItem weight  ← Epicor NetWeight + NetWeightUOM
  const $select = [
    "Company",
    "PartNum",
    "PartDescription",
    "InActive",
    "UnitPrice",
    "TypeCode",
    "SalesUM",
    "ProdCode",
    "NetWeight",
    "NetWeightUOM",
    "PriceListYN_c",
    "LitDesc_c",
    "ClassDescription",
    "SysRowID",
  ].join(",");

  // Only web-enabled parts are relevant to Shopify, so we filter server-side
  // on PriceListYN_c='Y'. Keyset pagination adds a PartNum cursor clause.
  const filters = ["PriceListYN_c eq 'Y'"];
  if (afterPartNum) {
    filters.push(`PartNum gt '${odataEscapeString(afterPartNum)}'`);
  }

  const params = {
    $top: top,
    $orderby: "PartNum",
    $select,
    $filter: filters.join(" and "),
    // Expand the PartWhses child collection so we can read OnHandQty for the
    // MAIN warehouse without an N+1 follow-up request per part.
    $expand: "PartWhses($select=WarehouseCode,OnHandQty)",
  };
  if (includeCount) params.$count = true;

  const { result } = await withRetry(
    `epicor:Parts page after='${afterPartNum ?? ""}' top=${top}`,
    async () => {
      const res = await axios.get(url, {
        headers: epicorAuthHeaders(),
        params,
        timeout: RUN_CONFIG.epicorRequestTimeoutMs,
      });
      return res.data;
    }
  );

  const items = Array.isArray(result.value) ? result.value : [];
  const totalCount =
    result["@odata.count"] !== undefined
      ? Number(result["@odata.count"])
      : null;
  return { items, totalCount };
}

/**
 * Fetch every Part from Epicor by paginating with $top/$skip. Returns the full
 * array in memory so the processing phase knows the true total up front.
 *
 * Epicor's @odata.count value is unreliable (it can mirror the page size rather
 * than the full row count), so we don't trust it — we paginate until we either
 * get a short page or hit MAX_ITEMS.
 */
async function fetchAllEpicorParts() {
  const all = [];
  let page = 0;
  let afterPartNum = null;

  logger.section("Phase 1 / 2  —  Fetching all Epicor Parts");

  while (true) {
    page++;
    logger.info(
      `📄 Fetching page ${hl.num(page)}  (after=${hl.key(afterPartNum ?? "(start)")}, top=${hl.num(RUN_CONFIG.pageSize)})`
    );

    const { items } = await fetchEpicorPartsPage({
      afterPartNum,
      top: RUN_CONFIG.pageSize,
      includeCount: false,
    });
    all.push(...items);
    logger.info(
      `   ✅ Page ${hl.num(page)}: received ${hl.num(items.length)} records  —  running total: ${hl.num(all.length)}`
    );

    if (items.length === 0 || items.length < RUN_CONFIG.pageSize) {
      logger.info("   ↩️  Last page detected.");
      break;
    }
    if (RUN_CONFIG.firstPageOnly) {
      logger.warn("FIRST_PAGE_ONLY=true — stopping after first page.");
      break;
    }
    if (RUN_CONFIG.maxItems > 0 && all.length >= RUN_CONFIG.maxItems) {
      logger.warn(
        `⛔ MAX_ITEMS cap reached during fetch (${RUN_CONFIG.maxItems}). Stopping fetch.`
      );
      break;
    }

    const lastPartNum = items[items.length - 1]?.PartNum;
    if (!lastPartNum) {
      logger.warn(
        "Last row has no PartNum — cannot advance keyset cursor. Stopping."
      );
      break;
    }
    if (afterPartNum === lastPartNum) {
      // Safety net: the filter should strictly advance each page.
      logger.warn(
        `Keyset cursor did not advance past PartNum='${afterPartNum}'. Stopping to avoid an infinite loop.`
      );
      break;
    }
    afterPartNum = lastPartNum;

    if (RUN_CONFIG.perPageDelayMs > 0) await sleep(RUN_CONFIG.perPageDelayMs);
  }

  return all;
}

// ─────────────────────────────────────────────────────────────────────────────
// Shopify client (GraphQL Admin API)
// ─────────────────────────────────────────────────────────────────────────────

const shopifyGraphqlEndpoint = () =>
  `https://${shopifyConfig.storeName}.myshopify.com/admin/api/${shopifyConfig.apiVersion}/graphql.json`;

async function shopifyGraphql(query, variables = {}, label = "shopify:graphql") {
  const { result } = await withRetry(label, async () => {
    const res = await axios.post(
      shopifyGraphqlEndpoint(),
      { query, variables },
      {
        headers: {
          "Content-Type": "application/json",
          "X-Shopify-Access-Token": shopifyConfig.accessToken,
        },
        timeout: RUN_CONFIG.shopifyRequestTimeoutMs,
      }
    );
    // Shopify throttle: if extensions.cost says we're close to the bucket,
    // back off briefly. Always honour top-level errors with THROTTLED code.
    const data = res.data;
    const throttled = (data?.errors || []).some(
      (e) => e?.extensions?.code === "THROTTLED"
    );
    if (throttled) {
      const retryErr = new Error("Shopify THROTTLED");
      retryErr.response = { status: 429, data };
      throw retryErr;
    }
    return data;
  });
  if (result.errors) {
    throw new Error(
      `Shopify GraphQL errors: ${JSON.stringify(result.errors)}`
    );
  }
  return result.data;
}

/**
 * Look up a Shopify product by variant SKU. Returns the matching variant+product
 * or null when no match is found.
 *
 * Note: the Shopify search index treats SKUs case-insensitively and may tokenize
 * on punctuation, so we post-filter on an exact match to avoid accidental hits.
 * API version 2026-01 (matches Coontail/coontail-migrateProducts.js).
 */
async function findShopifyByExactSku(sku) {
  if (!sku) return null;
  const query = `
    query findBySku($q: String!) {
      productVariants(first: 10, query: $q) {
        nodes {
          id
          sku
          inventoryItem { id }
          product {
            id
            title
            status
            handle
          }
        }
      }
    }
  `;
  const data = await shopifyGraphql(
    query,
    { q: `sku:${sku}` },
    `shopify:findBySku ${sku}`
  );
  const nodes = data?.productVariants?.nodes || [];
  const exact = nodes.find(
    (n) => String(n.sku || "").trim() === String(sku).trim()
  );
  return exact || null;
}

async function createShopifyDraftProduct(part) {
  const mutation = `
    mutation productCreate($product: ProductCreateInput!, $media: [CreateMediaInput!]) {
      productCreate(product: $product, media: $media) {
        product {
          id
          title
          status
          variants(first: 1) {
            nodes {
              id
              sku
              inventoryItem { id }
            }
          }
        }
        userErrors { field message }
      }
    }
  `;
  // Shopify 2025-01 productCreate does NOT accept variants inline; the default
  // variant is created automatically. We patch SKU, barcode, price and weight
  // with a follow-up productVariantsBulkUpdate call.
  const title = part.LitDesc_c || part.PartDescription;
  const variables = {
    product: {
      title,
      descriptionHtml: part.LitDesc_c || "",
      vendor: part.Company || "",
      productType: part.ClassDescription || "",
      status: "DRAFT",
    },
  };
  const data = await shopifyGraphql(
    mutation,
    variables,
    `shopify:productCreate ${part.PartNum}`
  );
  const errs = data?.productCreate?.userErrors || [];
  if (errs.length) {
    throw new Error(`productCreate userErrors: ${JSON.stringify(errs)}`);
  }
  const product = data.productCreate.product;
  const defaultVariant = product?.variants?.nodes?.[0];
  if (!defaultVariant?.id) {
    throw new Error("productCreate returned no default variant");
  }
  // Populate the default variant from Epicor fields.
  await bulkUpdateVariant(product.id, defaultVariant.id, buildVariantInput(part));
  return {
    productId: product.id,
    variantId: defaultVariant.id,
    inventoryItemId: defaultVariant.inventoryItem?.id || null,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Shopify inventory helpers
// ─────────────────────────────────────────────────────────────────────────────

let cachedLocationId = null;

/**
 * Resolve the Shopify location GID to set inventory at. Uses SHOPIFY_LOCATION_ID
 * when provided, otherwise picks the first active location from the shop
 * (typically the shop's primary location). Cached for the life of the process.
 */
async function getShopifyLocationId() {
  if (cachedLocationId) return cachedLocationId;
  if (shopifyConfig.locationId) {
    cachedLocationId = shopifyConfig.locationId;
    logger.info(`📍 Using Shopify location (from env): ${hl.key(cachedLocationId)}`);
    return cachedLocationId;
  }
  const query = `
    query firstLocation {
      locations(first: 10, includeInactive: false) {
        nodes { id name isPrimary }
      }
    }
  `;
  const data = await shopifyGraphql(query, {}, "shopify:getLocation");
  const nodes = data?.locations?.nodes || [];
  if (nodes.length === 0) {
    throw new Error("No active Shopify locations found.");
  }
  const chosen = nodes.find((n) => n.isPrimary) || nodes[0];
  cachedLocationId = chosen.id;
  logger.info(
    `📍 Using Shopify location: ${hl.key(chosen.name)} (${cachedLocationId})`
  );
  return cachedLocationId;
}

/**
 * Ensure the inventory item is activated at the location so on-hand quantities
 * can be set against it. Idempotent — returns the existing level if present.
 */
async function ensureInventoryActivated(inventoryItemId, locationId) {
  const mutation = `
    mutation activate($inventoryItemId: ID!, $locationId: ID!) {
      inventoryActivate(inventoryItemId: $inventoryItemId, locationId: $locationId) {
        inventoryLevel { id }
        userErrors { field message }
      }
    }
  `;
  const data = await shopifyGraphql(
    mutation,
    { inventoryItemId, locationId },
    `shopify:inventoryActivate ${inventoryItemId}`
  );
  const errs = data?.inventoryActivate?.userErrors || [];
  if (errs.length) {
    throw new Error(`inventoryActivate userErrors: ${JSON.stringify(errs)}`);
  }
}

/**
 * Set the absolute on-hand quantity for an inventory item at a given location.
 * Activates first (idempotent) so newly-created items can receive quantities.
 */
async function setInventoryOnHand(inventoryItemId, locationId, quantity) {
  await ensureInventoryActivated(inventoryItemId, locationId);
  const mutation = `
    mutation setOnHand($input: InventorySetOnHandQuantitiesInput!) {
      inventorySetOnHandQuantities(input: $input) {
        inventoryAdjustmentGroup { id }
        userErrors { field message code }
      }
    }
  `;
  const variables = {
    input: {
      reason: "correction",
      referenceDocumentUri: "logistics://fluidra-epicor-sync",
      setQuantities: [{ inventoryItemId, locationId, quantity }],
    },
  };
  const data = await shopifyGraphql(
    mutation,
    variables,
    `shopify:inventorySetOnHand ${inventoryItemId}`
  );
  const errs = data?.inventorySetOnHandQuantities?.userErrors || [];
  if (errs.length) {
    throw new Error(
      `inventorySetOnHandQuantities userErrors: ${JSON.stringify(errs)}`
    );
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Shopify publication helpers (Online Store sales channel)
// ─────────────────────────────────────────────────────────────────────────────

let cachedOnlineStorePublicationId = null;

/**
 * Resolve the "Online Store" publication GID. Cached for the life of the
 * process. Shopify's Online Store sales channel has app handle "online_store";
 * we also fall back to matching by name for safety.
 */
async function getOnlineStorePublicationId() {
  if (cachedOnlineStorePublicationId) return cachedOnlineStorePublicationId;
  const query = `
    query publications {
      publications(first: 50) {
        nodes { id name app { handle } }
      }
    }
  `;
  const data = await shopifyGraphql(query, {}, "shopify:getPublications");
  const nodes = data?.publications?.nodes || [];
  const chosen =
    nodes.find((n) => n.app?.handle === "online_store") ||
    nodes.find((n) => String(n.name || "").toLowerCase() === "online store");
  if (!chosen) {
    throw new Error(
      "Online Store publication not found — is the sales channel installed?"
    );
  }
  cachedOnlineStorePublicationId = chosen.id;
  logger.info(
    `🛒 Using Online Store publication: ${hl.key(chosen.name)} (${cachedOnlineStorePublicationId})`
  );
  return cachedOnlineStorePublicationId;
}

/**
 * Publish a product to the Online Store sales channel. Idempotent — publishing
 * an already-published product is a no-op on Shopify's side.
 */
async function publishToOnlineStore(productId) {
  const publicationId = await getOnlineStorePublicationId();
  const mutation = `
    mutation publish($id: ID!, $input: [PublicationInput!]!) {
      publishablePublish(id: $id, input: $input) {
        userErrors { field message }
      }
    }
  `;
  const data = await shopifyGraphql(
    mutation,
    { id: productId, input: [{ publicationId }] },
    `shopify:publishablePublish ${productId}`
  );
  const errs = data?.publishablePublish?.userErrors || [];
  if (errs.length) {
    throw new Error(`publishablePublish userErrors: ${JSON.stringify(errs)}`);
  }
}

/**
 * Map Epicor NetWeightUOM to a Shopify WeightUnit enum. Anything that looks
 * like pounds → POUNDS; everything else defaults to GRAMS (per the condition
 * shown in the mapping screenshots).
 */
function mapWeightUnit(epicorUom) {
  const u = String(epicorUom || "").trim().toUpperCase();
  if (u === "LB" || u === "LBS" || u === "POUND" || u === "POUNDS") return "POUNDS";
  if (u === "KG" || u === "KGS" || u === "KILOGRAM" || u === "KILOGRAMS") return "KILOGRAMS";
  if (u === "OZ" || u === "OUNCE" || u === "OUNCES") return "OUNCES";
  return "GRAMS";
}

/**
 * Build the variant input for productVariantsBulkUpdate from an Epicor Part row.
 * In Shopify Admin API 2026-01, `sku` is no longer a top-level field on
 * ProductVariantsBulkInput — it lives on `inventoryItem.sku`. Weight lives on
 * `inventoryItem.measurement.weight` with a WeightUnit enum. Weight is only
 * emitted when NetWeight is a positive number to avoid wiping existing data.
 *
 * We also turn on `tracked` (inventory tracking) and `requiresShipping`
 * (the "This is a physical product" toggle in the admin UI) so Shopify treats
 * new parts as stockable, shippable goods.
 */
function buildVariantInput(part) {
  const inventoryItem = {
    sku: part.PartNum,
    tracked: true,
    requiresShipping: true,
  };
  const weight = Number(part.NetWeight);
  if (Number.isFinite(weight) && weight > 0) {
    inventoryItem.measurement = {
      weight: {
        value: weight,
        unit: mapWeightUnit(part.NetWeightUOM),
      },
    };
  }
  const input = {
    price: formatPrice(part.UnitPrice),
    inventoryItem,
  };
  if (part.ProdCode) input.barcode = String(part.ProdCode);
  return input;
}

/**
 * Extract the OnHandQty from the Epicor MAIN warehouse row. Returns an integer
 * (floored, clamped to 0) so Shopify's InventoryQuantityInput.quantity is
 * valid. Returns null when the part has no MAIN row at all.
 */
function extractMainWarehouseQty(part) {
  const whses = Array.isArray(part.PartWhses) ? part.PartWhses : [];
  const main = whses.find(
    (w) => String(w.WarehouseCode || "").toUpperCase() === EPICOR_MAIN_WAREHOUSE_CODE.toUpperCase()
  );
  if (!main) return null;
  const qty = Number(main.OnHandQty);
  if (!Number.isFinite(qty)) return 0;
  return Math.max(0, Math.floor(qty));
}

async function bulkUpdateVariant(productId, variantId, fields) {
  const mutation = `
    mutation bulk($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
      productVariantsBulkUpdate(productId: $productId, variants: $variants) {
        productVariants { id sku price }
        userErrors { field message }
      }
    }
  `;
  const variables = {
    productId,
    variants: [{ id: variantId, ...fields }],
  };
  const data = await shopifyGraphql(
    mutation,
    variables,
    `shopify:variantBulkUpdate ${variantId}`
  );
  const errs = data?.productVariantsBulkUpdate?.userErrors || [];
  if (errs.length) {
    throw new Error(
      `productVariantsBulkUpdate userErrors: ${JSON.stringify(errs)}`
    );
  }
  return data.productVariantsBulkUpdate.productVariants[0];
}

async function archiveShopifyProduct(productId) {
  const mutation = `
    mutation productUpdate($product: ProductUpdateInput!) {
      productUpdate(product: $product) {
        product { id status }
        userErrors { field message }
      }
    }
  `;
  const data = await shopifyGraphql(
    mutation,
    { product: { id: productId, status: "ARCHIVED" } },
    `shopify:archive ${productId}`
  );
  const errs = data?.productUpdate?.userErrors || [];
  if (errs.length) {
    throw new Error(`productUpdate userErrors: ${JSON.stringify(errs)}`);
  }
  return data.productUpdate.product;
}

function formatPrice(n) {
  if (n === null || n === undefined || n === "") return "0.00";
  const num = Number(n);
  if (!Number.isFinite(num)) return "0.00";
  return num.toFixed(2);
}

// ─────────────────────────────────────────────────────────────────────────────
// Business rules
// ─────────────────────────────────────────────────────────────────────────────

function isActive(part) {
  // Rule 2: active when InActive === false.
  return part.InActive === false;
}
function isWebEnabled(part) {
  // Rule 3: web-enabled when PriceListYN_c === "Y".
  return String(part.PriceListYN_c || "").toUpperCase() === "Y";
}

/**
 * Decide the action for a given Epicor part based on the rules.
 * Returns { action, status, reason } where action is one of:
 *   created, updated_price, archived, skipped_not_web,
 *   skipped_not_found_in_shopify_for_archive, skipped_inactive_not_web
 */
function decideAction(part, shopifyMatch) {
  const active = isActive(part);
  const web = isWebEnabled(part);
  const exists = !!shopifyMatch;

  if (active && web && !exists) return { plan: "create" };
  if (active && web && exists) return { plan: "update_price" };
  if (!active && exists) return { plan: "archive" };
  if (!active && !exists) return { plan: "skipped_inactive_not_web" };
  // Active but not web-enabled:
  if (active && !web && !exists) return { plan: "skipped_not_web" };
  if (active && !web && exists) return { plan: "skipped_not_web" };
  // Fallbacks (should not reach):
  return { plan: "skipped_not_web" };
}

// ─────────────────────────────────────────────────────────────────────────────
// CSV report
// ─────────────────────────────────────────────────────────────────────────────

function buildCsvWriter() {
  if (!fs.existsSync(RUN_CONFIG.csvDir)) {
    fs.mkdirSync(RUN_CONFIG.csvDir, { recursive: true });
  }
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const csvPath = path.join(
    RUN_CONFIG.csvDir,
    `fluidra-epicor-sync-${stamp}.csv`
  );
  const writer = createObjectCsvWriter({
    path: csvPath,
    header: [
      { id: "part_number", title: "part_number" },
      { id: "shopify_sku", title: "shopify_sku" },
      { id: "epicor_active", title: "epicor_active" },
      { id: "epicor_web_enabled", title: "epicor_web_enabled" },
      { id: "shopify_exists", title: "shopify_exists" },
      { id: "action", title: "action" },
      { id: "status", title: "status" },
      { id: "attempt_count", title: "attempt_count" },
      { id: "error_message", title: "error_message" },
      { id: "shopify_product_id", title: "shopify_product_id" },
      { id: "shopify_variant_id", title: "shopify_variant_id" },
      { id: "epicor_record_identifier", title: "epicor_record_identifier" },
      { id: "started_at", title: "started_at" },
      { id: "finished_at", title: "finished_at" },
    ],
  });
  return { writer, csvPath };
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-item processor
// ─────────────────────────────────────────────────────────────────────────────

async function processPart(part, index, totalKnown) {
  const started_at = ts();
  const partNum = part.PartNum;
  const active = isActive(part);
  const web = isWebEnabled(part);

  // Extra blank line before each product (after the first) so every part's
  // processing block is separated by two blank lines in the console.
  if (index > 0) logger.blank();

  const position = totalKnown
    ? `${hl.num(index + 1)}/${hl.num(totalKnown)}`
    : `${hl.num(index + 1)}`;
  logger.info(
    `🔄 [${position}]  PartNum=${hl.sku(partNum)}  active=${active ? hl.ok("true") : hl.bad("false")}  web=${web ? hl.ok("true") : hl.warn("false")}  price=${hl.key(part.UnitPrice)}`
  );

  const row = {
    part_number: partNum,
    shopify_sku: partNum,
    epicor_active: active,
    epicor_web_enabled: web,
    shopify_exists: "",
    action: "",
    status: "",
    attempt_count: 0,
    error_message: "",
    shopify_product_id: "",
    shopify_variant_id: "",
    epicor_record_identifier: part.SysRowID || "",
    started_at,
    finished_at: "",
  };

  try {
    // 1. Look up by SKU (unless clearly unnecessary — but we always look up
    //    because we need to know "exists" for the CSV regardless of decision).
    let match = null;
    try {
      match = await findShopifyByExactSku(partNum);
    } catch (err) {
      // Lookup failure is terminal for this item after retries.
      row.action = "failed";
      row.status = "failed";
      row.shopify_exists = "";
      row.error_message = `shopify:findBySku failed: ${err.message}`;
      row.attempt_count = err.attempts || RUN_CONFIG.maxRetries;
      row.finished_at = ts();
      logger.error(`❌ ${partNum} — lookup failed after retries`, err.message);
      return row;
    }

    row.shopify_exists = !!match;
    if (match) {
      row.shopify_product_id = match.product?.id || "";
      row.shopify_variant_id = match.id || "";
    }

    const decision = decideAction(part, match);

    // 2. Branch on plan
    if (decision.plan === "skipped_not_web") {
      row.action = "skipped_not_web";
      row.status = "skipped";
      row.attempt_count = 1;
      logger.info(`   ⏭️  SKIP ${partNum} — not web-enabled`);
    } else if (decision.plan === "skipped_inactive_not_web") {
      row.action = "skipped_inactive_not_web";
      row.status = "skipped";
      row.attempt_count = 1;
      logger.info(`   ⏭️  SKIP ${partNum} — inactive and not in Shopify`);
    } else if (decision.plan === "archive" && !match) {
      row.action = "skipped_not_found_in_shopify_for_archive";
      row.status = "skipped";
      row.attempt_count = 1;
      logger.info(
        `   ⏭️  SKIP ${partNum} — inactive, not found in Shopify (nothing to archive)`
      );
    } else if (decision.plan === "create") {
      const mainQty = extractMainWarehouseQty(part);
      if (RUN_CONFIG.dryRun) {
        row.action = "created";
        row.status = "success";
        row.attempt_count = 0;
        const qtyNote =
          mainQty == null
            ? "no MAIN warehouse row — inventory unchanged"
            : `inventory qty=${mainQty}`;
        row.error_message = `[DRY RUN] would create DRAFT product + publish to Online Store; ${qtyNote}`;
        logger.info(
          `   🟨 [DRY RUN] would CREATE ${partNum} + publish to Online Store (${qtyNote})`
        );
      } else {
        const { productId, variantId, inventoryItemId } =
          await createShopifyDraftProduct(part);
        row.action = "created";
        row.status = "success";
        row.shopify_product_id = productId;
        row.shopify_variant_id = variantId;
        row.attempt_count = 1;
        logger.info(
          `   ✅ CREATED ${partNum} as DRAFT product=${productId} variant=${variantId}`
        );
        if (mainQty == null) {
          logger.warn(
            `   ⚠️  ${partNum} has no ${EPICOR_MAIN_WAREHOUSE_CODE} warehouse row — leaving inventory unset`
          );
        } else if (!inventoryItemId) {
          logger.warn(
            `   ⚠️  ${partNum} — productCreate returned no inventoryItem id; cannot set qty=${mainQty}`
          );
        } else {
          const locationId = await getShopifyLocationId();
          await setInventoryOnHand(inventoryItemId, locationId, mainQty);
          logger.info(
            `   📦 INVENTORY ${partNum} set qty=${hl.num(mainQty)} at ${hl.key(locationId)}`
          );
        }
        await publishToOnlineStore(productId);
        logger.info(`   🛒 PUBLISHED ${partNum} to Online Store`);
      }
    } else if (decision.plan === "update_price") {
      const mainQty = extractMainWarehouseQty(part);
      if (RUN_CONFIG.dryRun) {
        row.action = "updated_price";
        row.status = "success";
        row.attempt_count = 0;
        const qtyNote =
          mainQty == null
            ? "no MAIN warehouse row — inventory unchanged"
            : `inventory qty=${mainQty}`;
        row.error_message = `[DRY RUN] would set price=${formatPrice(part.UnitPrice)}; ${qtyNote}`;
        logger.info(
          `   🟨 [DRY RUN] would UPDATE price ${partNum} → ${formatPrice(part.UnitPrice)} (${qtyNote})`
        );
      } else {
        await bulkUpdateVariant(match.product.id, match.id, {
          price: formatPrice(part.UnitPrice),
        });
        row.action = "updated_price";
        row.status = "success";
        row.attempt_count = 1;
        logger.info(
          `   ✅ UPDATED ${partNum} price → ${formatPrice(part.UnitPrice)}`
        );
        const inventoryItemId = match.inventoryItem?.id || null;
        if (mainQty == null) {
          logger.warn(
            `   ⚠️  ${partNum} has no ${EPICOR_MAIN_WAREHOUSE_CODE} warehouse row — leaving inventory unchanged`
          );
        } else if (!inventoryItemId) {
          logger.warn(
            `   ⚠️  ${partNum} — variant has no inventoryItem id; cannot set qty=${mainQty}`
          );
        } else {
          const locationId = await getShopifyLocationId();
          await setInventoryOnHand(inventoryItemId, locationId, mainQty);
          logger.info(
            `   📦 INVENTORY ${partNum} set qty=${hl.num(mainQty)} at ${hl.key(locationId)}`
          );
        }
      }
    } else if (decision.plan === "archive") {
      if (RUN_CONFIG.dryRun) {
        row.action = "archived";
        row.status = "success";
        row.attempt_count = 0;
        row.error_message = "[DRY RUN] would archive product";
        logger.info(`   🟨 [DRY RUN] would ARCHIVE ${partNum}`);
      } else {
        await archiveShopifyProduct(match.product.id);
        row.action = "archived";
        row.status = "success";
        row.attempt_count = 1;
        logger.info(`   ✅ ARCHIVED ${partNum} product=${match.product.id}`);
      }
    }
  } catch (err) {
    row.action = "failed";
    row.status = "failed";
    row.attempt_count = err.attempts || RUN_CONFIG.maxRetries;
    row.error_message = (err.response?.data
      ? JSON.stringify(err.response.data)
      : err.message
    ).slice(0, 4000);
    logger.error(`❌ ${partNum} — processing failed after retries`, row.error_message);
  }

  row.finished_at = ts();
  return row;
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

async function main() {
  const runStart = Date.now();
  logger.banner("🚀  Fluidra Epicor → Shopify product sync");
  logger.info("Config summary", {
    epicorBaseUrl: epicorConfig.baseUrl,
    epicorCompany: epicorConfig.company,
    shopifyStore: shopifyConfig.storeName,
    shopifyApiVersion: shopifyConfig.apiVersion,
    pageSize: RUN_CONFIG.pageSize,
    maxRetries: RUN_CONFIG.maxRetries,
    dryRun: RUN_CONFIG.dryRun,
    firstPageOnly: RUN_CONFIG.firstPageOnly,
    maxItems: RUN_CONFIG.maxItems || "no cap",
  });

  const { writer: csvWriter, csvPath } = buildCsvWriter();
  logger.info(`📝 CSV report → ${hl.key(csvPath)}`);
  logger.blank();

  const totals = {
    processed: 0,
    created: 0,
    updated_price: 0,
    archived: 0,
    skipped_not_web: 0,
    skipped_inactive_not_web: 0,
    skipped_not_found_in_shopify_for_archive: 0,
    failed: 0,
  };

  // Always attempt to write whatever we have, even on unexpected shutdown.
  let pendingRows = [];
  const flushRows = async () => {
    if (pendingRows.length === 0) return;
    const toWrite = pendingRows;
    pendingRows = [];
    try {
      await csvWriter.writeRecords(toWrite);
    } catch (err) {
      logger.error("Failed to flush CSV rows", err.message);
      // Put them back so we don't lose them on next flush attempt.
      pendingRows = toWrite.concat(pendingRows);
    }
  };

  const shutdown = async (signal) => {
    logger.warn(`Received ${signal} — flushing CSV and exiting…`);
    await flushRows();
    process.exit(130);
  };
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  try {
    await epicorHealthCheck();
  } catch (err) {
    logger.error("Epicor authentication failed — aborting.", err.message);
    await flushRows();
    process.exitCode = 1;
    return;
  }

  // ── PHASE 1: Fetch every Epicor part into memory so we know the true total.
  let allParts = [];
  try {
    allParts = await fetchAllEpicorParts();
  } catch (err) {
    logger.error(
      "Epicor fetch failed after retries — aborting run.",
      err.message
    );
    await flushRows();
    process.exitCode = 1;
    return;
  }

  const totalCount = allParts.length;

  logger.blank();
  logger.info(
    `📊 ${hl.key("Total Epicor Parts fetched")}: ${hl.num(totalCount)}  (every one will be processed)`
  );
  logger.blank();

  if (RUN_CONFIG.maxItems > 0 && totalCount > RUN_CONFIG.maxItems) {
    logger.warn(
      `⛔ MAX_ITEMS=${RUN_CONFIG.maxItems} — only the first ${RUN_CONFIG.maxItems} of ${totalCount} will be processed.`
    );
    allParts = allParts.slice(0, RUN_CONFIG.maxItems);
  }

  // ── PHASE 2: Iterate the in-memory array and apply the Shopify sync rules.
  logger.section(`Phase 2 / 2  —  Syncing ${allParts.length} parts to Shopify`);
  logger.blank();

  const denom = allParts.length;
  for (let i = 0; i < allParts.length; i++) {
    const part = allParts[i];
    const row = await processPart(part, i, denom);
    totals.processed++;
    if (totals[row.action] !== undefined) totals[row.action]++;
    if (row.status === "failed") totals.failed++;

    pendingRows.push(row);
    // Flush frequently so the CSV survives crashes.
    if (pendingRows.length >= 25) await flushRows();

    if (totals.processed % 25 === 0 || totals.processed === denom) {
      const skipped =
        totals.skipped_not_web +
        totals.skipped_inactive_not_web +
        totals.skipped_not_found_in_shopify_for_archive;
      const remaining = Math.max(0, denom - totals.processed);
      const pct =
        denom > 0 ? ((totals.processed / denom) * 100).toFixed(1) + "%" : "0%";
      logger.blank();
      logger.info(
        `📈 Progress  ${hl.num(totals.processed)}/${hl.num(denom)}  (${hl.key(pct)})  remaining=${hl.num(remaining)}`
      );
      logger.info(
        `   created=${hl.ok(totals.created)}  updated=${hl.ok(totals.updated_price)}  archived=${hl.warn(totals.archived)}  skipped=${hl.key(skipped)}  failed=${totals.failed > 0 ? hl.bad(totals.failed) : hl.ok(totals.failed)}`
      );
      logger.blank();
    }

    if (RUN_CONFIG.perItemDelayMs > 0) {
      await sleep(RUN_CONFIG.perItemDelayMs);
    }
  }

  await flushRows();

  const elapsedSec = ((Date.now() - runStart) / 1000).toFixed(1);
  logger.blank();
  logger.banner("🏁  Sync run complete");
  logger.info(`CSV: ${hl.key(csvPath)}`);
  logger.info(`Elapsed: ${hl.num(elapsedSec)}s`);
  logger.info(
    `Epicor total: ${hl.num(totalCount)}  —  Processed: ${hl.num(totals.processed)}`
  );
  logger.info("Totals by action:", totals);
  logger.blank();
}

main().catch((err) => {
  logger.error("💥 Unhandled error in main()", err.message);
  process.exitCode = 1;
});
