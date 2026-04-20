/**
 * Fluidra — Sync customers from Epicor into Shopify Companies
 *
 * Reads all Customer records from Epicor (OpenAPI spec:
 * Fluidra/openapi-specs/CustomersSvc.json), expands the ShipToes child
 * collection, and applies B2B Company sync rules against Shopify:
 *
 *   - CREATE company (+ locations + metafield) when the Epicor customer is a
 *     web customer (WebCustomer=true), is not inactive, and no Shopify Company
 *     with a matching externalId exists.
 *   - UPDATE company (+ ensure metafield + sync locations) when the Shopify
 *     Company already exists and is not inactive in Epicor.
 *   - SKIP   when the Epicor customer is not a web customer and no Shopify
 *     company exists yet (skipped_not_web_customer / skipped_inactive).
 *   - MANUAL_REVIEW when the Epicor customer became inactive but a Shopify
 *     Company already exists. Shopify's Admin GraphQL API does not expose a
 *     real "inactive/archive" state for B2B Companies, so we surface the
 *     company in the CSV for an operator to handle manually.
 *
 * Matching rules:
 *   - Shopify Company match key: Company.externalId == Epicor CustNum.
 *   - Shopify CompanyLocation match key (preferred):
 *       CompanyLocation.externalId == Epicor ShipTo.ExternalID
 *     Fallback (when Epicor ShipTo.ExternalID is blank): a deterministic
 *     identifier built from the Epicor compound ShipTo key, formatted as
 *     `epicor:<CustNum>:<ShipToNum>`. We always *write* this fallback into
 *     the Shopify CompanyLocation.externalId on create, so subsequent runs
 *     can re-match the same ShipTo without depending on volatile data.
 *
 * Payment terms:
 *   Epicor TermsCode → Shopify PaymentTermsTemplate GID (see PAYMENT_TERMS_MAP).
 *   Unknown codes log a warning and the location is created/updated without
 *   payment terms — never fatal.
 *
 * Run:
 *   cd Fluidra && npm install && node fluidra-syncCustomersFromEpicor.js
 *
 * Required secrets in Fluidra/.env (copy from .env.example):
 *   EPICOR_BASE_URL, EPICOR_COMPANY, EPICOR_USERNAME, EPICOR_PASSWORD, EPICOR_API_KEY
 *   SHOPIFY_STORE_NAME, SHOPIFY_ACCESS_TOKEN
 * Optional: SHOPIFY_API_VERSION, EPICOR_PAGE_SIZE, EPICOR_TIMEOUT_MS, SHOPIFY_TIMEOUT_MS,
 *   MAX_RETRIES, RETRY_BASE_DELAY_MS, PER_ITEM_DELAY_MS, PER_PAGE_DELAY_MS,
 *   LOCATION_CONCURRENCY, MAX_ITEMS, DRY_RUN, FIRST_PAGE_ONLY, REPORT_DIR, NO_COLOR, DEBUG
 */

const { requireEnv } = require("./load-env");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { createObjectCsvWriter } = require("csv-writer");

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

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
};


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
  perItemDelayMs: parseInt(process.env.PER_ITEM_DELAY_MS || "150", 10),
  perPageDelayMs: parseInt(process.env.PER_PAGE_DELAY_MS || "500", 10),
  // Intra-customer concurrency for ShipTo → CompanyLocation work. Customers
  // with thousands of ship-tos benefit from parallelising the per-location
  // mutations; Shopify's throttle still serialises us under the hood, but
  // round-trip latency stops being the bottleneck.
  locationConcurrency: Math.max(
    1,
    parseInt(process.env.LOCATION_CONCURRENCY || "4", 10)
  ),
  // Inline-expand cap for ShipToes on the customers page request. If a
  // customer hits this cap we top up the rest with paginated child-endpoint
  // calls (see fetchRemainingShipToesForCustomer). Tuned to comfortably cover
  // typical customers without round-tripping while still being safe at scale.
  shipToExpandTop: parseInt(process.env.SHIPTO_EXPAND_TOP || "500", 10),
  // Page size used when topping up ShipToes for customers that exceeded the
  // inline expand cap. Also the page size for paginating existing Shopify
  // CompanyLocations in fetchAllCompanyLocations.
  shipToPageSize: parseInt(process.env.SHIPTO_PAGE_SIZE || "500", 10),
  maxItems: parseInt(process.env.MAX_ITEMS || "0", 10),
  dryRun: /^(1|true|yes)$/i.test(process.env.DRY_RUN || ""),
  firstPageOnly: /^(1|true|yes)$/i.test(process.env.FIRST_PAGE_ONLY || ""),
  csvDir:
    process.env.REPORT_DIR ||
    path.join(__dirname, "fluidra_sync_reports"),
};

// Epicor TermsCode → Shopify PaymentTermsTemplate GID. Unknown codes are
// tolerated: the location is created/updated without payment terms and a
// warning is logged + recorded in the CSV.
const PAYMENT_TERMS_MAP = {
  N90: "gid://shopify/PaymentTermsTemplate/6",
  N60: "gid://shopify/PaymentTermsTemplate/5",
  N30: "gid://shopify/PaymentTermsTemplate/4",
  G90: "gid://shopify/PaymentTermsTemplate/6",
  DUR: "gid://shopify/PaymentTermsTemplate/9",
};

// 3-letter ISO → 2-letter ISO for the most common cases we see in the Epicor
// data set. Unmapped 3-letter codes fall back to the first 2 chars uppercased
// only when the source value is exactly 3 letters; otherwise we trust the
// source if it is already 2 letters, and ultimately default to "US".
const COUNTRY_ISO3_TO_ISO2 = {
  USA: "US",
  CAN: "CA",
  MEX: "MX",
  GBR: "GB",
  FRA: "FR",
  DEU: "DE",
  ESP: "ES",
  ITA: "IT",
  PRT: "PT",
  AUS: "AU",
  NZL: "NZ",
  IRL: "IE",
  NLD: "NL",
  BEL: "BE",
  CHE: "CH",
};

// ─────────────────────────────────────────────────────────────────────────────
// Logging — ANSI colors + readable structure (mirrors fluidra-syncProductsFromEpicor.js).
// ─────────────────────────────────────────────────────────────────────────────

const COLOR_ENABLED =
  !process.env.NO_COLOR && process.stdout.isTTY !== false;

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
  const levelColored =
    {
      INFO: color.cyan("INFO "),
      WARN: color.yellow("WARN "),
      ERROR: color.boldRed("ERROR"),
      DEBUG: color.gray("DEBUG"),
    }[level] || level;
  const timestamp = color.gray(`[${ts()}]`);
  const body = `${timestamp} ${levelColored} ${msg}${formatExtra(extra)}`;
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
  blank: () => console.log(""),
  banner: (msg) => {
    const bar = color.boldCyan("═".repeat(78));
    console.log(`\n${bar}\n${color.boldCyan("  " + msg)}\n${bar}\n\n`);
  },
  section: (msg) => {
    console.log(`\n${color.boldMagenta("─── " + msg + " ───")}\n\n`);
  },
};

const hl = {
  cust: (s) => color.boldYellow(s),
  num: (n) => color.boldCyan(String(n)),
  ok: (s) => color.boldGreen(s),
  warn: (s) => color.boldYellow(s),
  bad: (s) => color.boldRed(s),
  key: (s) => color.bold(s),
};

// ─────────────────────────────────────────────────────────────────────────────
// Generic helpers: sleep, retry with exponential backoff + jitter, async pool
// ─────────────────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Escape a string for safe inclusion inside an OData single-quoted literal.
 * OData v4 escapes single quotes by doubling them: O'Brien → 'O''Brien'.
 */
function odataEscapeString(v) {
  return String(v).replace(/'/g, "''");
}

function isRetryableAxiosError(err) {
  if (!err) return false;
  if (
    err.code === "ECONNABORTED" ||
    err.code === "ETIMEDOUT" ||
    err.code === "ECONNRESET" ||
    err.code === "ENOTFOUND" ||
    err.code === "EAI_AGAIN"
  )
    return true;
  const status = err.response?.status;
  if (!status) return true;
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
      const delay =
        base * Math.pow(2, attempt - 1) + Math.floor(Math.random() * 500);
      logger.info(`   ↻ retrying ${label} in ${delay}ms…`);
      await sleep(delay);
    }
  }
}

/**
 * Run `worker(item, index)` over `items` with a fixed concurrency cap. Returns
 * an array of results in the same order as `items`. Worker errors are caught
 * and surfaced as { error } entries so a single bad customer never aborts the
 * whole run; callers are responsible for converting them into CSV rows.
 */
async function asyncPool(concurrency, items, worker) {
  const results = new Array(items.length);
  let cursor = 0;
  const runners = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (true) {
      const i = cursor++;
      if (i >= items.length) return;
      try {
        results[i] = await worker(items[i], i);
      } catch (err) {
        results[i] = { __workerError: err };
      }
    }
  });
  await Promise.all(runners);
  return results;
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
 * Verify Epicor credentials early via the CustomerSvc service document.
 * Spec: GET /{currentCompany}/Erp.BO.CustomerSvc/
 */
async function epicorHealthCheck() {
  const url = `${epicorConfig.baseUrl}/${encodeURIComponent(
    epicorConfig.company
  )}/Erp.BO.CustomerSvc/`;
  logger.info("🔐 Authenticating against Epicor (CustomerSvc)…", { url });
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
 * Fetch one Customers page with the ShipToes child collection expanded.
 * Spec: GET /{currentCompany}/Erp.BO.CustomerSvc/Customers
 * Keyset pagination is on CustNum (numeric, monotonic, unique within Company).
 */
async function fetchEpicorCustomersPage({ top, afterCustNum }) {
  const url = `${epicorConfig.baseUrl}/${encodeURIComponent(
    epicorConfig.company
  )}/Erp.BO.CustomerSvc/Customers`;

  // Customer-level fields required by the business rules. Names are taken
  // straight from the OpenAPI spec (Erp.Customer schema).
  const customerSelect = [
    "Company",
    "CustID",
    "CustNum",
    "Name",
    "City",
    "State",
    "Zip",
    "Country",
    "PhoneNum",
    "Inactive",
    "CountryISOCode",
    "BillToEmail_c",
    "WebCustomer",
    "TermsCode",
    "SysRowID",
  ].join(",");

  // ShipTo fields needed to build a Shopify CompanyLocation. Names taken from
  // Erp.ShipTo. Note the schema spells the postal code field as `ZIP` (upper
  // case) on ShipTo but `Zip` on the Customer — that asymmetry matters.
  const shipToSelect = [
    "Company",
    "CustNum",
    "ShipToNum",
    "Name",
    "PhoneNum",
    "LangNameID",
    "ExternalID",
    "RefNotes",
    "Address1",
    "Address2",
    "City",
    "ZIP",
    "State",
    "Country",
    "CountryISOCode",
    "PrimaryShipTo",
    "ContactName",
    "EMailAddress",
    "SysRowID",
  ].join(",");

  const filters = [];
  if (afterCustNum != null) {
    filters.push(`CustNum gt ${Number(afterCustNum)}`);
  }

  // Inline-expand ShipToes with a $top cap so we don't blow up payload sizes
  // on customers that own thousands of ship-tos. Anything beyond this cap is
  // paginated via fetchRemainingShipToesForCustomer.
  const params = {
    $top: top,
    $orderby: "CustNum",
    $select: customerSelect,
    $expand: `ShipToes($top=${RUN_CONFIG.shipToExpandTop};$orderby=ShipToNum;$select=${shipToSelect})`,
  };
  if (filters.length) params.$filter = filters.join(" and ");

  const { result } = await withRetry(
    `epicor:Customers page after=${afterCustNum ?? "(start)"} top=${top}`,
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
  return { items };
}

/**
 * Fetch additional ShipTo records for one customer beyond the inline $expand
 * cap. Hits the dedicated child endpoint defined in the OpenAPI spec:
 *   GET /{Company}/Erp.BO.CustomerSvc/Customers('{Company}',{CustNum})/ShipToes
 * Pages with $top/$skip ordered by ShipToNum until a short page is returned.
 *
 * `skip` is the number of rows the caller has already collected (typically
 * SHIPTO_EXPAND_TOP), so the first additional page starts at the right offset.
 */
async function fetchRemainingShipToesForCustomer(customer, skip) {
  const customerCompany = customer.Company || epicorConfig.company;
  const compSegment = encodeURIComponent(epicorConfig.company);
  // Compound key in the URL path: ('TAYLOR',12345). Apostrophes must be
  // percent-encoded so HTTP intermediaries don't reject the URL; the OData
  // string-literal escaping (doubling) is applied to the company value first.
  const keySegment = `(%27${encodeURIComponent(
    odataEscapeString(customerCompany)
  )}%27,${Number(customer.CustNum)})`;
  const url =
    `${epicorConfig.baseUrl}/${compSegment}` +
    `/Erp.BO.CustomerSvc/Customers${keySegment}/ShipToes`;

  const shipToSelect = [
    "Company",
    "CustNum",
    "ShipToNum",
    "Name",
    "PhoneNum",
    "LangNameID",
    "ExternalID",
    "RefNotes",
    "Address1",
    "Address2",
    "City",
    "ZIP",
    "State",
    "Country",
    "CountryISOCode",
    "PrimaryShipTo",
    "ContactName",
    "EMailAddress",
    "SysRowID",
  ].join(",");

  const top = RUN_CONFIG.shipToPageSize;
  const all = [];
  let currentSkip = Number(skip) || 0;
  let pageNo = 0;

  while (true) {
    pageNo++;
    const { result } = await withRetry(
      `epicor:ShipToes cust=${customer.CustNum} page=${pageNo} skip=${currentSkip}`,
      async () => {
        const res = await axios.get(url, {
          headers: epicorAuthHeaders(),
          params: {
            $top: top,
            $skip: currentSkip,
            $orderby: "ShipToNum",
            $select: shipToSelect,
          },
          timeout: RUN_CONFIG.epicorRequestTimeoutMs,
        });
        return res.data;
      }
    );
    const rows = Array.isArray(result.value) ? result.value : [];
    all.push(...rows);
    if (rows.length < top) break;
    currentSkip += rows.length;
  }

  return all;
}

/**
 * Fetch every Customer (with expanded ShipToes) using keyset pagination on
 * CustNum. Epicor's @odata.count is unreliable, so we rely on short pages /
 * MAX_ITEMS to know when to stop — same pattern as the products sync.
 *
 * For customers whose ShipToes hit the inline-expand cap, we top up the rest
 * via fetchRemainingShipToesForCustomer with bounded concurrency. This keeps
 * memory usage predictable while supporting customers with thousands of
 * ship-tos.
 */
async function fetchAllEpicorCustomers() {
  const all = [];
  let page = 0;
  let afterCustNum = null;

  logger.section("Phase 1 / 2  —  Fetching all Epicor Customers (with ShipToes)");

  while (true) {
    page++;
    logger.info(
      `📄 Fetching page ${hl.num(page)}  (afterCustNum=${hl.key(
        afterCustNum ?? "(start)"
      )}, top=${hl.num(RUN_CONFIG.pageSize)})`
    );

    const { items } = await fetchEpicorCustomersPage({
      afterCustNum,
      top: RUN_CONFIG.pageSize,
    });

    // Top up ShipToes for any customer that hit the inline-expand cap. We
    // detect "potentially truncated" by an exact-equality on the cap; in
    // theory a customer could have *exactly* the cap and trigger one wasted
    // round-trip that returns zero, which is acceptable.
    const truncated = items.filter(
      (c) =>
        Array.isArray(c.ShipToes) &&
        c.ShipToes.length === RUN_CONFIG.shipToExpandTop
    );
    if (truncated.length > 0) {
      logger.info(
        `   ↪︎ ${hl.num(truncated.length)} customer(s) on this page hit the ShipTo expand cap (${RUN_CONFIG.shipToExpandTop}) — fetching additional pages`
      );
      await asyncPool(3, truncated, async (cust) => {
        const more = await fetchRemainingShipToesForCustomer(
          cust,
          RUN_CONFIG.shipToExpandTop
        );
        if (more.length > 0) {
          cust.ShipToes.push(...more);
          logger.info(
            `     ↪︎ ${hl.cust(cust.CustID)} (CustNum=${cust.CustNum}): +${hl.num(
              more.length
            )} extra ShipToes  —  total ${hl.num(cust.ShipToes.length)}`
          );
        }
      });
    }

    all.push(...items);
    logger.info(
      `   ✅ Page ${hl.num(page)}: received ${hl.num(
        items.length
      )} customers  —  running total: ${hl.num(all.length)}`
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

    const lastCustNum = items[items.length - 1]?.CustNum;
    if (lastCustNum == null) {
      logger.warn("Last row has no CustNum — cannot advance keyset cursor. Stopping.");
      break;
    }
    if (afterCustNum === lastCustNum) {
      logger.warn(
        `Keyset cursor did not advance past CustNum=${afterCustNum}. Stopping to avoid an infinite loop.`
      );
      break;
    }
    afterCustNum = lastCustNum;

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
    throw new Error(`Shopify GraphQL errors: ${JSON.stringify(result.errors)}`);
  }
  return result.data;
}

/**
 * Look up a Shopify Company by externalId. Returns the Company's identity
 * fields only — locations are loaded lazily via fetchAllCompanyLocations
 * because companies can own thousands of locations and inlining the
 * connection would blow our GraphQL cost budget.
 *
 * Shopify's `companies` connection accepts a search query string — the
 * `external_id:<value>` filter is the documented way to look up by externalId.
 * We post-filter to guarantee an exact match, since search may tokenize.
 */
async function findShopifyCompanyByExternalId(externalId) {
  if (!externalId) return null;
  const query = `
    query findCompany($q: String!) {
      companies(first: 5, query: $q) {
        nodes {
          id
          name
          externalId
        }
      }
    }
  `;
  const data = await shopifyGraphql(
    query,
    { q: `external_id:${externalId}` },
    `shopify:findCompany ext=${externalId}`
  );
  const nodes = data?.companies?.nodes || [];
  const exact = nodes.find(
    (n) => String(n.externalId || "").trim() === String(externalId).trim()
  );
  return exact || null;
}

/**
 * Page through every CompanyLocation on a given Shopify Company. Used to
 * build the externalId → location dedupe map for ShipTo sync. Pages of 250
 * with cursor-based pagination — `companies(...)` is the search endpoint;
 * for a single Company we use the Node interface to scope the connection
 * tightly so the cost stays bounded per page.
 */
async function fetchAllCompanyLocations(companyId) {
  const query = `
    query CompanyLocations($id: ID!, $first: Int!, $after: String) {
      company(id: $id) {
        id
        locations(first: $first, after: $after) {
          pageInfo { hasNextPage endCursor }
          nodes { id name externalId }
        }
      }
    }
  `;
  const all = [];
  let cursor = null;
  let pageNo = 0;
  while (true) {
    pageNo++;
    const data = await shopifyGraphql(
      query,
      { id: companyId, first: 250, after: cursor },
      `shopify:companyLocations ${companyId} page=${pageNo}`
    );
    const conn = data?.company?.locations;
    if (!conn) break;
    all.push(...(conn.nodes || []));
    if (!conn.pageInfo?.hasNextPage) break;
    cursor = conn.pageInfo.endCursor;
  }
  return all;
}

/**
 * Create a Shopify Company. When `firstLocation` is provided, Shopify creates
 * that CompanyLocation atomically as part of the same mutation — this is how
 * we avoid ending up with a phantom default location plus a duplicate of the
 * first ShipTo. The returned `locations` array contains whatever Shopify
 * created (zero or one entry), which the caller uses to seed the dedupe map
 * for the rest of the ShipToes.
 */
async function createShopifyCompany({ name, externalId, firstLocation }) {
  const mutation = `
    mutation companyCreate($input: CompanyCreateInput!) {
      companyCreate(input: $input) {
        company {
          id
          name
          externalId
          locations(first: 5) {
            nodes { id name externalId }
          }
        }
        userErrors { field message code }
      }
    }
  `;
  const input = {
    company: {
      name,
      externalId: String(externalId),
    },
  };
  if (firstLocation) input.companyLocation = firstLocation;

  const data = await shopifyGraphql(
    mutation,
    { input },
    `shopify:companyCreate ext=${externalId}`
  );
  const errs = data?.companyCreate?.userErrors || [];
  if (errs.length) {
    throw new Error(`companyCreate userErrors: ${JSON.stringify(errs)}`);
  }
  const company = data.companyCreate.company;
  return {
    company: { id: company.id, name: company.name, externalId: company.externalId },
    locations: company.locations?.nodes || [],
  };
}

async function updateShopifyCompany(companyId, { name, externalId }) {
  const mutation = `
    mutation companyUpdate($companyId: ID!, $input: CompanyInput!) {
      companyUpdate(companyId: $companyId, input: $input) {
        company { id name externalId }
        userErrors { field message code }
      }
    }
  `;
  const input = {};
  if (name) input.name = name;
  if (externalId) input.externalId = String(externalId);
  if (Object.keys(input).length === 0) return null;
  const data = await shopifyGraphql(
    mutation,
    { companyId, input },
    `shopify:companyUpdate ${companyId}`
  );
  const errs = data?.companyUpdate?.userErrors || [];
  if (errs.length) {
    throw new Error(`companyUpdate userErrors: ${JSON.stringify(errs)}`);
  }
  return data.companyUpdate.company;
}

/**
 * Set the `custom.epicor_customer_id` metafield on the Shopify Company.
 * `metafieldsSet` is upsert by (ownerId, namespace, key) so it's safe to call
 * on every run.
 */
async function setCompanyEpicorMetafield(companyId, custId) {
  if (!custId) return null;
  const mutation = `
    mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields { id namespace key value }
        userErrors { field message code }
      }
    }
  `;
  const data = await shopifyGraphql(
    mutation,
    {
      metafields: [
        {
          ownerId: companyId,
          namespace: "custom",
          key: "epicor_customer_id",
          type: "single_line_text_field",
          value: String(custId),
        },
      ],
    },
    `shopify:metafieldsSet ${companyId}`
  );
  const errs = data?.metafieldsSet?.userErrors || [];
  if (errs.length) {
    throw new Error(`metafieldsSet userErrors: ${JSON.stringify(errs)}`);
  }
  return data.metafieldsSet.metafields?.[0] || null;
}

async function createShopifyCompanyLocation(companyId, input) {
  const mutation = `
    mutation companyLocationCreate($companyId: ID!, $input: CompanyLocationInput!) {
      companyLocationCreate(companyId: $companyId, input: $input) {
        companyLocation { id name externalId }
        userErrors { field message code }
      }
    }
  `;
  const data = await shopifyGraphql(
    mutation,
    { companyId, input },
    `shopify:companyLocationCreate ${input.externalId || input.name}`
  );
  const errs = data?.companyLocationCreate?.userErrors || [];
  if (errs.length) {
    throw new Error(`companyLocationCreate userErrors: ${JSON.stringify(errs)}`);
  }
  return data.companyLocationCreate.companyLocation;
}

async function updateShopifyCompanyLocation(locationId, input) {
  const mutation = `
    mutation companyLocationUpdate($companyLocationId: ID!, $input: CompanyLocationUpdateInput!) {
      companyLocationUpdate(companyLocationId: $companyLocationId, input: $input) {
        companyLocation { id name externalId }
        userErrors { field message code }
      }
    }
  `;
  const data = await shopifyGraphql(
    mutation,
    { companyLocationId: locationId, input },
    `shopify:companyLocationUpdate ${locationId}`
  );
  const errs = data?.companyLocationUpdate?.userErrors || [];
  if (errs.length) {
    throw new Error(`companyLocationUpdate userErrors: ${JSON.stringify(errs)}`);
  }
  return data.companyLocationUpdate.companyLocation;
}

async function assignCompanyLocationAddress(locationId, address, addressTypes) {
  const mutation = `
    mutation companyLocationAssignAddress(
      $locationId: ID!
      $address: CompanyAddressInput!
      $addressTypes: [CompanyAddressType!]!
    ) {
      companyLocationAssignAddress(
        locationId: $locationId
        address: $address
        addressTypes: $addressTypes
      ) {
        addresses { id addressType }
        userErrors { field message code }
      }
    }
  `;
  const data = await shopifyGraphql(
    mutation,
    { locationId, address, addressTypes },
    `shopify:companyLocationAssignAddress ${locationId}`
  );
  const errs = data?.companyLocationAssignAddress?.userErrors || [];
  if (errs.length) {
    throw new Error(
      `companyLocationAssignAddress userErrors: ${JSON.stringify(errs)}`
    );
  }
  return data.companyLocationAssignAddress.addresses || [];
}

// ─────────────────────────────────────────────────────────────────────────────
// Field mappers / sanitizers
// ─────────────────────────────────────────────────────────────────────────────

function isWebCustomer(cust) {
  return cust.WebCustomer === true;
}

function isInactive(cust) {
  return cust.Inactive === true;
}

/**
 * Map an Epicor country value to a Shopify ISO alpha-2 country code.
 * Order of preference: 2-letter input → as-is; 3-letter known input → mapped;
 * blank / unknown → "US" (the data set is US-centric).
 */
function normalizeCountryCode(raw) {
  const v = String(raw || "").trim().toUpperCase();
  if (!v) return "US";
  if (/^[A-Z]{2}$/.test(v)) return v;
  if (/^[A-Z]{3}$/.test(v) && COUNTRY_ISO3_TO_ISO2[v]) {
    return COUNTRY_ISO3_TO_ISO2[v];
  }
  // Unrecognised free-text country (e.g. "United States") — best-effort default.
  return "US";
}

/**
 * Strip double quotes from address lines per the prompt and collapse whitespace.
 */
function sanitizeAddressLine(s) {
  if (s == null) return "";
  return String(s).replace(/"/g, "").replace(/\s+/g, " ").trim();
}

/**
 * Sanitize a zip/postal code for Shopify. Shopify validates zips against
 * country-specific patterns and rejects free-text like "N/A", "TBD", or
 * malformed values. We strip obviously bad data here; if Shopify still
 * rejects the zip, the caller retries without it (see processOne).
 */
function sanitizeZip(raw, countryCode) {
  let v = String(raw || "").trim();
  if (!v) return "";
  // Strip surrounding quotes and collapse whitespace.
  v = v.replace(/"/g, "").replace(/\s+/g, " ").trim();
  // Reject placeholder / junk values.
  if (/^(n\/?a|tbd|none|unknown|\.\.\.|---|-|0+)$/i.test(v)) return "";
  // US: keep only 5-digit or ZIP+4 patterns.
  if (countryCode === "US") {
    const usMatch = v.match(/^(\d{5})(?:[-\s]?(\d{4}))?/);
    if (!usMatch) return "";
    return usMatch[2] ? `${usMatch[1]}-${usMatch[2]}` : usMatch[1];
  }
  // CA: normalize A1A 1A1 / A1A1A1 patterns.
  if (countryCode === "CA") {
    const ca = v.replace(/\s+/g, "").toUpperCase();
    if (/^[A-Z]\d[A-Z]\d[A-Z]\d$/.test(ca)) {
      return `${ca.slice(0, 3)} ${ca.slice(3)}`;
    }
    if (/^[A-Z]\d[A-Z]\s?\d[A-Z]\d$/.test(v.toUpperCase())) return v.toUpperCase();
    return "";
  }
  return v;
}

/**
 * Best-effort split of "First Last" / "Last, First" into firstName/lastName.
 * Used when we have only ContactName but Shopify wants split fields.
 */
function splitContactName(raw) {
  const name = String(raw || "").trim();
  if (!name) return { firstName: "", lastName: "" };
  if (name.includes(",")) {
    const [last, first] = name.split(",").map((s) => s.trim());
    return { firstName: first || "", lastName: last || "" };
  }
  const parts = name.split(/\s+/);
  if (parts.length === 1) return { firstName: parts[0], lastName: "" };
  return { firstName: parts[0], lastName: parts.slice(1).join(" ") };
}

/**
 * Resolve the Shopify CompanyLocation externalId we want to write for a given
 * Epicor ShipTo. Prefer ShipTo.ExternalID when present; otherwise fall back to
 * a deterministic `epicor:<CustNum>:<ShipToNum>` identifier so re-runs match
 * the same row without depending on volatile ShipTo fields.
 */
function resolveShipToExternalId(shipTo, custNum) {
  const fromEpicor = String(shipTo.ExternalID || "").trim();
  if (fromEpicor) return fromEpicor;
  const stNum = String(shipTo.ShipToNum || "").trim();
  return `epicor:${custNum}:${stNum}`;
}

/**
 * Map an Epicor TermsCode to a Shopify PaymentTermsTemplate GID.
 * Returns { id, code, mapped } where `mapped=false` means we couldn't resolve
 * the code and the caller should proceed without payment terms (and warn).
 */
function mapPaymentTerms(termsCode) {
  const code = String(termsCode || "").trim().toUpperCase();
  if (!code) return { id: null, code, mapped: false, reason: "no terms code" };
  const id = PAYMENT_TERMS_MAP[code];
  if (!id) return { id: null, code, mapped: false, reason: "unmapped terms code" };
  return { id, code, mapped: true };
}

/**
 * Build the CompanyLocationInput for create/update from an Epicor ShipTo plus
 * the parent customer's payment terms template (if any). Only fields backed
 * by real Epicor data are included so we don't blank out values Shopify may
 * have on existing locations.
 */
function buildLocationInput({ shipTo, custNum, paymentTermsTemplateId }) {
  const externalId = resolveShipToExternalId(shipTo, custNum);
  const name = String(shipTo.Name || "").trim() ||
    `Ship To ${shipTo.ShipToNum || ""}`.trim();

  const input = {
    name,
    externalId,
    billingSameAsShipping: true,
  };
  if (shipTo.PhoneNum) input.phone = String(shipTo.PhoneNum);
  if (shipTo.LangNameID) input.locale = String(shipTo.LangNameID);
  if (shipTo.RefNotes) input.note = String(shipTo.RefNotes);

  const buyerExperience = {};
  if (paymentTermsTemplateId) {
    buyerExperience.paymentTermsTemplateId = paymentTermsTemplateId;
    // Without a payment-terms template, "checkout to draft" doesn't apply —
    // unset checkoutToDraft so Shopify falls back to its default for the
    // location instead of forcing draft orders for cash buyers.
    buyerExperience.checkoutToDraft = false;
    buyerExperience.editableShippingAddress = true;
  }
  if (Object.keys(buyerExperience).length > 0) {
    input.buyerExperienceConfiguration = buyerExperience;
  }

  return input;
}

function buildLocationAddress({ shipTo, customer }) {
  const countryCode = normalizeCountryCode(
    shipTo.CountryISOCode || shipTo.Country || customer.CountryISOCode
  );
  const recipient = String(shipTo.Name || customer.Name || "").trim();
  const { firstName, lastName } = splitContactName(
    shipTo.ContactName || customer.Name
  );

  const address = {
    address1: sanitizeAddressLine(shipTo.Address1) || "",
    city: String(shipTo.City || "").trim(),
    countryCode,
  };
  const a2 = sanitizeAddressLine(shipTo.Address2);
  if (a2) address.address2 = a2;
  const zip = sanitizeZip(shipTo.ZIP, countryCode);
  if (zip) address.zip = zip;
  const state = String(shipTo.State || "").trim();
  if (state) address.zoneCode = countryCode === "US" ? state.toUpperCase() : state;
  if (recipient) address.recipient = recipient;
  if (firstName) address.firstName = firstName;
  if (lastName) address.lastName = lastName;
  if (shipTo.PhoneNum) address.phone = String(shipTo.PhoneNum);
  return address;
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
    `fluidra-epicor-customers-sync-${stamp}.csv`
  );
  const writer = createObjectCsvWriter({
    path: csvPath,
    header: [
      { id: "epicor_company", title: "epicor_company" },
      { id: "epicor_customer_id", title: "epicor_customer_id" },
      { id: "epicor_customer_number", title: "epicor_customer_number" },
      { id: "epicor_name", title: "epicor_name" },
      { id: "epicor_inactive", title: "epicor_inactive" },
      { id: "epicor_web_customer", title: "epicor_web_customer" },
      { id: "action", title: "action" },
      { id: "status", title: "status" },
      { id: "reason", title: "reason" },
      { id: "shopify_company_id", title: "shopify_company_id" },
      { id: "shopify_company_external_id", title: "shopify_company_external_id" },
      { id: "created_locations_count", title: "created_locations_count" },
      { id: "skipped_locations_count", title: "skipped_locations_count" },
      { id: "failed_locations_count", title: "failed_locations_count" },
      { id: "payment_terms_code", title: "payment_terms_code" },
      { id: "payment_terms_template_id", title: "payment_terms_template_id" },
      { id: "error_message", title: "error_message" },
      { id: "started_at", title: "started_at" },
      { id: "finished_at", title: "finished_at" },
    ],
  });
  return { writer, csvPath };
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-customer location sync
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Sort ShipToes deterministically: PrimaryShipTo first, then by ShipToNum
 * (lexicographic). The first entry of the sorted list is the canonical
 * "first" ShipTo we hand to companyCreate so the inline-created location is
 * predictable across runs.
 */
function sortShipToes(shipToes) {
  return [...shipToes].sort((a, b) => {
    const ap = a.PrimaryShipTo === true ? 0 : 1;
    const bp = b.PrimaryShipTo === true ? 0 : 1;
    if (ap !== bp) return ap - bp;
    return String(a.ShipToNum || "").localeCompare(String(b.ShipToNum || ""));
  });
}

/**
 * Sync the Epicor ShipToes for a single customer into Shopify CompanyLocations
 * on the matching Company. Returns { created, updated, skipped, failed, errors[] }.
 *
 * Idempotency:
 *   - The caller may pass `preloadedLocations` (e.g. the location Shopify
 *     created inline during companyCreate) to avoid an extra round-trip.
 *     Otherwise we paginate the full set via fetchAllCompanyLocations so
 *     companies with thousands of locations are matched correctly.
 *   - `skipExternalIds` lets the caller mark ShipToes that have already been
 *     created (typically the first ShipTo passed to companyCreate) so we
 *     don't try to create them a second time.
 *   - If a ShipTo's resolved externalId matches an existing location → UPDATE
 *     (name/phone/locale/note/buyerExperience) and re-assign shipping/billing
 *     addresses.
 *   - Otherwise → CREATE the location with shippingAddress (Shopify requires
 *     it on create); billing tracks shipping via billingSameAsShipping=true.
 *   - Locations are processed with bounded intra-customer concurrency
 *     (LOCATION_CONCURRENCY) so customers with thousands of ship-tos don't
 *     pay full sequential round-trip latency.
 */
async function syncLocationsForCustomer({
  customer,
  shopifyCompany,
  paymentTerms,
  preloadedLocations,
  skipExternalIds,
}) {
  const summary = { created: 0, updated: 0, skipped: 0, failed: 0, errors: [] };
  const allShipToes = Array.isArray(customer.ShipToes) ? customer.ShipToes : [];
  if (allShipToes.length === 0) {
    logger.info(
      `   ℹ️  ${hl.cust(customer.CustID)} has no ShipToes — nothing to sync.`
    );
    return summary;
  }

  const skipSet = skipExternalIds instanceof Set ? skipExternalIds : new Set();
  const shipToes = skipSet.size
    ? allShipToes.filter(
        (st) => !skipSet.has(resolveShipToExternalId(st, customer.CustNum))
      )
    : allShipToes;
  if (shipToes.length === 0) {
    return summary;
  }

  // Build the externalId → location lookup. Use the caller-provided seed when
  // available (CREATE branch passes the inline-created location); otherwise
  // page through every location on the company so 2000+ entries are covered.
  let existingLocations;
  if (Array.isArray(preloadedLocations)) {
    existingLocations = preloadedLocations;
  } else {
    existingLocations = await fetchAllCompanyLocations(shopifyCompany.id);
    logger.info(
      `   📚 Loaded ${hl.num(existingLocations.length)} existing CompanyLocation(s) for ${hl.cust(customer.CustID)}`
    );
  }
  const existingByExtId = new Map();
  for (const loc of existingLocations) {
    if (loc.externalId) existingByExtId.set(String(loc.externalId), loc);
  }

  const processOne = async (shipTo) => {
    const externalId = resolveShipToExternalId(shipTo, customer.CustNum);
    const baseInput = buildLocationInput({
      shipTo,
      custNum: customer.CustNum,
      paymentTermsTemplateId: paymentTerms.id,
    });
    const address = buildLocationAddress({ shipTo, customer });

    if (!address.address1) {
      logger.warn(
        `   ⚠️  ShipTo ${hl.key(shipTo.ShipToNum)} for ${hl.cust(
          customer.CustID
        )} has no Address1 — skipping location.`
      );
      return { kind: "skipped" };
    }

    const existing = existingByExtId.get(String(externalId));

    // Shopify validates zip codes against country-specific patterns. If the
    // mutation fails because of an invalid zip we retry once without the zip
    // field — losing the postal code is better than losing the entire location.
    const isZipError = (errMsg) =>
      /zip\b.*invalid/i.test(errMsg) || /invalid.*zip/i.test(errMsg);

    const stripZip = (addr) => {
      const copy = { ...addr };
      delete copy.zip;
      return copy;
    };

    try {
      if (existing) {
        if (RUN_CONFIG.dryRun) {
          logger.info(
            `   🟨 [DRY RUN] would UPDATE location ${hl.key(externalId)} (${existing.id})`
          );
        } else {
          const updateInput = { ...baseInput };
          delete updateInput.billingSameAsShipping;
          await updateShopifyCompanyLocation(existing.id, updateInput);
          try {
            await assignCompanyLocationAddress(existing.id, address, [
              "SHIPPING",
              "BILLING",
            ]);
          } catch (addrErr) {
            if (address.zip && isZipError(addrErr.message)) {
              logger.warn(
                `   ⚠️  ShipTo ${hl.key(shipTo.ShipToNum)} zip '${address.zip}' rejected by Shopify — retrying without zip`
              );
              await assignCompanyLocationAddress(
                existing.id,
                stripZip(address),
                ["SHIPPING", "BILLING"]
              );
            } else {
              throw addrErr;
            }
          }
          logger.info(
            `   ✅ UPDATED location ${hl.key(externalId)} (${existing.id})`
          );
        }
        return { kind: "updated" };
      }
      if (RUN_CONFIG.dryRun) {
        logger.info(
          `   🟨 [DRY RUN] would CREATE location ${hl.key(externalId)} for company ${shopifyCompany.id}`
        );
      } else {
        const createInput = { ...baseInput, shippingAddress: address };
        let created;
        try {
          created = await createShopifyCompanyLocation(
            shopifyCompany.id,
            createInput
          );
        } catch (createErr) {
          if (address.zip && isZipError(createErr.message)) {
            logger.warn(
              `   ⚠️  ShipTo ${hl.key(shipTo.ShipToNum)} zip '${address.zip}' rejected by Shopify — retrying without zip`
            );
            created = await createShopifyCompanyLocation(shopifyCompany.id, {
              ...createInput,
              shippingAddress: stripZip(address),
            });
          } else {
            throw createErr;
          }
        }
        logger.info(
          `   ✅ CREATED location ${hl.key(externalId)} (${created.id})`
        );
      }
      return { kind: "created" };
    } catch (err) {
      const msg = err.response?.data
        ? JSON.stringify(err.response.data)
        : err.message;
      logger.error(
        `   ❌ Location sync failed for ShipTo ${hl.key(shipTo.ShipToNum)}`,
        msg
      );
      return { kind: "failed", error: `ShipTo ${shipTo.ShipToNum}: ${msg}` };
    }
  };

  const results = await asyncPool(
    RUN_CONFIG.locationConcurrency,
    shipToes,
    processOne
  );

  for (const r of results) {
    if (!r) continue;
    if (r.__workerError) {
      summary.failed++;
      summary.errors.push(
        r.__workerError.message || String(r.__workerError)
      );
      continue;
    }
    if (r.kind === "created") summary.created++;
    else if (r.kind === "updated") summary.updated++;
    else if (r.kind === "skipped") summary.skipped++;
    else if (r.kind === "failed") {
      summary.failed++;
      if (r.error) summary.errors.push(r.error);
    }
  }

  return summary;
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-customer processor
// ─────────────────────────────────────────────────────────────────────────────

async function processCustomer(customer, index, totalKnown) {
  const started_at = ts();
  const custNum = customer.CustNum;
  const custId = customer.CustID;
  const web = isWebCustomer(customer);
  const inactive = isInactive(customer);
  const paymentTerms = mapPaymentTerms(customer.TermsCode);

  if (index > 0) logger.blank();

  const position = totalKnown
    ? `${hl.num(index + 1)}/${hl.num(totalKnown)}`
    : `${hl.num(index + 1)}`;
  logger.info(
    `🔄 [${position}]  CustNum=${hl.num(custNum)}  CustID=${hl.cust(custId)}  ` +
      `name=${hl.key(customer.Name || "")}  web=${web ? hl.ok("true") : hl.warn("false")}  ` +
      `inactive=${inactive ? hl.bad("true") : hl.ok("false")}  ` +
      `terms=${hl.key(customer.TermsCode || "")}`
  );

  if (!paymentTerms.mapped && customer.TermsCode) {
    logger.warn(
      `   ⚠️  Unmapped Epicor TermsCode '${customer.TermsCode}' for ${hl.cust(custId)} — proceeding without payment terms`
    );
  }

  const row = {
    epicor_company: customer.Company || "",
    epicor_customer_id: custId || "",
    epicor_customer_number: custNum,
    epicor_name: customer.Name || "",
    epicor_inactive: inactive,
    epicor_web_customer: web,
    action: "",
    status: "",
    reason: "",
    shopify_company_id: "",
    shopify_company_external_id: "",
    created_locations_count: 0,
    skipped_locations_count: 0,
    failed_locations_count: 0,
    payment_terms_code: paymentTerms.code,
    payment_terms_template_id: paymentTerms.id || "",
    error_message: "",
    started_at,
    finished_at: "",
  };

  if (!custNum) {
    row.action = "skipped_missing_required_data";
    row.status = "warning";
    row.reason = "Customer has no CustNum — cannot match Shopify externalId";
    row.finished_at = ts();
    logger.warn(`   ⏭️  SKIP — missing CustNum`);
    return row;
  }
  if (!customer.Name) {
    row.action = "skipped_missing_required_data";
    row.status = "warning";
    row.reason = "Customer has no Name — required by companyCreate";
    row.finished_at = ts();
    logger.warn(`   ⏭️  SKIP ${hl.cust(custId)} — missing Name`);
    return row;
  }

  let shopifyCompany = null;
  try {
    shopifyCompany = await findShopifyCompanyByExternalId(custNum);
  } catch (err) {
    row.action = "failed";
    row.status = "failed";
    row.reason = "Shopify company lookup failed after retries";
    row.error_message = err.message;
    row.finished_at = ts();
    logger.error(
      `❌ ${hl.cust(custId)} — companies lookup failed`,
      err.message
    );
    return row;
  }

  // Decision branches.
  try {
    if (!shopifyCompany && !web) {
      row.action = "skipped_not_web_customer";
      row.status = "success";
      row.reason = "Epicor WebCustomer != true and no Shopify Company exists";
      logger.info(
        `   ⏭️  SKIP ${hl.cust(custId)} — not a web customer & not in Shopify`
      );
    } else if (!shopifyCompany && inactive) {
      row.action = "skipped_inactive";
      row.status = "success";
      row.reason = "Epicor customer is Inactive=true and not yet in Shopify";
      logger.info(
        `   ⏭️  SKIP ${hl.cust(custId)} — inactive & not in Shopify`
      );
    } else if (!shopifyCompany && web && !inactive) {
      // CREATE path. Always hand the FIRST sorted ShipTo to companyCreate so
      // the location Shopify creates inline is one we know about and can
      // dedupe against. The remaining ShipToes are added via
      // companyLocationCreate calls inside syncLocationsForCustomer.
      const allShipToes = Array.isArray(customer.ShipToes)
        ? customer.ShipToes
        : [];
      const sortedShipToes = sortShipToes(allShipToes);

      // Find the first ShipTo with a usable Address1 — Shopify rejects a
      // CompanyLocationInput without one. If none qualify we create the
      // company without an inline location (Shopify still creates the
      // company; the missing locations are surfaced in the CSV warnings).
      let firstShipTo = null;
      let firstLocationInput = null;
      let firstShipToExternalId = null;
      for (const st of sortedShipToes) {
        const addr = buildLocationAddress({ shipTo: st, customer });
        if (!addr.address1) continue;
        firstShipTo = st;
        firstShipToExternalId = resolveShipToExternalId(st, customer.CustNum);
        firstLocationInput = {
          ...buildLocationInput({
            shipTo: st,
            custNum: customer.CustNum,
            paymentTermsTemplateId: paymentTerms.id,
          }),
          shippingAddress: addr,
        };
        break;
      }

      if (RUN_CONFIG.dryRun) {
        row.action = "created_company";
        row.status = "success";
        const inlineNote = firstLocationInput
          ? ` + inline first location ${firstShipToExternalId}`
          : " (no usable ShipTo for inline location)";
        row.reason = `[DRY RUN] would create Shopify Company${inlineNote} + metafield + ${Math.max(
          0,
          allShipToes.length - (firstLocationInput ? 1 : 0)
        )} more location(s)`;
        logger.info(
          `   🟨 [DRY RUN] would CREATE company ${hl.cust(custId)} (externalId=${custNum})${
            firstLocationInput ? ` with inline ShipTo ${firstShipToExternalId}` : ""
          }`
        );
      } else {
        const { company: created, locations: createdLocations } =
          await createShopifyCompany({
            name: customer.Name,
            externalId: custNum,
            firstLocation: firstLocationInput,
          });
        shopifyCompany = created;
        row.shopify_company_id = created.id;
        row.shopify_company_external_id =
          created.externalId || String(custNum);
        logger.info(
          `   ✅ CREATED company ${hl.cust(custId)} → ${hl.key(created.id)}` +
            (firstLocationInput
              ? ` (inline location ${hl.key(firstShipToExternalId)})`
              : "")
        );
        await setCompanyEpicorMetafield(created.id, custId);
        logger.info(
          `   🏷️  metafield custom.epicor_customer_id=${hl.key(custId)} set on ${created.id}`
        );

        const skipExternalIds = new Set();
        if (firstShipToExternalId && createdLocations.length > 0) {
          skipExternalIds.add(firstShipToExternalId);
        }
        const locSummary = await syncLocationsForCustomer({
          customer,
          shopifyCompany: created,
          paymentTerms,
          // Brand-new company — the only existing locations are the ones
          // Shopify just created inline, so we seed the dedupe map with them
          // and avoid an unnecessary fetchAllCompanyLocations round-trip.
          preloadedLocations: createdLocations,
          skipExternalIds,
        });
        // Account for the inline-created location in the CSV count.
        row.created_locations_count =
          locSummary.created + createdLocations.length;
        row.skipped_locations_count = locSummary.skipped;
        row.failed_locations_count = locSummary.failed;
        row.action = "created_company";
        row.status = locSummary.failed > 0 ? "warning" : "success";
        if (locSummary.failed > 0) {
          row.reason = "Company created; one or more locations failed";
          row.error_message = locSummary.errors.join(" | ").slice(0, 4000);
        }
      }
    } else if (shopifyCompany && inactive) {
      // Shopify Admin GraphQL has no real "inactive/archive" state for B2B
      // Companies — surface for manual review per the prompt's instructions.
      row.shopify_company_id = shopifyCompany.id;
      row.shopify_company_external_id = shopifyCompany.externalId || String(custNum);
      row.action = "manual_review";
      row.status = "warning";
      row.reason =
        "Epicor customer is Inactive=true but Shopify Company exists; Admin GraphQL exposes no company archive/disable state — handle manually.";
      logger.warn(
        `   🛑 MANUAL REVIEW ${hl.cust(custId)} (${shopifyCompany.id}) — inactive in Epicor, no Shopify equivalent`
      );
    } else if (shopifyCompany && !inactive) {
      // UPDATE path: refresh name + externalId (idempotent), ensure metafield,
      // sync ship-to locations.
      row.shopify_company_id = shopifyCompany.id;
      row.shopify_company_external_id =
        shopifyCompany.externalId || String(custNum);
      if (RUN_CONFIG.dryRun) {
        row.action = "updated_company";
        row.status = "success";
        row.reason = "[DRY RUN] would update Company + metafield + locations";
        logger.info(
          `   🟨 [DRY RUN] would UPDATE company ${hl.cust(custId)} (${shopifyCompany.id})`
        );
      } else {
        await updateShopifyCompany(shopifyCompany.id, {
          name: customer.Name,
          externalId: custNum,
        });
        logger.info(
          `   ✅ UPDATED company ${hl.cust(custId)} (${shopifyCompany.id})`
        );
        await setCompanyEpicorMetafield(shopifyCompany.id, custId);
        logger.info(
          `   🏷️  metafield custom.epicor_customer_id=${hl.key(custId)} ensured on ${shopifyCompany.id}`
        );
        const locSummary = await syncLocationsForCustomer({
          customer,
          shopifyCompany,
          paymentTerms,
        });
        row.created_locations_count = locSummary.created;
        row.skipped_locations_count = locSummary.skipped;
        row.failed_locations_count = locSummary.failed;
        row.action = locSummary.created > 0 ? "updated_company" : "synced_existing_company";
        row.status = locSummary.failed > 0 ? "warning" : "success";
        if (locSummary.failed > 0) {
          row.reason = "Company synced; one or more locations failed";
          row.error_message = locSummary.errors.join(" | ").slice(0, 4000);
        }
      }
    }
  } catch (err) {
    row.action = "failed";
    row.status = "failed";
    row.reason = row.reason || "Unhandled processing error";
    row.error_message = (err.response?.data
      ? JSON.stringify(err.response.data)
      : err.message
    ).slice(0, 4000);
    logger.error(
      `❌ ${hl.cust(custId)} — processing failed`,
      row.error_message
    );
  }

  if (!paymentTerms.mapped && customer.TermsCode) {
    // Append a non-fatal warning reason without clobbering an existing reason.
    const warn = `unmapped Epicor TermsCode '${customer.TermsCode}' (${paymentTerms.reason}) — payment terms not applied`;
    row.reason = row.reason ? `${row.reason}; ${warn}` : warn;
    if (row.status === "success") row.status = "warning";
  }

  row.finished_at = ts();
  return row;
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

async function main() {
  const runStart = Date.now();
  logger.banner("🚀  Fluidra Epicor → Shopify customer/company sync");
  logger.info("Config summary", {
    epicorBaseUrl: epicorConfig.baseUrl,
    epicorCompany: epicorConfig.company,
    shopifyStore: shopifyConfig.storeName,
    shopifyApiVersion: shopifyConfig.apiVersion,
    pageSize: RUN_CONFIG.pageSize,
    locationConcurrency: RUN_CONFIG.locationConcurrency,
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
    created_company: 0,
    updated_company: 0,
    synced_existing_company: 0,
    skipped_not_web_customer: 0,
    skipped_inactive: 0,
    skipped_missing_required_data: 0,
    manual_review: 0,
    failed: 0,
    locations_created: 0,
    locations_skipped: 0,
    locations_failed: 0,
  };

  let pendingRows = [];
  const flushRows = async () => {
    if (pendingRows.length === 0) return;
    const toWrite = pendingRows;
    pendingRows = [];
    try {
      await csvWriter.writeRecords(toWrite);
    } catch (err) {
      logger.error("Failed to flush CSV rows", err.message);
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

  let allCustomers = [];
  try {
    allCustomers = await fetchAllEpicorCustomers();
  } catch (err) {
    logger.error(
      "Epicor fetch failed after retries — aborting run.",
      err.message
    );
    await flushRows();
    process.exitCode = 1;
    return;
  }

  const totalCount = allCustomers.length;
  logger.blank();
  logger.info(
    `📊 ${hl.key("Total Epicor Customers fetched")}: ${hl.num(totalCount)}  (every one will be evaluated)`
  );
  logger.blank();

  if (RUN_CONFIG.maxItems > 0 && totalCount > RUN_CONFIG.maxItems) {
    logger.warn(
      `⛔ MAX_ITEMS=${RUN_CONFIG.maxItems} — only the first ${RUN_CONFIG.maxItems} of ${totalCount} will be processed.`
    );
    allCustomers = allCustomers.slice(0, RUN_CONFIG.maxItems);
  }

  logger.section(
    `Phase 2 / 2  —  Syncing ${allCustomers.length} customers to Shopify`
  );
  logger.blank();

  const denom = allCustomers.length;
  for (let i = 0; i < allCustomers.length; i++) {
    const customer = allCustomers[i];
    const row = await processCustomer(customer, i, denom);
    totals.processed++;
    if (totals[row.action] !== undefined) totals[row.action]++;
    if (row.status === "failed") totals.failed++;
    totals.locations_created += Number(row.created_locations_count || 0);
    totals.locations_skipped += Number(row.skipped_locations_count || 0);
    totals.locations_failed += Number(row.failed_locations_count || 0);

    pendingRows.push(row);
    if (pendingRows.length >= 25) await flushRows();

    if (totals.processed % 25 === 0 || totals.processed === denom) {
      const pct =
        denom > 0
          ? ((totals.processed / denom) * 100).toFixed(1) + "%"
          : "0%";
      const remaining = Math.max(0, denom - totals.processed);
      logger.blank();
      logger.info(
        `📈 Progress  ${hl.num(totals.processed)}/${hl.num(denom)}  (${hl.key(pct)})  remaining=${hl.num(remaining)}`
      );
      logger.info(
        `   companies: created=${hl.ok(totals.created_company)}  updated=${hl.ok(totals.updated_company)}  ` +
          `synced=${hl.ok(totals.synced_existing_company)}  skipped(not_web)=${hl.key(totals.skipped_not_web_customer)}  ` +
          `skipped(inactive)=${hl.key(totals.skipped_inactive)}  manual_review=${hl.warn(totals.manual_review)}  ` +
          `failed=${totals.failed > 0 ? hl.bad(totals.failed) : hl.ok(totals.failed)}`
      );
      logger.info(
        `   locations: created=${hl.ok(totals.locations_created)}  skipped=${hl.key(totals.locations_skipped)}  ` +
          `failed=${totals.locations_failed > 0 ? hl.bad(totals.locations_failed) : hl.ok(totals.locations_failed)}`
      );
      logger.blank();
    }

    if (RUN_CONFIG.perItemDelayMs > 0) await sleep(RUN_CONFIG.perItemDelayMs);
  }

  await flushRows();

  const elapsedSec = ((Date.now() - runStart) / 1000).toFixed(1);
  logger.blank();
  logger.banner("🏁  Customer sync run complete");
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
