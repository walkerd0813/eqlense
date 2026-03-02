// backend/mls/scripts/verifyListings.js
// Deep verification of normalized MLS data (listings + refs + agents/offices).

import fs from "node:fs";
import fsp from "node:fs/promises";
import readline from "node:readline";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const MLS_ROOT = path.join(__dirname, "..");
const NORMALIZED_DIR = path.join(MLS_ROOT, "normalized");
const REFERENCE_DIR = path.join(MLS_ROOT, "reference");
const LOG_DIR = path.join(MLS_ROOT, "logs");

const LISTINGS_PATH = path.join(NORMALIZED_DIR, "listings.ndjson");
const AGENTS_PATH = path.join(NORMALIZED_DIR, "agents.ndjson");
const OFFICES_PATH = path.join(NORMALIZED_DIR, "offices.ndjson");
const TOWNS_PATH = path.join(REFERENCE_DIR, "towns.json");
const AREAS_PATH = path.join(REFERENCE_DIR, "areas.json");
const SUMMARY_PATH = path.join(LOG_DIR, "verification-summary.json");

// ---------- small helpers ----------

async function fileExists(p) {
  try {
    await fsp.access(p, fs.constants.F_OK);
    return true;
  } catch {
    return false;
  }
}

function toNumber(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") {
    if (Number.isNaN(value)) return null;
    return value;
  }
  const s = String(value).trim();
  if (!s) return null;
  const n = Number(s.replace(/,/g, ""));
  return Number.isNaN(n) ? null : n;
}

function safeString(value) {
  if (value === null || value === undefined) return "";
  return String(value);
}

// stats.issueBuckets[key] = array of examples
function recordIssue(stats, bucket, listing, message) {
  if (!stats.issueBuckets[bucket]) {
    stats.issueBuckets[bucket] = [];
  }
  const arr = stats.issueBuckets[bucket];
  if (arr.length < 25) {
    arr.push({
      mlsNumber: listing.mlsNumber ?? listing.mls ?? listing.LIST_NO ?? "?",
      message,
    });
  }
  stats.counts[bucket] = (stats.counts[bucket] || 0) + 1;
}

// ---------- load helpers ----------

async function loadIdSet(ndjsonPath, idFieldCandidates) {
  const idSet = new Set();

  if (!(await fileExists(ndjsonPath))) {
    console.warn(`[verifyListings] NDJSON not found: ${ndjsonPath}`);
    return idSet;
  }

  const stream = fs.createReadStream(ndjsonPath, { encoding: "utf8" });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    let obj;
    try {
      obj = JSON.parse(trimmed);
    } catch {
      continue;
    }

    let id = null;
    for (const field of idFieldCandidates) {
      if (obj[field] != null) {
        id = String(obj[field]);
        break;
      }
    }
    if (id) {
      idSet.add(id);
    }
  }

  return idSet;
}

async function loadJson(pathname) {
  if (!(await fileExists(pathname))) return null;
  const raw = await fsp.readFile(pathname, "utf8");
  try {
    return JSON.parse(raw);
  } catch (err) {
    console.warn(`[verifyListings] Failed to parse JSON: ${pathname}`, err);
    return null;
  }
}

// ---------- main verification ----------

export async function verifyListings() {
  console.log("====================================================");
  console.log("            VERIFY NORMALIZED MLS DATA");
  console.log("====================================================");

  if (!(await fileExists(LISTINGS_PATH))) {
    console.error(
      `[verifyListings] listings.ndjson not found at ${LISTINGS_PATH}`
    );
    return;
  }

  // load reference tables
  const townsJson = await loadJson(TOWNS_PATH);
  const areasJson = await loadJson(AREAS_PATH);

  const townCodes = new Set();
  if (townsJson && Array.isArray(townsJson)) {
    for (const t of townsJson) {
      if (t && t.code != null) {
        townCodes.add(String(t.code));
      }
    }
  } else if (townsJson && typeof townsJson === "object") {
    for (const code of Object.keys(townsJson)) {
      townCodes.add(code);
    }
  }

  const areaCodes = new Set();
  if (areasJson && Array.isArray(areasJson)) {
    for (const a of areasJson) {
      if (a && a.code != null) {
        areaCodes.add(String(a.code));
      }
    }
  } else if (areasJson && typeof areasJson === "object") {
    for (const code of Object.keys(areasJson)) {
      areaCodes.add(code);
    }
  }

  console.log(
    `[verifyListings] Loaded ${townCodes.size} town codes from reference/towns.json`
  );
  console.log(
    `[verifyListings] Loaded ${areaCodes.size} area codes from reference/areas.json`
  );

  // load agents/offices id sets
  const agentIds = await loadIdSet(AGENTS_PATH, [
    "agentId",
    "AGENT_ID",
    "LIST_AGENT",
  ]);
  const officeIds = await loadIdSet(OFFICES_PATH, [
    "officeId",
    "OFFICE_ID",
    "LIST_OFFICE",
  ]);

  console.log(
    `[verifyListings] Loaded ${agentIds.size} agent IDs and ${officeIds.size} office IDs for cross-checks.`
  );

  const stats = {
    totalListings: 0,
    byPropertyType: {},
    byStatusCode: {},
    counts: {},
    issueBuckets: {},
  };

  const parseErrors = [];

  const stream = fs.createReadStream(LISTINGS_PATH, { encoding: "utf8" });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    let listing;
    try {
      listing = JSON.parse(trimmed);
    } catch (err) {
      if (parseErrors.length < 25) {
        parseErrors.push({ lineSample: trimmed.slice(0, 200) });
      }
      stats.counts.parseErrors = (stats.counts.parseErrors || 0) + 1;
      continue;
    }

    stats.totalListings++;

    const propTypeRaw =
      listing.propertyType ||
      listing.propType ||
      listing.PROP_TYPE ||
      listing.property_type;
    const propType = safeString(propTypeRaw) || "unknown";
    stats.byPropertyType[propType] =
      (stats.byPropertyType[propType] || 0) + 1;

    const statusRaw =
      listing.statusCode || listing.STATUS || listing.status || listing.Status;
    const statusCode = safeString(statusRaw).replace(/"/g, "") || "unknown";
    stats.byStatusCode[statusCode] =
      (stats.byStatusCode[statusCode] || 0) + 1;

    // ---------- required fields ----------
    const addr = listing.address || {};
    const requiredMissing = [];

    if (!listing.mlsNumber && !listing.mls && !listing.LIST_NO) {
      requiredMissing.push("mlsNumber");
    }
    if (!addr.streetName && !listing.streetName && !listing.STREET_NAME) {
      requiredMissing.push("address.streetName");
    }

    const zip =
      addr.zipCode || listing.zipCode || listing.ZIP_CODE || listing.ZIP;
    if (!zip) {
      requiredMissing.push("address.zipCode");
    }

    const townCode =
      addr.townCode || listing.townCode || listing.TOWN_NUM || listing.town_num;
    if (townCode === null || townCode === undefined || townCode === "") {
      requiredMissing.push("address.townCode");
    }

    if (requiredMissing.length > 0) {
      recordIssue(
        stats,
        "missingRequired",
        listing,
        `Missing required fields: ${requiredMissing.join(", ")}`
      );
    }

    // ---------- ZIP validation ----------
    const zipStr = zip != null ? String(zip).trim() : "";
    if (!zipStr || !/^\d{5}$/.test(zipStr)) {
      recordIssue(
        stats,
        "invalidZip",
        listing,
        `ZIP_CODE invalid or missing: ${zipStr || "null"}`
      );
    }

    // ---------- town / area codes ----------
    const townCodeStr =
      townCode != null ? String(townCode).trim() : "";
    if (townCodeStr && townCodes.size > 0 && !townCodes.has(townCodeStr)) {
      recordIssue(
        stats,
        "invalidTownCode",
        listing,
        `Town code not found in reference: ${townCodeStr}`
      );
    }

    const areaCode =
      addr.areaCode || listing.areaCode || listing.AREA || listing.area;
    const areaCodeStr =
      areaCode != null ? String(areaCode).trim() : "";
    if (areaCodeStr && areaCodes.size > 0 && !areaCodes.has(areaCodeStr)) {
      recordIssue(
        stats,
        "invalidAreaCode",
        listing,
        `Area code not found in reference: ${areaCodeStr}`
      );
    }

    // ---------- price issues ----------
    const listPrice = toNumber(listing.listPrice ?? listing.LIST_PRICE);
    const salePrice =
      toNumber(
        listing.salePrice ??
          listing.soldPrice ??
          listing.SALE_PRICE ??
          listing.SOLD_PRICE ??
          listing.CLOSING_PRICE
      ) ?? null;

    if (listPrice !== null && listPrice <= 0) {
      recordIssue(
        stats,
        "priceIssues",
        listing,
        `Invalid listPrice: ${listPrice}`
      );
    }

    const isSoldStatus = statusCode === "SLD" || statusCode === "SOLD";
    const saleDateRaw =
      listing.saleDate ||
      listing.soldDate ||
      listing.SALE_DATE ||
      listing.SETTLED_DATE ||
      listing.CLOSE_DATE ||
      listing.CLOSED_DATE ||
      listing.SOLD_DATE ||
      listing.OFFMARKET_DATE;
    const hasSoldDate = !!safeString(saleDateRaw).trim();

    if (isSoldStatus) {
      if (!salePrice || salePrice <= 0 || !hasSoldDate) {
        recordIssue(
          stats,
          "soldDataIssues",
          listing,
          "Sold status SLD but soldPrice or soldDate missing/invalid."
        );
      }
    }

    // ---------- square footage ----------
    const sqft = toNumber(
      listing.sqft ??
        listing.SQUARE_FEET ??
        listing.livingArea ??
        listing.AboveGradeFinishedArea
    );
    if (sqft != null) {
      // flag obviously bogus
      if (sqft <= 0 || sqft > 100000) {
        recordIssue(
          stats,
          "sqftIssues",
          listing,
          `Suspicious sqft value: ${sqft}`
        );
      }
    }

    // ---------- lot / acres verification ----------
    // Condos do not own individual land; skip lot/acres checks.
    if (propType !== "CC") {
      const lot = toNumber(listing.lotSizeSqFt ?? listing.LOT_SIZE);
      const acres = toNumber(listing.acres ?? listing.ACRE);

      if (lot == null && acres == null) {
        // MLS simply didn't provide land size.
        recordIssue(
          stats,
          "lotIssues",
          listing,
          "Missing LOT_SIZE and ACRES"
        );
      } else {
        if (lot != null && (lot <= 0 || lot > 2_000_000)) {
          recordIssue(
            stats,
            "lotIssues",
            listing,
            `Suspicious lotSizeSqFt value: ${lot}`
          );
        }
        if (acres != null && (acres <= 0 || acres > 5_000)) {
          recordIssue(
            stats,
            "lotIssues",
            listing,
            `Suspicious acres value: ${acres}`
          );
        }

        if (lot != null && acres != null) {
          const expected = acres * 43560;
          const diffRatio = Math.abs(expected - lot) / expected;
          if (diffRatio > 0.10) {
            recordIssue(
              stats,
              "lotIssues",
              listing,
              `Lot/acres mismatch: lotSizeSqFt=${lot}, acres=${acres}, expectedSqFt≈${Math.round(
                expected
              )}`
            );
          }
        }
      }
    }

    // ---------- year built ----------
    const yearBuilt = toNumber(listing.yearBuilt ?? listing.YEAR_BUILT);
    if (yearBuilt != null) {
      const currentYear = new Date().getFullYear();
      if (
        yearBuilt < 1600 ||
        yearBuilt > currentYear + 1 ||
        yearBuilt === 0 ||
        yearBuilt === 9999
      ) {
        recordIssue(
          stats,
          "yearBuiltIssues",
          listing,
          `Unreasonable yearBuilt: ${yearBuilt}`
        );
      }
    }

    // ---------- bed / bath sanity ----------
    const beds = toNumber(
      listing.beds ??
        listing.NO_BEDROOMS ??
        listing.totalBeds ??
        listing.TOTAL_BRS
    );
    if (beds != null && beds > 20) {
      recordIssue(
        stats,
        "bedBathIssues",
        listing,
        `Suspicious beds count: ${beds}`
      );
    }

    const bathsFull = toNumber(
      listing.fullBaths ??
        listing.NO_FULL_BATHS ??
        listing.totalFullBaths ??
        listing.TOTAL_FULL_BATHS
    );
    const bathsHalf = toNumber(
      listing.halfBaths ??
        listing.NO_HALF_BATHS ??
        listing.totalHalfBaths ??
        listing.TOTAL_HALF_BATHS
    );
    if (bathsFull != null && bathsFull > 15) {
      recordIssue(
        stats,
        "bedBathIssues",
        listing,
        `Suspicious full baths count: ${bathsFull}`
      );
    }
    if (bathsHalf != null && bathsHalf > 10) {
      recordIssue(
        stats,
        "bedBathIssues",
        listing,
        `Suspicious half baths count: ${bathsHalf}`
      );
    }

    // ---------- agent / office cross-check ----------
    const listAgentId =
      listing.listAgentId || listing.LIST_AGENT || listing.agentId;
    if (listAgentId) {
      const id = String(listAgentId);
      if (!agentIds.has(id)) {
        recordIssue(
          stats,
          "missingAgents",
          listing,
          `LIST_AGENT ${id} not found in agents.ndjson`
        );
      }
    }

    const listOfficeId =
      listing.listOfficeId || listing.LIST_OFFICE || listing.officeId;
    if (listOfficeId) {
      const id = String(listOfficeId);
      if (!officeIds.has(id)) {
        recordIssue(
          stats,
          "missingOffices",
          listing,
          `LIST_OFFICE ${id} not found in offices.ndjson`
        );
      }
    }
  }

  // produce summary object
  const summary = {
    totalListings: stats.totalListings,
    byPropertyType: stats.byPropertyType,
    byStatusCode: stats.byStatusCode,
    counts: stats.counts,
    issueExamples: stats.issueBuckets,
    parseErrors,
  };

  // ensure logs dir exists
  await fsp.mkdir(LOG_DIR, { recursive: true });
  await fsp.writeFile(SUMMARY_PATH, JSON.stringify(summary, null, 2), "utf8");

  // ----- console report -----
  console.log("====================================================");
  console.log("             MLS VERIFICATION REPORT");
  console.log("====================================================");
  console.log(`Listings processed: ${stats.totalListings}`);
  console.log();
  console.log("By propertyType:");
  for (const [type, count] of Object.entries(stats.byPropertyType)) {
    console.log(`  - ${type}: ${count}`);
  }
  console.log();
  console.log("By statusCode:");
  for (const [status, count] of Object.entries(stats.byStatusCode)) {
    console.log(`  - ${status}: ${count}`);
  }
  console.log();

  function printIssueBucket(label, key) {
    const count = stats.counts[key] || 0;
    console.log(`=== ${label} (${count}) ===`);
    const examples = stats.issueBuckets[key] || [];
    for (const ex of examples.slice(0, 10)) {
      console.log(`  • MLS ${ex.mlsNumber}: ${ex.message}`);
    }
    if (examples.length > 10) {
      console.log(`  ... and ${examples.length - 10} more examples.`);
    }
    console.log();
  }

  printIssueBucket("Parse errors", "parseErrors");
  printIssueBucket("Missing required fields", "missingRequired");
  printIssueBucket("Invalid ZIP codes", "invalidZip");
  printIssueBucket(
    "Invalid town codes (vs reference/towns.json)",
    "invalidTownCode"
  );
  printIssueBucket(
    "Invalid area codes (vs reference/areas.json)",
    "invalidAreaCode"
  );
  printIssueBucket("Price issues", "priceIssues");
  printIssueBucket("Sold data issues", "soldDataIssues");
  printIssueBucket("Square footage issues", "sqftIssues");
  printIssueBucket("Lot / acres issues", "lotIssues");
  printIssueBucket("Year built issues", "yearBuiltIssues");
  printIssueBucket("Bed / bath issues", "bedBathIssues");
  printIssueBucket("Missing agents in agents.ndjson", "missingAgents");
  printIssueBucket("Missing offices in offices.ndjson", "missingOffices");

  console.log(`Verification summary written to ${SUMMARY_PATH}`);
  console.log("====================================================");
  console.log("        MLS verification finished.");
  console.log("====================================================");
}

// Allow running directly: node mls/scripts/verifyListings.js
if (import.meta.url === `file://${process.argv[1]}`) {
  verifyListings().catch((err) => {
    console.error("verifyListings failed:", err);
    process.exit(1);
  });
}
