// mls/scripts/watchDownloadsAndIngest.js
// --------------------------------------------------
// Clean IDX Downloads watcher + ingestion runner
// --------------------------------------------------

import dotenvx from "@dotenvx/dotenvx";
import path from "node:path";
import { fileURLToPath } from "node:url";
import chokidar from "chokidar";
import { classifyAndRoute } from "./classifyIDX.js";

// Resolve __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// --------------------------------------------------
// Load ONLY mls/.env.mls (no root .env)
// --------------------------------------------------
dotenvx.config({
  path: path.join(__dirname, "..", ".env.mls"),
  override: true,
  dotenvxPath: false
});

console.log("[MLS WATCHER] Env loaded from mls/.env.mls");

// --------------------------------------------------
// Resolve Downloads folder
// --------------------------------------------------
const envDownloads = process.env.DOWNLOADS || null;
const fallbackDownloads = process.env.USERPROFILE
  ? path.join(process.env.USERPROFILE, "Downloads")
  : null;

const DOWNLOADS = envDownloads || fallbackDownloads;

// --------------------------------------------------
// Validate Downloads path
// --------------------------------------------------
function validateDownloadsPath() {
  console.log("==============================================");
  console.log(" IDX Watcher starting…");
  console.log(" DOWNLOADS =", DOWNLOADS);
  console.log(" USERPROFILE =", process.env.USERPROFILE);
  console.log("==============================================");

  if (!DOWNLOADS) {
    console.error("ERROR: DOWNLOADS is not set. Fix mls/.env.mls");
    process.exit(1);
  }
}

// --------------------------------------------------
// Safe dynamic module runner (for ingestion scripts)
// --------------------------------------------------
async function safeCallModule(relPath, fnNames) {
  const modulePath = path.join(__dirname, relPath);
  try {
    const mod = await import(modulePath);
    for (const name of fnNames) {
      const fn = mod[name];
      if (typeof fn === "function") {
        console.log(`[MLS WATCHER] Running ${relPath} → ${name}()`);
        await fn();
        return true;
      }
    }
    console.warn(
      `[MLS WATCHER] No valid export found in ${relPath}. Tried: ${fnNames.join(", ")}`
    );
    return false;
  } catch (err) {
    console.error(`[MLS WATCHER] Error calling ${relPath}:`, err);
    return false;
  }
}

// --------------------------------------------------
// Trigger ingestion based on classification
// --------------------------------------------------
async function runIngestionForClassification(c) {
  if (!c || !c.domain) {
    console.warn("[MLS WATCHER] Invalid classification:", c);
    return;
  }

  console.log("[MLS WATCHER] Classification result:", c);

  switch (c.domain) {
    case "listing":
      await safeCallModule("./ingestIDX.js", ["ingestAll"]);
      await safeCallModule("./verifyListings.js", ["verifyListings"]);
      break;

    case "reference":
      await safeCallModule("./ingestReferenceTables.js", ["ingestReferenceTables"]);
      await safeCallModule("./verifyListings.js", ["verifyListings"]);
      break;

    case "agents":
      await safeCallModule("./agentsIngest.js", ["ingestAgents"]);
      break;

    case "offices":
      await safeCallModule("./officesIngest.js", ["ingestOffices"]);
      break;

    default:
      console.log("[MLS WATCHER] No pipeline configured for domain:", c.domain);
  }
}

// --------------------------------------------------
// Main watcher
// --------------------------------------------------
export async function startWatcher() {
  validateDownloadsPath();

  console.log("[MLS WATCHER] Watching for new IDX files in:", DOWNLOADS);

  const watcher = chokidar.watch(DOWNLOADS, {
    persistent: true,
    ignoreInitial: true,
    depth: 0
  });

  watcher
    .on("ready", () => {
      console.log("[MLS WATCHER] Watcher is active and monitoring Downloads.");
    })
    .on("add", async (filePath) => {
      try {
        const name = path.basename(filePath).toLowerCase();
        const ext = path.extname(name);

        // Ignore temp/partial files
        if (
          name.endsWith(".crdownload") ||
          name.startsWith("~$") ||
          name.startsWith(".") ||
          ext === ".tmp"
        ) {
          console.log("[MLS WATCHER] Ignored temp/partial file:", name);
          return;
        }

        console.log("[MLS WATCHER] New file detected:", filePath);

        // Classify + route
        const classification = await classifyAndRoute(filePath);

        // Run appropriate ingestion
        await runIngestionForClassification(classification);
      } catch (err) {
        console.error("[MLS WATCHER] Error processing file:", err);
      }
    })
    .on("error", (err) => {
      console.error("[MLS WATCHER] Watcher error:", err);
    });
}

// --------------------------------------------------
// Auto-run watcher whenever this file is executed
// --------------------------------------------------
startWatcher().catch((err) => {
  console.error("[MLS WATCHER] Failed to start watcher:", err);
  process.exit(1);
});
