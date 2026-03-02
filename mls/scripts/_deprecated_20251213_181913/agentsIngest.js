// mls/scripts/agentsIngest.js
// Ingest agents.txt (pipe-delimited) → normalized/agents.ndjson + MongoDB

import dotenvx from "@dotenvx/dotenvx";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import mongoose from "mongoose";

// --------------------------------------------------
// Env + paths
// --------------------------------------------------
dotenvx.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const MLS_ROOT = path.join(__dirname, "..");
const RAW_DIR = path.join(MLS_ROOT, "raw", "agents");
const NORMALIZED_DIR = path.join(MLS_ROOT, "normalized");
const AGENTS_NDJSON = path.join(NORMALIZED_DIR, "agents.ndjson");

const MONGO_URI = process.env.MONGO_URI;

// --------------------------------------------------
// Mongo schema/model (simple, text phones, dedup by agentId)
// --------------------------------------------------
const AgentSchema = new mongoose.Schema(
  {
    agentId: { type: String, required: true, index: true, unique: true },
    firstName: { type: String },
    lastName: { type: String },
    fullName: { type: String },
    // keep raw; treat as text
    phoneRaw: { type: String },
    // simple metadata
    source: { type: String, default: "mlspin_idx" },
    updatedAt: { type: Date, default: Date.now },
  },
  { collection: "mls_agents" }
);

let AgentModel;
try {
  AgentModel =
    mongoose.models.mls_agents || mongoose.model("mls_agents", AgentSchema);
} catch {
  AgentModel = mongoose.model("mls_agents", AgentSchema);
}

// --------------------------------------------------
// Helpers
// --------------------------------------------------
async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

/**
 * Parse a single agents.txt row.
 * Format (based on your sample):
 *   AGENT_ID|FIRST_NAME|LAST_NAME
 * Some lines may be junk / header and should be skipped.
 */
function parseAgentLine(line) {
  const trimmed = line.trim();
  if (!trimmed) return null;

  const parts = trimmed.split("|");
  if (parts.length < 3) return null;

  const [agentIdRaw, firstNameRaw, lastNameRaw] = parts;

  const agentId = agentIdRaw?.trim();
  const firstName = firstNameRaw?.trim() || "";
  const lastName = lastNameRaw?.trim() || "";

  // Basic sanity: agentId must exist and not look like a random header
  if (!agentId || agentId.length < 3) return null;

  const fullName = [firstName, lastName].filter(Boolean).join(" ").trim();

  return {
    agentId,
    firstName,
    lastName,
    fullName,
    phoneRaw: null, // not present in this feed; reserved for future
    source: "mlspin_idx",
    updatedAt: new Date(),
  };
}

/**
 * Read all .txt-like files in raw/agents, parse, and return
 * a deduped list of agent records, where the LAST row for each
 * agentId wins.
 */
async function loadAndDedupeAgentsFromRaw() {
  let files;
  try {
    files = await fs.readdir(RAW_DIR, { withFileTypes: true });
  } catch (err) {
    console.warn(
      "[agentsIngest] No raw agents directory or error reading it:",
      err.message
    );
    return [];
  }

  const txtFiles = files
    .filter((d) => d.isFile())
    .map((d) => d.name)
    .filter((name) => name.toLowerCase().endsWith(".txt"));

  if (txtFiles.length === 0) {
    console.warn("[agentsIngest] No .txt files found in raw/agents.");
    return [];
  }

  const agentMap = new Map(); // agentId -> agent (LAST row wins)

  for (const fileName of txtFiles) {
    const fullPath = path.join(RAW_DIR, fileName);
    const content = await fs.readFile(fullPath, "utf8");
    const lines = content.split(/\r?\n/);

    for (const line of lines) {
      const agent = parseAgentLine(line);
      if (!agent) continue;

      // LAST row for each ID wins
      agentMap.set(agent.agentId, agent);
    }
  }

  return [...agentMap.values()];
}

/**
 * Write agents.ndjson to normalized/ directory.
 */
async function writeAgentsNdjson(agents) {
  await ensureDir(NORMALIZED_DIR);

  const chunks = agents.map((a) => JSON.stringify(a));
  const data = chunks.join("\n") + (chunks.length ? "\n" : "");
  await fs.writeFile(AGENTS_NDJSON, data, "utf8");

  console.log(
    `[agentsIngest] Wrote ${agents.length} agents to ${AGENTS_NDJSON}`
  );
}

/**
 * Upsert into MongoDB (replace by agentId, but with our deduped
 * list we just drop & insert so it's deterministic).
 */
async function storeAgentsInMongo(agents) {
  if (!MONGO_URI) {
    console.warn(
      "[agentsIngest] No MONGO_URI set; skipping MongoDB storage for agents."
    );
    return;
  }

  await mongoose.connect(MONGO_URI, {
    serverSelectionTimeoutMS: 15000,
  });

  console.log(
    `[agentsIngest] Connected to MongoDB. Replacing mls_agents with ${agents.length} documents…`
  );

  await AgentModel.deleteMany({});
  if (agents.length > 0) {
    await AgentModel.insertMany(agents, { ordered: false });
  }

  console.log("[agentsIngest] MongoDB upsert complete.");

  await mongoose.disconnect();
}

/**
 * Main ingestion function – called from runManualIngestion.js
 */
export async function ingestAgents() {
  console.log("👤 [agentsIngest] Ingesting agents from raw/agents…");

  const agents = await loadAndDedupeAgentsFromRaw();
  console.log(
    `👤 [agentsIngest] Parsed ${agents.length} unique agents after dedupe-by-ID.`
  );

  // Always write NDJSON snapshot
  await writeAgentsNdjson(agents);

  // Also push into MongoDB
  await storeAgentsInMongo(agents);

  console.log("✅ [agentsIngest] Agents ingestion complete.\n");
}

// Allow running directly: node mls/scripts/agentsIngest.js
if (import.meta.url === `file://${process.argv[1]}`) {
  ingestAgents().catch((err) => {
    console.error("[agentsIngest] Fatal error:", err);
    process.exit(1);
  });
}
