/**
 * extractHighOffset_v1_DROPIN.js (ESM)
 *
 * Extracts rows whose MAD-nearest distance is >= --minDistM into a separate NDJSON.
 * Flexible distance-field detection to support older apply scripts.
 *
 * Usage:
 *  node .\mls\scripts\extractHighOffset_v1_DROPIN.js --in <in.ndjson> --out <out.ndjson> --report <report.json> --minDistM 60
 */

import fs from "node:fs";
import path from "node:path";
import { Transform } from "node:stream";

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : "1";
    out[k] = v;
  }
  return out;
}

class LineSplitter extends Transform {
  constructor() {
    super({ readableObjectMode: true });
    this._buf = "";
  }
  _transform(chunk, enc, cb) {
    this._buf += chunk.toString("utf8");
    const parts = this._buf.split(/\r?\n/);
    this._buf = parts.pop() ?? "";
    for (const line of parts) {
      const t = line.trim();
      if (t) this.push(t);
    }
    cb();
  }
  _flush(cb) {
    const t = this._buf.trim();
    if (t) this.push(t);
    cb();
  }
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function toNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function getDistM(row) {
  const candidates = [
    row.addr_authority_dist_m,
    row.addrAuthorityDistM,
    row.mad_nearest_dist_m,
    row.madNearestDistM,
    row.nearestDistM,
    row.nearest_dist_m,
    row.dist_m,
    row.distance_m,
  ];
  for (const c of candidates) {
    const n = toNum(c);
    if (n != null) return n;
  }
  return null;
}

const args = parseArgs(process.argv);
const inPath = args.in;
const outPath = args.out;
const reportPath = args.report;
const minDistM = Number(args.minDistM ?? 60);

if (!inPath || !outPath || !reportPath) {
  console.error("Missing args. Required: --in --out --report [--minDistM]");
  process.exit(1);
}
if (!fs.existsSync(inPath)) {
  console.error("Input not found:", inPath);
  process.exit(1);
}

ensureDir(path.dirname(outPath));
ensureDir(path.dirname(reportPath));

console.log("===============================================");
console.log(" Extract High-Offset MAD Matches (V1)");
console.log("===============================================");
console.log("IN :", inPath);
console.log("OUT:", outPath);
console.log("minDistM:", minDistM);

const out = fs.createWriteStream(outPath, { encoding: "utf8" });
const rs = fs.createReadStream(inPath);
const splitter = new LineSplitter();
rs.pipe(splitter);

let total = 0;
let parseErr = 0;
let missingDist = 0;
let extracted = 0;

splitter.on("data", (line) => {
  total++;
  let row;
  try {
    row = JSON.parse(line);
  } catch {
    parseErr++;
    return;
  }

  const d = getDistM(row);
  if (d == null) {
    missingDist++;
    return;
  }

  if (d >= minDistM) {
    extracted++;
    out.write(JSON.stringify(row) + "\n");
  }
});

splitter.on("end", () => {
  out.end();
  const report = {
    inPath,
    outPath,
    minDistM,
    total,
    parseErr,
    missingDist,
    extracted,
    timestamp: new Date().toISOString(),
    note: "If extracted=0 unexpectedly, ensure your apply script writes a distance field (e.g., addr_authority_dist_m).",
  };
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");
  console.log("DONE:", report);
});

splitter.on("error", (e) => {
  console.error("Stream error:", e);
  process.exit(1);
});
