
/**
 * FIND GEOJSON FEATURES BY PROPERTY VALUE (streaming) - v1 DROPIN
 * --------------------------------------------------------------
 * Streams a huge GeoJSON FeatureCollection and reports whether a town/value exists.
 *
 * Usage examples:
 *   node .\mls\scripts\findGeojsonFeaturesByProp_v1_DROPIN.js --file C:\path\zoningBoundariesData.geojson --value NEEDHAM
 *   node .\mls\scripts\findGeojsonFeaturesByProp_v1_DROPIN.js --file C:\path\zoningBoundariesData.geojson --key TOWN --value "NEEDHAM"
 *   node .\mls\scripts\findGeojsonFeaturesByProp_v1_DROPIN.js --file ... --value ASHLAND --limit 5
 *
 * Notes:
 * - Does NOT JSON.parse the whole file.
 * - "key" is matched case-insensitively against properties keys.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function getArg(name, fallback = null) {
  const key = `--${name}`;
  const i = process.argv.findIndex((a) => a === key);
  if (i >= 0 && i + 1 < process.argv.length) return process.argv[i + 1];
  return fallback;
}

function normKey(k) {
  return String(k ?? "").toUpperCase().replace(/[^A-Z0-9]/g, "");
}

function collapse(v) {
  return String(v ?? "")
    .replace(/\u00A0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Streaming GeoJSON FeatureCollection parser (no deps)
 * Yields each feature object under top-level "features": [...]
 */
async function* streamGeojsonFeatures(filePath) {
  const rs = fs.createReadStream(filePath, { encoding: "utf8", highWaterMark: 1024 * 1024 });
  let buf = "";
  let state = "seekFeatures";
  let i = 0;

  let inString = false;
  let escape = false;
  let depth = 0;
  let objStart = -1;

  const keepTail = (n = 200) => {
    if (buf.length > n) buf = buf.slice(buf.length - n);
    i = 0;
  };

  for await (const chunk of rs) {
    buf += chunk;

    while (i < buf.length) {
      if (state === "seekFeatures") {
        const idx = buf.indexOf('"features"', i);
        if (idx < 0) {
          keepTail(200);
          break;
        }
        i = idx + 10;
        state = "seekArrayStart";
        continue;
      }
      if (state === "seekArrayStart") {
        const idx = buf.indexOf("[", i);
        if (idx < 0) {
          keepTail(200);
          break;
        }
        i = idx + 1;
        state = "seekObjStart";
        continue;
      }
      if (state === "seekObjStart") {
        while (i < buf.length && (buf[i] === " " || buf[i] === "\n" || buf[i] === "\r" || buf[i] === "\t" || buf[i] === ",")) i++;
        if (i >= buf.length) break;
        if (buf[i] === "]") return;
        if (buf[i] !== "{") {
          i++;
          continue;
        }
        state = "inObj";
        objStart = i;
        inString = false;
        escape = false;
        depth = 0;
        continue;
      }
      if (state === "inObj") {
        const ch = buf[i];
        if (inString) {
          if (escape) escape = false;
          else if (ch === "\\") escape = true;
          else if (ch === '"') inString = false;
        } else {
          if (ch === '"') inString = true;
          else if (ch === "{") depth++;
          else if (ch === "}") depth--;
        }
        i++;
        if (!inString && depth === 0 && objStart >= 0 && i > objStart) {
          const text = buf.slice(objStart, i);
          let obj = null;
          try {
            obj = JSON.parse(text);
          } catch {
            objStart = -1;
            state = "seekObjStart";
            continue;
          }
          yield obj;
          buf = buf.slice(i);
          i = 0;
          objStart = -1;
          state = "seekObjStart";
        }
      }
    }

    if (state !== "inObj" && buf.length > 5 * 1024 * 1024) keepTail(2000);
  }
}

function summarizeFeature(feat, hitKey, hitVal) {
  const props = feat?.properties || {};
  const geomType = feat?.geometry?.type ?? null;
  return {
    id: feat?.id ?? null,
    geomType,
    hitKey,
    hitVal,
    sampleProps: Object.fromEntries(
      Object.entries(props)
        .filter(([k, _]) => {
          const nk = normKey(k);
          return ["TOWN", "CITY", "MUNICIPAL", "MUNICIPALITY", "TOWNNAME", "NAME", "DISTRICT", "ZONE", "ZONING", "LOC_ID", "MAP_PAR_ID"].some((t) => nk.includes(t));
        })
        .slice(0, 12)
    ),
  };
}

async function main() {
  const file = getArg("file", null);
  const key = getArg("key", null);
  const value = getArg("value", null);
  const limit = Number(getArg("limit", "5"));

  if (!file || !value) {
    console.error("❌ Usage: --file <geojson> --value <searchValue> [--key <propKey>] [--limit 5]");
    process.exit(1);
  }

  const FILE = path.resolve(__dirname, file);
  if (!fs.existsSync(FILE)) {
    console.error("❌ Not found:", FILE);
    process.exit(1);
  }

  const want = collapse(value).toUpperCase();
  const wantKey = key ? normKey(key) : null;

  console.log("====================================================");
  console.log(" FIND GEOJSON FEATURES BY PROP (streaming)");
  console.log("====================================================");
  console.log("FILE :", FILE);
  console.log("KEY  :", key ?? "(any key)");
  console.log("VALUE:", value);
  console.log("LIMIT:", limit);
  console.log("----------------------------------------------------");

  let scanned = 0;
  let matches = 0;
  const examples = [];

  for await (const feat of streamGeojsonFeatures(FILE)) {
    scanned++;
    const props = feat?.properties || {};

    if (wantKey) {
      // find matching property key (case-insensitive)
      const hitPropKey = Object.keys(props).find((k) => normKey(k) === wantKey);
      if (!hitPropKey) continue;

      const v = props[hitPropKey];
      const s = collapse(v).toUpperCase();
      if (s === want || s.includes(want)) {
        matches++;
        if (examples.length < limit) examples.push(summarizeFeature(feat, hitPropKey, v));
      }
    } else {
      // search any property value as string
      let hit = null;
      for (const [k, v] of Object.entries(props)) {
        if (v == null) continue;
        const s = collapse(v).toUpperCase();
        if (!s) continue;
        if (s === want || s.includes(want)) {
          hit = { k, v };
          break;
        }
      }
      if (hit) {
        matches++;
        if (examples.length < limit) examples.push(summarizeFeature(feat, hit.k, hit.v));
      }
    }

    if (scanned % 500000 === 0) {
      console.log(`[progress] scanned ${scanned.toLocaleString()} matches ${matches.toLocaleString()}`);
    }
  }

  console.log("----------------------------------------------------");
  console.log("[done]", { scanned, matches });
  if (examples.length) {
    console.log("Examples:");
    console.log(JSON.stringify(examples, null, 2));
  } else {
    console.log("Examples: (none)");
  }
  console.log("====================================================");
}

main().catch((e) => {
  console.error("❌ find failed:", e);
  process.exit(1);
});
