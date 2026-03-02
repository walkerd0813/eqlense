import https from "node:https";
import http from "node:http";
import { URL } from "node:url";

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
      out[k] = v;
    }
  }
  return out;
}

function getJson(u) {
  return new Promise((resolve, reject) => {
    const url = new URL(u);
    const lib = url.protocol === "https:" ? https : http;
    const req = lib.get(url, (res) => {
      let data = "";
      res.setEncoding("utf8");
      res.on("data", (c) => (data += c));
      res.on("end", () => {
        try {
          const j = JSON.parse(data);
          resolve({ status: res.statusCode, json: j });
        } catch (e) {
          reject(new Error(`Non-JSON response (status ${res.statusCode}). First 200 chars: ${data.slice(0, 200)}`));
        }
      });
    });
    req.on("error", reject);
  });
}

function joinLayerUrl(base, suffix) {
  const b = base.replace(/\/+$/, "");
  return `${b}${suffix}`;
}

async function main() {
  console.log("====================================================");
  console.log("ArcGIS layer probe (count + meta) v1");
  console.log("====================================================");

  const args = parseArgs(process.argv);
  const layerUrl = String(args.url || "").trim();
  const where = encodeURIComponent(String(args.where || "1=1"));

  if (!layerUrl) throw new Error("Missing --url <layerUrl>");

  // 1) Fetch layer metadata
  const metaUrl = joinLayerUrl(layerUrl, "?f=pjson");
  const meta = await getJson(metaUrl);

  // 2) Count query
  const qUrl = joinLayerUrl(layerUrl, `/query?where=${where}&returnCountOnly=true&f=pjson`);
  const cnt = await getJson(qUrl);

  console.log("[meta] status:", meta.status);
  console.log("[meta] type:", meta.json?.type || meta.json?.layerType || "(unknown)");
  console.log("[meta] name:", meta.json?.name || "(unknown)");
  console.log("[meta] geometryType:", meta.json?.geometryType || "(none)");
  console.log("[meta] capabilities:", meta.json?.capabilities || "(unknown)");
  console.log("[meta] supportsPagination:", meta.json?.supportsPagination);
  console.log("[meta] maxRecordCount:", meta.json?.maxRecordCount);
  console.log("[count] status:", cnt.status);
  console.log("[count] value:", cnt.json?.count);

  if (cnt.json?.error) {
    console.log("[count] error:", cnt.json.error);
  }

  console.log("[done]");
}

main().catch((e) => {
  console.error("[fatal]", e?.message || e);
  process.exit(1);
});
