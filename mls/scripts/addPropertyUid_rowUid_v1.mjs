import fs from "node:fs";
import crypto from "node:crypto";
import readline from "node:readline";

function normTown(s){
  return String(s ?? "")
    .toUpperCase()
    .trim()
    .replace(/\bTOWN OF\b/g, "")
    .replace(/\bCITY OF\b/g, "")
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function sha1(s){
  return crypto.createHash("sha1").update(s).digest("hex");
}

function pickLng(o){ return (o?.lng ?? o?.lon ?? null); }

const args = Object.fromEntries(process.argv.slice(2).map((v,i,a)=> v.startsWith("--") ? [v.replace(/^--/,""), a[i+1]] : []).filter(Boolean));
const inPath  = args.in;
const outPath = args.out;
const repPath = args.report || null;

if(!inPath || !outPath){
  console.error("Usage: node addPropertyUid_rowUid_v1.mjs --in <in.ndjson> --out <out.ndjson> [--report <out.json>]");
  process.exit(1);
}

let total=0, placeholderUnknown=0, missingParcel=0, wrote=0;

const rl = readline.createInterface({ input: fs.createReadStream(inPath, { encoding:"utf8" }), crlfDelay: Infinity });
const out = fs.createWriteStream(outPath, { encoding:"utf8" });

for await (const line of rl){
  const t = line.trim();
  if(!t) continue;
  total++;

  let o;
  try { o = JSON.parse(t); } catch { continue; }

  const town = normTown(o.town);
  const parcel = String(o.parcel_id ?? "").trim();

  let id_quality = "OK";
  if(!parcel) { id_quality = "MISSING"; missingParcel++; }
  else if(parcel.toUpperCase() === "UNKNOWN") { id_quality = "PLACEHOLDER_UNKNOWN"; placeholderUnknown++; }

  const property_uid = (id_quality==="OK")
    ? `${town}|${parcel}`
    : null;

  // row_uid must be unique per row even when parcel_id collides
  const rowBasis = [
    property_uid ?? "(null)",
    String(o.unit ?? ""),
    String(o.site_key ?? o.address_key ?? o.address_label ?? ""),
    String(o.street_no ?? ""),
    String(o.street_name ?? ""),
    String(o.zip ?? ""),
    String(o.lat ?? ""),
    String(pickLng(o) ?? "")
  ].join("|");

  const row_uid = sha1(rowBasis);

  o.property_uid = property_uid;
  o.row_uid = row_uid;
  o.id_quality = id_quality;

  out.write(JSON.stringify(o) + "\n");
  wrote++;

  if(total % 500000 === 0) console.log(`...processed ${total.toLocaleString()} rows`);
}

out.end();

const report = { in: inPath, out: outPath, total_rows: total, wrote_rows: wrote, placeholderUnknown, missingParcel };
if(repPath) fs.writeFileSync(repPath, JSON.stringify(report, null, 2));
console.log("DONE.", report);
