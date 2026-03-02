import fs from "node:fs";
import readline from "node:readline";

const args = Object.fromEntries(process.argv.slice(2).map((v,i,a)=> v.startsWith("--") ? [v.replace(/^--/,""), a[i+1]] : []).filter(Boolean));

const inPath = args.in;
const outKeep = args.outKeep;
const outUnknown = args.outUnknown;
const outReport = args.report;

if(!inPath || !outKeep || !outUnknown || !outReport){
  console.error("Usage: node extractUnknownParcels_v1.mjs --in <in.ndjson> --outKeep <keep.ndjson> --outUnknown <unknown.ndjson> --report <report.json>");
  process.exit(1);
}

let total=0, unknown=0;

const rl = readline.createInterface({ input: fs.createReadStream(inPath,{encoding:"utf8"}), crlfDelay: Infinity });
const wKeep = fs.createWriteStream(outKeep,{encoding:"utf8"});
const wUnk  = fs.createWriteStream(outUnknown,{encoding:"utf8"});

for await (const line of rl){
  const t = line.trim();
  if(!t) continue;
  total++;
  const o = JSON.parse(t);

  const parcel = String(o.parcel_id ?? "").trim().toUpperCase();
  const isUnknown = (parcel === "UNKNOWN") || (o.id_quality === "PLACEHOLDER_UNKNOWN");

  if(isUnknown){
    unknown++;
    wUnk.write(JSON.stringify(o) + "\n");
  } else {
    wKeep.write(JSON.stringify(o) + "\n");
  }

  if(total % 500000 === 0) console.log(`...processed ${total.toLocaleString()} rows`);
}

wKeep.end(); wUnk.end();
fs.writeFileSync(outReport, JSON.stringify({ in: inPath, total_rows: total, unknown_rows: unknown, kept_rows: total-unknown }, null, 2));
console.log("DONE.");
