import fs from "fs";
import readline from "readline";

function arg(name) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : null;
}
const A = arg("--a");
const B = arg("--b");
if (!A || !B) {
  console.log("Usage: node qa_compareStreetNo_v27_vs_v28_v1_DROPIN.js --a <v27.ndjson> --b <v28.ndjson>");
  process.exit(1);
}

const rla = readline.createInterface({ input: fs.createReadStream(A, "utf8"), crlfDelay: Infinity });
const rlb = readline.createInterface({ input: fs.createReadStream(B, "utf8"), crlfDelay: Infinity });

const ita = rla[Symbol.asyncIterator]();
const itb = rlb[Symbol.asyncIterator]();

let total = 0, fixed = 0, regressed = 0, stillMissing = 0, unchangedPresent = 0;
let sampleRegressed = 0;

function missNo(o) {
  const v = (o.street_no ?? "").toString().trim();
  return !v || v === "0";
}

while (true) {
  const [a, b] = await Promise.all([ita.next(), itb.next()]);
  if (a.done || b.done) break;
  const la = a.value?.trim();
  const lb = b.value?.trim();
  if (!la || !lb) continue;

  total++;
  const oa = JSON.parse(la);
  const ob = JSON.parse(lb);

  const am = missNo(oa);
  const bm = missNo(ob);

  if (am && !bm) fixed++;
  else if (!am && bm) {
    regressed++;
    if (sampleRegressed < 5) {
      sampleRegressed++;
      console.log("[REGRESSED SAMPLE]", { parcel_id: oa.parcel_id, v27: oa.street_no, v28: ob.street_no });
    }
  } else if (am && bm) stillMissing++;
  else unchangedPresent++;
}

console.log({ total, fixed, regressed, stillMissing, unchangedPresent });
