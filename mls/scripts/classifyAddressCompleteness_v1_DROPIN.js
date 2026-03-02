import fs from "fs";
import readline from "readline";

const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i], process.argv[i + 1]);

const inPath = args.get("--in");
const metaPath = args.get("--meta");
const outMissingZip = args.get("--outMissingZip");
const outNoSiteAddr = args.get("--outNoSiteAddr");

if (!inPath || !metaPath || !outMissingZip || !outNoSiteAddr) {
  console.log(`Usage:
node mls/scripts/classifyAddressCompleteness_v1_DROPIN.js ^
  --in <properties.ndjson> ^
  --meta <meta.json> ^
  --outMissingZip <missing_zip.ndjson> ^
  --outNoSiteAddr <no_site_address.ndjson>`);
  process.exit(1);
}

const norm = (v) => (v == null ? null : String(v).trim() || null);
const isBlank = (v) => !norm(v);
const zipDigits = (v) => (norm(v) ? norm(v).replace(/\D/g, "") : "");
const isZipMissing = (v) => zipDigits(v).length < 5;
const isStreetNoMissing = (v) => {
  const s = norm(v);
  if (!s) return true;
  const t = s.replace(/\s+/g, "");
  return t === "0" || t === "00" || t === "000";
};

async function main() {
  const counts = {
    total: 0,

    has_coords: 0,
    has_zip: 0,
    has_city: 0,
    has_street_name: 0,
    has_street_no: 0,

    tier_A_mail_like: 0,   // street_no + street_name + city + zip
    tier_B_street_only: 0, // street_name + city + zip (no number)
    tier_C_no_site_addr: 0, // missing street_name (regardless of others)

    missing_zip_rows: 0,
    no_site_addr_rows: 0,
  };

  const wsZip = fs.createWriteStream(outMissingZip, "utf8");
  const wsNoSite = fs.createWriteStream(outNoSiteAddr, "utf8");

  const rl = readline.createInterface({
    input: fs.createReadStream(inPath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    counts.total++;

    let row;
    try { row = JSON.parse(t); } catch { continue; }

    const lat = row.lat ?? row.latitude;
    const lng = row.lng ?? row.lon ?? row.longitude;
    const hasCoords = Number.isFinite(Number(lat)) && Number.isFinite(Number(lng));
    if (hasCoords) counts.has_coords++;

    const zip = row.zip ?? row.ZIP ?? row.zip_code ?? row.zipCode;
    const city = row.city ?? row.town ?? row.CITY ?? row.TOWN;

    const streetName = row.street_name ?? row.streetName ?? row.street ?? row.STREET;
    const streetNo = row.street_no ?? row.streetNo ?? row.addr_num ?? row.ADDR_NUM;

    const hasZip = !isZipMissing(zip);
    const hasCity = !isBlank(city);
    const hasStreetName = !isBlank(streetName);
    const hasStreetNo = !isStreetNoMissing(streetNo);

    if (hasZip) counts.has_zip++;
    if (hasCity) counts.has_city++;
    if (hasStreetName) counts.has_street_name++;
    if (hasStreetNo) counts.has_street_no++;

    // ZIP missing file
    if (!hasZip) {
      counts.missing_zip_rows++;
      wsZip.write(JSON.stringify(row) + "\n");
    }

    // Site-address classification
    if (!hasStreetName) {
      counts.tier_C_no_site_addr++;
      counts.no_site_addr_rows++;
      wsNoSite.write(JSON.stringify(row) + "\n");
    } else if (hasStreetNo && hasCity && hasZip) {
      counts.tier_A_mail_like++;
    } else if (hasCity && hasZip) {
      counts.tier_B_street_only++;
    } else {
      // still a site street, but missing city/zip
      counts.tier_B_street_only++;
    }
  }

  wsZip.end();
  wsNoSite.end();

  fs.writeFileSync(metaPath, JSON.stringify({ in: inPath, counts }, null, 2), "utf8");
  console.log("[done]", counts);
}

main().catch((e) => { console.error(e); process.exit(1); });
