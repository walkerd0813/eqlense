import fs from "node:fs";
import readline from "node:readline";

const files = [
  "publicData/parcels/parcelCentroids_EAST.geojson",
  "publicData/parcels/parcelCentroids_WEST.geojson"
];

const out = fs.createWriteStream(
  "publicData/parcels/parcelCentroids.geojson"
);

out.write('{"type":"FeatureCollection","features":[\n');

let first = true;

async function stream(file) {
  const rl = readline.createInterface({
    input: fs.createReadStream(file),
    crlfDelay: Infinity
  });

  let inFeatures = false;

  for await (const line of rl) {
    const t = line.trim();

    if (t.startsWith('"features"')) {
      inFeatures = true;
      continue;
    }

    if (!inFeatures) continue;
    if (t === "]" || t === "]}") break;
    if (!t.startsWith("{")) continue;

    if (!first) out.write(",\n");
    out.write(t.replace(/,$/, ""));
    first = false;
  }
}

(async () => {
  for (const f of files) await stream(f);
  out.write("\n]}");
  out.end();
  console.log("Parcel centroids merged");
})();
