import fs from "node:fs";
import readline from "node:readline";
import crypto from "node:crypto";

function sha256File(p){
  const h = crypto.createHash("sha256");
  const s = fs.createReadStream(p);
  return new Promise((resolve,reject)=>{
    s.on("data", d=>h.update(d));
    s.on("error", reject);
    s.on("end", ()=>resolve(h.digest("hex").toUpperCase()));
  });
}

function getPath(obj, path){
  if (!obj || !path) return undefined;
  const parts = path.split(".");
  let cur = obj;
  for (const part of parts){
    if (cur && Object.prototype.hasOwnProperty.call(cur, part)){
      cur = cur[part];
    } else {
      return undefined;
    }
  }
  return cur;
}

function pick(obj, candidates){
  for (const p of candidates){
    const v = getPath(obj, p);
    if (v !== undefined && v !== null && v !== "") return v;
  }
  return undefined;
}

function deriveCoordGrade(coordSource){
  if (!coordSource) return null;
  const s = String(coordSource).toLowerCase();
  if (s.includes("address_point")) return "A";
  if (s.includes("parcel_centroid")) return "B";
  if (s.includes("external")) return "C";
  return null;
}

async function run(){
  const args = process.argv.slice(2);
  const arg = (k)=>{
    const i = args.indexOf(k);
    if (i === -1) return null;
    return args[i+1] ?? null;
  };

  const inPath = arg("--in");
  const outPath = arg("--out");
  const asOf = arg("--asOf") || new Date().toISOString().slice(0,10);
  const datasetHash = arg("--datasetHash"); // sha256 of input file

  if (!inPath || !outPath) {
    console.error("usage: node build_properties_contract_view_v1.mjs --in <in.ndjson> --out <out.ndjson> --asOf YYYY-MM-DD --datasetHash <sha>");
    process.exit(2);
  }

  const inSha = datasetHash || await sha256File(inPath);

  fs.mkdirSync(new URL(".", "file://" + process.cwd().replace(/\\/g,"/") + "/" + outPath.replace(/\\/g,"/")), { recursive: true });

  const input = fs.createReadStream(inPath, { encoding: "utf8" });
  const rl = readline.createInterface({ input, crlfDelay: Infinity });
  const out = fs.createWriteStream(outPath, { encoding: "utf8" });

  let read = 0;
  let wrote = 0;
  let missing = 0;

  for await (const line of rl){
    if (!line || !line.trim()) continue;
    read++;
    let o;
    try { o = JSON.parse(line); } catch { continue; }

    // Your v46 uses: parcel_id, town, state, zip, lat, lon, etc.
    const property_id = pick(o, ["property_id","id"]);
    const parcel_id_raw = pick(o, ["parcel_id_raw","parcel_id","parcelId"]);
    const source_city = pick(o, ["source_city","town","city"]);
    const source_state = pick(o, ["source_state","state"]);
    const address_city = pick(o, ["address_city","address.city","city","town"]);
    const address_state = pick(o, ["address_state","address.state","state"]);
    const address_zip = pick(o, ["address_zip","address.zip","zip","zipcode"]);

    const latitude = pick(o, ["latitude","lat","location.lat","coord.lat"]);
    const longitude = pick(o, ["longitude","lon","lng","location.lon","location.lng","coord.lon","coord.lng"]);

    const coord_source = pick(o, ["coord_source","coord.source","coordSource"]);
    const coord_conf = pick(o, ["coord_confidence_grade","coord_confidence","coord.grade","coordGrade"]) ?? deriveCoordGrade(coord_source);

    const centroidLat = pick(o, ["parcel_centroid_lat","parcel_centroid.lat","centroid_lat","centroid.lat"]);
    const centroidLon = pick(o, ["parcel_centroid_lon","parcel_centroid.lon","parcel_centroid.lng","centroid_lon","centroid.lon","centroid.lng"]);

    const crs = pick(o, ["crs","geom.crs","parcel.crs"]) ?? "EPSG:4326";

    const baseStatus = pick(o, ["base_zoning_status","zoning_base_status","zoning.status","base_zoning.status","zoning_status"]) ?? "UNKNOWN";
    const baseCodeRaw = pick(o, ["base_zoning_code_raw","zoning_code_raw","base_zoning.code_raw","zoning.code_raw"]);
    const baseCodeNorm = pick(o, ["base_zoning_code_norm","zoning_code_norm","base_zoning.code_norm","zoning.code_norm"]);

    const attachMethod = pick(o, ["zoning_attach_method","base_zoning_attach_method","zoning.attach_method","base_zoning.attach_method"]);
    const attachConf = pick(o, ["zoning_attach_confidence","base_zoning_attach_confidence","zoning.attach_confidence","base_zoning.attach_confidence"]);

    // These may not exist in-row in v46; include keys anyway (null ok for schema).
    const zoning_source_city = pick(o, ["zoning_source_city","zoning.source_city","base_zoning.source_city"]) ?? source_city;
    const zoning_dataset_hash = pick(o, ["zoning_dataset_hash","zoning.dataset_hash","base_zoning.dataset_hash"]);
    const zoning_as_of_date = pick(o, ["zoning_as_of_date","zoning.as_of_date","base_zoning.as_of_date"]);

    const row = {
      // ---- Contract headers ----
      property_id: property_id ?? null,
      parcel_id_raw: parcel_id_raw ?? null,
      parcel_id_norm: pick(o, ["parcel_id_norm","parcel_id_normalized","parcel_id_canon"]) ?? null,

      source_city: source_city ?? null,
      source_state: source_state ?? null,

      dataset_hash: inSha,
      as_of_date: asOf,

      address_city: address_city ?? null,
      address_state: address_state ?? null,
      address_zip: address_zip ?? null,

      latitude: latitude ?? null,
      longitude: longitude ?? null,

      coord_source: coord_source ?? null,
      coord_confidence_grade: coord_conf ?? null,
      coord_distance_m: pick(o, ["coord_distance_m","coord.distance_m","coordDistanceM"]) ?? null,

      parcel_centroid_lat: centroidLat ?? null,
      parcel_centroid_lon: centroidLon ?? null,
      crs: crs,

      base_zoning_status: baseStatus,
      base_zoning_code_raw: baseCodeRaw ?? null,
      base_zoning_code_norm: baseCodeNorm ?? null,

      zoning_attach_method: attachMethod ?? null,
      zoning_attach_confidence: attachConf ?? null,
      zoning_source_city: zoning_source_city ?? null,
      zoning_dataset_hash: zoning_dataset_hash ?? null,
      zoning_as_of_date: zoning_as_of_date ?? null,

      // ---- Optional passthrough (keeps your original around for debugging; can remove later) ----
      _src: {
        town: o.town ?? null,
        parcel_id: o.parcel_id ?? null,
        lat: o.lat ?? null,
        lon: o.lon ?? null
      }
    };

    if (!row.property_id || !row.parcel_id_raw) missing++;

    out.write(JSON.stringify(row) + "\n");
    wrote++;

    if (read % 250000 === 0) {
      console.error([prog] read=${read} wrote=${wrote} missing_core=${missing});
    }
  }

  out.end();
  console.error([done] read=${read} wrote=${wrote} missing_core=${missing});
}

run().catch(e=>{ console.error(e); process.exit(1); });
