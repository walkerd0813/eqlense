import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";

function argMap(argv) {
  const m = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : "true";
      m[k] = v;
    }
  }
  return m;
}

function readJSON(p) {
  let s = fs.readFileSync(p, "utf8");
  if (s.charCodeAt(0) === 0xFEFF) s = s.slice(1);
  return JSON.parse(s);
}

function writeJSON(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
}

function sha256Text(s) {
  return crypto.createHash("sha256").update(s).digest("hex");
}

function nowTag() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function normParcelId(v) {
  if (v === null || v === undefined) return null;
  let s = String(v).trim();
  if (!s) return null;
  s = s.replace(/\s+/g, "");
  return s;
}

function toNumber(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).replace(/[$,]/g, "").trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function toInt(v) {
  const n = toNumber(v);
  return n === null ? null : Math.trunc(n);
}

function parseLSDate(ls) {
  if (ls === null || ls === undefined) return null;
  const s = String(ls).trim();
  if (!s) return null;
  if (/^\d{8}$/.test(s)) {
    const y = s.slice(0,4), m = s.slice(4,6), d = s.slice(6,8);
    return `${y}-${m}-${d}`;
  }
  const t = Date.parse(s);
  if (Number.isFinite(t)) return new Date(t).toISOString().slice(0,10);
  return null;
}

function normStr(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s ? s : null;
}

function normAddr(v) {
  const s = normStr(v);
  if (!s) return null;
  return s.replace(/\s+/g," ").replace(/[.,]/g,"").toUpperCase();
}

function deriveOwnerType(name1, name2, co) {
  const s = [name1, name2, co].filter(Boolean).join(" ").toUpperCase();
  if (!s) return "unknown";
  const has = (re) => re.test(s);
  if (has(/\bLLC\b|\bL\.L\.C\b/)) return "llc";
  if (has(/\bTRUST\b|\bTR\b/)) return "trust";
  if (has(/\bINC\b|\bCORP\b|\bCO\b|\bCOMPANY\b|\bLTD\b/)) return "corp";
  if (has(/\bBANK\b|\bN\.A\b|\bNATIONAL ASSOCIATION\b|\bCU\b|\bCREDIT UNION\b/)) return "bank";
  if (has(/\bCITY OF\b|\bTOWN OF\b|\bSTATE OF\b|\bCOMMONWEALTH\b|\bUSA\b|\bUNITED STATES\b/)) return "gov";
  if (has(/\bCHURCH\b|\bFOUNDATION\b|\bASSOCIATION\b|\bNONPROFIT\b/)) return "nonprofit";
  // If looks like "LAST, FIRST" or two words, we call it individual only when there's no entity token
  return "individual";
}

function derivePropertyClass(useCode) {
  const u = (useCode || "").toString().trim().toUpperCase();
  if (!u) return "unknown";
  // Minimal deterministic buckets (can be expanded later)
  // MA DOR style use codes often: 101 SF, 104 2F, 105 3F, 111 condo, etc. (varies by town)
  if (/^1\d\d$/.test(u)) {
    if (u === "111" || u === "112" || u === "113") return "condo";
    if (u === "104" || u === "105" || u === "106" || u === "107" || u === "108" || u === "109") return "multifamily";
    return "residential";
  }
  if (/^3\d\d$/.test(u) || /^4\d\d$/.test(u)) return "commercial";
  if (/^9\d\d$/.test(u)) return "land";
  return "mixed";
}

async function fetchJSON(url, params) {
  const u = new URL(url);
  Object.entries(params || {}).forEach(([k,v]) => u.searchParams.set(k, String(v)));
  const res = await fetch(u.toString(), { headers: { "User-Agent": "EquityLens/phase4_assessor_attach_v2" } });
  if (!res.ok) {
    const txt = await res.text().catch(()=> "");
    throw new Error(`[http ${res.status}] ${u} :: ${txt.slice(0,200)}`);
  }
  return res.json();
}

async function arcgisCount(layerUrl, where) {
  const q = layerUrl.replace(/\/+$/,"") + "/query";
  const j = await fetchJSON(q, { f:"json", where, returnCountOnly:"true" });
  if (j.error) throw new Error(JSON.stringify(j.error));
  return j.count ?? 0;
}

async function arcgisPage(layerUrl, where, offset, pageSize, outFields) {
  const q = layerUrl.replace(/\/+$/,"") + "/query";
  const j = await fetchJSON(q, {
    f:"json",
    where,
    outFields: outFields.join(","),
    returnGeometry: "false",
    resultRecordCount: pageSize,
    resultOffset: offset
  });
  if (j.error) throw new Error(JSON.stringify(j.error));
  return j.features || [];
}

async function downloadMassGISSubsetToMap(layerUrl, where, outFields) {
  const count = await arcgisCount(layerUrl, where);
  console.log(`[info] MassGIS where="${where}" count=${count}`);
  const pageSize = 2000;
  let offset = 0;
  const m = new Map();
  while (offset < count) {
    const feats = await arcgisPage(layerUrl, where, offset, pageSize, outFields);
    if (feats.length === 0) break;
    for (const f of feats) {
      const a = f.attributes || {};
      const pid = normParcelId(a.MAP_PAR_ID ?? a.MAP_PARID ?? a.PARCEL_ID ?? a.PARCELID);
      if (!pid) continue;
      if (!m.has(pid)) {
        m.set(pid, a);
      } else {
        const cur = m.get(pid);
        const curTot = toNumber(cur.TOTAL_VAL);
        const nxtTot = toNumber(a.TOTAL_VAL);
        if (curTot === null && nxtTot !== null) m.set(pid, a);
      }
    }
    offset += feats.length;
    console.log(`[progress] MassGIS fetched ${Math.min(offset, count)}/${count}`);
    await sleep(20);
  }
  return { count, map: m };
}

function findLatestNormalizedNdjson(root, city) {
  const dir = path.join(root, "publicData", "assessors", city, "_normalized");
  if (!fs.existsSync(dir)) return null;
  const files = fs.readdirSync(dir).filter(f => f.toLowerCase().endsWith(".ndjson"));
  if (files.length === 0) return null;
  const full = files.map(f => path.join(dir, f));
  full.sort((a,b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);
  return full[0];
}

function readNdjsonToMap(ndPath) {
  const m = new Map();
  const lines = fs.readFileSync(ndPath, "utf8").split(/\r?\n/);
  for (const line of lines) {
    if (!line.trim()) continue;
    let obj;
    try { obj = JSON.parse(line); } catch { continue; }
    const pid = normParcelId(obj.parcel_id_norm ?? obj.parcel_id_raw);
    if (!pid) continue;
    m.set(pid, obj);
  }
  return m;
}

function pick(munVal, mgVal, conv) {
  const a = conv ? conv(munVal) : munVal;
  if (a !== null && a !== undefined && `${a}`.trim?.() !== "") return a;
  const b = conv ? conv(mgVal) : mgVal;
  if (b !== null && b !== undefined && `${b}`.trim?.() !== "") return b;
  return null;
}

function buildCombinedRecord(city, municipalRec, mg, meta) {
  const out = {
    city,
    parcel_id_norm: null,

    // ownership + mailing
    owner_name_1: null,
    owner_name_2: null,
    owner_company: null,
    owner_type_norm: "unknown",
    mailing_address_full: null,
    mailing_city: null,
    mailing_state: null,
    mailing_zip: null,
    owner_occupied_flag: null,
    is_absentee_owner: null,

    // assessed
    assessed_total: null,
    assessed_land: null,
    assessed_building: null,
    assessed_other: null,
    assessed_exemptions: null,
    assessed_year: null,

    // transactions (assessor anchors)
    last_sale_date: null,
    last_sale_price: null,
    deed_book: null,
    deed_page: null,
    registry_id: null,

    // land/use/type
    use_code_norm: null,
    property_class: "unknown",
    zoning_raw: null,
    lot_size_raw: null,
    lot_units_raw: null,
    lot_sqft_est: null,   // numeric only; unit conversion handled later
    lot_frontage_ft: null,
    lot_depth_ft: null,
    corner_lot_flag: null,
    utilities_flag_raw: null,
    topography_raw: null,
    grade_raw: null,

    // structure + area
    year_built_est: null,
    effective_year_built: null,
    units_est: null,
    bedrooms: null,
    bathrooms: null,
    rooms_total: null,
    stories: null,
    style_raw: null,
    construction_type: null,
    exterior_walls: null,
    roof_type: null,
    foundation_type: null,
    heating_type: null,
    fuel_type: null,
    ac_type: null,
    electric_service: null,
    condition_raw: null,
    quality_raw: null,
    remodel_year: null,
    renovation_code: null,

    building_area_sqft: null,
    gross_area_sqft: null,
    finished_area_sqft: null,
    living_area_sqft: null,
    res_area_sqft: null,
    basement_area_sqft: null,
    basement_finished_flag: null,

    // condo
    condo_id: null,
    unit_designator: null,
    building_id: null,
    living_units: null,
    total_units_in_building: null,
    property_subtype: null,

    // tax/enforcement
    tax_year: null,
    tax_bill_amount: null,
    delinquency_flag: null,
    delinquent_amount: null,
    lien_flag: null,
    lien_amount: null,

    // evidence + qa
    evidence: {
      as_of: new Date().toISOString(),
      municipal_present: !!municipalRec,
      massgis_present: !!mg,
      municipal_source_id: municipalRec?.source_id ?? null,
      municipal_source_path: municipalRec?.evidence?.source_path ?? null,
      massgis_layer: meta.massgis_layer_url,
      massgis_where: meta.massgis_where,
      dataset_hash: meta.dataset_hash
    },
    qa_flags: []
  };

  out.parcel_id_norm = normParcelId(
    municipalRec?.parcel_id_norm ?? municipalRec?.parcel_id_raw ??
    mg?.MAP_PAR_ID
  );

  // Ownership: prefer municipal raw keys if exist, else MassGIS
  const munRaw = municipalRec?.raw || {};
  out.owner_name_1 = pick(munRaw.OWNER1 ?? munRaw.OWNER_1 ?? munRaw.OWNERNAME1, mg?.OWNER1, normStr);
  out.owner_name_2 = pick(munRaw.OWNER2 ?? munRaw.OWNER_2 ?? munRaw.OWNERNAME2, mg?.OWNER2, normStr);
  out.owner_company = pick(munRaw.OWN_CO ?? munRaw.OWNER_CO ?? munRaw.COMPANY, mg?.OWN_CO, normStr);

  out.mailing_address_full = pick(
    munRaw.OWN_ADDR ?? munRaw.MAIL_ADDR ?? munRaw.MAILING_ADDR,
    mg?.OWN_ADDR,
    normStr
  );
  out.mailing_city = pick(munRaw.OWN_CITY ?? munRaw.MAIL_CITY, mg?.OWN_CITY, normStr);
  out.mailing_state = pick(munRaw.OWN_STATE ?? munRaw.MAIL_STATE, mg?.OWN_STATE, normStr);
  out.mailing_zip = pick(munRaw.OWN_ZIP ?? munRaw.MAIL_ZIP, mg?.OWN_ZIP, normStr);

  out.owner_type_norm = deriveOwnerType(out.owner_name_1, out.owner_name_2, out.owner_company);

  // Owner occupied: explicit flag if present, else mailing vs site address compare
  const explicitOcc = munRaw.OWNER_OCC ?? munRaw.OWNER_OCCUPIED ?? munRaw.OCCUPIED ?? null;
  if (explicitOcc !== null && explicitOcc !== undefined && `${explicitOcc}`.trim() !== "") {
    const v = `${explicitOcc}`.toUpperCase();
    out.owner_occupied_flag = (v === "Y" || v === "YES" || v === "1" || v === "TRUE");
  } else {
    const site = pick(munRaw.SITE_ADDR ?? munRaw.SITEADDRESS ?? munRaw.ADDRESS, mg?.SITE_ADDR, normStr);
    const mail = out.mailing_address_full;
    const siteN = normAddr(site);
    const mailN = normAddr(mail);
    if (siteN && mailN) out.owner_occupied_flag = (siteN === mailN);
    else out.owner_occupied_flag = null;
  }
  if (out.owner_occupied_flag === null) out.is_absentee_owner = null;
  else out.is_absentee_owner = !out.owner_occupied_flag;

  // assessed
  out.assessed_land = pick(municipalRec?.land_value_raw, mg?.LAND_VAL, toNumber);
  out.assessed_building = pick(municipalRec?.building_value_raw, mg?.BLDG_VAL, toNumber);
  out.assessed_other = pick(munRaw.OTHER_VAL, mg?.OTHER_VAL, toNumber);
  out.assessed_total = pick(municipalRec?.total_value_raw, mg?.TOTAL_VAL, toNumber);
  out.assessed_year = pick(munRaw.FY ?? municipalRec?.assessed_year_raw, mg?.FY, toInt);

  // sale anchors + deed refs
  out.last_sale_date = pick(municipalRec?.last_sale_date_raw, mg?.LS_DATE, parseLSDate);
  out.last_sale_price = pick(municipalRec?.last_sale_price_raw, mg?.LS_PRICE, toNumber);
  out.deed_book = pick(munRaw.LS_BOOK ?? munRaw.BOOK, mg?.LS_BOOK, normStr);
  out.deed_page = pick(munRaw.LS_PAGE ?? munRaw.PAGE, mg?.LS_PAGE, normStr);
  out.registry_id = pick(munRaw.REG_ID ?? munRaw.REGISTRY_ID, mg?.REG_ID, normStr);

  // land/use/type
  out.use_code_norm = pick(munRaw.USE_CODE ?? municipalRec?.use_code_raw, mg?.USE_CODE, normStr);
  out.property_class = derivePropertyClass(out.use_code_norm);
  out.zoning_raw = pick(munRaw.ZONING, mg?.ZONING, normStr);

  out.lot_size_raw = pick(munRaw.LOT_SIZE ?? munRaw.LOTSZ ?? municipalRec?.lot_area_sqft_raw, mg?.LOT_SIZE, (v)=>v);
  out.lot_units_raw = pick(munRaw.LOT_UNITS ?? munRaw.LOTUNIT, mg?.LOT_UNITS, normStr);
  out.lot_sqft_est = pick(munRaw.LOT_SIZE_SQFT ?? municipalRec?.lot_area_sqft_raw, mg?.LOT_SIZE, toNumber);

  out.rooms_total = pick(munRaw.NUM_ROOMS ?? munRaw.ROOMS, mg?.NUM_ROOMS, toInt);
  out.stories = pick(munRaw.STORIES, mg?.STORIES, normStr);
  out.style_raw = pick(munRaw.STYLE, mg?.STYLE, normStr);

  // structure + area
  out.year_built_est = pick(munRaw.YEAR_BUILT ?? municipalRec?.year_built_raw, mg?.YEAR_BUILT, toInt);
  out.units_est = pick(munRaw.UNITS ?? municipalRec?.units_raw, mg?.UNITS, toInt);
  out.building_area_sqft = pick(munRaw.BLD_AREA ?? munRaw.BUILDING_AREA, mg?.BLD_AREA, toInt);
  out.res_area_sqft = pick(munRaw.RES_AREA, mg?.RES_AREA, toInt);
  out.living_area_sqft = pick(munRaw.LIVING_AREA ?? munRaw.LIV_AREA, null, toInt);

  // Additional optional keys from municipal only (keep if present)
  const opt = (keys) => {
    for (const k of keys) {
      if (munRaw[k] !== undefined && munRaw[k] !== null && `${munRaw[k]}`.trim() !== "") return munRaw[k];
    }
    return null;
  };
  out.bedrooms = toInt(opt(["BEDROOMS","BEDS","BED_RMS","BEDRM"]));
  out.bathrooms = toNumber(opt(["BATHROOMS","BATHS","BATH_RMS","BATHRM"]));
  out.basement_area_sqft = toInt(opt(["BASEMENT_AREA","BSMT_AREA","BASM_AREA"]));
  const bfin = opt(["BASEMENT_FINISHED","BSMT_FIN","FIN_BSMT"]);
  if (bfin !== null) {
    const v = `${bfin}`.toUpperCase();
    out.basement_finished_flag = (v === "Y" || v === "YES" || v === "1" || v === "TRUE");
  }

  out.construction_type = normStr(opt(["CONST_TYPE","CONSTRUCTION","CONST"]));
  out.exterior_walls = normStr(opt(["EXT_WALLS","EXTERIOR","EXTWALL"]));
  out.roof_type = normStr(opt(["ROOF_TYPE","ROOF"]));
  out.foundation_type = normStr(opt(["FOUNDATION","FOUND_TYPE"]));
  out.heating_type = normStr(opt(["HEAT_TYPE","HEATING"]));
  out.fuel_type = normStr(opt(["FUEL_TYPE","FUEL"]));
  out.ac_type = normStr(opt(["AC_TYPE","AIR_COND","AC"]));
  out.electric_service = normStr(opt(["ELECTRIC","ELEC_SERV","ELEC_SERVICE"]));
  out.condition_raw = normStr(opt(["CONDITION","COND"]));
  out.quality_raw = normStr(opt(["QUALITY","QUAL","GRADE"]));
  out.remodel_year = toInt(opt(["REMODEL_YR","REMODEL_YEAR","RENOV_YR"]));
  out.renovation_code = normStr(opt(["RENOV_CODE","RENOVATION_CODE"]));

  // QA
  if (!out.parcel_id_norm) out.qa_flags.push("MISSING_PARCEL_ID");
  if (out.assessed_total !== null && out.assessed_total < 0) out.qa_flags.push("NEGATIVE_TOTAL_VAL");
  if (out.property_class === "multifamily" && (out.units_est === null || out.units_est <= 1)) out.qa_flags.push("MF_MISSING_OR_LOW_UNITS");
  return out;
}

function coverageStats(records, fields) {
  const n = records.length;
  const c = {};
  for (const f of fields) c[f] = 0;
  for (const r of records) {
    for (const f of fields) {
      const v = r[f];
      if (v !== null && v !== undefined && `${v}`.trim?.() !== "") c[f] += 1;
    }
  }
  const pct = {};
  for (const f of fields) pct[f] = n ? Math.round((c[f]/n)*1000)/10 : 0;
  return { n, counts: c, pct };
}

const args = argMap(process.argv);
const root = args.root ? path.resolve(args.root) : process.cwd();
const configPath = args.config ? path.resolve(args.config) : path.join(root, "phase4_assessor_attach_config_v2.json");

const cfg = readJSON(configPath);
const massgisLayer = cfg.massgis_layer_url;

const outAuditDir = path.join(root, "publicData", "_audit", "phase4_assessor");
fs.mkdirSync(outAuditDir, { recursive: true });

const tag = nowTag();
const run = {
  created_at: new Date().toISOString(),
  config: configPath,
  massgis_layer_url: massgisLayer,
  per_city: [],
  outputs: {}
};

for (const c of (cfg.cities || [])) {
  const city = c.city;
  const where = c.massgis_where;
  console.log(`\n[city] ${city}`);

  const municipalLatest = findLatestNormalizedNdjson(root, city);
  const municipalMap = municipalLatest ? readNdjsonToMap(municipalLatest) : new Map();
  console.log(`[info] municipal_latest: ${municipalLatest ?? "(none)"}`);
  console.log(`[info] municipal_rows: ${municipalMap.size}`);

  const outFields = [
    "MAP_PAR_ID","CITY","ZIP","TOWN_ID","PROP_ID","LOC_ID",
    "BLDG_VAL","LAND_VAL","OTHER_VAL","TOTAL_VAL","FY",
    "LS_DATE","LS_PRICE","LS_BOOK","LS_PAGE","REG_ID",
    "USE_CODE","ZONING",
    "LOT_SIZE","LOT_UNITS",
    "YEAR_BUILT","BLD_AREA","RES_AREA","NUM_ROOMS","STORIES","STYLE",
    "UNITS",
    "OWNER1","OWN_ADDR","OWN_CITY","OWN_STATE","OWN_ZIP","OWN_CO",
    "SITE_ADDR","ADDR_NUM","FULL_STR"
  ];
  const mg = await downloadMassGISSubsetToMap(massgisLayer, where, outFields);
  console.log(`[info] massgis_rows: ${mg.map.size}`);

  const ids = new Set([...municipalMap.keys(), ...mg.map.keys()]);
  const combined = [];
  const meta = {
    dataset_hash: sha256Text(`${massgisLayer}||${where}||${tag}`),
    massgis_layer_url: massgisLayer,
    massgis_where: where
  };
  for (const pid of ids) {
    const mun = municipalMap.get(pid) || null;
    const mgRow = mg.map.get(pid) || null;
    const rec = buildCombinedRecord(city, mun, mgRow, meta);
    combined.push(rec);
  }

  const fieldsForCoverage = [
    "owner_name_1","mailing_address_full","owner_occupied_flag",
    "assessed_total","assessed_land","assessed_building","assessed_year",
    "last_sale_date","last_sale_price","deed_book","deed_page",
    "use_code_norm","property_class","units_est","year_built_est",
    "building_area_sqft","res_area_sqft","rooms_total","stories","style_raw"
  ];
  const after = coverageStats(combined, fieldsForCoverage);

  const outDir = path.join(root, "publicData", "assessors", city, "_master");
  fs.mkdirSync(outDir, { recursive: true });
  const outNd = path.join(outDir, `assessor_master__${city}__${tag}__V2.ndjson`);
  const ws = fs.createWriteStream(outNd, { encoding: "utf8" });
  for (const r of combined) ws.write(JSON.stringify(r) + "\n");
  ws.end();

  console.log(`[ok] wrote master ndjson: ${outNd}`);
  run.per_city.push({
    city,
    municipal_latest: municipalLatest,
    municipal_rows: municipalMap.size,
    massgis_where: where,
    massgis_count: mg.count,
    massgis_rows: mg.map.size,
    union_rows: combined.length,
    coverage_after: after,
    outputs: { master_ndjson: outNd }
  });
}

const auditPath = path.join(outAuditDir, `phase4_assessor_attach_pack_v2__${tag}.json`);
writeJSON(auditPath, run);
console.log(`\n[done] wrote audit: ${auditPath}`);

const currentPtr = path.join(root, "publicData", "assessors", "_frozen", "CURRENT_PHASE4_ASSESSOR_MASTER.json");
writeJSON(currentPtr, {
  updated_at: new Date().toISOString(),
  note: "AUTO: Phase4 Assessor Attach Pack v2 (expanded fields)",
  audit: auditPath,
  cities: run.per_city.map(x=>({city:x.city, master_ndjson:x.outputs.master_ndjson}))
});
console.log(`[ok] wrote CURRENT pointer: ${currentPtr}`);
