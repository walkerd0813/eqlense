/**
 * buildComps.js — XLSX Parsing Version (Corrected)
 * ------------------------------------------------
 * Converts XLSX files into normalized JSON with clean:
 *  - ZIP codes
 *  - numbers
 *  - Excel dates
 *  - PPSF calculation
 */

const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");

const COMPS_DIR = path.join(__dirname, "comps");

/* ------------------------------------------
   Helpers
------------------------------------------ */

// Fix Excel dates into real JS dates
function excelDateToJSDate(serial) {
  if (!serial || isNaN(serial)) return null;
  const excelEpoch = new Date(Date.UTC(1899, 11, 30));
  return new Date(excelEpoch.getTime() + serial * 86400000);
}

// Clean numbers (handles commas, symbols, text)
function parseNum(v) {
  if (v === undefined || v === null) return null;
  const cleaned = Number(String(v).replace(/[^0-9.-]/g, ""));
  return isNaN(cleaned) ? null : cleaned;
}

// Force ZIP to 5-digit string
function parseZip(zip) {
  if (!zip) return null;
  const z = String(zip).trim().padStart(5, "0");
  return /^[0-9]{5}$/.test(z) ? z : null;
}

/* ------------------------------------------
   XLSX → JSON Converter
------------------------------------------ */

function parseExcel(filePath) {
  const workbook = XLSX.readFile(filePath);
  const sheetName = workbook.SheetNames[0];
  const rows = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName], {
    defval: "",
  });

  const cleaned = [];

  for (const r of rows) {
    const obj = {
      address: r.ADDRESS?.trim() || null,
      town: r.TOWN_DESC?.trim() || null,
      zip: parseZip(r.ZIP_CODE),

      beds: parseNum(r.NO_BEDROOMS),
      baths: r.BTH_DESC || null,

      sqft: parseNum(r.SQUARE_FEET),
      salePrice: parseNum(r.SALE_PRICE),

      settledDate: excelDateToJSDate(parseNum(r.SETTLED_DATE)),

      lotSize: parseNum(r.LOT_SIZE),
      yearBuilt: parseNum(r.YEAR_BUILT),

      style: r.STYLE_SF || null,
      garageSpaces: parseNum(r.GARAGE_SPACES_SF),
      parkingSpaces: parseNum(r.PARKING_SPACES_SF),

      marketTime: parseNum(r.MARKET_TIME),
      listPrice: parseNum(r.LIST_PRICE),

      heating: r.HEATING_SF || null,
      siteCondition: r.SITE_CONDITION_CI || null,
      remarks: r.REMARKS || null,
    };

    // PPSF (computed)
    obj.ppsf =
      obj.salePrice && obj.sqft
        ? Number((obj.salePrice / obj.sqft).toFixed(2))
        : null;

    cleaned.push(obj);
  }

  return cleaned;
}

/* ------------------------------------------
   Build JSON Files
------------------------------------------ */

function build() {
  console.log("📘 Building comp JSON files using XLSX…\n");

  const files = [
    { excel: "MA_SingleFamily_Sales_Last12Months.xlsx", json: "singleFamily.json" },
    { excel: "MA_MultiFamily_Sales_Last12Months.xlsx", json: "multiFamily.json" },
    { excel: "MA_Condos_Sales_Last12Months.xlsx", json: "condos.json" },
  ];

  for (const file of files) {
    const excelPath = path.join(COMPS_DIR, file.excel);

    if (!fs.existsSync(excelPath)) {
      console.log(`⚠️ Missing file: ${excelPath}`);
      continue;
    }

    console.log(`📄 Reading: ${file.excel}`);

    const data = parseExcel(excelPath);

    const outFile = path.join(COMPS_DIR, file.json);
    fs.writeFileSync(outFile, JSON.stringify(data, null, 2));

    console.log(`✅ Saved → ${file.json} (${data.length} records)\n`);
  }

  console.log("🎉 All comps rebuilt from XLSX successfully!");
}

build();
