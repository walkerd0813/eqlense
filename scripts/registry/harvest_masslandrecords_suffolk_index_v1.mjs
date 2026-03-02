import fs from "fs";
import path from "path";
import crypto from "crypto";
import { chromium } from "playwright";

function arg(name, def=null){
  const i = process.argv.indexOf(`--${name}`);
  if(i>=0 && process.argv[i+1] && !process.argv[i+1].startsWith("--")) return process.argv[i+1];
  return def;
}
function argInt(name, def){
  const v = arg(name, null);
  return v==null ? def : parseInt(v,10);
}
function ensureDir(p){ fs.mkdirSync(p, { recursive:true }); }
function nowTag(){
  const d = new Date();
  const pad = n => String(n).padStart(2,"0");
  return `${d.getFullYear()}${pad(d.getMonth()+1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}
function sha1(s){ return crypto.createHash("sha1").update(s).digest("hex"); }
function writeText(p, s){ fs.writeFileSync(p, s, "utf8"); }
function writeJSON(p, o){ fs.writeFileSync(p, JSON.stringify(o,null,2), "utf8"); }

const from = arg("from");
const to = arg("to");
const docType = arg("docType");
const outTag = arg("outTag","deed");
const maxPages = argInt("maxPages", 25);
const headful = arg("headed","false")==="true";

if(!from || !to || !docType){
  console.error("usage: node harvest... --from MM/DD/YYYY --to MM/DD/YYYY --docType 100017 --outTag deed --maxPages 25");
  process.exit(2);
}

const backendRoot = process.cwd();
const rawHtmlRoot = path.join(backendRoot, "publicData", "registry", "_index", "raw_html");
const rowsRoot = path.join(backendRoot, "publicData", "registry", "_index", "rows");
const auditRoot = path.join(backendRoot, "publicData", "_audit", "registry");

ensureDir(rawHtmlRoot);
ensureDir(rowsRoot);
ensureDir(auditRoot);

const runId = `suffolk__${outTag}__${docType}__${from.replaceAll("/","-")}__${to.replaceAll("/","-")}__${nowTag()}__V2`;
const runDir = path.join(rawHtmlRoot, runId);
ensureDir(runDir);

const outRows = path.join(rowsRoot, `registry_index_rows__suffolk__${outTag}__${docType}__${from.replaceAll("/","-")}__${to.replaceAll("/","-")}__${nowTag()}__V2.ndjson`);
const outAudit = path.join(auditRoot, `index_harvest_audit__${runId}.json`);

const url = "https://www.masslandrecords.com/suffolk/D/Default.aspx";

function looksLikeBotPage(html){
  const h = html.toLowerCase();
  return (
    h.includes("incapsula") ||
    h.includes("imperva") ||
    h.includes("request unsuccessful") ||
    h.includes("access denied") ||
    h.includes("verify you are human") ||
    h.includes("captcha")
  );
}

function parseDocListTable(html){
  // Extract rows from DocList1_GridView_Document
  const rows = [];
  const tableMatch = html.match(/<table[^>]+id="DocList1_GridView_Document"[\s\S]*?<\/table>/i);
  if(!tableMatch) return rows;

  const table = tableMatch[0];
  const trMatches = table.match(/<tr[\s\S]*?<\/tr>/gi) || [];
  for(const tr of trMatches){
    if(tr.toLowerCase().includes("datagridheader")) continue;
    // crude TD scrape
    const tds = tr.match(/<td[\s\S]*?<\/td>/gi) || [];
    if(tds.length < 4) continue;
    const clean = s => s
      .replace(/<br\s*\/?>/gi, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/&nbsp;/g, " ")
      .replace(/&#39;/g, "'")
      .replace(/\s+/g, " ")
      .trim();

    const fileDate = clean(tds[0]);
    const bookPage = clean(tds[1]);
    const typeDesc = clean(tds[2]);
    const town = clean(tds[3]);

    if(!fileDate || !bookPage || !typeDesc) continue;
    rows.push({ file_date: fileDate, book_page: bookPage, type_desc: typeDesc, town });
  }
  return rows;
}

async function main(){
  const audit = {
    created_at: new Date().toISOString(),
    run_id: runId,
    url,
    from,
    to,
    docType,
    outTag,
    maxPages,
    pages_attempted: 0,
    pages_saved: 0,
    total_rows: 0,
    bot_page: false,
    bot_hint: null,
    out_rows: outRows,
    out_audit: outAudit,
    raw_html_dir: runDir,
    notes: []
  };

  const browser = await chromium.launch({ headless: !headful });
  const context = await browser.newContext({
    viewport: { width: 1400, height: 900 },
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    locale: "en-US"
  });
  const page = await context.newPage();

  console.log(`[open] ${url}`);
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
  await page.waitForTimeout(1500);

  // dump landing HTML always
  const landingHtml = await page.content();
  writeText(path.join(runDir, "landing.html"), landingHtml);
  await page.screenshot({ path: path.join(runDir, "landing.png"), fullPage: true });

  if(looksLikeBotPage(landingHtml)){
    audit.bot_page = true;
    audit.bot_hint = "Landing HTML looks like Imperva/bot page. See landing.html / landing.png";
    audit.notes.push("If bot blocked: try --headed true once, or run from a normal desktop session; may require manual session.");
    writeJSON(outAudit, audit);
    console.log(`[fatal] Bot/challenge page detected. See: ${path.join(runDir, "landing.html")}`);
    await browser.close();
    process.exit(3);
  }

  // Use stable ID selectors (ASP.NET uses underscores in id)
  const selOffice = "#SearchCriteriaOffice1_DDL_OfficeName";
  const selSearchName = "#SearchCriteriaName1_DDL_SearchName";
  const selFrom = "#SearchFormEx1_DRACSTextBox_DateFrom";
  const selTo = "#SearchFormEx1_DRACSTextBox_DateTo";
  const selDocType = "#SearchFormEx1_ACSDropDownList_DocumentType";
  const selTown = "#SearchFormEx1_ACSDropDownList_Towns";

  // Wait for core controls to exist
  await page.waitForSelector(selOffice, { timeout: 60000 });
  await page.waitForSelector(selSearchName, { timeout: 60000 });
  await page.waitForSelector(selFrom, { timeout: 60000 });
  await page.waitForSelector(selTo, { timeout: 60000 });
  await page.waitForSelector(selDocType, { timeout: 60000 });

  // Fill criteria
  await page.selectOption(selOffice, { label: "Recorded Land" });
  await page.selectOption(selSearchName, { label: "Recorded Land Recorded Date Search" });
  await page.fill(selFrom, from);
  await page.fill(selTo, to);
  await page.selectOption(selDocType, { value: docType });

  // Town = -2 (ALL) if exists
  const townExists = await page.$(selTown);
  if(townExists){
    await page.selectOption(selTown, { value: "-2" }).catch(()=>{});
  }

  // Click Search (button id usually SearchFormEx1_btnSearch)
  const btn = await page.$("#SearchFormEx1_btnSearch") || await page.$('input[name="SearchFormEx1$btnSearch"]');
  if(!btn) throw new Error("Search button not found (#SearchFormEx1_btnSearch / name SearchFormEx1$btnSearch).");

  await Promise.all([
    page.waitForLoadState("networkidle", { timeout: 90000 }).catch(()=>{}),
    btn.click()
  ]);

  // dump results html
  const html1 = await page.content();
  writeText(path.join(runDir, "results_page_001.html"), html1);
  await page.screenshot({ path: path.join(runDir, "results_page_001.png"), fullPage: true });

  let rows = parseDocListTable(html1);
  audit.pages_attempted = 1;
  audit.pages_saved = 1;
  audit.total_rows = rows.length;

  // write NDJSON
  const lines = rows.map(r => JSON.stringify({
    county: "suffolk",
    model: "Recorded Land Recorded Date Search",
    from,
    to,
    doc_type_code: docType,
    file_date: r.file_date,
    book_page: r.book_page,
    type_desc: r.type_desc,
    town: r.town,
    source: { url, run_id: runId, page: 1 }
  })).join("\n") + (rows.length ? "\n" : "");
  fs.writeFileSync(outRows, lines, "utf8");

  writeJSON(outAudit, audit);

  console.log(`[done] rows: ${rows.length}`);
  console.log(`[ok] wrote: ${outRows}`);
  console.log(`[ok] audit: ${outAudit}`);

  await browser.close();
}

main().catch(err => {
  console.error("[fatal]", err);
  process.exit(1);
});
