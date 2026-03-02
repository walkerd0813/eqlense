
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { chromium } from "playwright";

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const key = a.slice(2);
    const val = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
    args[key] = val;
  }
  return args;
}

function ensureDir(p) { fs.mkdirSync(p, { recursive: true }); }
function isoNow() { return new Date().toISOString(); }
function sha1(s) { return crypto.createHash("sha1").update(s).digest("hex"); }

function normDateTag(mmddyyyy) {
  const m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(mmddyyyy.trim());
  if (!m) return mmddyyyy.replace(/[^\d]/g, "");
  const mm = m[1].padStart(2, "0");
  const dd = m[2].padStart(2, "0");
  const yy = m[3];
  return `${yy}-${mm}-${dd}`;
}

async function main() {
  const args = parseArgs(process.argv);
  const from = args.from;
  const to = args.to;
  const docType = String(args.docType ?? "");
  const outTag = String(args.outTag ?? "run");
  const maxPages = Number(args.maxPages ?? 50);

  if (!from || !to || !docType) {
    console.error("Usage: node ... --from MM/DD/YYYY --to MM/DD/YYYY --docType 100017 [--outTag deed] [--maxPages 25]");
    process.exit(1);
  }

  const backendRoot = process.cwd();
  const runId = `suffolk_${outTag}_${normDateTag(from)}_to_${normDateTag(to)}__${new Date().toISOString().replace(/[:.]/g, "").slice(0,15)}`;

  const outRowsDir = path.join(backendRoot, "publicData", "registry", "_index", "rows");
  const outHtmlDir = path.join(backendRoot, "publicData", "registry", "_index", "raw_html", runId);
  const outAuditDir = path.join(backendRoot, "publicData", "_audit", "registry");
  ensureDir(outRowsDir); ensureDir(outHtmlDir); ensureDir(outAuditDir);

  const outRowsPath = path.join(outRowsDir, `registry_index_rows__suffolk__${docType}__${normDateTag(from)}__${normDateTag(to)}__V1.ndjson`);
  const outAuditPath = path.join(outAuditDir, `index_harvest_audit__${runId}__V1.json`);

  const baseUrl = "https://www.masslandrecords.com/suffolk/D/Default.aspx";
  const audit = {
    created_at: isoNow(),
    run_id: runId,
    county: "suffolk",
    base_url: baseUrl,
    criteria: { from, to, doc_type_code: docType },
    pages_scraped: 0,
    rows_total: 0,
    errors: [],
    samples: []
  };

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ locale: "en-US" });
  const page = await context.newPage();

  try {
    console.log("====================================================");
    console.log("RUN B — HARVEST REGISTRY INDEX (SUFFOLK)");
    console.log("====================================================");
    console.log("[open]", baseUrl);
    await page.goto(baseUrl, { waitUntil: "domcontentloaded", timeout: 120000 });

    await page.selectOption('select[name="SearchCriteriaOffice1$DDL_OfficeName"]', { label: "Recorded Land" });
    await page.selectOption('select[name="SearchCriteriaName1$DDL_SearchName"]', { label: "Recorded Land Recorded Date Search" });

    await page.fill('input[name="SearchFormEx1$DRACSTextBox_DateFrom"]', from);
    await page.fill('input[name="SearchFormEx1$DRACSTextBox_DateTo"]', to);
    await page.selectOption('select[name="SearchFormEx1$ACSDropDownList_DocumentType"]', docType);

    try { await page.selectOption('select[name="SearchFormEx1$ACSDropDownList_Towns"]', "-2"); } catch {}

    console.log("[submit] search");
    await Promise.all([
      page.waitForLoadState("networkidle", { timeout: 120000 }).catch(() => null),
      page.click('input[name="SearchFormEx1$btnSearch"]'),
    ]);

    await page.waitForSelector("#DocList1_GridView_Document", { timeout: 120000 });
    console.log("[ok] results table present: #DocList1_GridView_Document");

    async function scrapeCurrentPage(pageIndex) {
      const html = await page.content();
      const htmlPath = path.join(outHtmlDir, `page_${String(pageIndex).padStart(3, "0")}.html`);
      fs.writeFileSync(htmlPath, html, "utf8");

      const rows = await page.$$eval("#DocList1_GridView_Document tr", (trs) => {
        const out = [];
        for (let i = 1; i < trs.length; i++) {
          const tr = trs[i];
          const tds = Array.from(tr.querySelectorAll("td"));
          if (tds.length < 6) continue;

          const linkText = (td) => (td.querySelector("a")?.textContent || "").trim();
          const linkHref = (td) => (td.querySelector("a")?.getAttribute("href") || "").trim();
          const pb = (href) => {
            const m = href.match(/__doPostBack\('([^']+)','([^']*)'\)/);
            return m ? { target: m[1], argument: m[2] } : null;
          };

          const file_date = linkText(tds[0]);
          const book_page = linkText(tds[1]);
          const type_desc = linkText(tds[2]);
          const town = linkText(tds[3]);

          const imgInput = tds[4].querySelector("input[type=submit]");
          const bskInput = tds[5].querySelector("input[type=submit]");

          out.push({
            file_date, book_page, type_desc, town,
            postback_targets: {
              file_date: pb(linkHref(tds[0])),
              book_page: pb(linkHref(tds[1])),
              type_desc: pb(linkHref(tds[2])),
              town: pb(linkHref(tds[3])),
            },
            img_button_name: imgInput?.getAttribute("name") || null,
            add_to_basket_name: bskInput?.getAttribute("name") || null,
            row_css: tr.getAttribute("class") || null,
          });
        }
        return out;
      });

      return { htmlPath, rows };
    }

    async function findAndClickNext() {
      const candidates = await page.$$("table.DataGridFooter a, table.DataGridFooter input[type=submit]");
      for (const el of candidates) {
        const txt = (await el.innerText().catch(() => ""))?.trim();
        const val = (await el.getAttribute("value").catch(() => ""))?.trim();
        const combined = `${txt} ${val}`.toLowerCase();
        if (combined === ">" || combined.includes("next") || combined.includes("›") || combined.includes(">>")) {
          await Promise.all([
            page.waitForLoadState("networkidle", { timeout: 120000 }).catch(() => null),
            el.click(),
          ]);
          await page.waitForSelector("#DocList1_GridView_Document", { timeout: 120000 });
          return true;
        }
      }
      return false;
    }

    const ws = fs.createWriteStream(outRowsPath, { flags: "w" });

    for (let p = 1; p <= maxPages; p++) {
      const { htmlPath, rows } = await scrapeCurrentPage(p);
      audit.pages_scraped += 1;
      audit.rows_total += rows.length;

      for (const r of rows) {
        const eventKey = `${r.file_date}|${r.book_page}|${r.type_desc}|${r.town}|${docType}`;
        const out = {
          event_id: sha1(eventKey),
          county: "suffolk",
          instrument_type_code: docType,
          file_date: r.file_date,
          book_page: r.book_page,
          type_desc: r.type_desc,
          town: r.town,
          source: {
            harvested_at: isoNow(),
            method: "playwright_dom_scrape_v1",
            base_url: baseUrl,
            run_id: runId,
            html_snapshot: path.relative(backendRoot, htmlPath).replace(/\\/g, "/"),
          },
          postback: r.postback_targets,
          controls: {
            img_button_name: r.img_button_name,
            add_to_basket_name: r.add_to_basket_name,
          },
        };
        ws.write(JSON.stringify(out) + "\n");
      }

      if (audit.samples.length < 3 && rows.length > 0) audit.samples.push(rows[0]);

      const moved = await findAndClickNext();
      if (!moved) {
        console.log(`[done] no next page detected after page ${p}.`);
        break;
      } else {
        console.log(`[page] moved to page ${p + 1}…`);
      }
    }

    ws.end();
    fs.writeFileSync(outAuditPath, JSON.stringify(audit, null, 2), "utf8");

    console.log("[ok] wrote rows:", outRowsPath);
    console.log("[ok] wrote audit:", outAuditPath);
    console.log("[done] RUN B complete.");
  } catch (err) {
    audit.errors.push(String(err?.stack || err));
    fs.writeFileSync(outAuditPath, JSON.stringify(audit, null, 2), "utf8");
    console.error("[fatal]", err);
    process.exitCode = 2;
  } finally {
    await context.close().catch(() => null);
    await browser.close().catch(() => null);
  }
}

main();
