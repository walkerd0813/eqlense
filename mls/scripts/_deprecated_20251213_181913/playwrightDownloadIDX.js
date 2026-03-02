// backend/mls/scripts/playwrightDownloadIDX.js
import { chromium } from "playwright";
import fs from "fs";
import path from "path";
import dotenv from "dotenv";

dotenv.config({ path: path.join(process.cwd(), "mls/.env.mls") });

const LOGIN_URL = process.env.MLS_LOGIN_URL;
const USERNAME = process.env.MLS_USERNAME;
const PASSWORD = process.env.MLS_PASSWORD;

// FOLDER HELPERS
function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

// PROPERTY TYPES WE ARE USING (V1)
const PROPERTY_TYPES = [
  { key: "single_family", text: "Single Family (SF)" },
  { key: "multi_family",  text: "Multi-Family (MF)" },
  { key: "condo",         text: "Condo (CC)" },
  { key: "land",          text: "Land (LD)" },
];

// TWO STATUS MODES
const STATUSES = [
  { key: "active", nthIndex: 0 }, // FIRST occurrence on page = Active section
  { key: "sold",   nthIndex: 1 }, // SECOND occurrence on page = Sold section
];

// -----------------------------------------------------
// LOGIN FLOW
// -----------------------------------------------------
async function login(page) {
  console.log("🔐 Logging into MLS PIN...");
  await page.goto(LOGIN_URL, { waitUntil: "networkidle" });

  // Handle cookie popup if appears
  try {
    await page.getByRole("button", { name: "OK" }).click({ timeout: 3000 });
    console.log("🍪 Cookie popup dismissed.");
  } catch {
    console.log("🍪 No cookie popup appeared.");
  }

  // Fill Agent ID (username)
  await page.getByPlaceholder("Enter Your Agent ID").fill(USERNAME);

  // Fill Password
  await page.getByPlaceholder("Password").fill(PASSWORD);

  // Click Sign In button
  await page.getByRole("button", { name: "Sign In" }).click();

  // Handle "Sign-In Violation" page (sometimes appears)
  try {
    await page.waitForTimeout(2000);
    const continueBtn = page.getByRole("button", { name: "Click Here to Continue to Pinergy" });
    if (await continueBtn.isVisible({ timeout: 2000 })) {
      console.log("⚠️ Sign-In Violation screen detected. Continuing…");
      await continueBtn.click();
    } else {
      console.log("➡️ No violation screen. Continuing normally.");
    }
  } catch {
    console.log("➡️ No violation handler needed.");
  }

  await page.waitForLoadState("networkidle");
  console.log("✅ Logged in successfully.");
}

// -----------------------------------------------------
// NAVIGATE TO IDX DOWNLOADS
// -----------------------------------------------------
async function navigateToIDX(page) {
  console.log("🧭 Navigating to IDX Downloads...");

  // Click top nav "Tools"
  await page.getByRole("link", { name: "Tools" }).click();
  await page.waitForLoadState("networkidle");

  // Click the "IDX Downloads" tile
  await page.getByRole("link", { name: "IDX Downloads" }).click();
  await page.waitForLoadState("networkidle");

  console.log("📥 IDX Downloads page loaded.");
}

// -----------------------------------------------------
// DOWNLOAD IMPLEMENTATION (no Save As dialog)
// -----------------------------------------------------
async function downloadCombo(page, ptype, status) {
  console.log(`\n➡️ Downloading ${status.key.toUpperCase()} — ${ptype.text}`);

  // Find the correct link in Active or Sold section by text + index
  const locator = page.locator(`text="${ptype.text}"`).nth(status.nthIndex);

  // Get the raw href (this has the encrypted token / query, like the long string you pasted)
  const href = await locator.getAttribute("href");
  if (!href) {
    throw new Error(`No href found for ${ptype.text} (${status.key})`);
  }

  // Build absolute URL from href + current page URL
  const currentUrl = page.url();
  const url = new URL(href, currentUrl).toString();

  console.log(`🔗 Fetching IDX file directly from: ${url}`);

  // Fetch the file using Playwright's request API (shares cookies/session)
  const response = await page.request.get(url);
  if (!response.ok()) {
    throw new Error(`Bad response ${response.status()} for ${url}`);
  }

  const body = await response.body();

  // Destination folder + filename
  const rawFolder = path.join(process.cwd(), "mls/raw", ptype.key, status.key);
  ensureDir(rawFolder);

  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const finalName = `${ptype.key}_${status.key}_${timestamp}.txt`;
  const dest = path.join(rawFolder, finalName);

  // Save bytes directly – bypassing the Windows "Save As" dialog entirely
  fs.writeFileSync(dest, body);
  console.log(`✔ Saved → ${dest}`);
}

// -----------------------------------------------------
// MAIN EXECUTION
// -----------------------------------------------------
export async function runIDXDownloader() {
  const browser = await chromium.launch({ headless: false });
  const context = await browser.newContext({ acceptDownloads: true });
  const page = await context.newPage();

  try {
    await login(page);
    await navigateToIDX(page);

    for (const status of STATUSES) {
      for (const ptype of PROPERTY_TYPES) {
        await downloadCombo(page, ptype, status);
      }
    }

    console.log("\n🎉 IDX Active + Sold downloads complete.");
  } catch (err) {
    console.error("❌ Error in IDX downloader:", err);
  }

  await browser.close();
}

// Always run when executed directly
runIDXDownloader().catch(err => console.error("❌ Playwright error:", err));
