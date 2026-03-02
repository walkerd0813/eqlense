/**
 * fileHelpers.js
 * Safe JSON read/write helpers
 */

const fs = require("fs");

function readJSON(filePath, fallback = null) {
  try {
    const raw = fs.readFileSync(filePath, "utf8");
    return JSON.parse(raw);
  } catch (err) {
    console.warn(`⚠ Cannot read JSON: ${filePath}`, err.message);
    return fallback;
  }
}

function writeJSON(filePath, data) {
  try {
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf8");
    return true;
  } catch (err) {
    console.error(`❌ Failed to write JSON: ${filePath}`, err);
    return false;
  }
}

/**
 * Ensures a folder exists before writing
 */
function ensureDir(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

module.exports = {
  readJSON,
  writeJSON,
  ensureDir,
};
