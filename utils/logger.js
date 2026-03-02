/**
 * logger.js
 * Lightweight logger for AVM + Public Data modules
 */

const fs = require("fs");
const path = require("path");

const LOG_DIR = path.join(__dirname, "../logs");
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

function log(type, message, extra = null) {
  const timestamp = new Date().toISOString();
  const line = `[${timestamp}] [${type.toUpperCase()}] ${message}`;

  console.log(line);

  const logPath = path.join(LOG_DIR, `${type}.log`);
  fs.appendFileSync(
    logPath,
    extra
      ? `${line} | ${JSON.stringify(extra)}\n`
      : line + "\n"
  );
}

module.exports = {
  info: (msg, extra) => log("info", msg, extra),
  warn: (msg, extra) => log("warn", msg, extra),
  error: (msg, extra) => log("error", msg, extra),
};


