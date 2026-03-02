/**
 * rateLimiter.js
 * Simple in-memory IP rate limiter (per minute)
 */

const WINDOW_MS = 60 * 1000; // 1 minute
const MAX_REQUESTS = 30; // 30 requests/min per IP

const ipStore = new Map();

function rateLimiter(req, res, next) {
  const ip = req.ip || req.headers["x-forwarded-for"] || "unknown";
  const now = Date.now();

  if (!ipStore.has(ip)) {
    ipStore.set(ip, { count: 1, firstRequest: now });
    return next();
  }

  const record = ipStore.get(ip);

  // Window expired → reset
  if (now - record.firstRequest > WINDOW_MS) {
    ipStore.set(ip, { count: 1, firstRequest: now });
    return next();
  }

  record.count++;

  if (record.count > MAX_REQUESTS) {
    return res.status(429).json({
      success: false,
      error: "Too many requests — slow down.",
    });
  }

  next();
}

module.exports = rateLimiter;


