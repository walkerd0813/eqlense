/**
 * errorHandling.js
 * Centralized error middleware + async wrapper
 */

function errorHandler(err, req, res, next) {
  console.error("🔥 ERROR:", err.message);

  const status = err.status || 500;

  return res.status(status).json({
    success: false,
    error: err.message || "Server Error",
    ...(process.env.NODE_ENV !== "production"
      ? { stack: err.stack }
      : {}),
  });
}

/**
 * Wrap async route handlers so unhandled promise rejections
 * never crash the app.
 */
function asyncHandler(fn) {
  return (req, res, next) =>
    Promise.resolve(fn(req, res, next)).catch(next);
}

module.exports = {
  errorHandler,
  asyncHandler,
};
