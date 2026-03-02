// services/authService.js
// Simple auth stub – good enough for demos. Later: hook into real DB + JWT.

const jwt = require("jsonwebtoken");
const { getUserByEmail, createUser } = require("./userService");

const JWT_SECRET = process.env.JWT_SECRET || "dev-secret-change-me";
const JWT_EXPIRES_IN = "7d";

function issueToken(user) {
  const payload = { sub: user.id, role: user.role };
  return jwt.sign(payload, JWT_SECRET, { expiresIn: JWT_EXPIRES_IN });
}

function verifyToken(token) {
  try {
    return jwt.verify(token, JWT_SECRET);
  } catch (err) {
    return null;
  }
}

// Fake “login or create” – for early demos
function loginOrRegister({ email, name, role = "agent" }) {
  let user = getUserByEmail(email);
  if (!user) {
    user = createUser({ email, name, role });
  }
  const token = issueToken(user);
  return { user, token };
}

module.exports = {
  issueToken,
  verifyToken,
  loginOrRegister,
};