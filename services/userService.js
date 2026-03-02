// services/userService.js
// Lightweight user service – swap to real DB later

const { v4: uuid } = require("uuid");

const users = new Map(); // id -> user

function createUser({ email, name, role = "agent" }) {
  const id = uuid();
  const user = {
    id,
    email,
    name,
    role, // "agent" | "broker" | "admin"
    createdAt: new Date().toISOString(),
  };
  users.set(id, user);
  return user;
}

function getUserById(id) {
  return users.get(id) || null;
}

function getUserByEmail(email) {
  for (const user of users.values()) {
    if (user.email === email) return user;
  }
  return null;
}

function listUsers(filter = {}) {
  const { role } = filter;
  let all = Array.from(users.values());
  if (role) all = all.filter((u) => u.role === role);
  return all;
}

module.exports = {
  createUser,
  getUserById,
  getUserByEmail,
  listUsers,
};

