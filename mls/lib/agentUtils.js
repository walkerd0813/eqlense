// mls/lib/agentUtils.js

/**
 * Clean string safely.
 */
function cleanStr(value) {
  if (value === undefined || value === null) return null;
  const s = String(value).trim();
  return s.length ? s : null;
}

/**
 * Basic phone normalizer.
 */
function normalizePhone(num) {
  if (!num) return null;
  const digits = String(num).replace(/\D/g, "");
  return digits.length ? digits : null;
}

/**
 * Normalize an MLS agent record.
 */
export function normalizeAgent(raw) {
  if (!raw || typeof raw !== "object") return null;

  return {
    agentId: cleanStr(raw.AGENT_ID || raw.AgentID || raw.id),
    firstName: cleanStr(raw.FIRST_NAME || raw.FirstName || raw.first_name),
    lastName: cleanStr(raw.LAST_NAME || raw.LastName || raw.last_name),
    fullName:
      cleanStr(raw.FULL_NAME) ||
      cleanStr(raw.FullName) ||
      [cleanStr(raw.FIRST_NAME), cleanStr(raw.LAST_NAME)].filter(Boolean).join(" "),
    email: cleanStr(raw.EMAIL || raw.Email),
    phone: normalizePhone(raw.PHONE || raw.MobilePhone || raw.MOBILE_PHONE),

    officeId: cleanStr(raw.OFFICE_ID || raw.OfficeID),

    licenseNumber: cleanStr(raw.LICENSE || raw.LicenseNumber),
    status: cleanStr(raw.STATUS || raw.Status),

    raw,
    importedAt: new Date().toISOString(),
  };
}
