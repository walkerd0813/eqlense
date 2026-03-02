// mls/lib/officeUtils.js

function cleanStr(value) {
  if (value === undefined || value === null) return null;
  const s = String(value).trim();
  return s.length ? s : null;
}

export function normalizeOffice(raw) {
  if (!raw || typeof raw !== "object") return null;

  return {
    officeId: cleanStr(raw.OFFICE_ID || raw.OfficeID),
    name: cleanStr(raw.OFFICE_NAME || raw.OfficeName),
    brokerId: cleanStr(raw.BROKER_ID || raw.BrokerID),

    phone: cleanStr(raw.PHONE || raw.Phone),
    email: cleanStr(raw.EMAIL || raw.Email),

    address: {
      street: cleanStr(raw.STREET || raw.Street),
      city: cleanStr(raw.CITY || raw.City),
      state: cleanStr(raw.STATE || raw.State),
      zip: cleanStr(raw.ZIP || raw.Zip),
    },

    status: cleanStr(raw.STATUS || raw.Status),
    region: cleanStr(raw.REGION || raw.Region),

    raw,
    importedAt: new Date().toISOString(),
  };
}
