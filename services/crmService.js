// services/crmService.js
// In-memory CRM engine – leads & basic pipeline

const { v4: uuid } = require("uuid");

const PIPELINE_STAGES = [
  "new",
  "contacted",
  "nurture",
  "appointment_set",
  "listing_signed",
  "under_contract",
  "closed",
  "lost",
];

const leads = new Map(); // id -> lead

function createLead(payload) {
  const id = uuid();
  const now = new Date().toISOString();

  const {
    source = "manual",
    ownerEmail = null,
    ownerId = null,
    propertyAddress = "",
    propertyZip = "",
    propertyType = "single_family",
    estValue = null,
    sellerName = "",
    sellerEmail = "",
    sellerPhone = "",
    notes = "",
  } = payload;

  const lead = {
    id,
    source,
    stage: "new",
    ownerId,
    ownerEmail,
    propertyAddress,
    propertyZip,
    propertyType,
    estValue,
    sellerName,
    sellerEmail,
    sellerPhone,
    notes,
    createdAt: now,
    updatedAt: now,
    activity: [],
  };

  leads.set(id, lead);
  return lead;
}

function listLeads(filter = {}) {
  const { stage, ownerId, ownerEmail } = filter;
  let all = Array.from(leads.values());

  if (stage) {
    all = all.filter((l) => l.stage === stage);
  }
  if (ownerId) {
    all = all.filter((l) => l.ownerId === ownerId);
  }
  if (ownerEmail) {
    all = all.filter((l) => l.ownerEmail === ownerEmail);
  }

  // newest first
  all.sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));
  return all;
}

function getLeadById(id) {
  return leads.get(id) || null;
}

function updateLead(id, updates) {
  const lead = leads.get(id);
  if (!lead) return null;

  const allowed = [
    "stage",
    "ownerId",
    "ownerEmail",
    "propertyAddress",
    "propertyZip",
    "propertyType",
    "estValue",
    "sellerName",
    "sellerEmail",
    "sellerPhone",
    "notes",
  ];

  for (const key of allowed) {
    if (updates[key] !== undefined) {
      lead[key] = updates[key];
    }
  }

  lead.updatedAt = new Date().toISOString();
  return lead;
}

function updateLeadStage(id, stage, meta = {}) {
  const lead = leads.get(id);
  if (!lead) return null;

  if (!PIPELINE_STAGES.includes(stage)) {
    throw new Error(`Invalid stage: ${stage}`);
  }

  lead.stage = stage;
  lead.updatedAt = new Date().toISOString();
  lead.activity.push({
    type: "stage_change",
    stage,
    at: lead.updatedAt,
    ...meta,
  });

  return lead;
}

function addActivity(id, activity) {
  const lead = leads.get(id);
  if (!lead) return null;

  lead.activity.push({
    at: new Date().toISOString(),
    ...activity,
  });
  lead.updatedAt = new Date().toISOString();
  return lead;
}

function getPipelineSummary(filter = {}) {
  const filtered = listLeads(filter);
  const byStage = {};
  for (const stage of PIPELINE_STAGES) {
    byStage[stage] = 0;
  }
  for (const lead of filtered) {
    if (!byStage[lead.stage]) byStage[lead.stage] = 0;
    byStage[lead.stage] += 1;
  }
  return { total: filtered.length, byStage };
}

module.exports = {
  PIPELINE_STAGES,
  createLead,
  listLeads,
  getLeadById,
  updateLead,
  updateLeadStage,
  addActivity,
  getPipelineSummary,
};
