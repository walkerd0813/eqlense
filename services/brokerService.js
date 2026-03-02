// services/brokerService.js
// Broker-level view of CRM: rollup across agents in an office

const { listLeads, getPipelineSummary } = require("./crmService");

function getBrokerDashboard({ brokerId, brokerEmail, agents = [] }) {
  // For now we just roll up all leads; later filter by officeId or agent list.
  const allLeads = listLeads();
  const pipeline = getPipelineSummary();

  const closed = allLeads.filter((l) => l.stage === "closed").length;
  const lost = allLeads.filter((l) => l.stage === "lost").length;

  // Very simple “volume” metrics – later tie to actual sale prices
  const estVolume = allLeads.reduce((sum, l) => sum + (l.estValue || 0), 0);

  return {
    brokerId,
    brokerEmail,
    counts: {
      totalLeads: allLeads.length,
      closed,
      lost,
    },
    pipeline,
    estVolume,
    agentsCount: agents.length || null,
  };
}

module.exports = {
  getBrokerDashboard,
};



