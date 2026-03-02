// services/agentService.js
// Agent-specific helpers – wraps CRM for "my leads / my dashboard"

const { listLeads, getPipelineSummary } = require("./crmService");

function getAgentDashboard(agent) {
  if (!agent || !agent.id) {
    throw new Error("Agent required for dashboard");
  }

  const myLeads = listLeads({ ownerId: agent.id });
  const pipeline = getPipelineSummary({ ownerId: agent.id });

  const totalLeads = myLeads.length;
  const newLeads = myLeads.filter((l) => l.stage === "new").length;
  const active = myLeads.filter((l) =>
    ["contacted", "nurture", "appointment_set", "listing_signed", "under_contract"].includes(
      l.stage
    )
  ).length;
  const closed = myLeads.filter((l) => l.stage === "closed").length;

  return {
    totals: {
      totalLeads,
      newLeads,
      active,
      closed,
    },
    pipeline,
    recentLeads: myLeads.slice(0, 10),
  };
}

module.exports = {
  getAgentDashboard,
};




