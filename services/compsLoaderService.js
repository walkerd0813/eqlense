// backend/services/compsLoaderService.js
// ES Module — Master Comps Loader for Market Radar & Competition Tracker

import fs from "fs";
import path from "path";

/**
 * This loader expects your comps to be stored inside:
 * backend/publicData/sources/comps/<zip>.json
 *
 * If your files live elsewhere, tell me and I will adjust paths.
 */

export async function loadCompsForZip(zip, months = 3) {
  const datasetPath = path.resolve(
    process.cwd(),
    `backend/publicData/sources/comps/${zip}.json`
  );

  // Ensure file exists
  if (!fs.existsSync(datasetPath)) {
    console.warn(`[MarketRadar] No comps file found for ZIP ${zip}`);
    return {
      activeComps: [],
      pendingComps: [],
      soldComps: []
    };
  }

  // Load raw comps
  const rawData = JSON.parse(fs.readFileSync(datasetPath, "utf-8"));

  // Normalize dates and DOM
  const now = new Date();
  const soldComps = [];
  const activeComps = [];
  const pendingComps = [];

  for (const comp of rawData) {
    const normalized = normalizeComp(comp);

    if (normalized.status === "Sold") {
      if (filterByMonths(normalized.saleDate, months, now)) {
        soldComps.push(normalized);
      }
    }

    if (normalized.status === "Active") {
      activeComps.push(normalized);
    }

    if (normalized.status === "Pending") {
      pendingComps.push(normalized);
    }
  }

  return {
    activeComps,
    pendingComps,
    soldComps
  };
}

// ------------------------------------------------------------
// HELPERS
// ------------------------------------------------------------

function normalizeComp(comp) {
  const saleDate = comp.saleDate ? new Date(comp.saleDate) : null;
  const listDate = comp.listDate ? new Date(comp.listDate) : null;

  // DOM Calculation
  let dom = Number(comp.daysOnMarket);
  if (Number.isNaN(dom)) dom = computeDom(listDate, saleDate);

  return {
    ...comp,
    saleDate,
    listDate,
    daysOnMarket: dom,
    salePrice: Number(comp.salePrice) || null,
    listPrice: Number(comp.listPrice) || null,
    status: comp.status || "Unknown"
  };
}

function computeDom(listDate, saleDate) {
  if (!listDate || !saleDate) return null;
  const diff = (saleDate - listDate) / (1000 * 60 * 60 * 24);
  return Math.max(0, Math.round(diff));
}

function filterByMonths(date, months, now) {
  if (!date) return false;
  const diffMonths = (now - date) / (1000 * 60 * 60 * 24 * 30);
  return diffMonths <= months;
}
