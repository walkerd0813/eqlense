// civicLookup.js — Updated for Civic Boundary NativeX
// Wrap polygonLookup to return categorized civic hits

import { lookupPoint, loadCivicAtlas } from "./polygonLookup.js";

export function getCivicContext(lng, lat) {
  // ensure index loaded
  loadCivicAtlas();

  const hits = lookupPoint(lng, lat);

  const context = {
    mbta: [],
    neighborhood: [],
    police: [],
    fire: [],
    trash: [],
    snow: [],
    flood: [],
    openspace: [],
    historic: [],
    overlay: [],
    generic: [],
    all: hits,
  };

  for (const props of hits) {
    const layer = (props.__civic_layer || "").toLowerCase();
    const src = (props.__civic_source || "").toLowerCase();

    // --- MBTA ---
    if (layer.includes("mbta") || src.includes("mbta")) {
      context.mbta.push(props);
      continue;
    }

    // --- Neighborhood boundaries ---
    if (layer.includes("neighborhood")) {
      context.neighborhood.push(props);
      continue;
    }

    // --- Police ---
    if (layer.includes("police")) {
      context.police.push(props);
      continue;
    }

    // --- Fire ---
    if (layer.includes("fire")) {
      context.fire.push(props);
      continue;
    }

    // --- Trash collection ---
    if (layer.includes("trash")) {
      context.trash.push(props);
      continue;
    }

    // --- Snow emergency ---
    if (layer.includes("snow")) {
      context.snow.push(props);
      continue;
    }

    // --- Flood / floodplain / FEMA ---
    if (layer.includes("flood") || layer.includes("fema")) {
      context.flood.push(props);
      continue;
    }

    // --- Open space / park ---
    if (layer.includes("open") || layer.includes("park")) {
      context.openspace.push(props);
      continue;
    }

    // --- Historic districts ---
    if (layer.includes("historic")) {
      context.historic.push(props);
      continue;
    }

    // --- Overlays ---
    if (props.__civic_category === "overlays") {
      context.overlay.push(props);
      continue;
    }

    // --- Fallback ---
    context.generic.push(props);
  }

  return context;
}
