// backend/services/geocodeAddress.js
//------------------------------------------------------------
// Simple geocoder using OpenStreetMap (FREE API)
// Converts { address, unit, zip } → { lat, lng }
//------------------------------------------------------------

const axios = require("axios");

async function geocodeAddress({ address, unit, zip }) {
  try {
    const fullAddress = [address, unit, zip].filter(Boolean).join(", ");

    const url = `https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(fullAddress)}&addressdetails=1&limit=1`;

    const res = await axios.get(url, {
      headers: { "User-Agent": "EquityLens/1.0" }
    });

    if (!res.data || res.data.length === 0) {
      console.error("❌ Geocoding failed: Address not found");
      throw new Error("Address not found");
    }

    return {
      lat: Number(res.data[0].lat),
      lng: Number(res.data[0].lon),
    };

  } catch (err) {
    console.error("❌ Geocoding ERROR:", err.message);
    throw err;
  }
}

module.exports = geocodeAddress;
