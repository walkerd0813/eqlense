// mls/scripts/utility/safeCallModule.js
// -----------------------------------------------------
// Safe dynamic import wrapper for ingest scripts
// Ensures Windows paths are converted to file:// URLs
// -----------------------------------------------------

import path from "node:path";
import { pathToFileURL } from "node:url";

/**
 * Safely import a module and call one of its functions.
 *
 * @param {string} relPath - Relative path from scripts folder (e.g. "./ingestIDX.js")
 * @param {string[]} names - List of possible exported function names
 */
export async function safeCallModule(relPath, names = []) {
  try {
    // Build absolute filesystem path
    const absPath = path.resolve(path.dirname(import.meta.url.replace("file:///", "")), relPath);

    // Convert to file:/// URL (required for Windows)
    const fileUrl = pathToFileURL(absPath).href;

    const mod = await import(fileUrl);

    // Try each possible name in order
    for (const name of names) {
      const fn =
        name === "default"
          ? mod.default
          : mod[name];

      if (typeof fn === "function") {
        console.log(`[safeCall] Calling ${relPath} → ${name}()`);
        return await fn();
      }
    }

    console.warn(`[safeCall] No function found in ${relPath}. Tried: ${names.join(", ")}`);
    return false;

  } catch (err) {
    console.error(`[safeCall] Error calling ${relPath}:`, err);
    return false;
  }
}
