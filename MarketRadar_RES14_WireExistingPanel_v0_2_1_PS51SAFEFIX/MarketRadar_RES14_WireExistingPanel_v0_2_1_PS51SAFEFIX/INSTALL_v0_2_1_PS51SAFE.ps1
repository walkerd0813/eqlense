param(
  [Parameter(Mandatory=$true)][string]$BackendRoot,
  [Parameter(Mandatory=$true)][string]$FrontendRoot,
  [Parameter(Mandatory=$false)][switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
function Say($m){ Write-Host $m }
function BackupFile($path){
  if (Test-Path $path) {
    $bak = "$path.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
    Copy-Item -Path $path -Destination $bak -Force
    Say "[backup] $bak"
  }
}
function WriteUtf8NoBom($path, $content){
  $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
  $dir = Split-Path -Parent $path
  if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
  [System.IO.File]::WriteAllText($path, $content, $utf8NoBom)
}
function stripBomText($s){
  if ([string]::IsNullOrEmpty($s)) { return $s }
  if ($s.Length -gt 0 -and [int][char]$s[0] -eq 0xFEFF) { return $s.Substring(1) }
  return $s
}

Say "[start] Wire existing MarketRadarPanel.jsx to RES_1_4 via backend zip-summary endpoints (v0_2_1)"
Say "  backend:  $BackendRoot"
Say "  frontend: $FrontendRoot"
Say "  dryrun:   $DryRun"

if (-not (Test-Path $BackendRoot)) { throw "[error] BackendRoot not found: $BackendRoot" }
if (-not (Test-Path $FrontendRoot)) { throw "[error] FrontendRoot not found: $FrontendRoot" }

# -----------------------------
# Backend: route file + mount
# -----------------------------
$routeTarget = Join-Path $BackendRoot "routes\marketRadarRoutes.js"
if (-not (Test-Path (Split-Path -Parent $routeTarget))) {
  $routeTarget = Join-Path $BackendRoot "src\routes\marketRadarRoutes.js"
}

$routeContent = @'
import express from "express";
import fs from "fs";
import path from "path";
import readline from "readline";

const router = express.Router();

function stripBom(s) {
  if (!s || typeof s !== "string") return s;
  return s.charCodeAt(0) === 0xFEFF ? s.slice(1) : s;
}

function safeReadJson(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(stripBom(raw));
}

function resolveProjectRoot() {
  const here = path.dirname(new URL(import.meta.url).pathname);
  const norm = here.replace(/^\/([A-Za-z]:)/, "$1");
  const parts = norm.split(path.sep);
  const srcIdx = parts.lastIndexOf("src");
  if (srcIdx !== -1 && parts[srcIdx + 1] === "routes") return parts.slice(0, srcIdx).join(path.sep);
  return path.resolve(norm, "..");
}

function normalizeZip(z) {
  const s = String(z || "").trim();
  if (!s) return "";
  const d = s.replace(/\D/g, "");
  if (!d) return "";
  return d.padStart(5, "0").slice(0, 5);
}

async function findNdjsonRowByZip(filePath, zip) {
  if (!fs.existsSync(filePath)) return null;
  const input = fs.createReadStream(filePath, { encoding: "utf8" });
  const rl = readline.createInterface({ input, crlfDelay: Infinity });
  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    try {
      const obj = JSON.parse(stripBom(t));
      const z = normalizeZip(obj.zip || obj.zip_code || obj.zipCode);
      if (z === zip) return obj;
    } catch {
      // ignore
    }
  }
  return null;
}

router.get("/track/:track/pointers", (req, res) => {
  try {
    const track = String(req.params.track || "").toUpperCase();
    const root = resolveProjectRoot();
    const fp = path.join(root, "publicData", "marketRadar", "CURRENT", `CURRENT_MARKET_RADAR_POINTERS__${track}.json`);
    if (!fs.existsSync(fp)) return res.status(404).json({ state: "UNKNOWN", reason: "POINTERS_NOT_FOUND", radar_track: track });
    return res.json(safeReadJson(fp));
  } catch (e) {
    return res.status(500).json({ state: "ERROR", reason: "POINTERS_READ_FAILED", message: String(e) });
  }
});

router.get("/track/:track/zip/:zip/summary", async (req, res) => {
  try {
    const track = String(req.params.track || "").toUpperCase();
    const zip = normalizeZip(req.params.zip);
    const root = resolveProjectRoot();

    const ptrPath = path.join(root, "publicData", "marketRadar", "CURRENT", `CURRENT_MARKET_RADAR_POINTERS__${track}.json`);
    if (!fs.existsSync(ptrPath)) return res.status(404).json({ state: "UNKNOWN", reason: "POINTERS_NOT_FOUND", radar_track: track });

    const ptr = safeReadJson(ptrPath);
    if (ptr.state === "UNKNOWN") return res.json(ptr);

    const out = {
      schema: "equity_lens.market_radar.zip_summary.v0_1",
      radar_track: track,
      zip,
      as_of_date: ptr.as_of_date || ptr.market_radar?.as_of_date || null,
      state: "READY",
      rows: {}
    };

    const mr = ptr.market_radar || ptr.marketRadar || {};
    const base = mr.market_radar ? mr.market_radar : mr;

    const keys = [
      "velocity_zip",
      "absorption_zip",
      "liquidity_p01_zip",
      "price_discovery_p01_zip",
      "regime_zip",
      "explainability_zip"
    ];

    for (const k of keys) {
      const v = base[k] || mr[k];
      const fp = v?.ndjson || v?.path;
      if (!fp) continue;
      const row = await findNdjsonRowByZip(fp, zip);
      if (row) out.rows[k] = row;
    }

    out.missing_keys = keys.filter(k => !out.rows[k]);
    return res.json(out);
  } catch (e) {
    return res.status(500).json({ state: "ERROR", reason: "ZIP_SUMMARY_FAILED", message: String(e) });
  }
});

export default router;
'@

if (-not $DryRun) {
  WriteUtf8NoBom $routeTarget $routeContent
  Say "[ok] wrote backend route: $routeTarget"
} else {
  Say "[dryrun] would write backend route: $routeTarget"
}

# Mount route into server entry (best effort)
$serverCandidates = @(
  (Join-Path $BackendRoot "server.js"),
  (Join-Path $BackendRoot "src\server.js"),
  (Join-Path $BackendRoot "index.js"),
  (Join-Path $BackendRoot "src\index.js"),
  (Join-Path $BackendRoot "app.js"),
  (Join-Path $BackendRoot "src\app.js")
)

$mounted = $false
$importLine = 'import marketRadarRoutes from "./routes/marketRadarRoutes.js";'
$mountRegex = 'app\.use\(\s*["'']\/api\/market-radar["'']\s*,'
$mountLine = 'app.use("/api/market-radar", marketRadarRoutes);'

foreach ($sf in $serverCandidates) {
  if (-not (Test-Path $sf)) { continue }
  $txt = stripBomText (Get-Content $sf -Raw)
  if ([string]::IsNullOrWhiteSpace($txt)) { continue }

  if ($txt -notmatch "marketRadarRoutes") {
    if ($txt -match "import\s+express") {
      $lines = $txt -split "`r?`n"
      $idx = ($lines | Select-String -Pattern "import\s+express" | Select-Object -First 1).LineNumber
      if ($idx -gt 0) {
        $insAt = $idx
        $lines = @($lines[0..($insAt-1)] + $importLine + $lines[$insAt..($lines.Length-1)])
        $txt = ($lines -join "`r`n")
      } else {
        $txt = $importLine + "`r`n" + $txt
      }
    } else {
      $txt = $importLine + "`r`n" + $txt
    }
  }

  if ($txt -notmatch $mountRegex) {
    if ($txt -match "const\s+app\s*=\s*express\(\);") {
      $txt = $txt -replace "const\s+app\s*=\s*express\(\);\s*", "const app = express();`r`n$mountLine`r`n"
    } else {
      $txt = $txt + "`r`n" + $mountLine + "`r`n"
    }
  }

  if (-not $DryRun) {
    BackupFile $sf
    WriteUtf8NoBom $sf $txt
    Say "[ok] ensured /api/market-radar mounted in $sf"
  } else {
    Say "[dryrun] would patch server mount in $sf"
  }
  $mounted = $true
  break
}

if (-not $mounted) {
  Say "[warn] could not locate server entry to mount /api/market-radar. You may already mount routes elsewhere."
}

# -----------------------------
# Frontend: wire MarketRadarPanel.jsx
# -----------------------------
$panel = Join-Path $FrontendRoot "src\components\marketRadar\MarketRadarPanel.jsx"
if (-not (Test-Path $panel)) { throw "[error] missing MarketRadarPanel.jsx at $panel" }

$apiFile = Join-Path $FrontendRoot "src\api\marketRadar.js"
$apiContent = @'
export async function fetchMarketRadarZipSummary({ track = "RES_1_4", zip }) {
  const t = String(track || "").toUpperCase();
  const z = String(zip || "").trim();
  const r = await fetch(`/api/market-radar/track/${t}/zip/${z}/summary`);
  if (!r.ok) return { state: "UNKNOWN", reason: "FETCH_FAILED", radar_track: t, zip: z };
  return r.json();
}

export async function fetchMarketRadarPointers(track = "RES_1_4") {
  const t = String(track || "").toUpperCase();
  const r = await fetch(`/api/market-radar/track/${t}/pointers`);
  if (!r.ok) return { state: "UNKNOWN", reason: "FETCH_FAILED", radar_track: t };
  return r.json();
}
'@

$panelContent = @'
import React, { useEffect, useMemo, useState } from "react";
import { fetchMarketRadarZipSummary, fetchMarketRadarPointers } from "../../api/marketRadar";

function normalizeZip(input) {
  const d = String(input || "").trim().replace(/\D/g, "");
  if (!d) return "";
  return d.padStart(5, "0").slice(0, 5);
}

export default function MarketRadarPanel() {
  const urlZip = useMemo(() => {
    try {
      const u = new URL(window.location.href);
      return normalizeZip(u.searchParams.get("zip"));
    } catch {
      return "";
    }
  }, []);

  const [track] = useState("RES_1_4");
  const [zip, setZip] = useState(urlZip || "");
  const [pointers, setPointers] = useState(null);
  const [summary, setSummary] = useState(null);
  const [err, setErr] = useState("");

  useEffect(() => {
    (async () => {
      setErr("");
      const ptr = await fetchMarketRadarPointers(track);
      setPointers(ptr);
    })();
  }, [track]);

  async function run() {
    const z = normalizeZip(zip);
    if (!z) {
      setErr("Enter a 5-digit ZIP (or use ?zip=01103 in the URL).");
      return;
    }
    setErr("");
    setSummary(null);
    const s = await fetchMarketRadarZipSummary({ track, zip: z });
    setSummary(s);
  }

  const availability =
    pointers?.state === "UNKNOWN" ? `Unavailable: ${pointers?.reason || "UNKNOWN"}` : "Available";

  return (
    <div style={{ border: "1px solid #ddd", borderRadius: 12, padding: 16 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
        <h2 style={{ margin: 0 }}>Market Radar</h2>
        <span style={{ fontSize: 12, opacity: 0.8 }}>{track} • {availability}</span>
      </div>

      <div style={{ marginTop: 10, display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
        <label style={{ fontSize: 13, opacity: 0.9 }}>
          ZIP:&nbsp;
          <input
            value={zip}
            onChange={(e) => setZip(e.target.value)}
            placeholder="01103"
            style={{ padding: "6px 8px", borderRadius: 8, border: "1px solid #ccc", width: 110 }}
          />
        </label>
        <button
          onClick={run}
          style={{ padding: "7px 10px", borderRadius: 10, border: "1px solid #333", cursor: "pointer" }}
          disabled={pointers?.state === "UNKNOWN"}
          title={pointers?.state === "UNKNOWN" ? pointers?.reason : "Load ZIP summary"}
        >
          Load
        </button>
        <span style={{ fontSize: 12, opacity: 0.7 }}>
          Tip: open with <code>?zip=01103</code>
        </span>
      </div>

      {err && <div style={{ marginTop: 10, color: "#b00020", fontSize: 13 }}>{err}</div>}

      <div style={{ marginTop: 14, fontSize: 13 }}>
        {!summary && <div style={{ opacity: 0.8 }}>Loads the current RES_1_4 radar rollups for a ZIP. (MF_5_PLUS and LAND are disabled until built.)</div>}
        {summary && summary.state === "UNKNOWN" && (
          <div style={{ opacity: 0.85 }}>
            Track is unavailable: <b>{summary.reason}</b>
          </div>
        )}
        {summary && summary.state === "READY" && (
          <div style={{ marginTop: 10 }}>
            <div style={{ opacity: 0.85 }}>
              <b>ZIP:</b> {summary.zip} • <b>As-of:</b> {summary.as_of_date || "unknown"}
            </div>
            <pre style={{ marginTop: 10, maxHeight: 420, overflow: "auto", background: "#fafafa", padding: 12, borderRadius: 10 }}>
{JSON.stringify(summary.rows, null, 2)}
            </pre>
            {summary.missing_keys?.length > 0 && (
              <div style={{ marginTop: 8, opacity: 0.75 }}>
                Missing rollups for this ZIP: {summary.missing_keys.join(", ")}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
'@

if (-not $DryRun) {
  WriteUtf8NoBom $apiFile $apiContent
  Say "[ok] wrote $apiFile"
  BackupFile $panel
  WriteUtf8NoBom $panel $panelContent
  Say "[ok] rewired $panel (RES_1_4 ZIP summary loader)"
} else {
  Say "[dryrun] would write api + panel files"
}

Say "[done] Wire MarketRadarPanel complete (v0_2_1)"
