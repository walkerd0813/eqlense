import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";

function getArg(name) {
  const idx = process.argv.indexOf(name);
  return idx >= 0 ? process.argv[idx + 1] : null;
}

async function sha256AndStats(filePath) {
  const hash = crypto.createHash("sha256");
  const stream = fs.createReadStream(filePath);
  let bytes = 0;
  let newlines = 0;

  for await (const chunk of stream) {
    bytes += chunk.length;
    hash.update(chunk);
    // count '\n'
    for (let i = 0; i < chunk.length; i++) {
      if (chunk[i] === 10) newlines++;
    }
  }

  // If file doesn't end with '\n' and is non-empty, lineCount = newlines + 1
  let lineCount = 0;
  if (bytes === 0) {
    lineCount = 0;
  } else {
    const fd = await fsp.open(filePath, "r");
    try {
      const buf = Buffer.alloc(1);
      await fd.read(buf, 0, 1, bytes - 1);
      const endsWithNewline = buf[0] === 10;
      lineCount = endsWithNewline ? newlines : (newlines + 1);
    } finally {
      await fd.close();
    }
  }

  return { sha256: hash.digest("hex").toUpperCase(), bytes, lineCount };
}

function tsStamp(d = new Date()) {
  // YYYYMMDD_HHMMSS
  const pad = (n) => String(n).padStart(2, "0");
  return (
    d.getFullYear() +
    pad(d.getMonth() + 1) +
    pad(d.getDate()) +
    "_" +
    pad(d.getHours()) +
    pad(d.getMinutes()) +
    pad(d.getSeconds())
  );
}

async function main() {
  const inPath = getArg("--in");
  const outDir = getArg("--outDir");
  const pointerPath = getArg("--pointer");
  const artifactKey = getArg("--artifactKey") || "artifact";
  const notes = getArg("--notes") || "";
  const metaIn = getArg("--metaIn"); // optional sidecar to copy

  if (!inPath || !outDir || !pointerPath) {
    console.error("Usage: node freezeNdjsonArtifact_v1.mjs --in <path> --outDir <dir> --pointer <path> [--artifactKey <key>] [--metaIn <path>] [--notes <text>]");
    process.exit(1);
  }

  const absIn = path.resolve(inPath);
  const absOutDir = path.resolve(outDir);
  const absPointer = path.resolve(pointerPath);

  await fsp.mkdir(absOutDir, { recursive: true });

  const stamp = tsStamp();
  const freezeFolderName = `${artifactKey}__FREEZE__${stamp}`;
  const freezeDir = path.join(absOutDir, freezeFolderName);
  await fsp.mkdir(freezeDir, { recursive: true });

  const baseName = path.basename(absIn);
  const outFile = path.join(freezeDir, baseName);

  // copy first (so downstream always reads frozen path, not original)
  await fsp.copyFile(absIn, outFile);

  // optional meta copy
  let copiedMeta = null;
  if (metaIn) {
    const absMetaIn = path.resolve(metaIn);
    const metaOut = path.join(freezeDir, path.basename(absMetaIn));
    await fsp.copyFile(absMetaIn, metaOut);
    copiedMeta = metaOut;
  }

  const { sha256, bytes, lineCount } = await sha256AndStats(outFile);

  const manifest = {
    created_at: new Date().toISOString(),
    artifact_key: artifactKey,
    notes,
    input_path: absIn,
    frozen_path: outFile,
    sha256,
    bytes,
    line_count: lineCount,
    schema_version: 1,
    copied_meta_path: copiedMeta,
  };

  const manifestPath = path.join(freezeDir, "MANIFEST.json");
  await fsp.writeFile(manifestPath, JSON.stringify(manifest, null, 2), "utf8");

  // pointer content is RELATIVE path (stable inside repo) + sha
  const relFrozen = path.relative(process.cwd(), outFile).replaceAll("\\", "/");
  const relManifest = path.relative(process.cwd(), manifestPath).replaceAll("\\", "/");
  const pointerBody =
`# ${artifactKey}
frozen_path=${relFrozen}
manifest_path=${relManifest}
sha256=${sha256}
created_at=${manifest.created_at}
`;
  await fsp.mkdir(path.dirname(absPointer), { recursive: true });
  await fsp.writeFile(absPointer, pointerBody, "utf8");

  console.log("[done] froze:", relFrozen);
  console.log("[done] sha256:", sha256);
  console.log("[done] pointer:", path.relative(process.cwd(), absPointer).replaceAll("\\", "/"));
}

main().catch((err) => {
  console.error("[fatal]", err);
  process.exit(1);
});
