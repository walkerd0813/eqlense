import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

const [IN_DIR, OUT_FILE] = process.argv.slice(2);

if(!IN_DIR || !OUT_FILE){
  console.error("Usage: node mls/scripts/mergeNdjsonFolder.js <input_folder> <output.ndjson>");
  process.exit(1);
}
if(!fs.existsSync(IN_DIR) || !fs.statSync(IN_DIR).isDirectory()){
  console.error("❌ Not a folder:", IN_DIR);
  process.exit(1);
}

const files = fs.readdirSync(IN_DIR)
  .filter(f => f.toLowerCase().endsWith(".ndjson"))
  .sort((a,b)=>a.localeCompare(b, "en"));

console.log("====================================================");
console.log(" MERGE NDJSON FOLDER (STREAMING)");
console.log("====================================================");
console.log("Folder:", IN_DIR);
console.log("Files:", files.length);
console.log("Output:", OUT_FILE);
console.log("----------------------------------------------------");

const out = fs.createWriteStream(OUT_FILE, { flags:"w", encoding:"utf8" });

let totalLines = 0;
for(let i=0;i<files.length;i++){
  const fp = path.join(IN_DIR, files[i]);
  console.log(`[merge] ${i+1}/${files.length} -> ${files[i]}`);

  const rl = readline.createInterface({ input: fs.createReadStream(fp), crlfDelay: Infinity });
  for await (const line of rl){
    const l = line.trim();
    if(!l) continue;
    out.write(l + "\n");
    totalLines++;
    if(totalLines % 500000 === 0) console.log(`[merge] lines=${totalLines.toLocaleString()}`);
  }
}

out.end();
console.log("====================================================");
console.log("✅ MERGE COMPLETE");
console.log("Lines:", totalLines.toLocaleString());
console.log("====================================================");
