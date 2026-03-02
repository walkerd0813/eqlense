import fs from "node:fs";

const inFile = process.argv[process.argv.indexOf("--in")+1];
if(!inFile){ console.error("Usage: node inferCodeNameFields_v1.mjs --in <geojson>"); process.exit(1); }

const fc = JSON.parse(fs.readFileSync(inFile,"utf8"));
const feats = Array.isArray(fc.features) ? fc.features : [];

const DROP = new Set([
  "OBJECTID","OBJECTID_1","FID","GLOBALID","CREATED_USER","CREATED_DATE",
  "LAST_EDITED_USER","LAST_EDITED_DATE","SHAPE_LENGTH","SHAPE_AREA",
  "SHAPE__LENGTH","SHAPE__AREA"
]);

function normKey(k){ return String(k||"").trim(); }
function isDropped(k){
  const u = normKey(k).toUpperCase();
  if(DROP.has(u)) return true;
  if(u.startsWith("SHAPE")) return true;
  if(u.includes("OBJECTID")) return true;
  if(u.includes("GLOBALID")) return true;
  return false;
}
function looksCode(v){
  const s = String(v||"").trim();
  if(!s) return false;
  if(s.length <= 12 && !s.includes(" ")) return true;
  if(/[0-9]/.test(s) || /-/.test(s)) return true;
  return false;
}
function looksName(v){
  const s = String(v||"").trim();
  if(!s) return false;
  if(s.length >= 8 && s.includes(" ")) return true;
  if(s.length >= 16) return true;
  return false;
}

const stats = new Map();

for(const f of feats){
  const p = (f && f.properties && typeof f.properties==="object") ? f.properties : {};
  for(const k of Object.keys(p)){
    if(isDropped(k)) continue;
    const v = p[k];
    if(v==null) continue;
    const s = String(v).trim();
    if(!s) continue;
    if(!stats.has(k)) stats.set(k,{nonEmpty:0, uniq:new Set(), codeHits:0, nameHits:0, avgLenSum:0});
    const st = stats.get(k);
    st.nonEmpty++;
    if(st.uniq.size < 5000) st.uniq.add(s);
    st.avgLenSum += s.length;
    if(looksCode(s)) st.codeHits++;
    if(looksName(s)) st.nameHits++;
  }
}

const rows = [...stats.entries()].map(([k,st])=>{
  const nonEmpty = st.nonEmpty;
  const uniq = st.uniq.size;
  const avgLen = nonEmpty ? st.avgLenSum/nonEmpty : 0;
  // scoring: prefer keys that are populated, code-like or name-like, and not “too unique”
  const codeScore = nonEmpty * (st.codeHits/(nonEmpty||1)) * (1 - Math.min(uniq/nonEmpty, 0.95));
  const nameScore = nonEmpty * (st.nameHits/(nonEmpty||1)) * (Math.min(avgLen/30, 1));
  return { key:k, nonEmpty, uniq, avgLen:+avgLen.toFixed(2), codeHits:st.codeHits, nameHits:st.nameHits, codeScore:+codeScore.toFixed(2), nameScore:+nameScore.toFixed(2) };
}).sort((a,b)=> (b.codeScore+b.nameScore) - (a.codeScore+a.nameScore));

const topCode = [...rows].sort((a,b)=>b.codeScore-a.codeScore).slice(0,8);
const topName = [...rows].sort((a,b)=>b.nameScore-a.nameScore).slice(0,8);

console.log(JSON.stringify({
  inFile,
  features: feats.length,
  topCode,
  topName,
  suggested: {
    codeField: topCode[0]?.key || null,
    nameField: topName[0]?.key || null
  }
}, null, 2));
