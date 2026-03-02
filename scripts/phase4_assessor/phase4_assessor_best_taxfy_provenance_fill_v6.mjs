#!/usr/bin/env node
/**
 * Phase4 AssessorBest tax_fy provenance fill (v6)
 * Streaming + memory safe.
 */
import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import crypto from "node:crypto";

function readJSON(p){ return JSON.parse(fs.readFileSync(p,"utf8")); }
function ensureDir(p){ fs.mkdirSync(p,{recursive:true}); }
function nowIso(){ return new Date().toISOString(); }

async function sha256File(p){
  const h=crypto.createHash("sha256");
  const s=fs.createReadStream(p);
  return await new Promise((resolve,reject)=>{
    s.on("data",(d)=>h.update(d));
    s.on("end",()=>resolve(h.digest("hex")));
    s.on("error",reject);
  });
}

function inferWinnerSource(rec){
  const map = rec?.assessor_source_map || {};
  const src = map["valuation.assessment_year"] || map["valuation.total_value"] || null;
  if (src==="city_assessor") return "city_assessor";
  if (src==="massgis_statewide") return "massgis_statewide";
  if (rec?.assessor_by_source?.city_assessor_raw) return "city_assessor";
  if (rec?.assessor_by_source?.massgis_statewide_raw) return "massgis_statewide";
  return null;
}

function pickEvidence(rec,winner){
  const by = rec?.assessor_by_source || {};
  if (winner==="city_assessor" && by?.city_assessor_raw?.evidence){
    return {source:"city_assessor", evidence:by.city_assessor_raw.evidence, confidence:"A"};
  }
  if (winner==="massgis_statewide" && by?.massgis_statewide_raw?.evidence){
    return {source:"massgis_statewide", evidence:by.massgis_statewide_raw.evidence, confidence:"B"};
  }
  if (by?.city_assessor_raw?.evidence){
    return {source:"city_assessor", evidence:by.city_assessor_raw.evidence, confidence:"A"};
  }
  if (by?.massgis_statewide_raw?.evidence){
    return {source:"massgis_statewide", evidence:by.massgis_statewide_raw.evidence, confidence:"B"};
  }
  return {source:null,evidence:null,confidence:null};
}

function wrapProvenance(existing,prov){
  if (!existing || typeof existing!=="object" || !("value" in existing)) return existing;
  const out={...existing};
  if (out.source==null && prov.source!=null) out.source=prov.source;
  if (out.as_of==null && prov.as_of!=null) out.as_of=prov.as_of;
  if (out.dataset_hash==null && prov.dataset_hash!=null) out.dataset_hash=prov.dataset_hash;
  if (out.confidence==null && prov.confidence!=null) out.confidence=prov.confidence;
  if (!Array.isArray(out.flags)) out.flags=[];
  return out;
}

async function main(){
  const args=process.argv.slice(2);
  const i=args.indexOf("--config");
  if (i===-1 || !args[i+1]){
    console.log("usage: node scripts/phase4_assessor/phase4_assessor_best_taxfy_provenance_fill_v6.mjs --config <config.json>");
    process.exit(2);
  }
  const configPath=path.resolve(args[i+1]);
  const cfg=readJSON(configPath);

  if (!cfg.input || !cfg.outDir) throw new Error("config requires input and outDir");

  const inputAbs=path.resolve(cfg.input);
  const outDirAbs=path.resolve(cfg.outDir);
  const auditDir=path.resolve(cfg.auditDir || "publicData/_audit/phase4_assessor");
  ensureDir(outDirAbs); ensureDir(auditDir);

  const versionTag=cfg.versionTag || "V6";
  const stamp=new Date().toISOString().replace(/[:.]/g,"-");
  const outFile=path.join(outDirAbs,`properties__with_assessor_best_taxfy_provenance__${stamp}__${versionTag}.ndjson`);
  const auditPath=path.join(auditDir,`phase4_assessor_best_taxfy_provenance_fill__${stamp}__${versionTag}.json`);
  const currentPtr=path.join(outDirAbs,"CURRENT_PROPERTIES_WITH_ASSESSOR_BEST_TAXFY_PROVENANCE.json");

  console.log("[start] Phase4 assessor_best tax_fy provenance fill (v6)");
  console.log("[info] input:", inputAbs);
  console.log("[info] output:", outFile);

  const rl=readline.createInterface({input:fs.createReadStream(inputAbs,{encoding:"utf8"}), crlfDelay:Infinity});
  const out=fs.createWriteStream(outFile,{encoding:"utf8"});

  let processed=0, upgraded=0, touchedTaxFy=0, touchedAssessmentYear=0;

  for await (const line of rl){
    if (!line) continue;
    processed++;
    let rec;
    try{ rec=JSON.parse(line); } catch { continue; }

    let changed=false;

    const winner=inferWinnerSource(rec);
    const picked=pickEvidence(rec,winner);
    const prov={
      source: picked.source,
      as_of: picked.evidence?.as_of || null,
      dataset_hash: picked.evidence?.dataset_hash || null,
      confidence: picked.confidence
    };

    if (!rec.assessor_source_map || typeof rec.assessor_source_map!=="object"){
      rec.assessor_source_map={};
      changed=true;
    }

    const val=rec?.assessor_best?.valuation;
    if (val && typeof val==="object"){
      if (!val.tax_fy && val.assessment_year && typeof val.assessment_year==="object" && "value" in val.assessment_year){
        val.tax_fy={value: val.assessment_year.value, source:null, as_of:null, dataset_hash:null, confidence:null, flags:[]};
        changed=true;
      }

      if (val.tax_fy && typeof val.tax_fy==="object" && "value" in val.tax_fy){
        const before=JSON.stringify(val.tax_fy);
        val.tax_fy=wrapProvenance(val.tax_fy,prov);
        const after=JSON.stringify(val.tax_fy);
        if (before!=after){ touchedTaxFy++; changed=true; }

        if (!rec.assessor_source_map["valuation.tax_fy"]){
          if (rec.assessor_source_map["valuation.assessment_year"]) rec.assessor_source_map["valuation.tax_fy"]=rec.assessor_source_map["valuation.assessment_year"];
          else if (prov.source) rec.assessor_source_map["valuation.tax_fy"]=prov.source;
          changed=true;
        }
      }

      if (val.assessment_year && typeof val.assessment_year==="object" && "value" in val.assessment_year){
        const ay=val.assessment_year;
        const missing=(ay.source==null || ay.as_of==null || ay.dataset_hash==null || ay.confidence==null);
        if (missing){
          const before=JSON.stringify(ay);
          val.assessment_year=wrapProvenance(ay,prov);
          const after=JSON.stringify(val.assessment_year);
          if (before!=after){ touchedAssessmentYear++; changed=true; }
        }
      }

      if (!rec.assessor_best.meta || typeof rec.assessor_best.meta!=="object"){
        rec.assessor_best.meta={};
        changed=true;
      }
      if (!rec.assessor_best.meta.tax_fy_note){
        rec.assessor_best.meta.tax_fy_note="Assessor values are typically published by Fiscal Year (FY). Use assessor_best.valuation.tax_fy for UI/analytics. assessment_year is retained for backward compatibility only.";
        changed=true;
      }
      if (changed){
        rec.assessor_best.meta.tax_fy_provenance_filled_at=nowIso();
      }
    }

    if (changed) upgraded++;
    out.write(JSON.stringify(rec)+"\n");

    if (processed%500000===0) console.log("[progress] processed",processed,"upgraded",upgraded);
  }

  out.end();
  await new Promise((res)=>out.on("finish",res));
  const outSha=await sha256File(outFile);

  const audit={
    created_at: nowIso(),
    config: configPath,
    inputs: {properties_in: inputAbs},
    outputs: {properties_out: outFile, current_ptr: currentPtr, audit: auditPath},
    stats: {processed, upgraded, touchedTaxFy, touchedAssessmentYear},
    hashes: {properties_out_sha256: outSha},
    notes: [
      "v6: fills provenance for assessor_best.valuation.tax_fy using winning source evidence (city assessor preferred when present).",
      "v6: backfills provenance on deprecated assessor_best.valuation.assessment_year if missing.",
      "v6: ensures assessor_source_map includes valuation.tax_fy mapping."
    ]
  };
  fs.writeFileSync(auditPath,JSON.stringify(audit,null,2),"utf8");
  fs.writeFileSync(currentPtr,JSON.stringify({updated_at: nowIso(), note:"AUTO: Phase4 assessor_best tax_fy provenance fill v6", properties_ndjson: outFile, audit: auditPath},null,2),"utf8");

  console.log("[ok] wrote audit:", auditPath);
  console.log("[ok] wrote CURRENT pointer:", currentPtr);
  console.log("[ok] output sha256:", outSha);
  console.log("[done] Phase4 assessor_best tax_fy provenance fill v6 complete.");
}

main().catch((e)=>{ console.error("[fatal]", e?.stack||e); process.exit(1); });
