import fs from "node:fs";import readline from "node:readline";
const file=process.argv[2];let n=0,h=0;
const rl=readline.createInterface({input:fs.createReadStream(file,{encoding:"utf8"}),crlfDelay:Infinity});
for await (const line of rl){
  n++;
  if(line.includes('"assessor_by_source":{')){h++; console.log("[HIT] line",n); console.log(line.slice(0,1400)); process.exit(0);}
  if(n%500000===0) console.log("[progress] scanned",n,"hits",h);
}
console.log("[done] scanned",n,"hits",h);
process.exit(h?0:1);
