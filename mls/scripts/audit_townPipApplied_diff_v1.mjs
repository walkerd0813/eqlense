import fs from "fs";
import readline from "readline";

function arg(name) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : null;
}
const beforePath = arg("--before");
const afterPath = arg("--after");
const outPath = arg("--out") || null;

if (!beforePath || !afterPath) {
  console.error("Missing args. Required: --before --after [--out]");
  process.exit(1);
}

const up = (v) => String(v ?? "").trim().toUpperCase();
const sameAddr = (a, b) =>
  String(a.street_no ?? "") === String(b.street_no ?? "") &&
  up(a.street_name) === up(b.street_name) &&
  up(a.town) === up(b.town) &&
  String(a.zip ?? "") === String(b.zip ?? "");

async function main() {
  const r1 = readline.createInterface({ input: fs.createReadStream(beforePath) });
  const r2 = readline.createInterface({ input: fs.createReadStream(afterPath) });

  const it1 = r1[Symbol.asyncIterator]();
  const it2 = r2[Symbol.asyncIterator]();

  let scanned = 0;
  let uidMismatch = 0;

  let changed = 0;
  let changed_hasSuggest = 0;

  let changed_strictOK = 0;
  let changed_strictBad = 0;
  let changed_noVerifiedTown = 0;

  let changed_notEqualToSuggest = 0;

  const samplesBad = [];

  while (true) {
    const [a, b] = await Promise.all([it1.next(), it2.next()]);
    if (a.done || b.done) break;

    if (!a.value || !b.value) continue;

    const before = JSON.parse(a.value);
    const after = JSON.parse(b.value);

    scanned++;

    const ru1 = before.row_uid || null;
    const ru2 = after.row_uid || null;
    if (ru1 && ru2 && ru1 !== ru2) uidMismatch++;

    const isChanged = !sameAddr(before, after);
    if (!isChanged) continue;

    changed++;

    const s = after.mad_suggest || null;
    if (s) changed_hasSuggest++;

    // If we changed a row but it doesn't equal its own suggestion, that's a red flag
    if (s) {
      const equalsSuggest =
        String(after.street_no ?? "") === String(s.street_no ?? "") &&
        up(after.street_name) === up(s.street_name) &&
        up(after.town) === up(s.town) &&
        (s.zip ? String(after.zip ?? "") === String(s.zip) : true);

      if (!equalsSuggest) {
        changed_notEqualToSuggest++;
        if (samplesBad.length < 10) {
          samplesBad.push({
            row_uid: after.row_uid,
            before: { street_no: before.street_no, street_name: before.street_name, town: before.town, zip: before.zip },
            after:  { street_no: after.street_no,  street_name: after.street_name,  town: after.town,  zip: after.zip  },
            suggest:{ street_no: s.street_no,      street_name: s.street_name,      town: s.town,      zip: s.zip      },
          });
        }
      }
    }

    const vt = up(
      after.address_verified?.town_pip_stateplane?.verifiedTown ||
      after.address_verified?.town_pip?.verifiedTown ||
      ""
    );

    if (!vt) {
      changed_noVerifiedTown++;
      changed_strictBad++;
      continue;
    }

    // strict = verifiedTown must equal suggested town (if suggest exists), otherwise use after.town
    const strictTown = s?.town ? up(s.town) : up(after.town);
    if (vt === strictTown) changed_strictOK++;
    else {
      changed_strictBad++;
      if (samplesBad.length < 10) {
        samplesBad.push({ row_uid: after.row_uid, verifiedTown: vt, strictTown });
      }
    }

    if (scanned % 500000 === 0) console.log(`...audited ${scanned.toLocaleString()} rows`);
  }

  r1.close(); r2.close();

  const summary = {
    before: beforePath,
    after: afterPath,
    scanned,
    uidMismatch,
    changed,
    changed_hasSuggest,
    changed_strictOK,
    changed_strictBad,
    changed_noVerifiedTown,
    changed_notEqualToSuggest,
    samplesBad,
  };

  console.log("DONE.", summary);

  if (outPath) fs.writeFileSync(outPath, JSON.stringify(summary, null, 2));
}

main().catch((e) => {
  console.error("FATAL:", e);
  process.exit(1);
});
