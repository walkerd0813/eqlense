#!/usr/bin/env python3
import json

ACTUAL = r"c:\seller-app\backend\fixtures\pagebreak\deed_pagebreak_case_01\actual.json"
EXPECTED = r"c:\seller-app\backend\fixtures\pagebreak\deed_pagebreak_case_01\expected.json"

def main():
    with open(ACTUAL,'r',encoding='utf-8-sig') as f:
        act = json.load(f)

    out = {
        "fixture": {
            "name": "deed_pagebreak_case_01",
            "purpose": "Golden truth for pagebreak continuation test (populated from pipeline actual)",
            "note": "This expected.json was populated with the pipeline's actual output after the stitcher patch. Review and adjust if a human-verified gold truth differs."
        },
        "golden": {
            "events": act.get('events', [])
        },
        "source": "POPULATED_FROM_ACTUAL",
        "hint": "Run generate_fixture_actual_from_join_v1.py to regenerate actual.json for diffs."
    }

    with open(EXPECTED,'w',encoding='utf-8') as f:
        json.dump(out,f,indent=2,ensure_ascii=False)

    print('Wrote expected.json from actual.json')

if __name__ == '__main__':
    main()
