Phase1B Historic Split Headers — Upgrade Pack (v1)

What this does
- Leaves your existing Phase1B attachments + feature catalogs untouched.
- Streams the CURRENT_CONTRACT_VIEW_PHASEZO_MA contract view and adds the requested split headers:

  2.1 Enforceable Historic Controls (Zoning-like)
    historic_district_name
    historic_designation_type        // local|state|federal
    review_required_flag
    demolition_restricted_flag
    exterior_change_review_flag
    regulatory_body                 // BLC | local commission | ...

  2.2 Informational Historic Inventory (Non-enforceable)
    historic_inventory_flag
    historic_significance_level
    inventory_source

- Conservative: we DO NOT invent demolition/exterior restrictions unless the attachment key is clearly a historic district.
  When we can't safely assert a boolean, we write null.

Inputs
- Contract view: publicData/properties/_frozen/CURRENT_CONTRACT_VIEW_PHASEZO_MA.txt
- Phase1B attachments: auto-picks latest publicData/_audit/phase1B_local_legal_freeze/*/PHASE1B__attachments.ndjson

Outputs
- Frozen contract view with added fields:
  publicData/properties/_frozen/contract_view_phasezo_historic__ma__v1__FREEZE__<stamp>/contract_view_phasezo_historic__YYYY-MM-DD.ndjson
- Pointer update (with backup):
  publicData/properties/_frozen/CURRENT_CONTRACT_VIEW_PHASEZO_MA.txt
  publicData/properties/_frozen/CURRENT_CONTRACT_VIEW_PHASEZO_MA.prev.txt
- Additional pointer:
  publicData/properties/_frozen/CURRENT_CONTRACT_VIEW_PHASEZO_HISTORIC_MA.txt

How to run
1) Expand the zip into backend root.
2) Run:
   pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\phase1b\RUN_Upgrade_HistoricHeaders_OnCurrentContract_v1.ps1 -AsOfDate "2025-12-22" -VerifySampleLines 4000

Notes
- Mapping is in: mls/scripts/gis/PHASE1B_historic_mapping_v1.json
  You can expand match lists later if you add more historic layers.
