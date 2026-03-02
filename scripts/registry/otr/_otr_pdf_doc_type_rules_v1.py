import os
import re

# LOCKED: only these doc types are allowed to be emitted by OTR Hampden index ingest.
ALLOWED_DOC_TYPES = {
    "DEED",  # deeds
    "MTG",   # mortgages
    "ASN",   # assignments
    "REL",   # releases
    "DM",    # discharge mortgage
    "LIEN",  # liens
    "MTL",   # mass tax lien
    "ESMT",  # easement
    "LIS",   # lis pendens
    "FCD",   # foreclosure deed (if present; keep allowlisted)
    "MSDD",  # master deed (you have hamden_Master_deeds_)
}

# filename -> doctype (normalized)
# We intentionally use filename because it is stable, deterministic, and matches how the PDFs were requested.
_FILENAME_RULES = [
    (r"master[_\s-]*deeds", "MSDD"),
    (r"foreclos", "FCD"),
    (r"lispend", "LIS"),
    (r"easement", "ESMT"),
    (r"mass[_\s-]*taxliens", "MTL"),
    (r"(municipal|manicipal)[_\s-]*liens", "LIEN"),
    (r"fed[_\s-]*taxliens", "LIEN"),
    (r"taxliens", "LIEN"),
    (r"liens", "LIEN"),
    (r"discharge[_\s-]*mortgage", "DM"),
    (r"release", "REL"),
    (r"assignments", "ASN"),
    (r"mortgage", "MTG"),
    (r"deeds", "DEED"),
]

def infer_pdf_doc_type(pdf_name: str):
    """
    Determine the PDF-level doc type from the PDF filename.
    Returns (doc_type_code, source_str, flags_list)
    """
    flags = []
    base = os.path.basename(pdf_name or "").strip().lower()

    if not base:
        return ("UNKNOWN", "PDF_FILENAME", ["NO_PDF_NAME"])

    for pat, code in _FILENAME_RULES:
        if re.search(pat, base):
            if code not in ALLOWED_DOC_TYPES:
                return ("UNKNOWN", "PDF_FILENAME", [f"NOT_ALLOWED:{code}"])
            return (code, "PDF_FILENAME", flags)

    return ("UNKNOWN", "PDF_FILENAME", ["NO_FILENAME_MATCH"])

_money_re = re.compile(r"^\s*\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})\s*$|^\s*\d+(?:\.\d{2})\s*$")

def is_money_like(s: str) -> bool:
    if s is None:
        return False
    t = str(s).strip()
    if t == "":
        return False
    if t.upper() == "N/A":
        return True
    return _money_re.match(t) is not None

_digits_re = re.compile(r"^\d+$")

def is_digits(s: str) -> bool:
    if s is None:
        return False
    return _digits_re.match(str(s).strip()) is not None