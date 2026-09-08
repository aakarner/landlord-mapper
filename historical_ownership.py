#!/usr/bin/env python3
"""Build comparable 2024 and 2025 Travis corporate-ownership snapshots.

The 2024 certified export is a Deflate64 ZIP.  Python's standard ``zipfile``
module can inspect its central directory but cannot decompress method 9, so the
large PROP.TXT member is streamed through the system Info-ZIP ``unzip`` binary.
Only selected fixed-width fields are decoded; PROP.TXT is never expanded to
disk.

The 2025 side intentionally consumes the cached CSV extracts that produced the
current EWS parcel surface.  Both vintages are converted to the same row schema
before the shared classifier and deterministic parcel aggregation are applied.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import inspect
import json
import math
import os
import re
import string
import subprocess
import sys
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_EWS_PATH = REPO_ROOT.parent / "coa-displacement-ews" / "data" / "residential_parcels_for_hex.csv"
DEFAULT_UPSTREAM_EWS_PATH = REPO_ROOT / "output" / "residential_parcels_for_hex.csv"
DEFAULT_2024_ARCHIVE = (
    REPO_ROOT
    / "data"
    / "historical_ownership"
    / "2024"
    / "source"
    / "2024_Certified_Appraisal_Export_Supp_0_08212024_Rerun.zip"
)
DEFAULT_LAYOUT_ARCHIVE = (
    REPO_ROOT
    / "data"
    / "historical_ownership"
    / "2024"
    / "source"
    / "Website_Legacy8.0.30-AppraisalExportLayout.zip"
)
DEFAULT_2025_ARCHIVE = REPO_ROOT / "tcad_special_export.zip"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "output" / "historical_ownership"
DEFAULT_2024_INTERMEDIATE = (
    REPO_ROOT / "data" / "historical_ownership" / "2024" / "intermediate" / "travis_2024_owner_rows.csv"
)
DEFAULT_2025_INTERMEDIATE = (
    REPO_ROOT / "data" / "historical_ownership" / "2025" / "intermediate" / "travis_2025_owner_rows.csv"
)

TCAD_2024_CANONICAL_URL = (
    "https://traviscad.org/wp-content/largefiles/"
    "2024%20Certified%20Appraisal%20Export%20Supp%200_08212024_Rerun.zip"
)
TCAD_2024_RETRIEVAL_URL = (
    "https://web.archive.org/web/20250905131148id_/" + TCAD_2024_CANONICAL_URL
)
TCAD_LAYOUT_URL = (
    "https://traviscad.org/wp-content/largefiles/"
    "Website_Legacy8.0.30-AppraisalExportLayout.zip"
)
TCAD_2025_CANONICAL_URL = (
    "https://traviscad.org/wp-content/largefiles/"
    "2025%20Special%20export%20Supp%201%2007202025.zip"
)

SOURCE_2024_SNAPSHOT_ID = "tcad-2024-certified-supp0-rerun-20240821"
SOURCE_2025_SNAPSHOT_ID = "tcad-2025-special-export-supp1-20250720"
EXPECTED_2024_ARCHIVE_SHA256 = "c35da69f2baa53e1c1005672d432c62701b83e2234a250a0a6ee8637fe001b29"
EXPECTED_2024_LAYOUT_SHA256 = "36da0d34bd325395a7b0fb45046a4849c340ae3860a274cc0792e1e43185993a"
EXPECTED_2025_ARCHIVE_SHA256 = "8b9865a63f1c9a23e6425469148a8a1b39575a1424170828f9035385bbbd9259"
EXPECTED_EWS_SHA256 = "fb2ad8ee3c09ca5d5b578f2eef806d93b0bc5bb0e885edd626d8c04f7f37d299"
EXPECTED_2025_CACHE_SHA256 = {
    "owners": "6b585922cc8a8b8964c6fd1ee47f284ade0750d41509c7ce40647af9078f4c71",
    "situses": "225c5a23a5b69ca1c84685e59c25684b0609a98a61e0e599d75f1ecce800e8ab",
    "property_profile": "a62c14cebc6c68f66092ee59f77843a644d0d9139d2ba8e57d54769e6bb1b8de",
    "property_characteristics": "99fbc661a7ffd905988eeba56f496f8674d0d82770230dac13351e094352e084",
}
UPSTREAM_MARKER_SET_SHA256 = "8c66bd2556bace9abf421e1a86d24523a26d8169b9668708e5acd6d5f673e99f"
UPSTREAM_COLLAPSED_REGEX_SHA256 = "7adde858d5e87a53c5d80929a21c6eb99fe0349884d679ef4b23052dbdb0e060"


@dataclass(frozen=True)
class FixedWidthField:
    """One 1-indexed, inclusive field from TCAD's official layout."""

    start: int
    end: int
    description: str

    @property
    def length(self) -> int:
        return self.end - self.start + 1

    @property
    def slice(self) -> slice:
        return slice(self.start - 1, self.end)


# Encoded directly from Legacy8.0.30-AppraisalExportLayout.xlsx, Property tab.
# Fields not needed for ownership, occupancy, source validation, or residential
# linkage are intentionally skipped.
FWF_SCHEMA_2024: dict[str, FixedWidthField] = {
    "prop_id": FixedWidthField(1, 12, "Property ID"),
    "prop_type_cd": FixedWidthField(13, 17, "Property type code"),
    "prop_val_yr": FixedWidthField(18, 22, "Appraisal or tax year"),
    "sup_num": FixedWidthField(23, 34, "Supplement version number"),
    "sup_action": FixedWidthField(35, 36, "Supplement action"),
    "geo_id": FixedWidthField(547, 596, "Geographic ID"),
    "py_owner_id": FixedWidthField(597, 608, "Property-year owner ID"),
    "py_owner_name": FixedWidthField(609, 678, "Property-year owner name"),
    "partial_owner": FixedWidthField(679, 679, "Partial-owner flag"),
    "udi_group": FixedWidthField(680, 691, "Undivided-interest group"),
    "py_addr_line1": FixedWidthField(694, 753, "Property-year owner address line 1"),
    "py_addr_line2": FixedWidthField(754, 813, "Property-year owner address line 2"),
    "py_addr_line3": FixedWidthField(814, 873, "Property-year owner address line 3"),
    "py_addr_city": FixedWidthField(874, 923, "Property-year owner address city"),
    "py_addr_state": FixedWidthField(924, 973, "Property-year owner address state"),
    "py_addr_country": FixedWidthField(974, 978, "Property-year owner address country"),
    "py_addr_zip": FixedWidthField(979, 983, "Property-year owner ZIP"),
    "py_addr_zip_cass": FixedWidthField(984, 987, "Property-year owner ZIP+4"),
    "py_addr_zip_rt": FixedWidthField(988, 989, "Property-year owner ZIP route"),
    "py_confidential_flag": FixedWidthField(990, 990, "Property-year confidentiality flag"),
    "py_address_suppress_flag": FixedWidthField(991, 991, "Property-year address-suppression flag"),
    "situs_street_prefx": FixedWidthField(1040, 1049, "Situs street prefix"),
    "situs_street": FixedWidthField(1050, 1099, "Situs street name"),
    "situs_street_suffix": FixedWidthField(1100, 1109, "Situs street suffix"),
    "situs_city": FixedWidthField(1110, 1139, "Situs city"),
    "situs_zip": FixedWidthField(1140, 1149, "Situs ZIP"),
    "jan1_owner_id": FixedWidthField(2191, 2202, "January 1 owner ID"),
    "jan1_owner_name": FixedWidthField(2203, 2272, "January 1 owner name"),
    "jan1_addr_line1": FixedWidthField(2273, 2332, "January 1 owner address line 1"),
    "jan1_addr_line2": FixedWidthField(2333, 2392, "January 1 owner address line 2"),
    "jan1_addr_line3": FixedWidthField(2393, 2452, "January 1 owner address line 3"),
    "jan1_addr_city": FixedWidthField(2453, 2502, "January 1 owner address city"),
    "jan1_addr_state": FixedWidthField(2503, 2552, "January 1 owner address state"),
    "jan1_addr_country": FixedWidthField(2553, 2557, "January 1 owner address country"),
    "jan1_addr_zip": FixedWidthField(2558, 2562, "January 1 owner ZIP"),
    "jan1_addr_zip_cass": FixedWidthField(2563, 2566, "January 1 owner ZIP+4"),
    "jan1_addr_zip_rt": FixedWidthField(2567, 2568, "January 1 owner ZIP route"),
    "jan1_confidential_flag": FixedWidthField(2569, 2569, "January 1 confidentiality flag"),
    "jan1_address_suppress_flag": FixedWidthField(2570, 2570, "January 1 address-suppression flag"),
    "hs_exempt": FixedWidthField(2609, 2609, "Homestead-exemption flag"),
    "imprv_state_cd": FixedWidthField(2732, 2741, "Improvement state code"),
    "land_state_cd": FixedWidthField(2742, 2751, "Land state code"),
    "prop_owner_sequence": FixedWidthField(4051, 4090, "Property-owner sequence"),
    "situs_num": FixedWidthField(4460, 4474, "Situs number"),
    "situs_unit": FixedWidthField(4475, 4479, "Situs unit"),
    "appr_owner_id": FixedWidthField(4480, 4491, "Current-appraisal owner ID"),
    "appr_owner_name": FixedWidthField(4492, 4561, "Current-appraisal owner name"),
    "appr_addr_line1": FixedWidthField(4562, 4621, "Current-appraisal owner address line 1"),
    "appr_addr_line2": FixedWidthField(4622, 4681, "Current-appraisal owner address line 2"),
    "appr_addr_line3": FixedWidthField(4682, 4741, "Current-appraisal owner address line 3"),
    "appr_addr_city": FixedWidthField(4742, 4791, "Current-appraisal owner address city"),
    "appr_addr_state": FixedWidthField(4792, 4841, "Current-appraisal owner address state"),
    "appr_addr_country": FixedWidthField(4842, 4846, "Current-appraisal owner address country"),
    "appr_addr_zip": FixedWidthField(4847, 4851, "Current-appraisal owner ZIP"),
    "appr_addr_zip_cass": FixedWidthField(4852, 4855, "Current-appraisal owner ZIP+4"),
    "appr_addr_zip_rt": FixedWidthField(4856, 4857, "Current-appraisal owner ZIP route"),
    "appr_confidential_flag": FixedWidthField(4859, 4859, "Current-appraisal confidentiality flag"),
    "appr_address_suppress_flag": FixedWidthField(4860, 4860, "Current-appraisal address-suppression flag"),
}
FWF_RECORD_LENGTH_2024 = 9247
FWF_LINE_LENGTH_CRLF_2024 = 9249


STANDARD_OWNER_FIELDS = [
    "tax_year",
    "parcel_id",
    "owner_id",
    "owner_name",
    "owner_share",
    "owner_addr_line1",
    "owner_addr_line2",
    "owner_addr_line3",
    "owner_addr_city",
    "owner_addr_state",
    "owner_addr_country",
    "owner_addr_zip",
    "owner_confidential_flag",
    "owner_address_suppressed_flag",
    "homestead_flag",
    "situs_number",
    "situs_prefix",
    "situs_street",
    "situs_suffix",
    "situs_unit",
    "situs_city",
    "situs_state",
    "source_situs_state_imputed",
    "situs_zip",
    "property_type_code",
    "improvement_state_code",
    "land_state_code",
    "source_snapshot_id",
    "source_owner_field",
    "source_supplement_number",
    "source_supplement_action",
    "source_owner_sequence",
    "source_partial_owner_flag",
    "jan1_owner_id",
    "jan1_owner_name",
    "jan1_owner_address",
    "jan1_confidential_flag",
    "jan1_address_suppressed_flag",
    "appraisal_owner_id",
    "appraisal_owner_name",
    "appraisal_owner_address",
    "appraisal_confidential_flag",
    "appraisal_address_suppressed_flag",
]

SNAPSHOT_FIELDS = [
    "source_county",
    "tax_year",
    "parcel_id",
    "owner_ids",
    "owner_names",
    "n_owner_rows",
    "owner_name_available",
    "owner_address_available",
    "is_owner_occupied",
    "has_financialized_owner",
    "is_corporate_owned",
    "classification_status",
    "classification_rule_version",
    "source_snapshot_id",
    "source_owner_field",
    "source_supplement_number",
    "property_units",
    "residential_use_category",
    "source_property_type_code",
    "source_improvement_state_code",
    "source_land_state_code",
    "name_evidence_complete",
    "address_evidence_complete",
    "homestead_evidence_available",
    "homestead_positive",
    "address_match_positive",
    "situs_state_imputed_address_match",
    "classification_note",
]

MISSING_TEXT = {"", "NA", "N/A", "NULL", "NONE", "NAN"}
TRUE_TEXT = {"T", "TRUE", "Y", "YES", "1"}
FALSE_TEXT = {"F", "FALSE", "N", "NO", "0"}


# This is the bounded marker set in standalone_corporate_parcels.R.  The LLP
# variants are deliberate.  Python's ``\\d`` is the equivalent of R's
# ``[[:digit:]]``.  Names are normalized before matching, as in the R script.
FINANCIAL_MARKER_PATTERNS = (
    r"\bLTD\b",
    r"\bL T D\b",
    r"\bL\.?T\.?D\.?\b",
    r"\bLLC\b",
    r"\bL L C\b",
    r"\bL\.?L\.?C\.?\b",
    r"\bLLP\b",
    r"\bL L P\b",
    r"\bL\.?L\.?P\.?\b",
    r"\bLP\b",
    r"\bL P\b",
    r"\bL\.?P\.?\b",
    r"\bLLLP\b",
    r"\bL L L P\b",
    r"\bL\.?L\.?L\.?P\.?\b",
    r"\bINC\b",
    r"\bI N C\b",
    r"\bI\.?N\.?C\.?\b",
    r"\bLC\b",
    r"\bL C\b",
    r"\bL\.?C\.?\b",
    r"\bMORTG",
    r"\bRENT\b",
    r"\bMARKET\b",
    r"\bINVEST",
    r"\bPROP\b",
    r"\bMANAGE",
    r"\bMGT\b",
    r"\bMGMT\b",
    r"\bASSET",
    r"\bJOINT\b",
    r"\bVENTURE",
    r"\bVNT\b",
    r"\bLIMIT",
    r"\bPARTN",
    r"\bPRTN\b",
    r"\bBANK\b",
    r"\bASSOC",
    r"\bEQUIT",
    r"\bREALT",
    r"\bOWNER\b",
    r"\bHOLDING",
    r"\bDEVELOP",
    r"\bCOMP\b",
    r"\bCORP\b",
    r"\bAQUISI",
    r"\bCONDO\b",
    r"\bC/O\b",
    r"\d",
    r"\bBORROWER\b",
    r"\bFOUNDA",
)
FINANCIAL_MARKER_RE = re.compile("|".join(FINANCIAL_MARKER_PATTERNS))

ADDRESS_REPLACEMENTS = (
    ("RANCH ROAD", "RR"),
    ("DRIVE", "DR"),
    ("INTERSTATE", "IH"),
    ("LANE", "LN"),
    ("ROAD", "RD"),
    ("TRAIL", "TRL"),
    ("STREET", "ST"),
    ("FREEWAY", "FRWY"),
    ("AVENUE", "AVE"),
    ("CIRCLE", "CIR"),
    ("PARKWAY", "PKWY"),
    ("BOULEVARD", "BLVD"),
    ("MOUNTAIN", "MTN"),
    ("PLAZA", "PLZ"),
)


class SourceValidationError(RuntimeError):
    """Raised when a pinned source no longer satisfies its contract."""


class RecordLengthError(SourceValidationError):
    pass


class UnexpectedYearError(SourceValidationError):
    pass


class UnexpectedSupplementError(SourceValidationError):
    pass


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.upper() in MISSING_TEXT:
        return ""
    return text


def normalize_parcel_id(value: Any) -> str:
    text = clean_text(value)
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".", 1)[0]
    if not re.fullmatch(r"\d+", text):
        raise ValueError(f"Invalid Travis parcel ID: {value!r}")
    return text.lstrip("0") or "0"


def normalize_owner_id(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".", 1)[0]
    if re.fullmatch(r"\d+", text):
        return text.lstrip("0") or ""
    return text.upper()


def normalize_owner_name(value: Any) -> str:
    """Port the standalone R owner-name normalization to one shared function."""

    text = clean_text(value)
    if not text:
        return ""
    text = text.upper()
    # Preserve the configured C/O marker across the legacy punctuation-removal
    # step.  Without this sentinel the declared marker can never match.
    text = re.sub(r"\bC\s*/\s*O\b", "CODEXCAREOFMARKER", text)
    text = re.sub(r"\b(L\.?L\.?C\.?)\s*[-/]", r"\1 ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s{2,}", " ", text)
    text = re.sub(f"[{re.escape(string.punctuation)}]", "", text)
    text = text.replace("CODEXCAREOFMARKER", "C/O")
    return text.strip()


def zip5(value: Any) -> str:
    text = clean_text(value)
    return text.split("-", 1)[0].strip()


def normalize_address(value: Any) -> str:
    """Normalize addresses using the standalone rules with safe word bounds.

    The current R helper applies tokens such as ``STE`` inside street names
    (for example, ``BUSTER``).  That is a demonstrated defect, so the shared
    version keeps the same vocabulary but limits replacements to whole words.
    """

    text = clean_text(value)
    if not text:
        return ""
    text = text.upper()
    text = re.sub(r"\b(?:SUITE|STE|CONDO|UNIT|APT|BLDG)\b", "", text)
    text = re.sub(f"[{re.escape(string.punctuation)}]", "", text)
    text = re.sub(r"\s+NA\s+|\s+NO\s+", " ", text)
    text = re.sub(r"^NA*\s+|\s+NA*$", "", text)
    text = re.sub(r"\s{2,}", " ", text)
    for old, new in ADDRESS_REPLACEMENTS:
        text = re.sub(rf"\b{re.escape(old)}\b", new, text)
    text = re.sub(r"\bNORTH\b", "N", text)
    text = re.sub(r"\bSOUTH\b", "S", text)
    text = re.sub(r"\bEAST\b", "E", text)
    text = re.sub(r"\bWEST\b", "W", text)
    return re.sub(r"\s{2,}", " ", text).strip()


def join_address(*parts: Any) -> str:
    return normalize_address(" ".join(filter(None, (clean_text(part) for part in parts))))


def parse_optional_bool(value: Any) -> bool | None:
    text = clean_text(value).upper()
    if not text:
        return None
    if text in TRUE_TEXT:
        return True
    if text in FALSE_TEXT:
        return False
    return None


def exemption_list_has_homestead(value: Any) -> bool:
    return "HS" in {
        token.strip().upper()
        for token in clean_text(value).split(",")
        if token.strip()
    }


def bool_csv(value: bool | None) -> str:
    if value is None:
        return ""
    return "TRUE" if value else "FALSE"


def float_or_none(value: Any) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    try:
        result = float(text)
    except ValueError:
        return None
    return result if math.isfinite(result) else None


def natural_key(value: str) -> tuple[int, Any, str]:
    if value.isdigit():
        return (0, int(value), value)
    return (1, value, value)


def name_is_financialized(value: Any) -> bool | None:
    name = normalize_owner_name(value)
    if not name:
        return None
    return bool(FINANCIAL_MARKER_RE.search(name))


def addresses_match(owner_address: Any, situs_address: Any) -> bool | None:
    owner = normalize_address(owner_address)
    situs = normalize_address(situs_address)
    if not owner or not situs:
        return None
    return owner in situs


def three_value_corporate(
    is_residential: bool | None,
    is_owner_occupied: bool | None,
    has_financialized_owner: bool | None,
) -> bool | None:
    if is_residential is False or is_owner_occupied is True or has_financialized_owner is False:
        return False
    if is_residential is True and is_owner_occupied is False and has_financialized_owner is True:
        return True
    return None


def classifier_rule_version() -> str:
    pieces = [
        inspect.getsource(normalize_owner_name),
        inspect.getsource(normalize_address),
        inspect.getsource(name_is_financialized),
        inspect.getsource(addresses_match),
        inspect.getsource(three_value_corporate),
        inspect.getsource(exemption_list_has_homestead),
        inspect.getsource(source_owner_address),
        inspect.getsource(source_situs_address),
        inspect.getsource(classify_owner_row),
        inspect.getsource(aggregate_tristate),
        inspect.getsource(aggregate_owner_rows),
        inspect.getsource(fixed_2024_to_standard),
        inspect.getsource(owner_2025_to_standard),
        json.dumps(FINANCIAL_MARKER_PATTERNS),
        json.dumps(ADDRESS_REPLACEMENTS),
        "parcel aggregation: any TRUE; FALSE only when every row is FALSE; otherwise NA",
    ]
    digest = hashlib.sha256("\n".join(pieces).encode("utf-8")).hexdigest()[:12]
    return f"lm-historical-owner-v1-{digest}"


def source_owner_address(row: Mapping[str, Any]) -> str:
    return join_address(
        row.get("owner_addr_line1"),
        row.get("owner_addr_line2"),
        row.get("owner_addr_line3"),
        row.get("owner_addr_city"),
        row.get("owner_addr_state"),
        zip5(row.get("owner_addr_zip")),
    )


def source_situs_address(row: Mapping[str, Any]) -> str:
    # The standalone 2025 classifier does not include the secondary/unit field
    # in its situs comparison, so the shared implementation does the same.
    city = clean_text(row.get("situs_city")) or "AUSTIN"
    return join_address(
        row.get("situs_number"),
        row.get("situs_prefix"),
        row.get("situs_street"),
        row.get("situs_suffix"),
        city,
        row.get("situs_state") or "TX",
        zip5(row.get("situs_zip")),
    )


def classify_owner_row(
    row: Mapping[str, Any],
    *,
    classification_situs_address: Any = None,
) -> dict[str, Any]:
    confidential = parse_optional_bool(row.get("owner_confidential_flag")) is True
    address_suppressed = parse_optional_bool(row.get("owner_address_suppressed_flag")) is True
    normalized_name = normalize_owner_name(row.get("owner_name"))
    owner_address = source_owner_address(row)
    situs_address = (
        normalize_address(classification_situs_address)
        if clean_text(classification_situs_address)
        else source_situs_address(row)
    )

    name_available = bool(normalized_name) and not confidential
    # City/state/ZIP alone are not a usable mailing address for occupancy
    # matching.  At least one actual delivery/free-form line must be present.
    has_delivery_line = any(
        clean_text(row.get(field))
        # For 2025 line 2 is the unit designator, which is not independently
        # sufficient.  Fixed-width/free-form line 3 remains a valid fallback.
        for field in ("owner_addr_line1", "owner_addr_line3")
    )
    address_available = bool(owner_address) and has_delivery_line and not address_suppressed
    situs_available = bool(situs_address)
    homestead = parse_optional_bool(row.get("homestead_flag"))
    address_match = (
        addresses_match(owner_address, situs_address)
        if address_available and situs_available
        else None
    )

    # Pass the raw value so punctuation deletion happens exactly once.  Calling
    # the normalizer twice would collapse the double space left by names such
    # as "L & P" and incorrectly create the formal "L P" marker.
    financialized = name_is_financialized(row.get("owner_name")) if name_available else None
    if homestead is True:
        owner_occupied: bool | None = True
    elif address_match is not None:
        owner_occupied = address_match
    else:
        owner_occupied = None

    return {
        "owner_id": normalize_owner_id(row.get("owner_id")),
        "owner_name": normalized_name if name_available else "",
        "name_available": name_available,
        "address_available": address_available,
        "situs_available": situs_available,
        "homestead_available": homestead is not None,
        "homestead_positive": homestead is True,
        "address_match": address_match,
        "situs_state_imputed": (
            parse_optional_bool(row.get("source_situs_state_imputed")) is True
        ),
        "partial_owner": parse_optional_bool(row.get("source_partial_owner_flag")) is True,
        "owner_occupied": owner_occupied,
        "financialized": financialized,
        "confidential": confidential,
        "address_suppressed": address_suppressed,
        "owner_address": owner_address if address_available else "",
        "property_type_code": clean_text(row.get("property_type_code")),
        "improvement_state_code": clean_text(row.get("improvement_state_code")),
        "land_state_code": clean_text(row.get("land_state_code")),
    }


def aggregate_tristate(values: Sequence[bool | None]) -> bool | None:
    if any(value is True for value in values):
        return True
    if values and all(value is False for value in values):
        return False
    return None


def first_sorted(values: Iterable[str]) -> str:
    present = sorted({clean_text(value) for value in values if clean_text(value)}, key=natural_key)
    return present[0] if present else ""


def aggregate_owner_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    is_residential: bool = True,
    classification_situs_address: Any = None,
) -> dict[str, Any]:
    evidence = [
        classify_owner_row(row, classification_situs_address=classification_situs_address)
        for row in rows
    ]
    occupied = aggregate_tristate([item["owner_occupied"] for item in evidence])
    financialized = aggregate_tristate([item["financialized"] for item in evidence])
    corporate = three_value_corporate(is_residential, occupied, financialized)

    names = sorted({item["owner_name"] for item in evidence if item["owner_name"]})
    owner_ids = sorted({item["owner_id"] for item in evidence if item["owner_id"]}, key=natural_key)
    name_complete = bool(evidence) and all(item["name_available"] for item in evidence)
    address_complete = bool(evidence) and all(
        item["address_available"] and item["situs_available"] for item in evidence
    )

    signatures_by_id: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for item in evidence:
        if item["owner_id"]:
            signatures_by_id[item["owner_id"]].add((item["owner_name"], item["owner_address"]))
    conflicting_owner_key = any(len(signatures) > 1 for signatures in signatures_by_id.values())
    incomplete_partial_owner_evidence = len(evidence) == 1 and any(
        item["partial_owner"] for item in evidence
    )

    if conflicting_owner_key:
        occupied = financialized = corporate = None
        status = "matched_ambiguous"
        note = "same owner ID has conflicting normalized name or address evidence"
    elif incomplete_partial_owner_evidence:
        occupied = financialized = corporate = None
        status = "matched_ambiguous"
        note = "singleton row is flagged as a partial owner; co-owner evidence may be incomplete"
    elif occupied is not None and financialized is not None and corporate is not None:
        status = "matched_classified"
        note = ""
    elif any(item["confidential"] or item["address_suppressed"] for item in evidence):
        status = "matched_owner_suppressed"
        note = "confidentiality or address-suppression flag limits required evidence"
    elif not evidence or not any(item["name_available"] for item in evidence):
        status = "matched_owner_missing"
        note = "no usable owner-name evidence"
    elif not name_complete:
        status = "matched_owner_partial_missing"
        note = "at least one owner row lacks usable owner-name evidence"
    else:
        status = "matched_evidence_insufficient"
        note = "mailing-address or homestead evidence is insufficient for all owner rows"

    return {
        "owner_ids": "; ".join(owner_ids),
        "owner_names": "; ".join(names),
        "n_owner_rows": len(rows),
        "owner_name_available": any(item["name_available"] for item in evidence),
        "owner_address_available": any(item["address_available"] for item in evidence),
        "is_owner_occupied": occupied,
        "has_financialized_owner": financialized,
        "is_corporate_owned": corporate,
        "classification_status": status,
        "name_evidence_complete": name_complete,
        "address_evidence_complete": address_complete,
        "homestead_evidence_available": any(item["homestead_available"] for item in evidence),
        "homestead_positive": any(item["homestead_positive"] for item in evidence),
        "address_match_positive": any(item["address_match"] is True for item in evidence),
        "situs_state_imputed_address_match": any(
            item["situs_state_imputed"] and item["address_match"] is True
            for item in evidence
        ),
        "classification_note": note,
        "source_property_type_code": first_sorted(item["property_type_code"] for item in evidence),
        "source_improvement_state_code": first_sorted(
            item["improvement_state_code"] for item in evidence
        ),
        "source_land_state_code": first_sorted(item["land_state_code"] for item in evidence),
    }


def file_sha256(path: Path, block_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(block_size), b""):
            digest.update(block)
    return digest.hexdigest()


def file_metadata(path: Path, include_sha256: bool = True) -> dict[str, Any]:
    stat = path.stat()
    result: dict[str, Any] = {
        "path": str(path.resolve()),
        "bytes": stat.st_size,
        "modified_at_utc": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
    }
    if include_sha256:
        result["sha256"] = file_sha256(path)
    return result


def zip_members(path: Path) -> list[dict[str, Any]]:
    with zipfile.ZipFile(path) as archive:
        return [
            {
                "name": info.filename,
                "uncompressed_bytes": info.file_size,
                "compressed_bytes": info.compress_size,
                "crc32": f"{info.CRC:08x}",
                "compression_method": info.compress_type,
                "modified_at": "%04d-%02d-%02dT%02d:%02d:%02d"
                % info.date_time,
            }
            for info in archive.infolist()
        ]


def strip_line_ending(raw_line: bytes) -> bytes:
    if raw_line.endswith(b"\r\n"):
        return raw_line[:-2]
    if raw_line.endswith(b"\n"):
        return raw_line[:-1]
    return raw_line


def extract_fwf(record: bytes, field: str) -> str:
    spec = FWF_SCHEMA_2024[field]
    return record[spec.slice].decode("cp1252", errors="strict").strip()


def parse_2024_fixed_width_line(
    raw_line: bytes,
    *,
    expected_year: int = 2024,
    expected_supplement: int = 0,
) -> dict[str, str]:
    record = strip_line_ending(raw_line)
    if len(record) != FWF_RECORD_LENGTH_2024:
        raise RecordLengthError(
            f"Expected {FWF_RECORD_LENGTH_2024} data bytes, received {len(record)}"
        )
    values = {name: extract_fwf(record, name) for name in FWF_SCHEMA_2024}
    try:
        year = int(values["prop_val_yr"])
    except ValueError as exc:
        raise UnexpectedYearError(f"Unparseable tax year {values['prop_val_yr']!r}") from exc
    try:
        supplement = int(values["sup_num"])
    except ValueError as exc:
        raise UnexpectedSupplementError(f"Unparseable supplement {values['sup_num']!r}") from exc
    if year != expected_year:
        raise UnexpectedYearError(f"Expected {expected_year}, received {year}")
    if supplement != expected_supplement:
        raise UnexpectedSupplementError(
            f"Expected supplement {expected_supplement}, received {supplement}"
        )
    return values


def fixed_2024_to_standard(values: Mapping[str, str]) -> dict[str, Any]:
    return {
        "tax_year": "2024",
        "parcel_id": normalize_parcel_id(values["prop_id"]),
        "owner_id": normalize_owner_id(values["py_owner_id"]),
        "owner_name": clean_text(values["py_owner_name"]),
        # Legacy8.0.30 has no explicit ownership-share field.  partial_owner and
        # prop_owner_sequence are retained separately instead.
        "owner_share": "",
        "owner_addr_line1": values["py_addr_line1"],
        "owner_addr_line2": values["py_addr_line2"],
        "owner_addr_line3": values["py_addr_line3"],
        "owner_addr_city": values["py_addr_city"],
        "owner_addr_state": values["py_addr_state"],
        "owner_addr_country": values["py_addr_country"],
        "owner_addr_zip": values["py_addr_zip"],
        "owner_confidential_flag": values["py_confidential_flag"],
        "owner_address_suppressed_flag": values["py_address_suppress_flag"],
        "homestead_flag": values["hs_exempt"],
        "situs_number": values["situs_num"],
        "situs_prefix": values["situs_street_prefx"],
        "situs_street": values["situs_street"],
        "situs_suffix": values["situs_street_suffix"],
        "situs_unit": values["situs_unit"],
        "situs_city": values["situs_city"],
        # Legacy8.0.30 has no situs-state field.  All records in this source
        # are Travis County, Texas, so the shared address schema supplies TX.
        "situs_state": "TX",
        "source_situs_state_imputed": "TRUE",
        "situs_zip": values["situs_zip"],
        "property_type_code": values["prop_type_cd"],
        "improvement_state_code": values["imprv_state_cd"],
        "land_state_code": values["land_state_cd"],
        "source_snapshot_id": SOURCE_2024_SNAPSHOT_ID,
        "source_owner_field": "property_year_owner",
        "source_supplement_number": "0",
        "source_supplement_action": values["sup_action"],
        "source_owner_sequence": values["prop_owner_sequence"],
        "source_partial_owner_flag": values["partial_owner"],
        "jan1_owner_id": normalize_owner_id(values["jan1_owner_id"]),
        "jan1_owner_name": values["jan1_owner_name"],
        "jan1_owner_address": join_address(
            values["jan1_addr_line1"],
            values["jan1_addr_line2"],
            values["jan1_addr_line3"],
            values["jan1_addr_city"],
            values["jan1_addr_state"],
            values["jan1_addr_zip"],
        ),
        "jan1_confidential_flag": values["jan1_confidential_flag"],
        "jan1_address_suppressed_flag": values["jan1_address_suppress_flag"],
        "appraisal_owner_id": normalize_owner_id(values["appr_owner_id"]),
        "appraisal_owner_name": values["appr_owner_name"],
        "appraisal_owner_address": join_address(
            values["appr_addr_line1"],
            values["appr_addr_line2"],
            values["appr_addr_line3"],
            values["appr_addr_city"],
            values["appr_addr_state"],
            values["appr_addr_zip"],
        ),
        "appraisal_confidential_flag": values["appr_confidential_flag"],
        "appraisal_address_suppressed_flag": values["appr_address_suppress_flag"],
    }


def blank_standard_row() -> dict[str, str]:
    return {field: "" for field in STANDARD_OWNER_FIELDS}


def write_csv(path: Path, fields: Sequence[str], rows: Iterable[Mapping[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields), extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})
            count += 1
    return count


def run_unzip_member(path: Path, member: str) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        ["unzip", "-p", str(path), member],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1024 * 1024,
    )


def read_small_member_external(path: Path, member: str) -> bytes:
    result = subprocess.run(
        ["unzip", "-p", str(path), member],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise SourceValidationError(
            f"Could not read {member} from {path}: {result.stderr.decode(errors='replace')}"
        )
    return result.stdout


def read_member_prefix_external(path: Path, member: str, byte_count: int = 1024 * 1024) -> bytes:
    process = run_unzip_member(path, member)
    assert process.stdout is not None
    try:
        return process.stdout.read(byte_count)
    finally:
        process.terminate()
        process.wait()


def agreement_update(
    counters: Counter[str],
    label: str,
    left: str,
    right: str,
) -> None:
    if left and right:
        counters[f"{label}_comparable_rows"] += 1
        if left == right:
            counters[f"{label}_agree_rows"] += 1


def source_qa_metrics_template() -> Counter[str]:
    return Counter(
        {
            "raw_rows": 0,
            "invalid_record_length_rows": 0,
            "unexpected_year_rows": 0,
            "unexpected_supplement_rows": 0,
            "parsing_failure_rows": 0,
            "duplicate_property_owner_keys": 0,
            "blank_owner_id_rows": 0,
            "blank_owner_name_rows": 0,
            "blank_owner_address_rows": 0,
            "confidential_owner_rows": 0,
            "address_suppressed_owner_rows": 0,
        }
    )


def scan_2024_source(
    archive_path: Path,
    intermediate_path: Path,
    target_ids: set[str],
) -> tuple[dict[str, list[dict[str, Any]]], set[str], dict[str, Any]]:
    members = {item["name"]: item for item in zip_members(archive_path)}
    if "PROP.TXT" not in members or "APPR_HDR.TXT" not in members:
        raise SourceValidationError("2024 archive lacks PROP.TXT or APPR_HDR.TXT")
    prop_size = members["PROP.TXT"]["uncompressed_bytes"]
    if prop_size % FWF_LINE_LENGTH_CRLF_2024 != 0:
        raise SourceValidationError(
            f"PROP.TXT byte size {prop_size} is not divisible by {FWF_LINE_LENGTH_CRLF_2024}"
        )
    expected_rows = prop_size // FWF_LINE_LENGTH_CRLF_2024
    header = read_small_member_external(archive_path, "APPR_HDR.TXT").decode("cp1252")
    if "Appraisal Export - 2024" not in header or "20240000" not in header:
        raise SourceValidationError(f"Unexpected 2024 appraisal header: {header!r}")

    intermediate_path.parent.mkdir(parents=True, exist_ok=True)
    rows_by_target: dict[str, list[dict[str, Any]]] = defaultdict(list)
    source_property_ids: set[str] = set()
    owner_counts: Counter[str] = Counter()
    qa = source_qa_metrics_template()
    distribution: Counter[tuple[str, str]] = Counter()
    property_type_counts: Counter[str] = Counter()
    seen_owner_keys: set[tuple[str, str, str, str]] = set()
    partial_owner_property_ids: set[str] = set()
    homestead_flags_by_property: dict[str, set[bool]] = defaultdict(set)
    agreement = Counter()
    failures: list[str] = []

    process = run_unzip_member(archive_path, "PROP.TXT")
    assert process.stdout is not None
    with intermediate_path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=STANDARD_OWNER_FIELDS, lineterminator="\n")
        writer.writeheader()
        for line_number, raw_line in enumerate(process.stdout, 1):
            qa["raw_rows"] += 1
            try:
                values = parse_2024_fixed_width_line(raw_line)
            except RecordLengthError as exc:
                qa["invalid_record_length_rows"] += 1
                if len(failures) < 10:
                    failures.append(f"line {line_number}: {exc}")
                continue
            except UnexpectedYearError as exc:
                qa["unexpected_year_rows"] += 1
                if len(failures) < 10:
                    failures.append(f"line {line_number}: {exc}")
                continue
            except UnexpectedSupplementError as exc:
                qa["unexpected_supplement_rows"] += 1
                if len(failures) < 10:
                    failures.append(f"line {line_number}: {exc}")
                continue
            except (UnicodeDecodeError, ValueError) as exc:
                qa["parsing_failure_rows"] += 1
                if len(failures) < 10:
                    failures.append(f"line {line_number}: {exc}")
                continue

            standard = fixed_2024_to_standard(values)
            writer.writerow(standard)
            pid = standard["parcel_id"]
            source_property_ids.add(pid)
            owner_counts[pid] += 1
            distribution[(standard["tax_year"], standard["source_supplement_number"])] += 1
            property_type_counts[standard["property_type_code"] or "blank"] += 1
            owner_key = (
                pid,
                standard["tax_year"],
                standard["source_supplement_number"],
                standard["owner_id"],
            )
            if standard["owner_id"]:
                if owner_key in seen_owner_keys:
                    qa["duplicate_property_owner_keys"] += 1
                seen_owner_keys.add(owner_key)

            py_name = normalize_owner_name(standard["owner_name"])
            py_address = source_owner_address(standard)
            if not standard["owner_id"]:
                qa["blank_owner_id_rows"] += 1
            if not py_name:
                qa["blank_owner_name_rows"] += 1
            if not py_address:
                qa["blank_owner_address_rows"] += 1
            if parse_optional_bool(standard["owner_confidential_flag"]) is True:
                qa["confidential_owner_rows"] += 1
            if parse_optional_bool(standard["owner_address_suppressed_flag"]) is True:
                qa["address_suppressed_owner_rows"] += 1
            if parse_optional_bool(standard["source_partial_owner_flag"]) is True:
                qa["partial_owner_flag_rows"] += 1
                partial_owner_property_ids.add(pid)
            homestead = parse_optional_bool(standard["homestead_flag"])
            if homestead is not None:
                homestead_flags_by_property[pid].add(homestead)
            if homestead is True:
                qa["homestead_true_rows"] += 1

            jan1_name = normalize_owner_name(standard["jan1_owner_name"])
            appr_name = normalize_owner_name(standard["appraisal_owner_name"])
            jan1_address = normalize_address(standard["jan1_owner_address"])
            appr_address = normalize_address(standard["appraisal_owner_address"])
            if not standard["jan1_owner_id"]:
                qa["blank_jan1_owner_id_rows"] += 1
            if not jan1_name:
                qa["blank_jan1_owner_name_rows"] += 1
            if not jan1_address:
                qa["blank_jan1_owner_address_rows"] += 1
            if parse_optional_bool(standard["jan1_confidential_flag"]) is True:
                qa["confidential_jan1_owner_rows"] += 1
            if parse_optional_bool(standard["jan1_address_suppressed_flag"]) is True:
                qa["address_suppressed_jan1_owner_rows"] += 1
            if not standard["appraisal_owner_id"]:
                qa["blank_appraisal_owner_id_rows"] += 1
            if not appr_name:
                qa["blank_appraisal_owner_name_rows"] += 1
            if not appr_address:
                qa["blank_appraisal_owner_address_rows"] += 1
            if parse_optional_bool(standard["appraisal_confidential_flag"]) is True:
                qa["confidential_appraisal_owner_rows"] += 1
            if parse_optional_bool(standard["appraisal_address_suppressed_flag"]) is True:
                qa["address_suppressed_appraisal_owner_rows"] += 1
            agreement_update(agreement, "py_jan1_owner_id", standard["owner_id"], standard["jan1_owner_id"])
            agreement_update(agreement, "py_appr_owner_id", standard["owner_id"], standard["appraisal_owner_id"])
            agreement_update(agreement, "jan1_appr_owner_id", standard["jan1_owner_id"], standard["appraisal_owner_id"])
            agreement_update(agreement, "py_jan1_owner_name", py_name, jan1_name)
            agreement_update(agreement, "py_appr_owner_name", py_name, appr_name)
            agreement_update(agreement, "jan1_appr_owner_name", jan1_name, appr_name)
            agreement_update(agreement, "py_jan1_owner_address", py_address, jan1_address)
            agreement_update(agreement, "py_appr_owner_address", py_address, appr_address)
            agreement_update(agreement, "jan1_appr_owner_address", jan1_address, appr_address)

            if pid in target_ids:
                rows_by_target[pid].append(standard)

    stderr = process.stderr.read().decode(errors="replace") if process.stderr else ""
    return_code = process.wait()
    if return_code != 0:
        raise SourceValidationError(f"unzip failed while streaming PROP.TXT: {stderr}")
    if qa["raw_rows"] != expected_rows:
        failures.append(f"expected {expected_rows} PROP rows; streamed {qa['raw_rows']}")

    fatal_count = sum(
        qa[key]
        for key in (
            "invalid_record_length_rows",
            "unexpected_year_rows",
            "unexpected_supplement_rows",
            "parsing_failure_rows",
        )
    )
    if fatal_count or qa["raw_rows"] != expected_rows:
        raise SourceValidationError("2024 parser validation failed: " + "; ".join(failures))

    qa["unique_property_ids"] = len(source_property_ids)
    qa["parcels_with_multiple_owner_rows"] = sum(count > 1 for count in owner_counts.values())
    qa["max_owner_rows_per_parcel"] = max(owner_counts.values(), default=0)
    qa["partial_owner_flag_parcels"] = len(partial_owner_property_ids)
    qa["singleton_partial_owner_flag_parcels"] = sum(
        owner_counts[pid] == 1 for pid in partial_owner_property_ids
    )
    qa["parcels_with_mixed_homestead_flags"] = sum(
        flags == {False, True} for flags in homestead_flags_by_property.values()
    )
    for metric in (
        "partial_owner_flag_rows",
        "homestead_true_rows",
        "blank_jan1_owner_id_rows",
        "blank_jan1_owner_name_rows",
        "blank_jan1_owner_address_rows",
        "confidential_jan1_owner_rows",
        "address_suppressed_jan1_owner_rows",
        "blank_appraisal_owner_id_rows",
        "blank_appraisal_owner_name_rows",
        "blank_appraisal_owner_address_rows",
        "confidential_appraisal_owner_rows",
        "address_suppressed_appraisal_owner_rows",
    ):
        qa.setdefault(metric, 0)
    qa.update(agreement)
    return rows_by_target, source_property_ids, {
        "metrics": dict(qa),
        "rows_by_year_supplement": {
            f"{year}|{supplement}": count
            for (year, supplement), count in sorted(distribution.items())
        },
        "rows_by_property_type": dict(sorted(property_type_counts.items())),
        "header": header.strip(),
        "expected_rows_from_member_size": expected_rows,
    }


def csv_rows(path: Path) -> Iterable[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        yield from csv.DictReader(handle)


def choose_primary_situs_2025(path: Path) -> tuple[dict[str, dict[str, str]], dict[str, Any]]:
    selected: dict[str, tuple[tuple[int, int, str], dict[str, str]]] = {}
    raw_rows = 0
    invalid_ids = 0
    for row in csv_rows(path):
        raw_rows += 1
        try:
            pid = normalize_parcel_id(row.get("situs_pID"))
        except ValueError:
            invalid_ids += 1
            continue
        primary = int(float(clean_text(row.get("situs_primarySitus")) or "0"))
        address_id_text = clean_text(row.get("situs_situsAddressID"))
        address_id = int(float(address_id_text)) if re.fullmatch(r"\d+(?:\.0+)?", address_id_text) else 10**30
        sort_key = (-primary, address_id, address_id_text)
        if pid not in selected or sort_key < selected[pid][0]:
            selected[pid] = (sort_key, row)
    return {pid: item[1] for pid, item in selected.items()}, {
        "situs_raw_rows": raw_rows,
        "situs_unique_property_ids": len(selected),
        "situs_invalid_property_ids": invalid_ids,
    }


def load_property_use_2025(
    profile_path: Path,
    characteristics_path: Path,
) -> tuple[dict[str, dict[str, str]], dict[str, Any]]:
    result: dict[str, dict[str, str]] = {}
    profile_rows = 0
    profile_invalid = 0
    for row in csv_rows(profile_path):
        profile_rows += 1
        try:
            pid = normalize_parcel_id(row.get("propertyProf_pID"))
        except ValueError:
            profile_invalid += 1
            continue
        result.setdefault(pid, {}).update(
            {
                "improvement_state_code": clean_text(row.get("propertyProf_imprvStateCd")),
                "land_state_code": clean_text(row.get("propertyProf_landStateCd")),
                # This cached field is the actual exemption list.  Blank means
                # no listed exemption; match HS as a complete comma token.
                "homestead_flag": bool_csv(
                    exemption_list_has_homestead(row.get("propertyProf_exemptions"))
                ),
            }
        )
    characteristics_rows = 0
    characteristics_invalid = 0
    for row in csv_rows(characteristics_path):
        characteristics_rows += 1
        try:
            pid = normalize_parcel_id(row.get("propertyChar_pID"))
        except ValueError:
            characteristics_invalid += 1
            continue
        result.setdefault(pid, {})["zoning"] = clean_text(row.get("propertyChar_zoning"))
    return result, {
        "property_profile_raw_rows": profile_rows,
        "property_profile_invalid_ids": profile_invalid,
        "property_characteristics_raw_rows": characteristics_rows,
        "property_characteristics_invalid_ids": characteristics_invalid,
        "unique_property_ids_across_use_extracts": len(result),
    }


def owner_2025_to_standard(
    owner: Mapping[str, str],
    situs: Mapping[str, str] | None,
    use: Mapping[str, str] | None,
) -> dict[str, Any]:
    standard = blank_standard_row()
    pid = normalize_parcel_id(owner.get("owner_pID"))
    situs = situs or {}
    use = use or {}
    raw_situs_state = clean_text(situs.get("situs_state"))
    free_form_mode = parse_optional_bool(owner.get("owner_addrFreeForm")) is True
    if free_form_mode:
        address_line1 = clean_text(owner.get("owner_addrFreeForm1"))
        address_line2 = clean_text(owner.get("owner_addrFreeForm2"))
        address_line3 = clean_text(owner.get("owner_addrFreeForm3"))
    else:
        address_line1 = clean_text(owner.get("owner_addrDeliveryLine"))
        address_line2 = clean_text(owner.get("owner_addrUnitDesignator"))
        address_line3 = ""
    standard.update(
        {
            "tax_year": "2025",
            "parcel_id": pid,
            "owner_id": normalize_owner_id(owner.get("owner_ownerID")),
            "owner_name": clean_text(owner.get("owner_name")),
            "owner_share": clean_text(owner.get("owner_ownerPct")),
            "owner_addr_line1": address_line1,
            "owner_addr_line2": address_line2,
            "owner_addr_line3": address_line3,
            "owner_addr_city": clean_text(owner.get("owner_addrCity")),
            "owner_addr_state": clean_text(owner.get("owner_addrState")),
            "owner_addr_country": clean_text(owner.get("owner_addrCountry")),
            "owner_addr_zip": zip5(owner.get("owner_addrZip")),
            # These fields were not retained by the cached JSON extraction.
            "owner_confidential_flag": "",
            "owner_address_suppressed_flag": "",
            "homestead_flag": clean_text(use.get("homestead_flag")),
            "situs_number": clean_text(situs.get("situs_streetNum")),
            "situs_prefix": clean_text(situs.get("situs_streetPrefix")),
            "situs_street": clean_text(situs.get("situs_streetName")),
            "situs_suffix": clean_text(situs.get("situs_streetSuffix")),
            "situs_unit": clean_text(situs.get("situs_streetSecondary")),
            "situs_city": clean_text(situs.get("situs_city")),
            # The target universe is Travis-only.  Supply TX when the cached
            # situs state is blank and retain an explicit audit flag.
            "situs_state": raw_situs_state or "TX",
            "source_situs_state_imputed": bool_csv(not raw_situs_state),
            "situs_zip": zip5(situs.get("situs_zip")),
            "property_type_code": "",
            "improvement_state_code": clean_text(use.get("improvement_state_code")),
            "land_state_code": clean_text(use.get("land_state_code")),
            "source_snapshot_id": SOURCE_2025_SNAPSHOT_ID,
            "source_owner_field": "current_special_export_owner",
            "source_supplement_number": "1",
            "source_supplement_action": "",
            "source_owner_sequence": "",
            "source_partial_owner_flag": "",
        }
    )
    return standard


CLASSIFICATION_RULE_VERSION = classifier_rule_version()


def scan_2025_source(
    owners_path: Path,
    situses_path: Path,
    profile_path: Path,
    characteristics_path: Path,
    intermediate_path: Path,
    target_ids: set[str],
) -> tuple[dict[str, list[dict[str, Any]]], set[str], dict[str, Any]]:
    situs_by_pid, situs_qa = choose_primary_situs_2025(situses_path)
    use_by_pid, use_qa = load_property_use_2025(profile_path, characteristics_path)
    source_property_ids = set(situs_by_pid) | set(use_by_pid)
    rows_by_target: dict[str, list[dict[str, Any]]] = defaultdict(list)
    qa = source_qa_metrics_template()
    owner_counts: Counter[str] = Counter()
    seen_owner_keys: set[tuple[str, str, str, str]] = set()
    invalid_ids = 0

    intermediate_path.parent.mkdir(parents=True, exist_ok=True)
    with intermediate_path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=STANDARD_OWNER_FIELDS, lineterminator="\n")
        writer.writeheader()
        for owner in csv_rows(owners_path):
            qa["raw_rows"] += 1
            try:
                pid = normalize_parcel_id(owner.get("owner_pID"))
            except ValueError:
                invalid_ids += 1
                qa["parsing_failure_rows"] += 1
                continue
            source_property_ids.add(pid)
            standard = owner_2025_to_standard(owner, situs_by_pid.get(pid), use_by_pid.get(pid))
            writer.writerow(standard)
            owner_counts[pid] += 1
            owner_key = (pid, "2025", "1", standard["owner_id"])
            if standard["owner_id"]:
                if owner_key in seen_owner_keys:
                    qa["duplicate_property_owner_keys"] += 1
                seen_owner_keys.add(owner_key)
            if not standard["owner_id"]:
                qa["blank_owner_id_rows"] += 1
            if not normalize_owner_name(standard["owner_name"]):
                qa["blank_owner_name_rows"] += 1
            if not source_owner_address(standard):
                qa["blank_owner_address_rows"] += 1
            if not classify_owner_row(standard)["address_available"]:
                qa["owner_address_evidence_unavailable_rows"] += 1
            if parse_optional_bool(owner.get("owner_addrFreeForm")) is True:
                qa["free_form_owner_address_rows"] += 1
            if parse_optional_bool(standard["source_situs_state_imputed"]) is True:
                qa["situs_state_imputed_rows"] += 1
            if parse_optional_bool(standard["homestead_flag"]) is True:
                qa["homestead_true_rows"] += 1
            if pid in target_ids:
                rows_by_target[pid].append(standard)

    qa["unique_property_ids"] = len(owner_counts)
    qa["cached_owner_rows"] = qa["raw_rows"]
    qa["parcels_with_multiple_owner_rows"] = sum(count > 1 for count in owner_counts.values())
    qa["max_owner_rows_per_parcel"] = max(owner_counts.values(), default=0)
    qa["invalid_property_ids"] = invalid_ids
    # These attributes are not retained or applicable in the cached owner CSV.
    # Omit numeric metrics rather than representing unavailable values as zero.
    for metric in (
        "invalid_record_length_rows",
        "unexpected_year_rows",
        "unexpected_supplement_rows",
        "confidential_owner_rows",
        "address_suppressed_owner_rows",
    ):
        qa.pop(metric, None)
    for metric in (
        "free_form_owner_address_rows",
        "situs_state_imputed_rows",
        "homestead_true_rows",
    ):
        qa.setdefault(metric, 0)
    return rows_by_target, source_property_ids, {
        "metrics": dict(qa),
        "rows_by_year_supplement": {"2025|1": qa["raw_rows"]},
        "component_extracts": {**situs_qa, **use_qa},
        "evidence_availability": {
            "confidentiality_flags": "not retained in cached owners.csv",
            "address_suppression_flags": "not retained in cached owners.csv",
            "invalid_record_length_rows": "not applicable to cached CSV inputs",
            "unexpected_year_rows": "not testable because tax year is not retained in cached rows",
            "unexpected_supplement_rows": (
                "not testable because supplement is not retained in cached rows"
            ),
            "homestead_exemptions": (
                "owner_exemptions is blank in owners.csv; exact HS tokens are recovered from "
                "property_profile.csv propertyProf_exemptions"
            ),
            "tax_year_and_supplement": (
                "assigned to cached rows from the pinned 2025 Special Export archive metadata; "
                "the cached CSVs do not retain these fields, so per-row unexpected-value counts "
                "are not testable"
            ),
            "record_length": "not applicable to the cached CSV inputs",
            "raw_row_definition": (
                "raw_rows and rows_by_year_supplement count records in the pinned owners.csv cache, "
                "not top-level objects independently enumerated from the 29 GB JSON member"
            ),
        },
    }


def load_ews_surface(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    duplicate_ids: list[str] = []
    invalid_ids = 0
    missing_units = 0
    for raw in csv_rows(path):
        try:
            pid = normalize_parcel_id(raw.get("parcel_id"))
        except ValueError:
            invalid_ids += 1
            continue
        if pid in seen:
            duplicate_ids.append(pid)
        seen.add(pid)
        units = float_or_none(raw.get("property_units"))
        if units is None:
            missing_units += 1
            units = 0.0
        improvement_code = clean_text(raw.get("propertyProf_imprvStateCd"))
        land_code = clean_text(raw.get("propertyProf_landStateCd"))
        zoning = clean_text(raw.get("propertyChar_zoning"))
        category = improvement_code or land_code or zoning or "unknown"
        rows.append(
            {
                **raw,
                "parcel_id": pid,
                "property_units_numeric": units,
                "residential_use_category": category,
                "ews_is_owner_occupied": parse_optional_bool(raw.get("is_owner_occupied")),
                "ews_has_financialized_owner": parse_optional_bool(raw.get("has_financialized_owner")),
                "ews_is_corporate_owned": parse_optional_bool(raw.get("is_corporate_owned")),
            }
        )
    if invalid_ids or duplicate_ids:
        raise SourceValidationError(
            f"EWS surface has {invalid_ids} invalid and {len(duplicate_ids)} duplicate parcel IDs"
        )
    rows.sort(key=lambda row: natural_key(row["parcel_id"]))
    return rows, {
        "row_count": len(rows),
        "unique_parcel_ids": len(seen),
        "missing_property_units_rows": missing_units,
        "property_units_total": sum(row["property_units_numeric"] for row in rows),
    }


def build_snapshot_rows(
    *,
    tax_year: int,
    ews_rows: Sequence[Mapping[str, Any]],
    rows_by_target: Mapping[str, Sequence[Mapping[str, Any]]],
    source_property_ids: set[str],
    source_snapshot_id: str,
    source_owner_field: str,
    source_supplement_number: str,
) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    for target in ews_rows:
        pid = str(target["parcel_id"])
        owner_rows = list(rows_by_target.get(pid, ()))
        if pid not in source_property_ids:
            aggregate = {
                "owner_ids": "",
                "owner_names": "",
                "n_owner_rows": 0,
                "owner_name_available": False,
                "owner_address_available": False,
                "is_owner_occupied": None,
                "has_financialized_owner": None,
                "is_corporate_owned": None,
                "classification_status": "source_parcel_not_found",
                "name_evidence_complete": False,
                "address_evidence_complete": False,
                "homestead_evidence_available": False,
                "homestead_positive": False,
                "address_match_positive": False,
                "situs_state_imputed_address_match": False,
                "classification_note": "target parcel ID is absent from the pinned source snapshot",
                "source_property_type_code": "",
                "source_improvement_state_code": "",
                "source_land_state_code": "",
            }
        else:
            # Use each pinned source's raw situs components.  The EWS surface's
            # pre-cleaned address contains known legacy mutations (for example,
            # TRAILSIDE -> TRLSIDE), while parcel geography/eligibility/units
            # remain fixed as required.
            aggregate = aggregate_owner_rows(owner_rows, is_residential=True)
        snapshots.append(
            {
                "source_county": "Travis",
                "tax_year": tax_year,
                "parcel_id": pid,
                **aggregate,
                "owner_name_available": bool_csv(aggregate["owner_name_available"]),
                "owner_address_available": bool_csv(aggregate["owner_address_available"]),
                "is_owner_occupied": bool_csv(aggregate["is_owner_occupied"]),
                "has_financialized_owner": bool_csv(aggregate["has_financialized_owner"]),
                "is_corporate_owned": bool_csv(aggregate["is_corporate_owned"]),
                "classification_rule_version": CLASSIFICATION_RULE_VERSION,
                "source_snapshot_id": source_snapshot_id,
                "source_owner_field": source_owner_field,
                "source_supplement_number": source_supplement_number,
                "property_units": target["property_units_numeric"],
                "residential_use_category": target["residential_use_category"],
                "name_evidence_complete": bool_csv(aggregate["name_evidence_complete"]),
                "address_evidence_complete": bool_csv(aggregate["address_evidence_complete"]),
                "homestead_evidence_available": bool_csv(
                    aggregate["homestead_evidence_available"]
                ),
                "homestead_positive": bool_csv(aggregate["homestead_positive"]),
                "address_match_positive": bool_csv(aggregate["address_match_positive"]),
                "situs_state_imputed_address_match": bool_csv(
                    aggregate["situs_state_imputed_address_match"]
                ),
            }
        )
    snapshots.sort(key=lambda row: (int(row["tax_year"]), natural_key(str(row["parcel_id"]))))
    return snapshots


def snapshot_bool(row: Mapping[str, Any], field: str) -> bool | None:
    return parse_optional_bool(row.get(field))


QA_FIELDS = [
    "record_type",
    "source_county",
    "tax_year",
    "weighting",
    "dimension",
    "category",
    "metric",
    "numerator",
    "denominator",
    "value",
    "unit",
    "note",
]


def qa_row(
    record_type: str,
    tax_year: int,
    metric: str,
    value: Any,
    *,
    weighting: str = "",
    dimension: str = "all",
    category: str = "all",
    numerator: Any = "",
    denominator: Any = "",
    unit: str = "count",
    note: str = "",
) -> dict[str, Any]:
    return {
        "record_type": record_type,
        "source_county": "Travis",
        "tax_year": tax_year,
        "weighting": weighting,
        "dimension": dimension,
        "category": category,
        "metric": metric,
        "numerator": numerator,
        "denominator": denominator,
        "value": value,
        "unit": unit,
        "note": note,
    }


def rate_qa_rows(
    tax_year: int,
    snapshots: Sequence[Mapping[str, Any]],
    *,
    dimension: str = "all",
    category: str = "all",
) -> list[dict[str, Any]]:
    definitions = {
        "source_match_rate": lambda row: row["classification_status"] != "source_parcel_not_found",
        "usable_owner_name_coverage": lambda row: snapshot_bool(row, "owner_name_available") is True,
        "complete_classification_coverage": lambda row: all(
            snapshot_bool(row, field) is not None
            for field in ("is_owner_occupied", "has_financialized_owner", "is_corporate_owned")
        ),
    }
    output: list[dict[str, Any]] = []
    for metric, predicate in definitions.items():
        parcel_num = sum(1 for row in snapshots if predicate(row))
        parcel_den = len(snapshots)
        output.append(
            qa_row(
                "linkage_qa",
                tax_year,
                metric,
                parcel_num / parcel_den if parcel_den else "",
                weighting="parcel",
                dimension=dimension,
                category=category,
                numerator=parcel_num,
                denominator=parcel_den,
                unit="proportion",
            )
        )
        unit_num = sum(float(row["property_units"]) for row in snapshots if predicate(row))
        unit_den = sum(float(row["property_units"]) for row in snapshots)
        output.append(
            qa_row(
                "linkage_qa",
                tax_year,
                metric,
                unit_num / unit_den if unit_den else "",
                weighting="ews_units",
                dimension=dimension,
                category=category,
                numerator=unit_num,
                denominator=unit_den,
                unit="proportion",
            )
        )
    return output


def build_qa_rows(
    source_qa_by_year: Mapping[int, Mapping[str, Any]],
    snapshots_by_year: Mapping[int, Sequence[Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for year in sorted(source_qa_by_year):
        source = source_qa_by_year[year]
        for metric, value in sorted(source["metrics"].items()):
            output.append(qa_row("source_qa", year, metric, value))
        for category, value in sorted(source["rows_by_year_supplement"].items()):
            output.append(
                qa_row(
                    "source_distribution",
                    year,
                    "rows_by_tax_year_and_supplement",
                    value,
                    dimension="tax_year|supplement",
                    category=category,
                )
            )
        for category, value in sorted(source.get("rows_by_property_type", {}).items()):
            output.append(
                qa_row(
                    "source_distribution",
                    year,
                    "rows_by_property_type",
                    value,
                    dimension="property_type_code",
                    category=category,
                )
            )
        for metric, value in sorted(source.get("component_extracts", {}).items()):
            output.append(qa_row("source_component_qa", year, metric, value))
        for metric, value in sorted(source.get("evidence_availability", {}).items()):
            output.append(
                qa_row(
                    "source_evidence_note",
                    year,
                    metric,
                    "",
                    unit="text",
                    note=str(value),
                )
            )

    for year in sorted(snapshots_by_year):
        snapshots = list(snapshots_by_year[year])
        output.extend(rate_qa_rows(year, snapshots))
        for category in sorted({str(row["residential_use_category"]) for row in snapshots}):
            subset = [row for row in snapshots if str(row["residential_use_category"]) == category]
            output.extend(
                rate_qa_rows(
                    year,
                    subset,
                    dimension="residential_use_category",
                    category=category,
                )
            )
        status_counts = Counter(str(row["classification_status"]) for row in snapshots)
        for status, count in sorted(status_counts.items()):
            output.append(
                qa_row(
                    "classification_status",
                    year,
                    "parcel_count",
                    count,
                    dimension="classification_status",
                    category=status,
                )
            )
        unmatched = status_counts.get("source_parcel_not_found", 0)
        ambiguous = status_counts.get("matched_ambiguous", 0)
        output.append(qa_row("linkage_qa", year, "unmatched_parcels", unmatched))
        output.append(qa_row("linkage_qa", year, "ambiguous_parcels", ambiguous))
        corporate_rows = [row for row in snapshots if snapshot_bool(row, "is_corporate_owned") is True]
        output.append(
            qa_row("classification_total", year, "corporate_owned_parcels", len(corporate_rows))
        )
        output.append(
            qa_row(
                "classification_total",
                year,
                "corporate_owned_ews_units",
                sum(float(row["property_units"]) for row in corporate_rows),
                weighting="ews_units",
                unit="ews_units",
            )
        )
        if year == 2024:
            for metric in (
                "py_jan1_owner_id",
                "py_appr_owner_id",
                "jan1_appr_owner_id",
                "py_jan1_owner_name",
                "py_appr_owner_name",
                "jan1_appr_owner_name",
                "py_jan1_owner_address",
                "py_appr_owner_address",
                "jan1_appr_owner_address",
            ):
                comparable = source_qa_by_year[year]["metrics"].get(f"{metric}_comparable_rows", 0)
                agree = source_qa_by_year[year]["metrics"].get(f"{metric}_agree_rows", 0)
                output.append(
                    qa_row(
                        "owner_concept_agreement",
                        year,
                        metric,
                        agree / comparable if comparable else "",
                        weighting="source_row",
                        numerator=agree,
                        denominator=comparable,
                        unit="proportion",
                    )
                )
    return output


PARITY_FIELDS = [
    "record_type",
    "source_county",
    "tax_year",
    "flag",
    "weighting",
    "reference_value",
    "shared_value",
    "numerator",
    "denominator",
    "value",
    "unit",
    "note",
]


def label_bool(value: bool | None) -> str:
    return "NA" if value is None else ("TRUE" if value else "FALSE")


def parity_explanation_code(
    snapshot: Mapping[str, Any],
    shared_field: str,
    shared_value: bool | None,
) -> str:
    """Return a categorical, inspectable reason for an intentional correction."""

    homestead = snapshot_bool(snapshot, "homestead_positive") is True
    imputed_state_match = (
        snapshot_bool(snapshot, "situs_state_imputed_address_match") is True
    )
    address_match = snapshot_bool(snapshot, "address_match_positive") is True

    if shared_value is None:
        if shared_field == "has_financialized_owner":
            return "missing_usable_owner_name_now_NA"
        return "missing_usable_owner_mailing_address_now_NA"
    if shared_field == "is_owner_occupied":
        if homestead:
            return "recovered_property_profile_homestead"
        if imputed_state_match:
            return "imputed_travis_situs_state_address_match"
        if address_match:
            return "word_bounded_or_free_form_address_match"
        return "shared_owner_occupancy_evidence_semantics"
    if shared_field == "has_financialized_owner":
        return "shared_pinned_name_normalization_or_marker_rule"
    if homestead:
        return "corporate_rederived_after_property_profile_homestead"
    if imputed_state_match:
        return "corporate_rederived_after_imputed_travis_situs_state_match"
    if address_match:
        return "corporate_rederived_after_shared_address_match"
    return "corporate_rederived_from_shared_parcel_flags"


DOCUMENTED_PARITY_CODES = frozenset(
    {
        "recovered_property_profile_homestead",
        "imputed_travis_situs_state_address_match",
        "word_bounded_or_free_form_address_match",
        "missing_usable_owner_mailing_address_now_NA",
        "missing_usable_owner_name_now_NA",
        "corporate_rederived_after_property_profile_homestead",
        "corporate_rederived_after_imputed_travis_situs_state_match",
        "corporate_rederived_after_shared_address_match",
    }
)


def build_parity_rows(
    snapshots_2025: Sequence[Mapping[str, Any]],
    ews_rows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ews_by_pid = {str(row["parcel_id"]): row for row in ews_rows}
    mapping = {
        "is_owner_occupied": "ews_is_owner_occupied",
        "has_financialized_owner": "ews_has_financialized_owner",
        "is_corporate_owned": "ews_is_corporate_owned",
    }
    parity: list[dict[str, Any]] = []
    discrepancies_by_pid: dict[str, dict[str, Any]] = {}

    for shared_field, ews_field in mapping.items():
        comparisons: list[tuple[Mapping[str, Any], bool | None, bool | None]] = []
        for snapshot in snapshots_2025:
            reference = ews_by_pid[str(snapshot["parcel_id"])][ews_field]
            shared = snapshot_bool(snapshot, shared_field)
            comparisons.append((snapshot, reference, shared))
            if reference != shared:
                pid = str(snapshot["parcel_id"])
                item = discrepancies_by_pid.setdefault(
                    pid,
                    {
                        "source_county": "Travis",
                        "tax_year": 2025,
                        "parcel_id": pid,
                        "property_units": snapshot["property_units"],
                        "residential_use_category": snapshot["residential_use_category"],
                        "classification_status": snapshot["classification_status"],
                        "owner_names": snapshot["owner_names"],
                        "homestead_positive": snapshot["homestead_positive"],
                        "address_match_positive": snapshot["address_match_positive"],
                        "situs_state_imputed_address_match": snapshot[
                            "situs_state_imputed_address_match"
                        ],
                        "differing_flags": [],
                        "explanation_codes": [],
                    },
                )
                item["differing_flags"].append(shared_field)
                item[f"ews_{shared_field}"] = label_bool(reference)
                item[f"shared_{shared_field}"] = label_bool(shared)
                reason = parity_explanation_code(snapshot, shared_field, shared)
                if reason not in item["explanation_codes"]:
                    item["explanation_codes"].append(reason)

        for weighting in ("parcel", "ews_units"):
            weight = (lambda row: 1.0) if weighting == "parcel" else (
                lambda row: float(row["property_units"])
            )
            denominator = sum(weight(row) for row, _, _ in comparisons)
            numerator = sum(weight(row) for row, reference, shared in comparisons if reference == shared)
            parity.append(
                {
                    "record_type": "agreement",
                    "source_county": "Travis",
                    "tax_year": 2025,
                    "flag": shared_field,
                    "weighting": weighting,
                    "reference_value": "",
                    "shared_value": "",
                    "numerator": numerator,
                    "denominator": denominator,
                    "value": numerator / denominator if denominator else "",
                    "unit": "proportion",
                    "note": "reference is current EWS parcel flag",
                }
            )
            for reference_value in (False, True, None):
                for shared_value in (False, True, None):
                    cell = sum(
                        weight(row)
                        for row, reference, shared in comparisons
                        if reference is reference_value and shared is shared_value
                    )
                    parity.append(
                        {
                            "record_type": "confusion_matrix",
                            "source_county": "Travis",
                            "tax_year": 2025,
                            "flag": shared_field,
                            "weighting": weighting,
                            "reference_value": label_bool(reference_value),
                            "shared_value": label_bool(shared_value),
                            "numerator": "",
                            "denominator": "",
                            "value": cell,
                            "unit": "parcels" if weighting == "parcel" else "ews_units",
                            "note": "",
                        }
                    )

    discrepancy_fields = [
        "source_county",
        "tax_year",
        "parcel_id",
        "property_units",
        "residential_use_category",
        "classification_status",
        "owner_names",
        "homestead_positive",
        "address_match_positive",
        "situs_state_imputed_address_match",
        "differing_flags",
        "ews_is_owner_occupied",
        "shared_is_owner_occupied",
        "ews_has_financialized_owner",
        "shared_has_financialized_owner",
        "ews_is_corporate_owned",
        "shared_is_corporate_owned",
        "explanation_codes",
    ]
    discrepancies: list[dict[str, Any]] = []
    for pid in sorted(discrepancies_by_pid, key=natural_key):
        row = discrepancies_by_pid[pid]
        row["differing_flags"] = "; ".join(row["differing_flags"])
        row["explanation_codes"] = "; ".join(row["explanation_codes"])
        discrepancies.append({field: row.get(field, "") for field in discrepancy_fields})
    return parity, discrepancies


def build_review_rows(snapshots: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    review_statuses = {
        "source_parcel_not_found",
        "matched_ambiguous",
        "matched_owner_missing",
        "matched_owner_partial_missing",
        "matched_owner_suppressed",
        "matched_evidence_insufficient",
    }
    fields = [
        "source_county",
        "tax_year",
        "parcel_id",
        "property_units",
        "residential_use_category",
        "classification_status",
        "n_owner_rows",
        "owner_name_available",
        "owner_address_available",
        "is_owner_occupied",
        "has_financialized_owner",
        "is_corporate_owned",
        "classification_note",
    ]
    return [
        {field: row.get(field, "") for field in fields}
        for row in snapshots
        if row["classification_status"] in review_statuses
    ]


def git_output(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout.strip()


def output_summary(snapshots: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_year: dict[str, Any] = {}
    for year in sorted({int(row["tax_year"]) for row in snapshots}):
        rows = [row for row in snapshots if int(row["tax_year"]) == year]
        matched = [row for row in rows if row["classification_status"] != "source_parcel_not_found"]
        complete = [
            row
            for row in rows
            if all(
                snapshot_bool(row, field) is not None
                for field in ("is_owner_occupied", "has_financialized_owner", "is_corporate_owned")
            )
        ]
        corporate = [row for row in rows if snapshot_bool(row, "is_corporate_owned") is True]
        total_units = sum(float(row["property_units"]) for row in rows)
        by_year[str(year)] = {
            "target_parcels": len(rows),
            "target_ews_units": total_units,
            "matched_parcels": len(matched),
            "parcel_match_rate": len(matched) / len(rows) if rows else None,
            "unit_weighted_match_rate": (
                sum(float(row["property_units"]) for row in matched) / total_units if total_units else None
            ),
            "complete_classification_parcels": len(complete),
            "parcel_complete_classification_rate": len(complete) / len(rows) if rows else None,
            "unit_weighted_complete_classification_rate": (
                sum(float(row["property_units"]) for row in complete) / total_units
                if total_units
                else None
            ),
            "corporate_owned_parcels": len(corporate),
            "corporate_owned_ews_units": sum(float(row["property_units"]) for row in corporate),
            "classification_status_counts": dict(
                sorted(Counter(str(row["classification_status"]) for row in rows).items())
            ),
        }
    return by_year


def validate_pinned_hash(path: Path, expected: str, label: str) -> str:
    actual = file_sha256(path)
    if actual != expected:
        raise SourceValidationError(
            f"{label} SHA-256 mismatch: expected {expected}, received {actual}"
        )
    return actual


def build(args: argparse.Namespace) -> None:
    ews_path = args.ews_path.resolve()
    upstream_ews_path = args.upstream_ews_path.resolve()
    archive_2024 = args.archive_2024.resolve()
    layout_archive = args.layout_archive.resolve()
    archive_2025 = args.archive_2025.resolve()
    output_dir = args.output_dir.resolve()
    intermediate_2024 = args.intermediate_2024.resolve()
    intermediate_2025 = args.intermediate_2025.resolve()
    owners_2025 = args.owners_2025.resolve()
    situses_2025 = args.situses_2025.resolve()
    profile_2025 = args.profile_2025.resolve()
    characteristics_2025 = args.characteristics_2025.resolve()

    required = [
        ews_path,
        upstream_ews_path,
        archive_2024,
        layout_archive,
        archive_2025,
        owners_2025,
        situses_2025,
        profile_2025,
        characteristics_2025,
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise SourceValidationError("Missing required input(s): " + ", ".join(missing))

    print("Validating pinned source hashes...", flush=True)
    hash_2024 = validate_pinned_hash(archive_2024, EXPECTED_2024_ARCHIVE_SHA256, "2024 archive")
    layout_hash = validate_pinned_hash(layout_archive, EXPECTED_2024_LAYOUT_SHA256, "layout archive")
    hash_2025 = validate_pinned_hash(archive_2025, EXPECTED_2025_ARCHIVE_SHA256, "2025 archive")
    ews_hash = validate_pinned_hash(ews_path, EXPECTED_EWS_SHA256, "EWS surface")
    upstream_ews_hash = validate_pinned_hash(
        upstream_ews_path, EXPECTED_EWS_SHA256, "upstream EWS generator output"
    )
    cache_paths_2025 = {
        "owners": owners_2025,
        "situses": situses_2025,
        "property_profile": profile_2025,
        "property_characteristics": characteristics_2025,
    }
    cache_hashes_2025 = {
        name: validate_pinned_hash(path, EXPECTED_2025_CACHE_SHA256[name], f"2025 {name} cache")
        for name, path in cache_paths_2025.items()
    }

    members_2025 = zip_members(archive_2025)
    json_members = [item for item in members_2025 if item["name"].lower().endswith(".json")]
    if len(json_members) != 1 or "20250720" not in json_members[0]["name"]:
        raise SourceValidationError("Pinned 2025 archive does not contain the expected 20250720 JSON member")
    prefix_2025 = read_member_prefix_external(archive_2025, json_members[0]["name"])
    if not re.search(rb'"pYear"\s*:\s*2025\b', prefix_2025):
        raise SourceValidationError("Pinned 2025 JSON prefix does not identify property year 2025")

    ews_rows, ews_qa = load_ews_surface(ews_path)
    target_ids = {str(row["parcel_id"]) for row in ews_rows}
    print(f"Loaded {len(ews_rows):,} fixed EWS Travis parcels.", flush=True)

    print("Streaming and validating 2024 PROP.TXT...", flush=True)
    rows_2024, source_ids_2024, qa_2024 = scan_2024_source(
        archive_2024, intermediate_2024, target_ids
    )
    print(
        f"2024: {qa_2024['metrics']['raw_rows']:,} rows, "
        f"{qa_2024['metrics']['unique_property_ids']:,} properties.",
        flush=True,
    )

    print("Standardizing cached 2025 owner extracts...", flush=True)
    rows_2025, source_ids_2025, qa_2025 = scan_2025_source(
        owners_2025,
        situses_2025,
        profile_2025,
        characteristics_2025,
        intermediate_2025,
        target_ids,
    )
    print(
        f"2025: {qa_2025['metrics']['raw_rows']:,} rows, "
        f"{qa_2025['metrics']['unique_property_ids']:,} owner properties.",
        flush=True,
    )

    snapshots_2024 = build_snapshot_rows(
        tax_year=2024,
        ews_rows=ews_rows,
        rows_by_target=rows_2024,
        source_property_ids=source_ids_2024,
        source_snapshot_id=SOURCE_2024_SNAPSHOT_ID,
        source_owner_field="property_year_owner",
        source_supplement_number="0",
    )
    snapshots_2025 = build_snapshot_rows(
        tax_year=2025,
        ews_rows=ews_rows,
        rows_by_target=rows_2025,
        source_property_ids=source_ids_2025,
        source_snapshot_id=SOURCE_2025_SNAPSHOT_ID,
        source_owner_field="current_special_export_owner",
        source_supplement_number="1",
    )
    snapshots = snapshots_2024 + snapshots_2025
    snapshots.sort(key=lambda row: (int(row["tax_year"]), natural_key(str(row["parcel_id"]))))
    expected_snapshot_rows = len(ews_rows) * 2
    snapshot_keys = {(int(row["tax_year"]), str(row["parcel_id"])) for row in snapshots}
    if len(snapshots) != expected_snapshot_rows or len(snapshot_keys) != expected_snapshot_rows:
        raise SourceValidationError(
            "Snapshot cardinality failure: expected one row per target parcel and year "
            f"({expected_snapshot_rows:,}), received {len(snapshots):,} rows and "
            f"{len(snapshot_keys):,} unique keys"
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = output_dir / "travis_owner_snapshots_2024_2025.csv"
    qa_path = output_dir / "travis_owner_snapshot_qa.csv"
    parity_path = output_dir / "travis_owner_classifier_parity_2025.csv"
    parity_review_path = output_dir / "travis_owner_classifier_parity_2025_discrepancies.csv"
    review_path = output_dir / "travis_owner_snapshot_review.csv"
    manifest_path = output_dir / "travis_owner_snapshot_manifest.json"

    write_csv(snapshot_path, SNAPSHOT_FIELDS, snapshots)
    qa_rows = build_qa_rows({2024: qa_2024, 2025: qa_2025}, {2024: snapshots_2024, 2025: snapshots_2025})
    write_csv(qa_path, QA_FIELDS, qa_rows)
    parity_rows, parity_discrepancies = build_parity_rows(snapshots_2025, ews_rows)
    write_csv(parity_path, PARITY_FIELDS, parity_rows)
    parity_review_fields = [
        "source_county",
        "tax_year",
        "parcel_id",
        "property_units",
        "residential_use_category",
        "classification_status",
        "owner_names",
        "homestead_positive",
        "address_match_positive",
        "situs_state_imputed_address_match",
        "differing_flags",
        "ews_is_owner_occupied",
        "shared_is_owner_occupied",
        "ews_has_financialized_owner",
        "shared_has_financialized_owner",
        "ews_is_corporate_owned",
        "shared_is_corporate_owned",
        "explanation_codes",
    ]
    write_csv(parity_review_path, parity_review_fields, parity_discrepancies)
    review_rows = build_review_rows(snapshots)
    review_fields = [
        "source_county",
        "tax_year",
        "parcel_id",
        "property_units",
        "residential_use_category",
        "classification_status",
        "n_owner_rows",
        "owner_name_available",
        "owner_address_available",
        "is_owner_occupied",
        "has_financialized_owner",
        "is_corporate_owned",
        "classification_note",
    ]
    write_csv(review_path, review_fields, review_rows)

    summaries = output_summary(snapshots)
    category_coverage_below_95 = [
        {
            "tax_year": int(row["tax_year"]),
            "category": row["category"],
            "weighting": row["weighting"],
            "metric": row["metric"],
            "numerator": row["numerator"],
            "denominator": row["denominator"],
            "value": row["value"],
        }
        for row in qa_rows
        if row["dimension"] == "residential_use_category"
        and row["metric"] in {"source_match_rate", "complete_classification_coverage"}
        and isinstance(row["value"], (float, int))
        and row["value"] < 0.95
    ]
    low_coverage = {
        year: summary
        for year, summary in summaries.items()
        if (summary["parcel_match_rate"] or 0) < 0.95
        or (summary["unit_weighted_match_rate"] or 0) < 0.95
        or (summary["parcel_complete_classification_rate"] or 0) < 0.95
        or (summary["unit_weighted_complete_classification_rate"] or 0) < 0.95
    }
    parity_difference_parcels = len(parity_discrepancies)
    parity_explanation_counts = Counter(
        code
        for row in parity_discrepancies
        for code in str(row["explanation_codes"]).split("; ")
        if code
    )
    unexplained_parity_rows = [
        row
        for row in parity_discrepancies
        if not set(filter(None, str(row["explanation_codes"]).split("; "))).issubset(
            DOCUMENTED_PARITY_CODES
        )
    ]

    initial_status = (
        "## main...origin/main\n"
        " M hays-parcel-pull.R\n"
        " M standalone_corporate_parcels.R\n"
        " M williamson-parcel-pull.R\n"
        "?? data/\n"
        "?? output/\n"
        "?? standalone_travis_corporate_deed_history.R\n"
        "?? stream_links_only.R\n"
        "?? test_condoid_coordinate_recovery.R"
    )
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "pipeline": {
            "script": file_metadata(Path(__file__).resolve()),
            "tests": file_metadata(REPO_ROOT / "tests" / "test_historical_ownership.py"),
            "classification_rule_version": CLASSIFICATION_RULE_VERSION,
            "reproduction_command": "python3 historical_ownership.py build",
        },
        "repository_provenance_before_implementation": {
            "branch": "main",
            "head_commit": "2d8d44fbdca952a2514d7a62c9593131e178e499",
            "git_status_short_branch": initial_status,
            "standalone_classifier_file_sha256": "685c039dee0e5296ecebf0923968d95cba03329b3eaf0d2c75487e9d1ba71e89",
            "target_helper_functions_file_sha256": "b864640ddec1ea11c0812236d159a15e7a20b2130808a150c447e94d595db043",
            "standalone_classifier_worktree_diff_sha256": "869aa58c4f0ead4e236243562f3abc07679ce9d1ff38b2aa708d9911b0f895c0",
            "all_preexisting_tracked_changes_patch_sha256": "d1e2241d85c40b9450f74e9600f328b5ee445c5d27917fdcaeb069e02391c6c9",
            "uncommitted_hays_williamson_llp_diff_sha256": "80d4899fa3f2666184a56104a26e45b32163a7c6dfeeb7293ffe839ec9a3c3a1",
            "note": "Pre-existing user changes were preserved; no commit or push was performed.",
        },
        "sources": {
            "2024": {
                **file_metadata(archive_2024, include_sha256=False),
                "sha256": hash_2024,
                "canonical_tcad_url": TCAD_2024_CANONICAL_URL,
                "retrieval_url": TCAD_2024_RETRIEVAL_URL,
                "canonical_url_status_at_retrieval": "HTTP 404; exact archived capture used",
                "retrieved_at_utc": file_metadata(archive_2024, False)["modified_at_utc"],
                "internet_archive_capture_timestamp": "2025-09-05T13:11:48Z",
                "original_last_modified_utc": "2024-08-21T15:38:00Z",
                "capture_semantics": (
                    "TCAD certified appraisal export, supplement 0, rerun dated 2024-08-21; "
                    "property-year owner is the classification owner concept"
                ),
                "source_snapshot_id": SOURCE_2024_SNAPSHOT_ID,
                "zip_members": zip_members(archive_2024),
                "appraisal_header": qa_2024["header"],
                "source_qa": qa_2024,
                "layout": {
                    **file_metadata(layout_archive, include_sha256=False),
                    "sha256": layout_hash,
                    "url": TCAD_LAYOUT_URL,
                    "retrieved_at_utc": file_metadata(layout_archive, False)["modified_at_utc"],
                    "zip_members": zip_members(layout_archive),
                    "record_length_bytes_excluding_crlf": FWF_RECORD_LENGTH_2024,
                    "record_length_bytes_including_crlf": FWF_LINE_LENGTH_CRLF_2024,
                    "schema": {
                        name: {
                            "start_1_indexed": spec.start,
                            "end_1_indexed_inclusive": spec.end,
                            "length": spec.length,
                            "description": spec.description,
                        }
                        for name, spec in FWF_SCHEMA_2024.items()
                    },
                },
                "intermediate": file_metadata(intermediate_2024),
            },
            "2025": {
                **file_metadata(archive_2025, include_sha256=False),
                "sha256": hash_2025,
                "canonical_tcad_url": TCAD_2025_CANONICAL_URL,
                "local_download_completed_at": file_metadata(archive_2025, False)["modified_at_utc"],
                "url_provenance": (
                    "Forensic evidence recorded from macOS com.apple.metadata:kMDItemWhereFroms "
                    "on the cached archive; not re-decoded by this build"
                ),
                "recorded_where_from_values": [
                    TCAD_2025_CANONICAL_URL,
                    "https://traviscad.org/publicinformation/",
                ],
                "local_download_started_at": "2026-04-21T11:48:47-05:00",
                "capture_semantics": (
                    "TCAD 2025 Special Export supplement 1 dated 2025-07-20; current special-export "
                    "owner records used by the existing EWS parcel surface"
                ),
                "source_snapshot_id": SOURCE_2025_SNAPSHOT_ID,
                "zip_members": members_2025,
                "raw_json_validation_scope": (
                    "ZIP member identity and a streamed prefix pYear=2025 check; cached rows do not "
                    "retain pYear or supplement for per-row validation"
                ),
                "source_qa": qa_2025,
                "cached_intermediates": {
                    name: {
                        **file_metadata(path, include_sha256=False),
                        "sha256": cache_hashes_2025[name],
                    }
                    for name, path in cache_paths_2025.items()
                },
                "standardized_intermediate": file_metadata(intermediate_2025),
            },
            "ews_surface": {
                **file_metadata(ews_path, include_sha256=False),
                "sha256": ews_hash,
                **ews_qa,
                "upstream_generator_output": {
                    **file_metadata(upstream_ews_path, include_sha256=False),
                    "sha256": upstream_ews_hash,
                    "byte_identical_by_size_and_sha256": (
                        upstream_ews_hash == ews_hash
                        and upstream_ews_path.stat().st_size == ews_path.stat().st_size
                    ),
                    "generator": "standalone_corporate_parcels.R",
                },
            },
        },
        "classification": {
            "owner_name_normalization": "shared normalize_owner_name implementation",
            "address_normalization": "shared normalize_address implementation",
            "marker_patterns": list(FINANCIAL_MARKER_PATTERNS),
            "python_marker_patterns_sha256": hashlib.sha256(
                "\n".join(FINANCIAL_MARKER_PATTERNS).encode("utf-8")
            ).hexdigest(),
            "upstream_r_marker_set_sha256": UPSTREAM_MARKER_SET_SHA256,
            "upstream_r_collapsed_regex_sha256": UPSTREAM_COLLAPSED_REGEX_SHA256,
            "owner_occupancy": (
                "TRUE if any owner row has homestead evidence or normalized mailing address matches "
                "normalized situs; FALSE only when all owner rows have sufficient negative evidence; "
                "otherwise NA"
            ),
            "financialized_owner": (
                "TRUE if any usable normalized owner name matches; FALSE only when every owner row has "
                "a usable nonmatching name; otherwise NA"
            ),
            "corporate_owner": (
                "fixed EWS residential parcel AND not owner occupied AND financialized owner, evaluated "
                "with three-valued logic"
            ),
            "multiple_owner_aggregation": (
                "commutative any-positive/all-negative/otherwise-unknown aggregation; unique owner IDs "
                "and names sorted naturally; source row order cannot change results"
            ),
            "intentional_defect_corrections": [
                "Use exact HS tokens from property_profile.csv because cached owner_exemptions is blank.",
                "Use retained free-form owner address lines when owner_addrFreeForm is true.",
                "Supply TX only for blank situs state on this Travis-only target surface and audit the imputation.",
                "Apply street and unit replacements at word boundaries to avoid mutating words such as BUSTER.",
                "Preserve the configured C/O name marker through punctuation normalization.",
                "Propagate missing required evidence as NA instead of treating it as a negative classification.",
            ],
            "classification_statuses": {
                "matched_classified": "all three classification flags are known",
                "matched_owner_missing": "source parcel exists but no usable owner name is available",
                "matched_owner_partial_missing": "some but not all owner rows have usable names",
                "matched_owner_suppressed": "confidentiality or address-suppression limits evidence",
                "matched_evidence_insufficient": "other required owner/address evidence is incomplete",
                "matched_ambiguous": "one owner key has conflicting normalized evidence",
                "source_parcel_not_found": "target parcel is absent from the pinned source snapshot",
            },
        },
        "summary": summaries,
        "acceptance_gates": {
            "coverage_below_95_percent": low_coverage,
            "category_diagnostics_below_95_percent": category_coverage_below_95,
            "coverage_gate_scope": (
                "The blocking 95% gate applies to overall parcel- and EWS-unit-weighted matching "
                "and complete classification. Subcategory exceptions are listed explicitly and "
                "their parcel IDs are retained in the review output."
            ),
            "parity_difference_parcels": parity_difference_parcels,
            "parity_explanation_counts": dict(sorted(parity_explanation_counts.items())),
            "parity_differences_all_have_explanation_codes": all(
                bool(row["explanation_codes"]) for row in parity_discrepancies
            ),
            "unexplained_parity_difference_parcels": len(unexplained_parity_rows),
            "zero_unexplained_parity_differences": not unexplained_parity_rows,
            "ready_for_ews_import": not low_coverage and not unexplained_parity_rows,
        },
        "limitations": [
            "The live 2024 TCAD URL returned HTTP 404 on 2026-09-07; the exact URL's archived 2025-09-05 capture was used.",
            "Legacy8.0.30 exposes partial_owner and prop_owner_sequence but no explicit property-year owner share.",
            "The cached 2025 owners.csv does not retain confidentiality or address-suppression flags and its owner_exemptions column is blank.",
            "The cached 2025 extracts omit row-level tax year and supplement; those values are attached from the pinned raw archive metadata.",
            "The original 2025 generator invocation was not logged; exact source identity and cache-to-EWS lineage are established by metadata, checksums, code paths, timestamps, and byte identity.",
            "The 2025 Special Export owner concept is not asserted to be ownership on 2025-04-01.",
            "Residential eligibility, parcel geography, coordinates, and EWS units are held fixed to the supplied EWS surface.",
        ],
        "outputs": {},
    }
    for path in (snapshot_path, qa_path, parity_path, parity_review_path, review_path):
        manifest["outputs"][path.name] = file_metadata(path)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(json.dumps({"summary": summaries, "parity_difference_parcels": parity_difference_parcels}, indent=2))
    if low_coverage:
        raise SourceValidationError(
            "Coverage below the 95% acceptance threshold; outputs and manifest were written for review: "
            + json.dumps(low_coverage, sort_keys=True)
        )
    if unexplained_parity_rows:
        raise SourceValidationError(
            "Unexplained 2025 classifier parity differences remain; outputs and manifest were written "
            f"for review ({len(unexplained_parity_rows):,} parcels)"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    build_parser = subparsers.add_parser("build", help="build snapshots and QA outputs")
    build_parser.add_argument("--ews-path", type=Path, default=DEFAULT_EWS_PATH)
    build_parser.add_argument(
        "--upstream-ews-path", type=Path, default=DEFAULT_UPSTREAM_EWS_PATH
    )
    build_parser.add_argument("--archive-2024", type=Path, default=DEFAULT_2024_ARCHIVE)
    build_parser.add_argument("--layout-archive", type=Path, default=DEFAULT_LAYOUT_ARCHIVE)
    build_parser.add_argument("--archive-2025", type=Path, default=DEFAULT_2025_ARCHIVE)
    build_parser.add_argument("--owners-2025", type=Path, default=REPO_ROOT / "output" / "owners.csv")
    build_parser.add_argument("--situses-2025", type=Path, default=REPO_ROOT / "output" / "situses.csv")
    build_parser.add_argument(
        "--profile-2025", type=Path, default=REPO_ROOT / "output" / "property_profile.csv"
    )
    build_parser.add_argument(
        "--characteristics-2025",
        type=Path,
        default=REPO_ROOT / "output" / "property_characteristics.csv",
    )
    build_parser.add_argument("--intermediate-2024", type=Path, default=DEFAULT_2024_INTERMEDIATE)
    build_parser.add_argument("--intermediate-2025", type=Path, default=DEFAULT_2025_INTERMEDIATE)
    build_parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    build_parser.set_defaults(func=build)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
    except SourceValidationError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
