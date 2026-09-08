import itertools
import tempfile
import unittest
from pathlib import Path

import historical_ownership as ho


def synthetic_fixed_width(**values: str) -> bytes:
    record = bytearray(b" " * ho.FWF_RECORD_LENGTH_2024)
    defaults = {
        "prop_id": "000000100012",
        "prop_type_cd": "R",
        "prop_val_yr": "02024",
        "sup_num": "000000000000",
        "py_owner_id": "000001518342",
        "py_owner_name": "1219 SOUTH LAMAR VENTURE LLC",
        "py_addr_line1": "1219 S LAMAR BLVD",
        "py_addr_city": "AUSTIN",
        "py_addr_state": "TX",
        "py_addr_zip": "78704",
        "py_confidential_flag": "F",
        "py_address_suppress_flag": "F",
        "situs_num": "1219",
        "situs_street_prefx": "S",
        "situs_street": "LAMAR",
        "situs_street_suffix": "BLVD",
        "situs_city": "AUSTIN",
        "situs_zip": "78704",
        "jan1_owner_id": "000001518342",
        "jan1_owner_name": "1219 SOUTH LAMAR VENTURE LLC",
        "jan1_addr_line1": "1219 S LAMAR BLVD",
        "jan1_addr_city": "AUSTIN",
        "jan1_addr_state": "TX",
        "jan1_addr_zip": "78704",
        "jan1_confidential_flag": "F",
        "jan1_address_suppress_flag": "F",
        "hs_exempt": "F",
        "imprv_state_cd": "B1",
        "land_state_cd": "B1",
        "prop_owner_sequence": "100012-1",
        "appr_owner_id": "000001518342",
        "appr_owner_name": "1219 SOUTH LAMAR VENTURE LLC",
        "appr_addr_line1": "1219 S LAMAR BLVD",
        "appr_addr_city": "AUSTIN",
        "appr_addr_state": "TX",
        "appr_addr_zip": "78704",
        "appr_confidential_flag": "F",
        "appr_address_suppress_flag": "F",
    }
    defaults.update(values)
    for field, value in defaults.items():
        spec = ho.FWF_SCHEMA_2024[field]
        encoded = value.encode("ascii")
        if len(encoded) > spec.length:
            raise ValueError(f"{field} synthetic value is too long")
        record[spec.start - 1 : spec.end] = encoded.ljust(spec.length, b" ")
    return bytes(record) + b"\r\n"


def standard_owner(**updates):
    row = ho.blank_standard_row()
    row.update(
        {
            "tax_year": "2025",
            "parcel_id": "1",
            "owner_id": "10",
            "owner_name": "JANE SMITH",
            "owner_addr_line1": "100 MAIN STREET",
            "owner_addr_city": "AUSTIN",
            "owner_addr_state": "TX",
            "owner_addr_zip": "78701",
            "owner_confidential_flag": "F",
            "owner_address_suppressed_flag": "F",
            "homestead_flag": "F",
            "situs_number": "100",
            "situs_street": "MAIN",
            "situs_suffix": "STREET",
            "situs_city": "AUSTIN",
            "situs_state": "TX",
            "situs_zip": "78701",
        }
    )
    row.update(updates)
    return row


class FixedWidthTests(unittest.TestCase):
    def test_schema_offsets_match_official_layout(self):
        expected = {
            "prop_id": (1, 12),
            "prop_type_cd": (13, 17),
            "prop_val_yr": (18, 22),
            "sup_num": (23, 34),
            "sup_action": (35, 36),
            "py_owner_id": (597, 608),
            "py_owner_name": (609, 678),
            "partial_owner": (679, 679),
            "py_addr_line1": (694, 753),
            "py_addr_city": (874, 923),
            "py_confidential_flag": (990, 990),
            "py_address_suppress_flag": (991, 991),
            "situs_street_prefx": (1040, 1049),
            "situs_street": (1050, 1099),
            "situs_street_suffix": (1100, 1109),
            "situs_city": (1110, 1139),
            "situs_zip": (1140, 1149),
            "jan1_owner_id": (2191, 2202),
            "jan1_owner_name": (2203, 2272),
            "jan1_addr_line1": (2273, 2332),
            "jan1_confidential_flag": (2569, 2569),
            "jan1_address_suppress_flag": (2570, 2570),
            "hs_exempt": (2609, 2609),
            "imprv_state_cd": (2732, 2741),
            "land_state_cd": (2742, 2751),
            "prop_owner_sequence": (4051, 4090),
            "situs_num": (4460, 4474),
            "situs_unit": (4475, 4479),
            "appr_owner_id": (4480, 4491),
            "appr_owner_name": (4492, 4561),
            "appr_addr_line1": (4562, 4621),
            "appr_confidential_flag": (4859, 4859),
            "appr_address_suppress_flag": (4860, 4860),
        }
        self.assertEqual(ho.FWF_RECORD_LENGTH_2024, 9247)
        for field, (start, end) in expected.items():
            with self.subTest(field=field):
                self.assertEqual((ho.FWF_SCHEMA_2024[field].start, ho.FWF_SCHEMA_2024[field].end), (start, end))

    def test_synthetic_line_extracts_documented_offsets(self):
        parsed = ho.parse_2024_fixed_width_line(synthetic_fixed_width())
        self.assertEqual(parsed["prop_id"], "000000100012")
        self.assertEqual(parsed["prop_val_yr"], "02024")
        self.assertEqual(parsed["sup_num"], "000000000000")
        self.assertEqual(parsed["py_owner_name"], "1219 SOUTH LAMAR VENTURE LLC")
        self.assertEqual(parsed["jan1_owner_id"], "000001518342")
        self.assertEqual(parsed["appr_owner_name"], "1219 SOUTH LAMAR VENTURE LLC")
        self.assertEqual(parsed["situs_num"], "1219")
        self.assertEqual(parsed["hs_exempt"], "F")
        self.assertEqual(parsed["imprv_state_cd"], "B1")
        self.assertEqual(parsed["prop_owner_sequence"], "100012-1")
        standard = ho.fixed_2024_to_standard(parsed)
        self.assertEqual(standard["parcel_id"], "100012")
        self.assertEqual(standard["source_owner_field"], "property_year_owner")
        self.assertEqual(standard["owner_share"], "")

    def test_unexpected_length_is_rejected(self):
        with self.assertRaises(ho.RecordLengthError):
            ho.parse_2024_fixed_width_line(synthetic_fixed_width()[:-3])

    def test_unexpected_year_is_rejected(self):
        with self.assertRaises(ho.UnexpectedYearError):
            ho.parse_2024_fixed_width_line(synthetic_fixed_width(prop_val_yr="02023"))

    def test_unexpected_supplement_is_rejected(self):
        with self.assertRaises(ho.UnexpectedSupplementError):
            ho.parse_2024_fixed_width_line(synthetic_fixed_width(sup_num="000000000001"))


class NormalizationAndMarkerTests(unittest.TestCase):
    def test_owner_name_normalization(self):
        self.assertEqual(ho.normalize_owner_name(" acme, l.l.c.-series a "), "ACME LLC SERIES A")
        self.assertEqual(ho.normalize_owner_name("NA"), "")

    def test_address_normalization_and_word_boundaries(self):
        self.assertEqual(
            ho.normalize_address("11605 Buster Crabbe Drive, Unit 2"),
            "11605 BUSTER CRABBE DR 2",
        )
        self.assertEqual(
            ho.normalize_address("100 South Main Street Austin TX 78701"),
            "100 S MAIN ST AUSTIN TX 78701",
        )

    def test_every_formal_entity_variant(self):
        groups = {
            "LTD": ("LTD", "L T D", "L.T.D."),
            "LLC": ("LLC", "L L C", "L.L.C."),
            "LLP": ("LLP", "L L P", "L.L.P."),
            "LP": ("LP", "L P", "L.P."),
            "LLLP": ("LLLP", "L L L P", "L.L.L.P."),
            "INC": ("INC", "I N C", "I.N.C."),
            "LC": ("LC", "L C", "L.C."),
        }
        for label, variants in groups.items():
            for variant in variants:
                with self.subTest(label=label, variant=variant):
                    self.assertTrue(ho.name_is_financialized(f"ACME {variant}"))

    def test_supplemental_marker_families_and_digit(self):
        samples = (
            "MORTGAGE FUND",
            "RENT TRUST",
            "MARKET GROUP",
            "INVESTMENT FUND",
            "PROP PARTNERS",
            "MANAGEMENT GROUP",
            "MGT GROUP",
            "MGMT GROUP",
            "ASSET FUND",
            "JOINT GROUP",
            "VENTURE GROUP",
            "VNT GROUP",
            "LIMITED GROUP",
            "PARTNERS",
            "PRTN GROUP",
            "BANK",
            "ASSOCIATES",
            "EQUITY FUND",
            "REALTY GROUP",
            "OWNER GROUP",
            "HOLDINGS",
            "DEVELOPMENT GROUP",
            "COMP GROUP",
            "CORP GROUP",
            "AQUISITION GROUP",
            "CONDO GROUP",
            "123 MAIN TRUST",
            "BORROWER FUND",
            "FOUNDATION",
            "ACME C/O JOHN DOE",
        )
        for sample in samples:
            with self.subTest(sample=sample):
                self.assertTrue(ho.name_is_financialized(sample))

    def test_boundary_and_missing_negatives(self):
        for name in (
            "JANE SMITH",
            "BRENT SMITH",
            "VINCENT WELCH",
            "PHILLIP COMPAIN",
            "L & P FAMILY TRUST",
        ):
            with self.subTest(name=name):
                self.assertFalse(ho.name_is_financialized(name))
        self.assertIsNone(ho.name_is_financialized(None))

    def test_exact_homestead_token(self):
        self.assertTrue(ho.exemption_list_has_homestead("OV65,HS,DP"))
        self.assertTrue(ho.exemption_list_has_homestead("hs"))
        self.assertFalse(ho.exemption_list_has_homestead("CHS,OV65"))
        self.assertFalse(ho.exemption_list_has_homestead(""))


class ClassificationTests(unittest.TestCase):
    def test_address_match_and_homestead(self):
        address_match = ho.classify_owner_row(standard_owner())
        self.assertTrue(address_match["owner_occupied"])
        hs = ho.classify_owner_row(
            standard_owner(
                owner_addr_line1="900 ELSEWHERE ROAD",
                homestead_flag="TRUE",
            )
        )
        self.assertTrue(hs["owner_occupied"])

    def test_missing_delivery_address_is_unknown(self):
        evidence = ho.classify_owner_row(
            standard_owner(
                owner_addr_line1="",
                owner_addr_line2="",
                owner_addr_line3="",
                owner_addr_city="AUSTIN",
                owner_addr_state="TX",
                owner_addr_zip="78701",
            )
        )
        self.assertFalse(evidence["address_available"])
        self.assertIsNone(evidence["owner_occupied"])

    def test_confidential_and_suppressed_evidence_produces_na(self):
        aggregate = ho.aggregate_owner_rows(
            [
                standard_owner(
                    owner_name="ACME LLC",
                    owner_confidential_flag="T",
                    owner_address_suppressed_flag="T",
                    owner_addr_line1="900 ELSEWHERE ROAD",
                )
            ]
        )
        self.assertIsNone(aggregate["has_financialized_owner"])
        self.assertIsNone(aggregate["is_owner_occupied"])
        self.assertIsNone(aggregate["is_corporate_owned"])
        self.assertEqual(aggregate["classification_status"], "matched_owner_suppressed")
        self.assertEqual(aggregate["owner_names"], "")

    def test_confidentiality_and_address_suppression_are_independent(self):
        confidential = ho.aggregate_owner_rows(
            [
                standard_owner(
                    owner_name="ACME LLC",
                    owner_confidential_flag="T",
                    owner_addr_line1="900 ELSEWHERE ROAD",
                )
            ]
        )
        self.assertIsNone(confidential["has_financialized_owner"])
        self.assertFalse(confidential["is_owner_occupied"])
        self.assertIsNone(confidential["is_corporate_owned"])
        self.assertEqual(confidential["classification_status"], "matched_owner_suppressed")

        suppressed = ho.aggregate_owner_rows(
            [
                standard_owner(
                    owner_name="ACME LLC",
                    owner_address_suppressed_flag="T",
                    owner_addr_line1="900 ELSEWHERE ROAD",
                )
            ]
        )
        self.assertTrue(suppressed["has_financialized_owner"])
        self.assertIsNone(suppressed["is_owner_occupied"])
        self.assertIsNone(suppressed["is_corporate_owned"])
        self.assertEqual(suppressed["classification_status"], "matched_owner_suppressed")

    def test_nonresident_financialized_owner_is_corporate(self):
        aggregate = ho.aggregate_owner_rows(
            [standard_owner(owner_name="ACME LLP", owner_addr_line1="900 ELSEWHERE ROAD")]
        )
        self.assertFalse(aggregate["is_owner_occupied"])
        self.assertTrue(aggregate["has_financialized_owner"])
        self.assertTrue(aggregate["is_corporate_owned"])
        self.assertEqual(aggregate["classification_status"], "matched_classified")

    def test_missing_and_insufficient_owner_evidence_are_unknown(self):
        missing = ho.aggregate_owner_rows(
            [
                standard_owner(
                    owner_name="",
                    owner_addr_line1="",
                    owner_addr_line2="",
                    owner_addr_line3="",
                    homestead_flag="",
                )
            ]
        )
        self.assertIsNone(missing["is_owner_occupied"])
        self.assertIsNone(missing["has_financialized_owner"])
        self.assertIsNone(missing["is_corporate_owned"])
        self.assertEqual(missing["classification_status"], "matched_owner_missing")

        insufficient = ho.aggregate_owner_rows(
            [
                standard_owner(
                    owner_name="ACME LLC",
                    owner_addr_line1="",
                    owner_addr_line2="",
                    owner_addr_line3="",
                    homestead_flag="",
                )
            ]
        )
        self.assertIsNone(insufficient["is_owner_occupied"])
        self.assertTrue(insufficient["has_financialized_owner"])
        self.assertIsNone(insufficient["is_corporate_owned"])
        self.assertEqual(insufficient["classification_status"], "matched_evidence_insufficient")

    def test_partial_and_conflicting_owner_evidence_are_ambiguous(self):
        partial = ho.aggregate_owner_rows(
            [standard_owner(owner_name="ACME LLC", source_partial_owner_flag="T")]
        )
        self.assertEqual(partial["classification_status"], "matched_ambiguous")
        self.assertIsNone(partial["is_corporate_owned"])

        conflicting = ho.aggregate_owner_rows(
            [
                standard_owner(owner_id="10", owner_name="JANE SMITH"),
                standard_owner(
                    owner_id="10",
                    owner_name="ACME LLC",
                    owner_addr_line1="900 ELSEWHERE ROAD",
                ),
            ]
        )
        self.assertEqual(conflicting["classification_status"], "matched_ambiguous")
        self.assertIsNone(conflicting["is_owner_occupied"])
        self.assertIsNone(conflicting["has_financialized_owner"])
        self.assertIsNone(conflicting["is_corporate_owned"])

    def test_partial_missing_multi_owner_evidence_is_unknown(self):
        aggregate = ho.aggregate_owner_rows(
            [
                standard_owner(
                    owner_id="1", owner_name="JANE SMITH", owner_addr_line1="900 ELSEWHERE ROAD"
                ),
                standard_owner(
                    owner_id="2",
                    owner_name="",
                    owner_addr_line1="901 ELSEWHERE ROAD",
                ),
            ]
        )
        self.assertEqual(aggregate["classification_status"], "matched_owner_partial_missing")
        self.assertFalse(aggregate["is_owner_occupied"])
        self.assertIsNone(aggregate["has_financialized_owner"])
        self.assertIsNone(aggregate["is_corporate_owned"])

    def test_multiple_owner_aggregation_applies_parcel_definition(self):
        resident = standard_owner(owner_id="1", owner_name="JANE SMITH")
        corporate = standard_owner(
            owner_id="2",
            owner_name="ACME LLC",
            owner_addr_line1="900 ELSEWHERE ROAD",
        )
        aggregate = ho.aggregate_owner_rows([resident, corporate])
        self.assertTrue(aggregate["is_owner_occupied"])
        self.assertTrue(aggregate["has_financialized_owner"])
        self.assertFalse(aggregate["is_corporate_owned"])
        self.assertEqual(aggregate["classification_status"], "matched_classified")

    def test_all_natural_nonresident_owners_are_noncorporate(self):
        rows = [
            standard_owner(owner_id="2", owner_name="JANE SMITH", owner_addr_line1="900 OAK ROAD"),
            standard_owner(owner_id="10", owner_name="JOHN SMITH", owner_addr_line1="901 OAK ROAD"),
        ]
        aggregate = ho.aggregate_owner_rows(rows)
        self.assertFalse(aggregate["is_owner_occupied"])
        self.assertFalse(aggregate["has_financialized_owner"])
        self.assertFalse(aggregate["is_corporate_owned"])

    def test_order_invariant_rollups(self):
        rows = [
            standard_owner(owner_id="10", owner_name="ZETA LLC", owner_addr_line1="900 OAK ROAD"),
            standard_owner(owner_id="2", owner_name="ALPHA LP", owner_addr_line1="901 OAK ROAD"),
        ]
        expected = ho.aggregate_owner_rows(rows)
        for permutation in itertools.permutations(rows):
            self.assertEqual(ho.aggregate_owner_rows(list(permutation)), expected)
        self.assertEqual(expected["owner_ids"], "2; 10")
        self.assertEqual(expected["owner_names"], "ALPHA LP; ZETA LLC")

    def test_free_form_2025_address_is_retained(self):
        owner = {
            "owner_pID": "000000000001",
            "owner_ownerID": "10",
            "owner_name": "JANE SMITH",
            "owner_ownerPct": "100",
            "owner_addrFreeForm": "1",
            "owner_addrFreeForm1": "PO BOX 10",
            "owner_addrFreeForm2": "AUSTIN TX 78701",
            "owner_addrFreeForm3": "",
        }
        standard = ho.owner_2025_to_standard(owner, {}, {"homestead_flag": "FALSE"})
        self.assertEqual(standard["owner_addr_line1"], "PO BOX 10")
        self.assertTrue(ho.classify_owner_row(standard)["address_available"])

    def test_cross_vintage_equivalent_evidence_classifies_identically(self):
        parsed_2024 = ho.fixed_2024_to_standard(ho.parse_2024_fixed_width_line(synthetic_fixed_width()))
        owner_2025 = {
            "owner_pID": "100012",
            "owner_ownerID": "1518342",
            "owner_name": "1219 SOUTH LAMAR VENTURE LLC",
            "owner_ownerPct": "100",
            "owner_addrFreeForm": "0",
            "owner_addrDeliveryLine": "1219 S LAMAR BLVD",
            "owner_addrCity": "AUSTIN",
            "owner_addrState": "TX",
            "owner_addrZip": "78704",
        }
        situs_2025 = {
            "situs_streetNum": "1219",
            "situs_streetPrefix": "S",
            "situs_streetName": "LAMAR",
            "situs_streetSuffix": "BLVD",
            "situs_city": "AUSTIN",
            "situs_state": "TX",
            "situs_zip": "78704",
        }
        parsed_2025 = ho.owner_2025_to_standard(
            owner_2025, situs_2025, {"homestead_flag": "FALSE"}
        )
        fields = ("owner_occupied", "financialized")
        evidence_2024 = ho.classify_owner_row(parsed_2024)
        evidence_2025 = ho.classify_owner_row(parsed_2025)
        self.assertEqual(
            {field: evidence_2024[field] for field in fields},
            {field: evidence_2025[field] for field in fields},
        )

    def test_unmatched_target_remains_na(self):
        snapshots = ho.build_snapshot_rows(
            tax_year=2024,
            ews_rows=[
                {
                    "parcel_id": "1",
                    "property_units_numeric": 1.0,
                    "residential_use_category": "A1",
                }
            ],
            rows_by_target={},
            source_property_ids=set(),
            source_snapshot_id="test",
            source_owner_field="property_year_owner",
            source_supplement_number="0",
        )
        self.assertEqual(snapshots[0]["classification_status"], "source_parcel_not_found")
        self.assertEqual(snapshots[0]["is_owner_occupied"], "")
        self.assertEqual(snapshots[0]["has_financialized_owner"], "")
        self.assertEqual(snapshots[0]["is_corporate_owned"], "")


class DeterminismTests(unittest.TestCase):
    def test_snapshot_order_and_repeat_csv_bytes(self):
        ews = [
            {
                "parcel_id": "10",
                "property_units_numeric": 1.0,
                "residential_use_category": "A1",
                "situs_address": "100 MAIN ST AUSTIN TX 78701",
            },
            {
                "parcel_id": "2",
                "property_units_numeric": 2.0,
                "residential_use_category": "B2",
                "situs_address": "200 MAIN ST AUSTIN TX 78701",
            },
        ]
        by_pid = {
            "2": [standard_owner(parcel_id="2", situs_number="200")],
            "10": [standard_owner(parcel_id="10")],
        }
        first = ho.build_snapshot_rows(
            tax_year=2025,
            ews_rows=list(reversed(ews)),
            rows_by_target=by_pid,
            source_property_ids={"2", "10"},
            source_snapshot_id="test",
            source_owner_field="test_owner",
            source_supplement_number="1",
        )
        second = ho.build_snapshot_rows(
            tax_year=2025,
            ews_rows=ews,
            rows_by_target=by_pid,
            source_property_ids={"10", "2"},
            source_snapshot_id="test",
            source_owner_field="test_owner",
            source_supplement_number="1",
        )
        self.assertEqual(first, second)
        self.assertEqual([row["parcel_id"] for row in first], ["2", "10"])
        with tempfile.TemporaryDirectory() as temp:
            one = Path(temp) / "one.csv"
            two = Path(temp) / "two.csv"
            ho.write_csv(one, ho.SNAPSHOT_FIELDS, first)
            ho.write_csv(two, ho.SNAPSHOT_FIELDS, second)
            self.assertEqual(one.read_bytes(), two.read_bytes())

    def test_rule_version_is_stable_shape(self):
        self.assertEqual(ho.CLASSIFICATION_RULE_VERSION, "lm-historical-owner-v1-a8aadd8b18bb")
        self.assertEqual(
            ho.UPSTREAM_MARKER_SET_SHA256,
            "8c66bd2556bace9abf421e1a86d24523a26d8169b9668708e5acd6d5f673e99f",
        )


if __name__ == "__main__":
    unittest.main()
