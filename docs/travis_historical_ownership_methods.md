# Travis historical ownership snapshots: methods

## Purpose and scope

This workflow creates comparable Travis County corporate-ownership classifications for tax years 2024 and 2025 and joins each year to the same 223,381-parcel EWS residential surface. Parcel geography, coordinates, residential eligibility, use category, and `property_units` come from that fixed EWS surface; they are not rebuilt for either historical year. The output is therefore an ownership comparison over a fixed analytical surface, not a reconstruction of each year's parcel geography.

The source vintages are appraisal-export snapshots. In particular, the 2025 Special Export has no ownership-event effective date. Neither vintage is represented as ownership on April 1, 2025.

## Pinned inputs and provenance

The build validates the pinned 2024 archive, layout archive, 2025 archive, all four consumed 2025 cached extracts, the upstream EWS generator output, and the downstream EWS-surface SHA-256 values before processing. It also verifies that the two EWS copies have identical sizes and hashes. Full file metadata, ZIP member inventories, source QA, the classifier definition, repository provenance, and hashes for every generated output are recorded in `output/historical_ownership/travis_owner_snapshot_manifest.json`.

| Input | Snapshot semantics and local artifact | Bytes | SHA-256 |
|---|---|---:|---|
| 2024 TCAD certified appraisal export | Supplement 0 rerun dated 2024-08-21; `data/historical_ownership/2024/source/2024_Certified_Appraisal_Export_Supp_0_08212024_Rerun.zip`; snapshot ID `tcad-2024-certified-supp0-rerun-20240821` | 427,509,840 | `c35da69f2baa53e1c1005672d432c62701b83e2234a250a0a6ee8637fe001b29` |
| 2024 Legacy 8.0.30 layout | `data/historical_ownership/2024/source/Website_Legacy8.0.30-AppraisalExportLayout.zip` | 259,533 | `36da0d34bd325395a7b0fb45046a4849c340ae3860a274cc0792e1e43185993a` |
| 2025 TCAD Special Export | Supplement 1 dated 2025-07-20; `tcad_special_export.zip`; snapshot ID `tcad-2025-special-export-supp1-20250720` | 3,454,758,601 | `8b9865a63f1c9a23e6425469148a8a1b39575a1424170828f9035385bbbd9259` |
| Fixed EWS Travis parcel surface | `/Users/alexkarner/Repositories/coa-displacement-ews/data/residential_parcels_for_hex.csv`; byte-identical to upstream `output/residential_parcels_for_hex.csv` | 42,176,380 | `fb2ad8ee3c09ca5d5b578f2eef806d93b0bc5bb0e885edd626d8c04f7f37d299` |

The canonical 2024 URL is `https://traviscad.org/wp-content/largefiles/2024%20Certified%20Appraisal%20Export%20Supp%200_08212024_Rerun.zip`. It returned HTTP 404 when retrieved on 2026-09-07, so the build used the exact Internet Archive capture from `2025-09-05T13:11:48Z` at `https://web.archive.org/web/20250905131148id_/https://traviscad.org/wp-content/largefiles/2024%20Certified%20Appraisal%20Export%20Supp%200_08212024_Rerun.zip`. The durable download completed at `2026-09-07T23:21:04Z`. The official layout URL is `https://traviscad.org/wp-content/largefiles/Website_Legacy8.0.30-AppraisalExportLayout.zip`.

The canonical 2025 URL is `https://traviscad.org/wp-content/largefiles/2025%20Special%20export%20Supp%201%2007202025.zip`. Its identity was recovered from the cached archive's macOS download metadata. The archive contains one member, `Travis-protaxExport-20250720.json` (29,091,595,059 uncompressed bytes; CRC32 `6b6e0f8d`). The cached extracts that generated the EWS source parcel file are pinned as follows:

| 2025 cached extract | SHA-256 |
|---|---|
| `output/owners.csv` | `6b585922cc8a8b8964c6fd1ee47f284ade0750d41509c7ce40647af9078f4c71` |
| `output/situses.csv` | `225c5a23a5b69ca1c84685e59c25684b0609a98a61e0e599d75f1ecce800e8ab` |
| `output/property_profile.csv` | `a62c14cebc6c68f66092ee59f77843a644d0d9139d2ba8e57d54769e6bb1b8de` |
| `output/property_characteristics.csv` | `99fbc661a7ffd905988eeba56f496f8674d0d82770230dac13351e094352e084` |

The implementation began from branch `main` at commit `2d8d44fbdca952a2514d7a62c9593131e178e499`. Pre-existing user changes were preserved. Their hashes, including the intended LLP additions, are in the manifest.

## Source parsing and owner concepts

The 2024 archive uses Deflate64. `historical_ownership.py` streams only `PROP.TXT` through the system Info-ZIP `unzip` executable and never expands the roughly 4.46 GB member to disk. The documented schema requires 9,247 data bytes plus CRLF per record and rejects unexpected lengths, tax years, or supplement numbers. It retains the property ID, property-year owner, January 1 owner, current-appraisal owner, confidentiality and address-suppression flags, situs components, homestead flag, property-use fields, supplement metadata, and available owner sequence/partial-owner fields. The property-year owner is the 2024 classification owner concept; January 1 and current-appraisal fields are retained only for audit comparisons. Legacy 8.0.30 does not expose a property-year owner-share field.

For 2025, the workflow standardizes the pinned cached owner, situs, profile, and characteristic extracts rather than downloading a later rolling export. The classification owner concept is `current_special_export_owner`. Primary situs selection is deterministic. When `owner_addrFreeForm` is true, `owner_addrFreeForm1` through `owner_addrFreeForm3` supply the mailing address; otherwise the delivery-line fields are used.

Both paths write the same ignored row-level intermediate schema under `data/historical_ownership/<year>/intermediate/`. It includes owner identity and address fields, suppression/confidentiality evidence where available, homestead evidence, situs and use fields, source snapshot and supplement identifiers, and the 2024 alternative-owner audit fields. Raw mailing addresses remain only in these ignored intermediates.

## Shared classifier

Both vintages pass through one implementation. The rule identifier is derived from the source-standardization, shared normalization, marker, evidence, and aggregation functions; the latest completed run used `lm-historical-owner-v1-a8aadd8b18bb`. The manifest's whole-script hash additionally pins all surrounding pipeline code.

Owner names are uppercased and normalized using the bounded marker rules from `standalone_corporate_parcels.R`, including plain, spaced, and dotted `LLP` variants and the existing LLC-series handling. Missing or confidential names are unavailable rather than nonfinancialized. `has_financialized_owner` is true when any usable owner name matches, false only when all represented owner rows have usable nonmatching names, and otherwise unknown.

`is_owner_occupied` is true when any owner row has homestead evidence or when its normalized mailing address matches the normalized source situs address. The shared implementation deliberately corrects several defects in the earlier EWS flags:

- 2025 homestead evidence comes from exact `HS` tokens in `property_profile.csv:propertyProf_exemptions`; the cached owner-level exemption column is blank.
- TCAD free-form mailing-address fields are used when their mode flag is set.
- Missing Travis situs state is imputed to `TX` for both vintages. The 2024 layout has no situs-state field, and some 2025 situs rows omit it.
- Address abbreviations and unit-designator removal use word boundaries, avoiding legacy mutations such as applying `STE` inside `BUSTER`.
- The declared `C/O` name marker is protected through punctuation normalization; otherwise that configured rule could never match. No 2025 cached owner row contains `C/O`, so this repair creates no 2025 parity change.
- City/state/ZIP alone are not treated as a usable mailing address.

Evidence is aggregated at parcel level with commutative three-valued logic: any positive row yields true, all sufficient negative rows yield false, and incomplete evidence yields `NA`. Thus row ordering cannot change a parcel result. A parcel is corporate-owned only when the fixed EWS parcel is residential, owner occupancy is false, and a financialized owner is true. Owner-occupied parcels are not corporate-owned even when another co-owner name matches a marker. Missing evidence remains `NA` for the affected component; a derived corporate flag is false only when another known condition rules corporate ownership out.

Owner IDs and normalized names are deduplicated and naturally sorted. A singleton 2024 row carrying TCAD's `partial_owner=T` flag is treated as ambiguous because co-owner evidence may be incomplete; none of the three such source parcels occurs on the fixed EWS target surface. One output row is emitted for every EWS parcel and tax year. Classification statuses distinguish complete matches, insufficient or partial evidence, suppressed evidence, ambiguous owner keys, and source parcels not found.

## Reproduction, outputs, and acceptance

Run from the repository root with Python 3 and system Info-ZIP `unzip` available:

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
python3 historical_ownership.py build
```

The build validates the pinned archives, layout, four consumed 2025 caches, upstream EWS generator output, and downstream EWS copy; streams and validates 2024; standardizes 2025; classifies and joins both vintages; writes deterministic CSV ordering; and then evaluates the 95% parcel- and unit-weighted match/classification gates. It writes:

- `output/historical_ownership/travis_owner_snapshots_2024_2025.csv` — consumer file, exactly one row per EWS parcel and year;
- `output/historical_ownership/travis_owner_snapshot_qa.csv` — source, linkage, coverage, owner-concept, category, and classification QA;
- `output/historical_ownership/travis_owner_classifier_parity_2025.csv` — parcel- and unit-weighted agreement and confusion matrices;
- `output/historical_ownership/travis_owner_classifier_parity_2025_discrepancies.csv` — every parity difference with explanation codes;
- `output/historical_ownership/travis_owner_snapshot_review.csv` — unmatched, ambiguous, and insufficient-evidence review rows; and
- `output/historical_ownership/travis_owner_snapshot_manifest.json` — authoritative provenance, rules, summaries, limitations, and hashes.

The final completed run at `2026-09-08T00:33:54Z` reported:

| Metric | 2024 | 2025 |
|---|---:|---:|
| Source rows / unique property IDs | 481,874 / 481,796 | 486,936 / 486,859 |
| EWS parcels matched | 222,557 (99.6311%) | 223,381 (100%) |
| Complete classifications | 222,552 (99.6289%) | 223,370 (99.9951%) |
| Unit-weighted match rate | 99.3191% | 100% |
| Unit-weighted complete-classification rate | 99.3191% | 99.9999% |
| Corporate-owned parcels | 15,831 | 15,770 |
| Corporate-owned EWS units | 440,388.2721 | 431,986.9564 |

All four overall coverage gates—parcel- and unit-weighted matching and complete classification—exceeded 95%. Two very small current-use subgroups fall below 95% in 2024: M1 matches 35/38 parcels (92.11%; 90.33% by units), and O1 matches 13/15 (86.67%; 48.68% by units). Every missing subgroup parcel is explicitly retained as `source_parcel_not_found`; the mismatch arises because its current EWS ID is absent from the pinned 2024 roll, and no ownership or parcel-history event is inferred. The 2024 review set contains 824 source parcels not found and 5 matched parcels with insufficient evidence; the 2025 set contains 11 matched parcels with insufficient evidence.

The 2024 stream reported zero invalid record lengths, parsing failures, unexpected years/supplements, or duplicate property-owner keys. The 2025 cache reported zero parsing failures and duplicate property-owner keys. Record length is not applicable to those CSV caches, and their rows do not retain year, supplement, confidentiality, or address-suppression fields; 2025 year and supplement are assigned from the pinned archive metadata rather than misreported as row-level validations. The source contains 31 multi-owner parcels in 2024 and 30 in 2025.

The shared 2025 financialized-owner flag agrees with EWS on all 223,381 parcels. There are 38,287 unique parity-review parcels overall. Owner occupancy changes from false to true for 38,276 parcels: 37,944 have exact-token HS evidence recovered from the property-profile cache, and 332 gain an address match when blank situs state is explicitly supplied as Texas on this Travis-only surface. Another 11 owner-occupancy values become `NA` because a usable mailing delivery line is absent (9 false-to-NA and 2 true-to-NA). Consequently, 399 corporate flags change true-to-false through recovered homestead evidence, 13 change true-to-false through the imputed-state address match, and 1 changes true-to-NA for missing address evidence. These corporate categories overlap the occupancy differences. Every discrepancy has a categorical explanation code, and zero unexplained differences remain.

For downstream import, force `parcel_id` and `owner_ids` to character columns. Seven target parcels have multiple owner IDs represented as deterministic semicolon-delimited rollups, so type guessing from early numeric-looking rows can otherwise produce parse warnings.

## Limitations

- The official live 2024 URL was unavailable, so an exact archived capture was used and hash-pinned.
- The 2024 property-year, January 1, and current-appraisal owners can differ. The output classifies the property-year owner and reports agreement QA; it does not infer ownership events between snapshots.
- The 2025 cached owner extract does not retain confidentiality or address-suppression flags. Homestead evidence is recoverable from the profile extract, but suppression coverage cannot be audited for that vintage.
- The fixed current EWS parcel surface can contain IDs created, retired, split, or combined relative to 2024. A missing 2024 source ID remains `NA`, not noncorporate.
- The corporate/financialized signal is a name-marker heuristic, not a beneficial-ownership determination.
- Neither appraisal export establishes ownership on April 1, 2025, and this workflow makes no such claim.
