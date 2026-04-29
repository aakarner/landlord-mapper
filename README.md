# Austin Landlord Mapper

A data pipeline and interactive web application that identifies and maps residential landlords in Austin, TX—with a focus on corporate and "financialized" ownership—by integrating Travis County Appraisal District (TCAD) property records, Austin Open Data, Texas Comptroller business filings, and US Census socioeconomic data.

**Live app:** https://ontheseams.shinyapps.io/landlord_mapper_app/

**Additional project materials:** https://drive.google.com/drive/folders/1e2Ahq9sNNQ2K_Q-RuTrdkWzDL6FH_gAa?usp=sharing

---

## What this repository does

The pipeline answers the question: *who owns residential rental property in Austin, and how are those owners connected to one another?* It produces a geocoded, enriched dataset that can be explored through an interactive Shiny application allowing users to:

- Search for a property by address, owner name, corporate name, or ownership group ID
- See all properties held by a single owner or corporate family on a map
- Explore a network graph of landlord connections via shared ownership
- Download filtered tabular data for further analysis

---

## Repository structure

| File / Folder | Language | Purpose |
|---|---|---|
| `_targets.R` | R | Pipeline orchestration (via the [`targets`](https://books.ropensci.org/targets/) package). Defines every step as a reproducible target. Run `targets::tar_make()` to execute the full pipeline. |
| `TCAD_parse.py` | Python | Parses the large TCAD JSON export (a ZIP file with hundreds of thousands of records) into flat CSV files using streaming JSON parsing (`ijson`). |
| `target_helper_functions.R` | R | Core data-merging and property classification. Joins all CSV files into a single parcel dataset, derives owner-occupancy and financialization flags, and counts property units. |
| `scrape_helper_functions.R` | R | Downloads the TCAD special export from [traviscad.org](https://traviscad.org/publicinformation); scrapes Austin Open Data (code complaints) via headless Chrome + Selenium; queries the Texas Comptroller Franchise Tax API for corporate details. |
| `supplementary_scrape_helper_functions.R` | R | Downloads Census ACS data (used to build a Social Vulnerability Index), generates per-property owner "fingerprint" strings, computes pairwise cosine string-distance matrices, clusters properties into ownership groups, and geocodes parcel addresses via the Census geocoder. |
| `final_output_helper_functions.R` | R | Merges the geocoded parcel dataset with the Housing Hardship Index and SVI data to produce the final `owners_info_total` dataset consumed by the Shiny app. |
| `HHI Data 2024 United States.xlsx` | Data | Housing Hardship Index scores and ranks for US ZIP codes (2024). |
| `shinyApp/app.R` | R | Interactive Shiny dashboard for exploring the final dataset: property map (Leaflet), owner table (DT), and landlord network graph (networkD3). |

---

## Pipeline walkthrough

The pipeline is managed by the [`targets`](https://books.ropensci.org/targets/) R package, which tracks dependencies between steps and re-runs only what has changed.

### Step 1 – Download TCAD data (`tcad_data`)
`download_tcad_austin()` scrapes [traviscad.org/publicinformation](https://traviscad.org/publicinformation) for the latest "Special Export (JSON)" ZIP file and downloads it if it is new.

### Step 2 – Parse TCAD data (`tcad_parse`)
`TCAD_parseYear()` (Python, called via `reticulate`) streams through the TCAD JSON with `ijson` and writes five flat CSV files:

| Output file | Contents |
|---|---|
| `austin_propertyChar_data.csv` | Zoning codes per parcel |
| `austin_propertyProf_data.csv` | Improvement/land state codes, area, stories, year built |
| `austin_situs_data.csv` | Property street address components |
| `austin_owner_data.csv` | Owner name, mailing address, exemptions, ownership percentage |
| `austin_deeds_data.csv` | Deed history: buyer/seller names and dates |

### Step 3 – Merge and classify parcels (`austin_parcel_data_merged`)
`target_property_gen()` joins the five CSV files on parcel ID (`situs_pID`), then:

- **Filters** to residential parcels (state codes `A*`/`B*` or SF/MF zoning).
- **Estimates unit count** from improvement state codes and total floor area.
- **Flags owner-occupancy** by comparing the owner's mailing address to the situs address, or by checking for a homestead exemption (`HS`).
- **Flags financialization** by matching owner names against a list of corporate entity markers (LLC, LP, LTD, INC, etc.) and real-estate-sector keywords (INVEST, MANAGE, REALT, HOLDING, …).
- **Derives `is_target`**: non-owner-occupied residential parcels held by a financialized entity.
- **Derives `is_mom_and_pop`**: owner-occupied residential parcels held by a non-financialized entity.

### Step 4 – Append code-complaint counts (`austin_parcel_data_merged_code`)
`code_compl_merge()` downloads the Austin Code Complaint dataset from Austin Open Data via headless Selenium and counts complaints per parcel address (parallelised with `doFuture`).

### Step 5 – Scrape Texas Comptroller data (`austin_parcel_data_merged_owner`)
`owner_scrape_actual()` looks up each financialized owner's name in the Texas Comptroller Franchise Tax API, retrieving:
- Legal business name, Texas Taxpayer Number (TTN), mailing address
- State of formation, SOS registration status
- Registered agent name and address
- Officer / owner names and addresses

### Step 6 – Cluster properties into ownership groups (`situs_group_assignments`)
`situs_owner_string_gen()` builds a composite "fingerprint" string for each parcel that concatenates all known names and addresses (owner, corporation, registered agent, scraped officer names). `situs_owner_string_dist_matrix()` then computes a pairwise cosine string-distance matrix over these fingerprints for all target/large-building parcels. `situs_neighor_gen()` translates close distances plus exact-match links (shared owner name, mailing address, corporate name, registered agent) into connected ownership groups, iterating until transitive closure is reached.

### Step 7 – Geocode parcels (`owners_info_total`)
`parcel_geolocate()` sends unique situs addresses to the US Census geocoder (in batches of 10,000) and joins back latitude/longitude and census block/tract/ZCTA geography identifiers.

### Step 8 – Enrich with socioeconomic context (`owners_data_total_supp`)
`final_data_merge()` joins the parcel dataset with:
- **Housing Hardship Index** (`HHI Data 2024 United States.xlsx`) – overall score and rank by ZIP code.
- **Social Vulnerability Index** built from Census ACS 5-year estimates (via `censusapi`): poverty, education, unemployment, housing cost burden, insurance coverage, age, disability, minority population, housing type, vehicle access, and limited English proficiency, organised into four SVI themes following the CDC/ATSDR methodology.

### Step 9 – Shiny application (`shinyApp/app.R`)
Reads `owners_info_total.csv` and `owners_info_d3graph.rds` and provides three dashboard panels:

- **Property Search** – Leaflet map with cosine-matched address search; table with full parcel/owner/corporate metadata; CSV download.
- **Landlord Network Analysis** – Force-directed network graph (networkD3) showing properties connected by common ownership.
- **Tenant Stress** *(planned)* – Heatmap of rent burden and evictions.
- **Property Quality** *(planned)* – Heatmap of code violations, fines, and building age.

---

## Setup and usage

### Prerequisites

**R packages** (installed automatically by `renv` if a lockfile is present, otherwise install manually):

```r
install.packages(c(
  "targets", "tarchetypes", "crew",
  "tibble", "dplyr", "purrr", "readr", "lubridate",
  "rvest", "selenider", "selenium", "httr", "httr2",
  "reticulate", "stringdist", "tidygeocoder",
  "censusapi", "acs", "tidycensus",
  "forecast", "xgboost", "doFuture", "doRNG", "foreach", "future",
  "qs2", "readxl",
  # Shiny app
  "shiny", "shinydashboard", "leaflet", "DT", "plotly",
  "networkD3", "igraph", "sp", "stringi", "listviewer"
))
```

**Python packages** (used via `reticulate`):

```bash
pip install pandas numpy ijson
```

> `ijson` requires the `yajl2_c` backend. Install the `yajl` C library for your OS (e.g., `brew install yajl` on macOS or `apt install libyajl-dev` on Ubuntu).

**Other tools:**
- Google Chrome + ChromeDriver (for headless Selenium scraping)
- A [Census API key](https://api.census.gov/data/key_signup.html)

### Configuration

1. Open `_targets.R` and replace `YOUR OWN CENSUS API KEY GOES HERE` with your Census API key:
   ```r
   Sys.setenv(CENSUS_KEY = "your_key_here")
   ```
2. If Chrome downloads files to a non-default location, update `download_location` in `supplementary_scrape_helper_functions.R` → `austin_open_data_dl()`.

### Running the pipeline

```r
library(targets)
tar_make()        # run the full pipeline
tar_visnetwork()  # inspect the dependency graph
tar_read(owners_data_total_supp)  # inspect the final output
```

Intermediate files (CSV, RDS) are cached in the working directory and in the `_targets/` store. Only targets whose upstream inputs have changed will be re-run.

### Running the Shiny app locally

```r
shiny::runApp("shinyApp")
```

The app expects `owners_info_total.csv` and `owners_info_d3graph.rds` to be present inside `shinyApp/`. Copy or symlink these files from the pipeline output directory before launching.

---

## Key derived fields in the output dataset

| Field | Description |
|---|---|
| `is_residential` | Property has a residential improvement or zoning code |
| `is_owner_occupied` | Owner mailing address matches situs address, or homestead exemption present |
| `is_financialized` | Owner name contains a corporate entity marker or real-estate keyword |
| `is_target` | Non-owner-occupied + financialized + residential |
| `is_mom_and_pop` | Owner-occupied + non-financialized + residential |
| `property_units` | Estimated number of housing units (derived from state codes and floor area) |
| `is_owner_out_of_state` | Owner's mailing state differs from property state |
| `group_assign` | Numeric group ID linking properties to a common ownership cluster |
| `veneer_owner` | Shell entity name on the TCAD record |
| `corp_business_name` | Legal business name from Texas Comptroller |
| `corp_TTN` | Texas Taxpayer Number |
| `corp_registered_agent_name` | Registered agent name |
| `situs_lat` / `situs_long` | Geocoded coordinates (Census geocoder) |
| `HHI_score` / `HHI_rank` | Housing Hardship Index for the parcel's ZIP code |
| `rpl_themes` | Overall Social Vulnerability Index percentile rank |

---

## Standalone script: `standalone_corporate_parcels.R`

For users who want to produce a filtered dataset of likely corporate-owned parcels without running the full `targets` pipeline, `standalone_corporate_parcels.R` provides a self-contained alternative. It requires only R, `jq` (a command-line JSON processor), and the TCAD Special Export ZIP file.

### TCAD Special Export structure

The TCAD Special Export is a large JSON file (several GB uncompressed) distributed as a ZIP archive from [traviscad.org/publicinformation](https://traviscad.org/publicinformation). It contains a top-level JSON array where each element represents a single tax account (parcel). Each parcel object has:

- **Top-level scalar fields** — `pID` (parcel ID), `propType`, `inactive`, `geometry` (see below), and others.
- **Nested arrays** — `owners`, `situses`, `propertyProfile`, `propertyCharacteristics`, `deeds`, `taxingunits`, `valuations`, `sales`, `permits`, `appeals`, and more.

Because the file is too large to load into memory wholesale, the script uses a streaming approach: `jq` is invoked via shell pipelines to expand each nested section into newline-delimited JSON (NDJSON), which `jsonlite::stream_in()` then reads page-by-page (`PAGE_SIZE` rows at a time). Peak memory usage is roughly proportional to a single page, not the full file.

### Parcel geometry and coordinates

Each parcel object contains a `geometry` field holding a **JSON-encoded string** of the form `"[lat, lon]"` — for example, `"[30.2545186553, -97.7620645363]"`. This must be parsed with `jq`'s `fromjson` filter before the coordinate values can be extracted:

```jq
(try (.geometry | fromjson) catch null) as $g
| {
    pID: .pID,
    lat: (if ($g|type)=="array" then ($g[0] // null) else null end),
    lon: (if ($g|type)=="array" then ($g[1] // null) else null end)
  }
```

Many parcel records have `geometry = "[null, null]"`. These missing records are not simply bad rows: they are often condominium units, apartment-style accounts, multi-parcel ownership records, utility/common-area records, or other tax accounts where TCAD represents the taxable account separately from the physical parcel geometry. In other words, the JSON is closer to an appraisal-account export than a clean one-feature-per-polygon GIS layer.

The standalone script therefore uses a staged coordinate workflow. Each stage only fills records that are still missing coordinates, and the final output includes `coord_source` so approximate fills can be audited or filtered out later:

1. **JSON geometry**: parse top-level `geometry` into `lat` and `lon`.
2. **JSON `geoID` siblings**: propagate coordinates among JSON records sharing `propertyIdentification[0].geoID`.
3. **JSON `links.linkedPID`**: use TCAD-linked account relationships when a linked parcel already has coordinates.
4. **JSON repeated situs address**: when multiple JSON records share the same situs street number, street name, and ZIP, use the median known coordinate for that address.
5. **Parcel polygon exact ID fallback**: if `data/Parcel_poly.zip` exists, join `coords_df$geoID` to `Parcel_poly$PID_10`, compute a representative point with `st_point_on_surface()`, and fill from that polygon point.
6. **Address point exact fallback**: if `data/Addresses.zip` exists, join by exact street number, normalized street name, normalized street type, and ZIP. Blank/missing street suffixes are allowed to match each other.
7. **Unique missing-ZIP address fallback**: for remaining records with street number/name/type but no ZIP, fill only when that address key maps to exactly one address point in `Addresses.zip`.
8. **Address-point parcel-ID fallback**: use `Addresses.zip` parcel identifiers (`PID` / `PARCEL_ID`) only when they match JSON `geoID` and the address point also agrees with the TCAD situs street number and street name.
9. **Nearest address point fallback**: for remaining numeric addresses, use nearby address points on the same street, street type, and ZIP within `ADDRESS_NEAREST_MAX_DELTA` house numbers. If the missing address falls between two known points, the script uses the median of those two coordinates; otherwise it uses the nearest point.

The JSON-native stages are preferred because they do not require any external GIS file. The parcel polygon fallback is the next safest option because it uses an identifier match (`geoID` to `PID_10`). Address-point matching is looser, so the script moves from exact address matches to carefully constrained missing-ZIP and parcel-ID matches before using nearest-address interpolation. Nearest-address matching is explicitly approximate, but it is useful for records where the physical location is clear from neighboring address points even though TCAD did not publish a direct geometry for the account.

For example, the `geoID` sibling fill is:

```r
geoid_lookup <- coords_df |>
  filter(!is.na(lat), !is.na(lon), !is.na(geoID)) |>
  group_by(geoID) |>
  summarise(
    lat_fill = median(lat),
    lon_fill = median(lon),
    .groups = "drop"
  )

coords_df <- coords_df |>
  left_join(geoid_lookup, by = "geoID") |>
  mutate(
    lat = dplyr::coalesce(lat, lat_fill),
    lon = dplyr::coalesce(lon, lon_fill)
  ) |>
  select(-lat_fill, -lon_fill)
```

The median is used as a conservative representative point when several records match the same key. In most cases there is only one coordinate, so the median is identical to the source value. When there are several nearby points, the median is less sensitive to an outlier than a mean.

The script writes `output/coords_summary.csv` and `output/missing_coord_diagnostics.csv` during development runs. These files show how many records each stage recovered and why any remaining records could not be matched. Remaining unmatched records usually fall into one of two categories:

- They have no usable `geoID`, so neither JSON sibling matching nor parcel polygon matching can work.
- They have a `geoID`, but that ID is not present in `Parcel_poly$PID_10`, which suggests the record is an account/unit/common-area representation rather than a standalone polygon feature in the parcel layer.

Because some remaining records are residential and can be corporate-owned, they should not be dismissed as harmless junk by default. They can be excluded from spatial filtering only with the understanding that the mapped result may undercount some condo, multifamily, or account-level records.

### Address aliases and external geocoding audit trail

After the local TCAD/Travis County GIS coordinate recovery stages, a meaningful number of records can still have complete-looking situs addresses but no coordinates. The remaining clusters are not all the same kind of problem. Some are straightforward street-name convention mismatches between TCAD and `Addresses.zip`; others are true missing address-point coverage, missing ZIPs, new subdivision streets, or bad/ambiguous address strings.

To keep this auditable, the standalone workflow uses two additional data artifacts rather than hard-coding one-off fixes in the script:

| File | Purpose |
|---|---|
| `data/address_aliases.csv` | Manual, high-confidence street/address aliases used before address matching. Examples include ordinal street expansion (`12 ST` to `12TH ST`), legacy street spelling (`MENCHACA RD` to `MANCHACA RD`), route normalization (`RANCH RD 2222` to `2222 RD`), and selected type splits (`NORTH PLAZA` to `NORTH PLZ`). |
| `output/missing_coords_geocodable_clusters.csv` | Groups still-missing, structurally geocodable addresses by street name, suffix, and ZIP. This is the main review file for deciding whether additional aliases are safe. |
| `output/missing_coords_geocodable_addresses.csv` | Row-level still-missing geocodable addresses, used to build external geocoding requests. |
| `output/geocoding_candidates.csv` | Unique address strings prepared for external geocoding. Multiple parcel records at the same address are collapsed to one query with `n_records`. |
| `output/geocoding_results_arcgis.csv` | Raw ArcGIS Pro geocoding export. This is not merged directly because it can contain low-quality or out-of-area matches. |
| `output/geocoding_arcgis_accepted_queries.csv` | Query-level ArcGIS results that passed the acceptance rules. |
| `output/geocoding_arcgis_review_queries.csv` | Query-level ArcGIS results rejected or held for manual review. This file is useful for spotting errors such as matches in Houston, Dallas, the Northeast, or `(0, 0)` coordinates. |
| `output/geocoding_arcgis_accepted_lookup.csv` | Parcel-level lookup derived from accepted ArcGIS results. If this file exists, `standalone_corporate_parcels.R` applies it as a final coordinate fill with `coord_source = "arcgis_geocoder"`. |

The alias table is deliberately conservative. It should contain only transformations that are supported by the local address reference and that can be explained row-by-row. Ambiguous subdivision clusters should stay out of the alias table even if they are high-volume, because an alias would hide uncertainty rather than resolve it.

The ArcGIS geocoder results are also filtered conservatively before merging. The accepted lookup currently requires:

- `Status == "M"`
- `Score >= 95`
- `Addr_type` is one of `PointAddress`, `Subaddress`, `StreetAddress`, or `StreetAddressExt`
- `RegionAbbr == "TX"`
- `Subregion == "Travis County"`
- Coordinates fall inside a broad Austin/Travis bounding box

This rejects obvious false positives and coarse matches, including records geocoded to other states, other Texas metros, ZIP centroids, locality centroids, or unmatched `(0, 0)` points. In one development run, `output/geocoding_candidates.csv` contained 11,561 unique address queries representing 16,345 parcel rows. The strict ArcGIS acceptance pass kept 10,894 queries and produced `output/geocoding_arcgis_accepted_lookup.csv` with 15,242 parcel-level coordinate fills. After applying that lookup, `output/coords_summary.csv` showed 479,416 of 486,859 coordinate rows filled, with 7,443 still missing.

The important principle is that every coordinate has a provenance. Downstream analysis can keep all coordinates, exclude approximate sources, or inspect particular stages using `coord_source`.

### Identifying corporate ownership

The script classifies each parcel along three dimensions:

1. **Residential** — improvement or land state code begins with `A` (single-family) or `B` (multi-family), or the zoning code contains `SF` or `MF`.

2. **Owner-occupied** — the owner's mailing address matches the situs (property) address, or a homestead exemption (`HS`) is present. Address matching uses the `clean_address()` function, which normalises street type abbreviations, removes punctuation, and strips unit designators before comparison.

3. **Financialized / corporate-owned** — the owner name is matched against a regular expression covering:
   - Formal entity suffixes: `LLC`, `LP`, `LTD`, `INC`, `LC`, `LLLP` (including spaced and punctuated variants)
   - Real-estate sector keywords: `INVEST`, `MANAGE`, `HOLDING`, `DEVELOP`, `REALT`, `ASSET`, `EQUITY`, `PARTNER`, `VENTURE`, and others
   - Numeric characters in the owner name (a proxy for numbered holding companies)

A parcel is flagged as `is_target = TRUE` when it is **residential**, **not owner-occupied**, and **financialized**. These are the records most likely to represent corporate landlord activity.

The script then spatially filters results to the City of Austin boundary using the Census Bureau's Places layer (via the `tigris` package) and writes the final dataset to `corporate_owned_parcels.csv`.

---
