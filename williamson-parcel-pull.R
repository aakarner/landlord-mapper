# ============================================================
# Williamson CAD parcel downloader and normalizer
# ============================================================
#
# Scope
# -----
# This script downloads the raw Williamson CAD / WCAD datasets needed for later
# integration with the Austin corporate ownership workflow, then normalizes them
# to a parcel-level CSV that is shaped like output/residential_parcels_for_hex.csv.

# Install packages once if needed:
# install.packages(c("sf", "dplyr", "readr", "purrr", "tibble", "glue", "jsonlite"))

required_pkgs <- c("sf", "dplyr", "readr", "purrr", "tibble", "glue", "jsonlite")
missing_pkgs <- required_pkgs[!required_pkgs %in% installed.packages()[, "Package"]]
if (length(missing_pkgs)) {
  message("Installing missing packages: ", paste(missing_pkgs, collapse = ", "))
  install.packages(missing_pkgs)
}

library(sf)
library(dplyr)
library(readr)
library(purrr)
library(tibble)
library(glue)
library(jsonlite)

options(timeout = 7200)

# -----------------------------
# 1. WCAD Socrata datasets
# -----------------------------

domain <- "https://data.wcad.org"

datasets <- tibble::tribble(
  ~dataset_key,          ~dataset_id,  ~output_stem,                  ~kind,
  "parcels",             "an3x-cnmw",  "wcad_parcels",                "geojson",
  "property_certified",  "ai3c-c9pf",  "wcad_property_certified",     "csv",
  "owners",              "bbia-wsxs",  "wcad_owners",                 "csv"
)

# Socrata usually allows up to 50,000 rows per page.  Using pagination avoids
# the common `/resource/*.geojson` default limit trap.
PAGE_SIZE <- 50000L

# Leave FALSE for normal use once the raw files exist. Set TRUE when you want to
# refresh the WCAD Socrata exports.
REFRESH_RAW_DOWNLOADS <- FALSE

# Spatially trim to Austin before joining property and owner tables. This keeps
# the normalization step focused on the Williamson portion of Austin instead of
# churning through the full county.
FILTER_TO_AUSTIN_FIRST <- TRUE

SQ_FT_PER_UNIT <- 900

# -----------------------------
# 2. Output folder
# -----------------------------

out_dir <- "data/wcad"
output_dir <- "output"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

# -----------------------------
# 3. URL and download helpers
# -----------------------------

resource_url <- function(dataset_id, format = "csv", limit = NULL, offset = NULL) {
  url <- glue("{domain}/resource/{dataset_id}.{format}")
  query <- c()
  if (!is.null(limit)) query <- c(query, glue("$limit={limit}"))
  if (!is.null(offset)) query <- c(query, glue("$offset={offset}"))
  if (length(query)) url <- paste0(url, "?", paste(query, collapse = "&"))
  url
}

metadata_url <- function(dataset_id) {
  glue("{domain}/api/views/{dataset_id}")
}

rows_download_url <- function(dataset_id) {
  glue("{domain}/api/views/{dataset_id}/rows.csv?accessType=DOWNLOAD")
}

socrata_count <- function(dataset_id) {
  count_url <- resource_url(dataset_id, "csv", limit = NULL, offset = NULL)
  count_url <- paste0(count_url, "?$select=count(*)")
  out <- readr::read_csv(count_url, show_col_types = FALSE)
  as.integer(out[[1]][[1]])
}

write_field_inventory <- function(dataset_id, output_stem) {
  metadata <- jsonlite::fromJSON(metadata_url(dataset_id))
  fields <- metadata$columns |>
    transmute(
      name = .data$name,
      field_name = .data$fieldName,
      data_type = .data$dataTypeName
    )

  readr::write_csv(fields, file.path(out_dir, paste0(output_stem, "_fields.csv")))
  invisible(fields)
}

download_csv_dataset <- function(dataset_id, output_stem) {
  csv_path <- file.path(out_dir, paste0(output_stem, ".csv"))
  message("Downloading CSV: ", output_stem)

  # `rows.csv?accessType=DOWNLOAD` is the Socrata full-export endpoint.  It is
  # preferable to `/resource/*.csv` for non-spatial tables because it does not
  # require pagination and preserves the source column names.
  utils::download.file(
    rows_download_url(dataset_id),
    csv_path,
    mode = "wb",
    quiet = FALSE
  )

  message("  Wrote: ", csv_path)
  invisible(csv_path)
}

download_geojson_dataset <- function(dataset_id, output_stem, page_size = PAGE_SIZE) {
  total_rows <- socrata_count(dataset_id)
  offsets <- seq.int(0L, max(total_rows - 1L, 0L), by = page_size)

  message(
    "Downloading GeoJSON pages: ", output_stem,
    " (", format(total_rows, big.mark = ","), " rows)"
  )

  pages <- purrr::map(offsets, function(offset) {
    page_url <- resource_url(dataset_id, "geojson", limit = page_size, offset = offset)
    message(
      "  Rows ", format(offset + 1L, big.mark = ","),
      "-", format(min(offset + page_size, total_rows), big.mark = ",")
    )
    sf::st_read(page_url, quiet = TRUE)
  })

  geo <- do.call(rbind, pages) |>
    sf::st_make_valid()

  gpkg_path <- file.path(out_dir, paste0(output_stem, ".gpkg"))
  rds_path <- file.path(out_dir, paste0(output_stem, ".rds"))
  csv_path <- file.path(out_dir, paste0(output_stem, "_flat.csv"))

  sf::st_write(
    geo,
    gpkg_path,
    layer = output_stem,
    delete_dsn = TRUE,
    quiet = TRUE
  )

  saveRDS(geo, rds_path)

  geo |>
    mutate(geometry_wkt = sf::st_as_text(geometry)) |>
    sf::st_drop_geometry() |>
    readr::write_csv(csv_path)

  message("  Wrote: ", gpkg_path)
  message("  Wrote: ", rds_path)
  message("  Wrote: ", csv_path)

  invisible(list(gpkg = gpkg_path, rds = rds_path, csv = csv_path, rows = nrow(geo)))
}

# -----------------------------
# 4. Download raw datasets, if requested or missing
# -----------------------------

raw_output_exists <- function(dataset_key, dataset_id, output_stem, kind) {
  if (kind == "geojson") {
    file.exists(file.path(out_dir, paste0(output_stem, ".rds")))
  } else {
    file.exists(file.path(out_dir, paste0(output_stem, ".csv")))
  }
}

download_one_dataset <- function(dataset_key, dataset_id, output_stem, kind) {
  message("\nDataset: ", dataset_key, " [", dataset_id, "]")
  total_rows <- socrata_count(dataset_id)
  fields <- write_field_inventory(dataset_id, output_stem)

  if (kind == "geojson") {
    paths <- download_geojson_dataset(dataset_id, output_stem)
    output_files <- paste(unlist(paths[c("gpkg", "rds", "csv")]), collapse = "; ")
  } else if (kind == "csv") {
    csv_path <- download_csv_dataset(dataset_id, output_stem)
    output_files <- csv_path
  } else {
    stop("Unknown dataset kind: ", kind, call. = FALSE)
  }

  tibble(
    dataset_key = dataset_key,
    dataset_id = dataset_id,
    source_domain = domain,
    kind = kind,
    source_row_count = total_rows,
    field_count = nrow(fields),
    output_files = output_files,
    downloaded_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")
  )
}

if (REFRESH_RAW_DOWNLOADS || !all(purrr::pmap_lgl(datasets, raw_output_exists))) {
  manifest_rows <- purrr::pmap_dfr(datasets, download_one_dataset)

  manifest_path <- file.path(out_dir, "wcad_download_manifest.csv")
  readr::write_csv(manifest_rows, manifest_path)

  message("\nDone. WCAD raw data written to: ", normalizePath(out_dir))
  message("Manifest: ", normalizePath(manifest_path))
} else {
  message("Using existing WCAD raw files in: ", normalizePath(out_dir))
}

# -----------------------------
# 5. Normalize to Travis-compatible parcel rows
# -----------------------------

clean_string <- function(x) {
  x <- toupper(as.character(x))
  x <- gsub("[[:punct:]]+", " ", x)
  x <- gsub("[[:space:]]+", " ", x)
  trimws(x)
}

first_non_missing <- function(x) {
  x <- x[!is.na(x) & x != ""]
  if (length(x) == 0L) NA else x[[1]]
}

coalesce_numeric <- function(...) {
  args <- list(...)
  out <- suppressWarnings(as.numeric(args[[1]]))
  if (length(args) == 1L) return(out)
  for (arg in args[-1]) {
    candidate <- suppressWarnings(as.numeric(arg))
    out[is.na(out)] <- candidate[is.na(out)]
  }
  out
}

address_matches <- function(owner_address, situs_address) {
  owner_address <- clean_string(owner_address)
  situs_address <- clean_string(situs_address)
  ifelse(
    is.na(owner_address) | is.na(situs_address) | owner_address == "" | situs_address == "",
    FALSE,
    mapply(grepl, owner_address, situs_address, fixed = TRUE, USE.NAMES = FALSE)
  )
}

financial_markers <- paste(
  "\\bLTD\\b", "\\bL T D\\b", "\\bL\\.?T\\.?D\\.?\\b",
  "\\bLLC\\b", "\\bL L C\\b", "\\bL\\.?L\\.?C\\.?\\b",
  "\\bLP\\b",  "\\bL P\\b",   "\\bL\\.?P\\.?\\b",
  "\\bLLLP\\b","\\bL L L P\\b","\\bL\\.?L\\.?L\\.?P\\.?\\b",
  "\\bINC\\b", "\\bI N C\\b", "\\bI\\.?N\\.?C\\.?\\b",
  "\\bLC\\b",  "\\bL C\\b",   "\\bL\\.?C\\.?\\b",
  "\\bMORTG", "\\bRENT\\b",   "\\bMARKET\\b", "\\bINVEST",  "\\bPROP\\b",
  "\\bMANAGE","\\bMGT\\b",    "\\bMGMT\\b",   "\\bASSET",   "\\bJOINT\\b",
  "\\bVENTURE","\\bVNT\\b",   "\\bLIMIT",     "\\bPARTN",   "\\bPRTN\\b",
  "\\bBANK\\b","\\bASSOC",    "\\bEQUIT",     "\\bREALT",   "\\bOWNER\\b",
  "\\bHOLDING","\\bDEVELOP",  "\\bCOMP\\b",   "\\bCORP\\b", "\\bAQUISI",
  "\\bCONDO\\b","\\bC/O\\b",
  "[[:digit:]]",
  "\\bBORROWER\\b", "\\bFOUNDA",
  sep = "|"
)

residential_text_markers <- paste(
  "\\bAPARTMENTS?\\b", "\\bAPTS?\\b", "\\bMULTI[- ]?FAMILY\\b",
  "\\bDUPLEX\\b", "\\bTRIPLEX\\b", "\\bFOURPLEX\\b",
  "\\bCONDOMINIUM\\b", "\\bCONDO\\b", "\\bTOWNHOME\\b", "\\bTOWNHOUSE\\b",
  sep = "|"
)

message("\nReading WCAD raw files ...")

property_df <- readr::read_csv(
  file.path(out_dir, "wcad_property_certified.csv"),
  show_col_types = FALSE,
  col_types = readr::cols(.default = "c")
) |>
  dplyr::distinct(.data$PropertyID, .keep_all = TRUE)

owners_df <- readr::read_csv(
  file.path(out_dir, "wcad_owners.csv"),
  show_col_types = FALSE,
  col_types = readr::cols(.default = "c")
)

parcel_sf <- readRDS(file.path(out_dir, "wcad_parcels.rds")) |>
  dplyr::mutate(
    propertyid = as.character(.data$propertyid),
    wcad_parcel_id = as.character(.data$parcelid)
  )

austin_boundary_path <- "data/BOUNDARIES_jurisdictions_20260429.geojson"

parcel_points <- sf::st_point_on_surface(parcel_sf)

if (FILTER_TO_AUSTIN_FIRST && file.exists(austin_boundary_path)) {
  message("Filtering WCAD parcel geometry to City of Austin FULL jurisdiction before joins ...")

  austin_boundary <- sf::st_read(austin_boundary_path, quiet = TRUE) |>
    dplyr::filter(.data$city_name == "CITY OF AUSTIN", .data$jurisdiction_type == "FULL") |>
    sf::st_make_valid() |>
    sf::st_union()

  parcel_points_for_filter <- sf::st_transform(parcel_points, sf::st_crs(austin_boundary))
  in_austin <- lengths(sf::st_intersects(parcel_points_for_filter, austin_boundary)) > 0

  parcel_sf <- parcel_sf[in_austin, ]
  parcel_points <- parcel_points[in_austin, ]

  message(
    "  Keeping ", format(nrow(parcel_sf), big.mark = ","),
    " WCAD parcel geometry rows in Austin FULL jurisdiction."
  )
}

point_xy <- sf::st_coordinates(parcel_points)

target_property_ids <- unique(na.omit(as.character(parcel_sf$propertyid)))

if (FILTER_TO_AUSTIN_FIRST && length(target_property_ids)) {
  property_df <- property_df |>
    dplyr::filter(.data$PropertyID %in% target_property_ids)
  owners_df <- owners_df |>
    dplyr::filter(.data$PropertyID %in% target_property_ids)

  message(
    "  Keeping ", format(nrow(property_df), big.mark = ","),
    " property rows and ", format(nrow(owners_df), big.mark = ","),
    " owner rows after the Austin spatial trim."
  )
}

parcel_lookup <- parcel_sf |>
  dplyr::mutate(
    lon = point_xy[, "X"],
    lat = point_xy[, "Y"]
  ) |>
  sf::st_drop_geometry() |>
  dplyr::transmute(
    PropertyID = as.character(.data$propertyid),
    wcad_parcel_id = .data$wcad_parcel_id,
    parcel_siteaddress = .data$siteaddress,
    parcel_use_code = .data$usecd,
    parcel_use_desc = .data$usedscrp,
    parcel_building_sqft = suppressWarnings(as.numeric(.data$bldgarea)),
    parcel_residential_sqft = suppressWarnings(as.numeric(.data$resflrarea)),
    parcel_assessed_acres = suppressWarnings(as.numeric(.data$assessedacres)),
    lon = .data$lon,
    lat = .data$lat
  ) |>
  dplyr::group_by(.data$PropertyID) |>
  dplyr::summarise(
    wcad_parcel_id = first_non_missing(.data$wcad_parcel_id),
    parcel_siteaddress = first_non_missing(.data$parcel_siteaddress),
    parcel_use_code = first_non_missing(.data$parcel_use_code),
    parcel_use_desc = first_non_missing(.data$parcel_use_desc),
    parcel_building_sqft = suppressWarnings(as.numeric(first_non_missing(.data$parcel_building_sqft))),
    parcel_residential_sqft = suppressWarnings(as.numeric(first_non_missing(.data$parcel_residential_sqft))),
    parcel_assessed_acres = suppressWarnings(as.numeric(first_non_missing(.data$parcel_assessed_acres))),
    lon = suppressWarnings(as.numeric(first_non_missing(.data$lon))),
    lat = suppressWarnings(as.numeric(first_non_missing(.data$lat))),
    .groups = "drop"
  )

owners_standard <- owners_df |>
  dplyr::transmute(
    PropertyID = as.character(.data$PropertyID),
    owner_name = clean_string(.data$FullName),
    owner_address = clean_string(.data$MailingAddress),
    owner_exemptions = as.character(.data$ExemptionList),
    owner_is_financialized = grepl(financial_markers, owner_name),
    owner_has_hs = grepl("\\bHS\\b", owner_exemptions)
  )

property_base <- property_df |>
  dplyr::left_join(parcel_lookup, by = "PropertyID", relationship = "one-to-one") |>
  dplyr::mutate(
    parcel_id = dplyr::coalesce(.data$wcad_parcel_id, .data$QuickRefID, paste0("WCAD-", .data$PropertyID)),
    parcel_id = paste0("WILLIAMSON:", .data$parcel_id),
    situs_address = dplyr::coalesce(.data$SitusAddress, .data$Address, .data$PropertyAddress, .data$parcel_siteaddress),
    situs_city = .data$City,
    situs_state = dplyr::coalesce(.data$State, "TX"),
    situs_zip = sub("-.*", "", .data$Zip),
    property_type_desc = .data$PropertyTypeDesc,
    residential_text = paste(.data$PropertyAddress, .data$SitusAddress, .data$LegalDescription, .data$PropertyComment, .data$DBA),
    is_residential = .data$property_type_desc %in% c(
      "Residential",
      "Manufactured Home",
      "LTRR-Land Transitional Residential"
    ) |
      .data$parcel_use_code %in% c("RES", "RAD", "LTRR") |
      grepl(residential_text_markers, residential_text, ignore.case = TRUE),
    improvement_sqft = coalesce_numeric(.data$TotalSqFtLivingArea, .data$parcel_residential_sqft, .data$parcel_building_sqft),
    land_sqft = coalesce_numeric(.data$Acres, .data$parcel_assessed_acres) * 43560,
    property_units = dplyr::case_when(
      !is_residential ~ 0,
      is.na(improvement_sqft) | improvement_sqft <= 0 ~ 0,
      improvement_sqft <= 5000 ~ 1,
      TRUE ~ improvement_sqft / SQ_FT_PER_UNIT
    ),
    propertyChar_zoning = NA_character_,
    propertyProf_imprvStateCd = .data$property_type_desc,
    propertyProf_landStateCd = .data$parcel_use_code,
    propertyProf_imprvActualYearBuilt = NA_real_,
    coord_source = dplyr::if_else(
      !is.na(.data$lat) & !is.na(.data$lon),
      "wcad_parcel_point_on_surface",
      NA_character_
    )
  )

message("Classifying Williamson owner rows ...")

williamson_owner_rows <- property_base |>
  dplyr::left_join(owners_standard, by = "PropertyID", relationship = "one-to-many") |>
  dplyr::mutate(
    owner_occupies_property = address_matches(.data$owner_address, .data$situs_address),
    is_owner_occupied = .data$owner_occupies_property | .data$owner_has_hs,
    is_target = .data$is_residential &
      !.data$is_owner_occupied &
      .data$owner_is_financialized
  )

williamson_residential_parcels <- williamson_owner_rows |>
  dplyr::filter(.data$is_residential) |>
  dplyr::group_by(.data$parcel_id) |>
  dplyr::summarise(
    situs_address = first_non_missing(.data$situs_address),
    situs_city = first_non_missing(.data$situs_city),
    situs_state = first_non_missing(.data$situs_state),
    situs_zip = first_non_missing(.data$situs_zip),
    propertyChar_zoning = first_non_missing(.data$propertyChar_zoning),
    propertyProf_imprvStateCd = first_non_missing(.data$propertyProf_imprvStateCd),
    propertyProf_landStateCd = first_non_missing(.data$propertyProf_landStateCd),
    propertyProf_imprvActualYearBuilt = suppressWarnings(as.numeric(first_non_missing(.data$propertyProf_imprvActualYearBuilt))),
    improvement_sqft = suppressWarnings(as.numeric(first_non_missing(.data$improvement_sqft))),
    land_sqft = suppressWarnings(as.numeric(first_non_missing(.data$land_sqft))),
    property_units = suppressWarnings(as.numeric(first_non_missing(.data$property_units))),
    lat = suppressWarnings(as.numeric(first_non_missing(.data$lat))),
    lon = suppressWarnings(as.numeric(first_non_missing(.data$lon))),
    coord_source = first_non_missing(.data$coord_source),
    is_residential = any(.data$is_residential, na.rm = TRUE),
    is_owner_occupied = any(.data$is_owner_occupied, na.rm = TRUE),
    has_financialized_owner = any(.data$owner_is_financialized, na.rm = TRUE),
    is_corporate_owned = any(.data$is_target, na.rm = TRUE),
    owner_names = paste(sort(unique(na.omit(.data$owner_name))), collapse = "; "),
    n_owner_rows = sum(!is.na(.data$owner_name)),
    parcel_count = 1L,
    corporate_parcel_count = as.integer(any(.data$is_target, na.rm = TRUE)),
    corporate_units = ifelse(any(.data$is_target, na.rm = TRUE), property_units, 0),
    corporate_improvement_sqft = ifelse(any(.data$is_target, na.rm = TRUE), improvement_sqft, 0),
    .groups = "drop"
  ) |>
  dplyr::mutate(
    corporate_units = suppressWarnings(as.numeric(.data$corporate_units)),
    corporate_improvement_sqft = suppressWarnings(as.numeric(.data$corporate_improvement_sqft))
  )

hex_columns <- c(
  "parcel_id",
  "situs_address", "situs_city", "situs_state", "situs_zip",
  "propertyChar_zoning",
  "propertyProf_imprvStateCd", "propertyProf_landStateCd",
  "propertyProf_imprvActualYearBuilt",
  "improvement_sqft", "land_sqft", "property_units",
  "lat", "lon", "coord_source",
  "is_residential", "is_owner_occupied",
  "has_financialized_owner", "is_corporate_owned",
  "owner_names", "n_owner_rows",
  "parcel_count", "corporate_parcel_count",
  "corporate_units", "corporate_improvement_sqft"
)

williamson_residential_parcels <- williamson_residential_parcels |>
  dplyr::select(dplyr::all_of(hex_columns))

normalized_scope <- if (FILTER_TO_AUSTIN_FIRST) "austin_full" else "countywide"
normalized_path <- file.path(
  out_dir,
  paste0("wcad_residential_parcels_for_hex_", normalized_scope, ".csv")
)
readr::write_csv(williamson_residential_parcels, normalized_path)
message("Wrote normalized Williamson parcels: ", normalizePath(normalized_path))

# -----------------------------
# 6. Austin full-purpose subset, ready to append to Travis output
# -----------------------------

austin_output_path <- file.path(output_dir, "williamson_residential_parcels_for_hex.csv")

if (FILTER_TO_AUSTIN_FIRST) {
  williamson_austin_parcels <- williamson_residential_parcels
  readr::write_csv(williamson_austin_parcels, austin_output_path)
  message("Wrote Austin-ready Williamson parcels: ", normalizePath(austin_output_path))
} else if (file.exists(austin_boundary_path)) {
  message("Filtering Williamson parcels to City of Austin FULL jurisdiction ...")

  austin_boundary <- sf::st_read(austin_boundary_path, quiet = TRUE) |>
    dplyr::filter(.data$city_name == "CITY OF AUSTIN", .data$jurisdiction_type == "FULL") |>
    sf::st_make_valid() |>
    sf::st_union()

  williamson_points <- williamson_residential_parcels |>
    dplyr::filter(!is.na(.data$lat), !is.na(.data$lon)) |>
    sf::st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)

  austin_boundary <- sf::st_transform(austin_boundary, sf::st_crs(williamson_points))
  in_austin <- lengths(sf::st_intersects(williamson_points, austin_boundary)) > 0

  williamson_austin_parcels <- williamson_points[in_austin, ] |>
    sf::st_drop_geometry() |>
    dplyr::select(dplyr::all_of(hex_columns))

  readr::write_csv(williamson_austin_parcels, austin_output_path)
  message("Wrote Austin-ready Williamson parcels: ", normalizePath(austin_output_path))
} else {
  warning("Austin jurisdiction boundary not found; writing countywide rows to ", austin_output_path)
  williamson_austin_parcels <- williamson_residential_parcels
  readr::write_csv(williamson_austin_parcels, austin_output_path)
}

qa_summary <- tibble::tibble(
  output = c(normalized_scope, "austin_full_output"),
  rows = c(nrow(williamson_residential_parcels), nrow(williamson_austin_parcels)),
  corporate_owned_parcels = c(
    sum(williamson_residential_parcels$is_corporate_owned, na.rm = TRUE),
    sum(williamson_austin_parcels$is_corporate_owned, na.rm = TRUE)
  ),
  estimated_units = c(
    sum(williamson_residential_parcels$property_units, na.rm = TRUE),
    sum(williamson_austin_parcels$property_units, na.rm = TRUE)
  ),
  corporate_estimated_units = c(
    sum(williamson_residential_parcels$corporate_units, na.rm = TRUE),
    sum(williamson_austin_parcels$corporate_units, na.rm = TRUE)
  )
)

qa_path <- file.path(out_dir, "wcad_residential_parcels_for_hex_summary.csv")
readr::write_csv(qa_summary, qa_path)

message("\nDone. Normalized Williamson summary:")
print(qa_summary)
