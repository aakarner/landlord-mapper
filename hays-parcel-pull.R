# ============================================================
# Hays CAD parcel downloader and normalizer
# ============================================================
#
# Scope
# -----
# This script pulls Hays County parcel geometry from the public ArcGIS feature
# service and combines it with the Hays CAD property export. The final output is
# shaped to append to output/residential_parcels_for_hex.csv and
# output/williamson_residential_parcels_for_hex.csv.
#
# Hays CAD's download page can challenge scripted downloads. If the automatic
# ZIP download fails, download the latest "Property Data Export Files" ZIP from:
# https://hayscad.com/data-downloads/
# and place it at data/hays/hays_property_export.zip, then rerun this script.

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
# 1. Sources and options
# -----------------------------

hays_arcgis_layer <- "https://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/EXTERNAL_hcad_parcels/FeatureServer/0"

# Latest Hays CAD export visible on 2026-04-29. If this stops working, leave the
# ZIP at data/hays/hays_property_export.zip and the script will use that copy.
hays_property_export_url <- "https://hayscad.com/wp-content/uploads/2026/04/2025-Property-Data-Export-Files-as-of-4-28-2026.zip"

REFRESH_RAW_DOWNLOADS <- FALSE
FILTER_TO_AUSTIN_FIRST <- TRUE
ARCGIS_PAGE_SIZE <- 2000L
SQ_FT_PER_UNIT <- 900

out_dir <- "data/hays"
output_dir <- "output"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

property_zip_path <- file.path(out_dir, "hays_property_export.zip")
parcel_rds_path <- file.path(out_dir, "hays_parcels.rds")
parcel_gpkg_path <- file.path(out_dir, "hays_parcels.gpkg")
parcel_fields_path <- file.path(out_dir, "hays_parcels_fields.csv")

austin_boundary_path <- "data/BOUNDARIES_jurisdictions_20260429.geojson"
hays_output_path <- file.path(output_dir, "hays_residential_parcels_for_hex.csv")
hays_normalized_path <- file.path(out_dir, "hays_residential_parcels_for_hex_austin_full.csv")
hays_summary_path <- file.path(out_dir, "hays_residential_parcels_for_hex_summary.csv")

# -----------------------------
# 2. Helpers
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

download_if_missing <- function(url, path) {
  if (!REFRESH_RAW_DOWNLOADS && file.exists(path)) {
    message("Using existing file: ", normalizePath(path))
    return(invisible(path))
  }

  message("Downloading: ", url)
  ok <- tryCatch(
    {
      utils::download.file(url, path, mode = "wb", quiet = FALSE)
      TRUE
    },
    error = function(e) {
      message("Download failed: ", conditionMessage(e))
      FALSE
    }
  )

  if (!ok || !file.exists(path) || file.info(path)$size == 0) {
    stop(
      "Could not download Hays CAD property export.\n",
      "Download the latest Property Data Export ZIP from https://hayscad.com/data-downloads/ ",
      "and save it as: ", property_zip_path,
      call. = FALSE
    )
  }

  invisible(path)
}

arcgis_query_url <- function(params) {
  query <- paste(
    paste0(names(params), "=", vapply(params, URLencode, character(1), reserved = TRUE)),
    collapse = "&"
  )
  paste0(hays_arcgis_layer, "/query?", query)
}

arcgis_count <- function() {
  url <- arcgis_query_url(list(f = "json", where = "1=1", returnCountOnly = "true"))
  jsonlite::fromJSON(url)$count
}

download_hays_parcels <- function() {
  if (!REFRESH_RAW_DOWNLOADS && file.exists(parcel_rds_path)) {
    message("Using existing parcel geometry: ", normalizePath(parcel_rds_path))
    return(readRDS(parcel_rds_path))
  }

  layer_meta <- jsonlite::fromJSON(paste0(hays_arcgis_layer, "?f=json"))
  fields <- tibble(
    name = vapply(layer_meta$fields$name, as.character, character(1)),
    type = vapply(layer_meta$fields$type, as.character, character(1)),
    alias = vapply(layer_meta$fields$alias, as.character, character(1))
  )
  readr::write_csv(fields, parcel_fields_path)

  total_rows <- arcgis_count()
  offsets <- seq.int(0L, max(total_rows - 1L, 0L), by = ARCGIS_PAGE_SIZE)

  message("Downloading Hays parcel geometry (", format(total_rows, big.mark = ","), " rows) ...")
  pages <- purrr::map(offsets, function(offset) {
    message(
      "  Rows ", format(offset + 1L, big.mark = ","),
      "-", format(min(offset + ARCGIS_PAGE_SIZE, total_rows), big.mark = ",")
    )
    page_url <- arcgis_query_url(list(
      f = "geojson",
      where = "1=1",
      outFields = "OBJECTID,REFNAME,TEXT",
      returnGeometry = "true",
      outSR = "4326",
      resultOffset = as.character(offset),
      resultRecordCount = as.character(ARCGIS_PAGE_SIZE)
    ))
    sf::st_read(page_url, quiet = TRUE)
  })

  parcels <- do.call(rbind, pages) |>
    sf::st_make_valid()

  sf::st_write(parcels, parcel_gpkg_path, layer = "hays_parcels", delete_dsn = TRUE, quiet = TRUE)
  saveRDS(parcels, parcel_rds_path)

  invisible(parcels)
}

read_hays_export_records <- function(zip_path) {
  extract_dir <- file.path(out_dir, "property_export_unzipped")
  dir.create(extract_dir, showWarnings = FALSE, recursive = TRUE)
  utils::unzip(zip_path, exdir = extract_dir)

  nested_zips <- list.files(
    extract_dir,
    pattern = "\\.zip$",
    full.names = TRUE,
    recursive = TRUE,
    ignore.case = TRUE
  )

  if (length(nested_zips)) {
    nested_dir <- file.path(extract_dir, "nested")
    dir.create(nested_dir, showWarnings = FALSE, recursive = TRUE)
    purrr::walk(nested_zips, function(path) {
      target_dir <- file.path(nested_dir, tools::file_path_sans_ext(basename(path)))
      dir.create(target_dir, showWarnings = FALSE, recursive = TRUE)
      utils::unzip(path, exdir = target_dir)
    })
  }

  data_files <- list.files(
    extract_dir,
    pattern = "\\.(csv|txt)$",
    full.names = TRUE,
    recursive = TRUE,
    ignore.case = TRUE
  )

  if (!length(data_files)) {
    stop("No CSV/TXT files found inside ", zip_path, call. = FALSE)
  }

  message("Reading Hays CAD export records from ", length(data_files), " file(s) ...")
  rows <- purrr::map_dfr(data_files, function(path) {
    readr::read_csv(
      path,
      col_types = readr::cols(.default = "c"),
      trim_ws = TRUE,
      progress = FALSE
    ) |>
      dplyr::mutate(source_file = basename(path))
  })

  rows
}

split_hays_records <- function(rows) {
  pick <- function(df, name) {
    if (name %in% names(df)) {
      df[[name]]
    } else {
      rep(NA_character_, nrow(df))
    }
  }

  property_raw <- rows |> dplyr::filter(.data$RecordType == "1")
  owner_raw <- rows |> dplyr::filter(.data$RecordType == "2")
  land_raw <- rows |> dplyr::filter(.data$RecordType == "3")
  improvement_raw <- rows |> dplyr::filter(.data$RecordType == "4")
  segment_raw <- rows |> dplyr::filter(.data$RecordType == "5")

  list(
    property = tibble::tibble(
      PropertyID = pick(property_raw, "PropertyID"),
      QuickRefID = pick(property_raw, "QuickRefID"),
      PropertyNumber = pick(property_raw, "PropertyNumber"),
      LegalDescription = pick(property_raw, "LegalDesc"),
      Acreage = pick(property_raw, "LegalAcres"),
      TaxingUnitList = pick(property_raw, "TaxingUnitList"),
      TotalSqFtLivingArea = pick(property_raw, "SquareFootage"),
      SitusAddress = pick(property_raw, "Situs"),
      SitusCity = pick(property_raw, "SitusCity"),
      SitusState = pick(property_raw, "SitusState"),
      SitusZip = pick(property_raw, "SitusZip"),
      source_file = pick(property_raw, "source_file")
    ),
    owners = tibble::tibble(
      PropertyID = pick(owner_raw, "PropertyID"),
      QuickRefID = pick(owner_raw, "QuickRefID"),
      PropertyNumber = pick(owner_raw, "PropertyNumber"),
      OwnerName = pick(owner_raw, "OwnerName"),
      OwnerAddress1 = pick(owner_raw, "Address1"),
      OwnerAddress2 = pick(owner_raw, "Address2"),
      OwnerAddress3 = pick(owner_raw, "Address3"),
      OwnerCity = pick(owner_raw, "City"),
      OwnerState = pick(owner_raw, "State"),
      OwnerZip = pick(owner_raw, "Zip"),
      PercentOwnership = pick(owner_raw, "OwnershipPercent"),
      ExemptionList = pick(owner_raw, "ExemptionList"),
      HSCapAdjustment = pick(owner_raw, "HSCapAdj"),
      source_file = pick(owner_raw, "source_file")
    ),
    land = tibble::tibble(
      PropertyID = pick(land_raw, "PropertyID"),
      QuickRefID = pick(land_raw, "QuickRefID"),
      PropertyNumber = pick(land_raw, "PropertyNumber"),
      LandTypeCode = pick(land_raw, "LandType"),
      LandDescription = pick(land_raw, "Description"),
      LandStateCode = pick(land_raw, "StateCode"),
      LandSizeAcres = pick(land_raw, "Acres"),
      LandSizeSquareFeet = pick(land_raw, "SquareFeet"),
      source_file = pick(land_raw, "source_file")
    ),
    improvements = tibble::tibble(
      PropertyID = pick(improvement_raw, "PropertyID"),
      QuickRefID = pick(improvement_raw, "QuickRefID"),
      PropertyNumber = pick(improvement_raw, "PropertyNumber"),
      ImprovementStateCode = pick(improvement_raw, "StateCode"),
      source_file = pick(improvement_raw, "source_file")
    ),
    segments = tibble::tibble(
      PropertyID = pick(segment_raw, "PropertyID"),
      QuickRefID = pick(segment_raw, "QuickRefID"),
      PropertyNumber = pick(segment_raw, "PropertyNumber"),
      ActualYearBuilt = pick(segment_raw, "ActYrBuilt"),
      SegmentArea = pick(segment_raw, "Area"),
      source_file = pick(segment_raw, "source_file")
    )
  )
}

# -----------------------------
# 3. Download/read raw inputs
# -----------------------------

download_if_missing(hays_property_export_url, property_zip_path)
records <- read_hays_export_records(property_zip_path)
tables <- split_hays_records(records)

parcels <- download_hays_parcels()

parcel_points <- sf::st_point_on_surface(parcels)

if (FILTER_TO_AUSTIN_FIRST && file.exists(austin_boundary_path)) {
  message("Filtering Hays parcel geometry to City of Austin FULL jurisdiction before joins ...")
  austin_boundary <- sf::st_read(austin_boundary_path, quiet = TRUE) |>
    dplyr::filter(.data$city_name == "CITY OF AUSTIN", .data$jurisdiction_type == "FULL") |>
    sf::st_make_valid() |>
    sf::st_union()

  parcel_points_for_filter <- sf::st_transform(parcel_points, sf::st_crs(austin_boundary))
  in_austin <- lengths(sf::st_intersects(parcel_points_for_filter, austin_boundary)) > 0

  parcels <- parcels[in_austin, ]
  parcel_points <- parcel_points[in_austin, ]

  message(
    "  Keeping ", format(nrow(parcels), big.mark = ","),
    " Hays parcel geometry rows in Austin FULL jurisdiction."
  )
}

# -----------------------------
# 4. Normalize to Travis-compatible parcel rows
# -----------------------------

point_xy <- sf::st_coordinates(parcel_points)

parcel_lookup <- parcels |>
  dplyr::mutate(
    lon = point_xy[, "X"],
    lat = point_xy[, "Y"]
  ) |>
  sf::st_drop_geometry() |>
  dplyr::transmute(
    QuickRefID = dplyr::coalesce(as.character(.data$REFNAME), as.character(.data$TEXT)),
    hays_object_id = as.character(.data$OBJECTID),
    lon = .data$lon,
    lat = .data$lat
  ) |>
  dplyr::filter(!is.na(.data$QuickRefID), .data$QuickRefID != "") |>
  dplyr::group_by(.data$QuickRefID) |>
  dplyr::summarise(
    hays_object_id = first_non_missing(.data$hays_object_id),
    lon = suppressWarnings(as.numeric(first_non_missing(.data$lon))),
    lat = suppressWarnings(as.numeric(first_non_missing(.data$lat))),
    .groups = "drop"
  )

target_quickrefs <- unique(parcel_lookup$QuickRefID)

property_df <- tables$property |>
  dplyr::filter(.data$QuickRefID %in% target_quickrefs) |>
  dplyr::distinct(.data$PropertyID, .keep_all = TRUE)

owners_df <- tables$owners |>
  dplyr::filter(.data$QuickRefID %in% target_quickrefs)

land_summary <- tables$land |>
  dplyr::filter(.data$QuickRefID %in% target_quickrefs) |>
  dplyr::group_by(.data$PropertyID) |>
  dplyr::summarise(
    propertyProf_landStateCd = first_non_missing(.data$LandStateCode),
    land_sqft_from_land = sum(suppressWarnings(as.numeric(.data$LandSizeSquareFeet)), na.rm = TRUE),
    land_acres_from_land = sum(suppressWarnings(as.numeric(.data$LandSizeAcres)), na.rm = TRUE),
    .groups = "drop"
  ) |>
  dplyr::mutate(
    land_sqft_from_land = dplyr::if_else(.data$land_sqft_from_land > 0, .data$land_sqft_from_land, NA_real_),
    land_acres_from_land = dplyr::if_else(.data$land_acres_from_land > 0, .data$land_acres_from_land, NA_real_)
  )

improvement_summary <- tables$improvements |>
  dplyr::filter(.data$QuickRefID %in% target_quickrefs) |>
  dplyr::group_by(.data$PropertyID) |>
  dplyr::summarise(
    propertyProf_imprvStateCd = first_non_missing(.data$ImprovementStateCode),
    .groups = "drop"
  )

segment_summary <- tables$segments |>
  dplyr::filter(.data$QuickRefID %in% target_quickrefs) |>
  dplyr::group_by(.data$PropertyID) |>
  dplyr::summarise(
    segment_sqft = sum(suppressWarnings(as.numeric(.data$SegmentArea)), na.rm = TRUE),
    propertyProf_imprvActualYearBuilt = suppressWarnings(min(as.numeric(.data$ActualYearBuilt), na.rm = TRUE)),
    .groups = "drop"
  ) |>
  dplyr::mutate(
    segment_sqft = dplyr::if_else(.data$segment_sqft > 0, .data$segment_sqft, NA_real_),
    propertyProf_imprvActualYearBuilt = dplyr::if_else(
      is.infinite(.data$propertyProf_imprvActualYearBuilt),
      NA_real_,
      .data$propertyProf_imprvActualYearBuilt
    )
  )

owners_standard <- owners_df |>
  dplyr::transmute(
    PropertyID = as.character(.data$PropertyID),
    owner_name = clean_string(.data$OwnerName),
    owner_address = clean_string(paste(.data$OwnerAddress1, .data$OwnerAddress2, .data$OwnerAddress3, .data$OwnerCity, .data$OwnerState, .data$OwnerZip)),
    owner_exemptions = as.character(.data$ExemptionList),
    owner_is_financialized = grepl(financial_markers, owner_name),
    owner_has_hs = grepl("\\bHS\\b", owner_exemptions)
  )

property_base <- property_df |>
  dplyr::left_join(parcel_lookup, by = "QuickRefID", relationship = "many-to-one") |>
  dplyr::left_join(land_summary, by = "PropertyID", relationship = "one-to-one") |>
  dplyr::left_join(improvement_summary, by = "PropertyID", relationship = "one-to-one") |>
  dplyr::left_join(segment_summary, by = "PropertyID", relationship = "one-to-one") |>
  dplyr::mutate(
    parcel_id = paste0("HAYS:", dplyr::coalesce(.data$QuickRefID, .data$PropertyID)),
    situs_address = .data$SitusAddress,
    situs_city = .data$SitusCity,
    situs_state = dplyr::coalesce(.data$SitusState, "TX"),
    situs_zip = sub("-.*", "", .data$SitusZip),
    improvement_sqft = coalesce_numeric(.data$TotalSqFtLivingArea, .data$segment_sqft),
    land_sqft = coalesce_numeric(.data$land_sqft_from_land, .data$land_acres_from_land * 43560, suppressWarnings(as.numeric(.data$Acreage)) * 43560),
    propertyChar_zoning = NA_character_,
    is_residential = grepl("^A|^B", .data$propertyProf_imprvStateCd) |
      grepl("^A|^B", .data$propertyProf_landStateCd),
    property_units = suppressWarnings(as.numeric(.data$improvement_sqft)) / SQ_FT_PER_UNIT,
    coord_source = dplyr::if_else(
      !is.na(.data$lat) & !is.na(.data$lon),
      "hays_parcel_point_on_surface",
      NA_character_
    )
  )

single_fam_pattern <- "^A"
two_unit_codes   <- "B2"
three_unit_codes <- "B3"
four_unit_codes  <- "B4"
commercial_codes <- c("C1", "C2", "C3", "D1", "D2", "E1", "F1", "F2")

property_base$property_units[
  grepl(single_fam_pattern, property_base$propertyProf_imprvStateCd) |
    grepl(single_fam_pattern, property_base$propertyProf_landStateCd)
] <- 1
property_base$property_units[
  property_base$propertyProf_imprvStateCd %in% two_unit_codes |
    property_base$propertyProf_landStateCd %in% two_unit_codes
] <- 2
property_base$property_units[
  property_base$propertyProf_imprvStateCd %in% three_unit_codes |
    property_base$propertyProf_landStateCd %in% three_unit_codes
] <- 3
property_base$property_units[
  property_base$propertyProf_imprvStateCd %in% four_unit_codes |
    property_base$propertyProf_landStateCd %in% four_unit_codes
] <- 4
property_base$property_units[
  property_base$propertyProf_imprvStateCd %in% commercial_codes |
    property_base$propertyProf_landStateCd %in% commercial_codes
] <- 0
property_base$property_units[is.na(property_base$property_units) & property_base$is_residential] <- 1

message("Classifying Hays owner rows ...")

hays_owner_rows <- property_base |>
  dplyr::left_join(owners_standard, by = "PropertyID", relationship = "one-to-many") |>
  dplyr::mutate(
    owner_occupies_property = address_matches(.data$owner_address, .data$situs_address),
    is_owner_occupied = .data$owner_occupies_property | .data$owner_has_hs,
    is_target = .data$is_residential &
      !.data$is_owner_occupied &
      .data$owner_is_financialized
  )

hays_residential_parcels <- hays_owner_rows |>
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

hays_residential_parcels <- hays_residential_parcels |>
  dplyr::select(dplyr::all_of(hex_columns))

readr::write_csv(hays_residential_parcels, hays_normalized_path)
readr::write_csv(hays_residential_parcels, hays_output_path)

qa_summary <- tibble::tibble(
  output = "austin_full",
  rows = nrow(hays_residential_parcels),
  corporate_owned_parcels = sum(hays_residential_parcels$is_corporate_owned, na.rm = TRUE),
  estimated_units = sum(hays_residential_parcels$property_units, na.rm = TRUE),
  corporate_estimated_units = sum(hays_residential_parcels$corporate_units, na.rm = TRUE)
)

readr::write_csv(qa_summary, hays_summary_path)

message("Wrote Austin-ready Hays parcels: ", normalizePath(hays_output_path))
message("\nDone. Normalized Hays summary:")
print(qa_summary)
