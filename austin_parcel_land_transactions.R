# ============================================================
# Austin parcel land value and transaction history
# ============================================================
#
# Produces one citywide parcel-year file for all parcels inside City of Austin
# FULL jurisdiction across Travis, Williamson, and Hays counties.

required_pkgs <- c("sf", "dplyr", "readr", "purrr", "tibble", "glue", "jsonlite", "lubridate")
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
library(lubridate)

options(timeout = 7200)

if (nchar(Sys.which("jq")) == 0L) {
  stop("'jq' is required but was not found on your PATH.", call. = FALSE)
}

# -----------------------------
# Inputs / outputs
# -----------------------------

output_dir <- "output"
data_dir <- "data"
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

austin_boundary_path <- file.path(data_dir, "BOUNDARIES_jurisdictions_20260429.geojson")

travis_zip_path <- "tcad_special_export.zip"
travis_situs_path <- file.path(output_dir, "situses.csv")
travis_coords_path <- file.path(output_dir, "coords.csv")
travis_valuations_cache_path <- file.path(output_dir, "travis_valuations.csv")
travis_deeds_cache_path <- file.path(output_dir, "travis_deeds.csv")
travis_selected_fields_path <- file.path(output_dir, "travis_land_transaction_selected_fields.csv")

wcad_property_path <- file.path(data_dir, "wcad", "wcad_property_certified.csv")
wcad_parcels_path <- file.path(data_dir, "wcad", "wcad_parcels.rds")

hays_property_zip_path <- file.path(data_dir, "hays", "hays_property_export.zip")
hays_parcels_path <- file.path(data_dir, "hays", "hays_parcels.rds")

PAGE_SIZE <- 10000L
all_counties <- c("Travis", "Williamson", "Hays")
COUNTIES_TO_BUILD <- strsplit(Sys.getenv("AUSTIN_LAND_TX_COUNTIES", "Travis,Williamson,Hays"), ",")[[1]]
COUNTIES_TO_BUILD <- trimws(COUNTIES_TO_BUILD)
is_county_subset <- !setequal(COUNTIES_TO_BUILD, all_counties) || length(COUNTIES_TO_BUILD) != length(all_counties)
out_suffix <- if (is_county_subset) {
  paste0("_", tolower(paste(COUNTIES_TO_BUILD, collapse = "_")))
} else {
  ""
}
out_path <- file.path(output_dir, paste0("austin_parcel_year_land_transactions", out_suffix, ".csv"))
qa_path <- file.path(output_dir, paste0("austin_parcel_year_land_transactions_summary", out_suffix, ".csv"))

output_columns <- c(
  "county",
  "parcel_id",
  "source_property_id",
  "situs_address",
  "situs_city",
  "situs_state",
  "situs_zip",
  "lat",
  "lon",
  "coord_source",
  "current_land_value",
  "land_value_tax_year",
  "transaction_year",
  "transaction_count",
  "corporate_buyer_transaction_count",
  "corporate_seller_transaction_count",
  "corporate_party_transaction_count",
  "transaction_source",
  "land_value_source"
)

# -----------------------------
# Shared helpers
# -----------------------------

clean_string <- function(x) {
  x <- toupper(as.character(x))
  x <- gsub("[[:punct:]]+", " ", x)
  x <- gsub("[[:space:]]+", " ", x)
  trimws(x)
}

financial_markers <- paste(
  "\\bLTD\\b", "\\bL T D\\b", "\\bL\\.?T\\.?D\\.?\\b",
  "\\bLLC\\b", "\\bL L C\\b", "\\bL\\.?L\\.?C\\.?\\b",
  "\\bLP\\b", "\\bL P\\b", "\\bL\\.?P\\.?\\b",
  "\\bLLLP\\b", "\\bL L L P\\b", "\\bL\\.?L\\.?L\\.?P\\.?\\b",
  "\\bINC\\b", "\\bI N C\\b", "\\bI\\.?N\\.?C\\.?\\b",
  "\\bLC\\b", "\\bL C\\b", "\\bL\\.?C\\.?\\b",
  "\\bMORTG", "\\bRENT\\b", "\\bMARKET\\b", "\\bINVEST", "\\bPROP\\b",
  "\\bMANAGE", "\\bMGT\\b", "\\bMGMT\\b", "\\bASSET", "\\bJOINT\\b",
  "\\bVENTURE", "\\bVNT\\b", "\\bLIMIT", "\\bPARTN", "\\bPRTN\\b",
  "\\bBANK\\b", "\\bASSOC", "\\bEQUIT", "\\bREALT", "\\bOWNER\\b",
  "\\bHOLDING", "\\bDEVELOP", "\\bCOMP\\b", "\\bCORP\\b", "\\bAQUISI",
  "\\bCONDO\\b", "\\bC/O\\b",
  "[[:digit:]]",
  "\\bBORROWER\\b", "\\bFOUNDA",
  sep = "|"
)

is_corporate_name <- function(x) {
  x <- clean_string(x)
  !is.na(x) & x != "" & grepl(financial_markers, x)
}

blank_to_na <- function(x) {
  x <- trimws(as.character(x))
  x[x == "" | toupper(x) == "NA"] <- NA_character_
  x
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

coalesce_character <- function(...) {
  args <- lapply(list(...), as.character)
  out <- args[[1]]
  out[out == ""] <- NA_character_
  if (length(args) == 1L) return(out)
  for (arg in args[-1]) {
    arg[arg == ""] <- NA_character_
    out[is.na(out)] <- arg[is.na(out)]
  }
  out
}

choose_first_existing <- function(df, candidates) {
  hit <- candidates[candidates %in% names(df)]
  if (length(hit)) hit[[1]] else NA_character_
}

choose_by_regex <- function(df, pattern) {
  hit <- grep(pattern, names(df), ignore.case = TRUE, value = TRUE)
  if (length(hit)) hit[[1]] else NA_character_
}

serialize_list_columns <- function(df) {
  list_cols <- names(df)[vapply(df, is.list, logical(1))]
  for (col in list_cols) {
    df[[col]] <- vapply(
      df[[col]],
      function(x) {
        if (length(x) == 0L || all(is.na(x))) {
          NA_character_
        } else {
          jsonlite::toJSON(x, auto_unbox = TRUE, null = "null")
        }
      },
      character(1)
    )
  }
  df
}

parse_any_date <- function(x) {
  x <- blank_to_na(x)
  out <- suppressWarnings(ymd(x, quiet = TRUE))
  missing <- is.na(out) & !is.na(x)
  out[missing] <- suppressWarnings(mdy(x[missing], quiet = TRUE))
  missing <- is.na(out) & !is.na(x)
  out[missing] <- as.Date(suppressWarnings(mdy_hms(x[missing], quiet = TRUE)))
  missing <- is.na(out) & !is.na(x)
  out[missing] <- as.Date(suppressWarnings(ymd_hms(x[missing], quiet = TRUE)))
  out
}

load_austin_boundary <- function() {
  if (!file.exists(austin_boundary_path)) {
    stop("Austin jurisdiction boundary not found: ", austin_boundary_path, call. = FALSE)
  }
  sf::st_read(austin_boundary_path, quiet = TRUE) |>
    dplyr::filter(.data$city_name == "CITY OF AUSTIN", .data$jurisdiction_type == "FULL") |>
    sf::st_make_valid() |>
    sf::st_union() |>
    sf::st_as_sf()
}

austin_boundary <- load_austin_boundary()

filter_points_to_austin <- function(df, lon_col = "lon", lat_col = "lat", label = "rows") {
  has_coords <- !is.na(df[[lon_col]]) & !is.na(df[[lat_col]])
  if (!any(has_coords)) {
    warning("No ", label, " have coordinates; returning zero rows.")
    return(df[0, , drop = FALSE])
  }
  pts <- sf::st_as_sf(df[has_coords, , drop = FALSE], coords = c(lon_col, lat_col), crs = 4326, remove = FALSE)
  boundary <- sf::st_transform(austin_boundary, sf::st_crs(pts))
  in_austin <- lengths(sf::st_intersects(pts, boundary)) > 0L
  out <- sf::st_drop_geometry(pts[in_austin, ])
  message(
    "  ", label, ": retained ", format(nrow(out), big.mark = ","),
    " of ", format(nrow(df), big.mark = ","), " rows inside Austin FULL."
  )
  out
}

write_empty_if_missing <- function(df) {
  for (col in output_columns) {
    if (!col %in% names(df)) df[[col]] <- NA
  }
  df |> dplyr::select(dplyr::all_of(output_columns))
}

append_transactions <- function(parcel_df, transaction_df, no_transaction_source) {
  if (nrow(transaction_df) == 0L) {
    return(parcel_df |>
      dplyr::mutate(
        transaction_year = NA_integer_,
        transaction_count = 0L,
        corporate_buyer_transaction_count = 0L,
        corporate_seller_transaction_count = 0L,
        corporate_party_transaction_count = 0L,
        transaction_source = no_transaction_source
      ) |>
      write_empty_if_missing())
  }

  out <- parcel_df |>
    dplyr::left_join(transaction_df, by = "parcel_id", relationship = "one-to-many")

  out |>
    dplyr::mutate(
      transaction_count = dplyr::coalesce(.data$transaction_count, 0L),
      corporate_buyer_transaction_count = dplyr::coalesce(.data$corporate_buyer_transaction_count, 0L),
      corporate_seller_transaction_count = dplyr::coalesce(.data$corporate_seller_transaction_count, 0L),
      corporate_party_transaction_count = dplyr::coalesce(.data$corporate_party_transaction_count, 0L),
      transaction_source = dplyr::coalesce(.data$transaction_source, no_transaction_source)
    ) |>
    write_empty_if_missing()
}

stream_tcad_section <- function(section, prefix, cache_path) {
  if (file.exists(cache_path)) {
    message("Using cached Travis ", section, ": ", cache_path)
    return(readr::read_csv(cache_path, show_col_types = FALSE, col_types = readr::cols(.default = "c")))
  }
  if (!file.exists(travis_zip_path)) {
    stop("Missing TCAD export ZIP: ", travis_zip_path, call. = FALSE)
  }
  manifest <- unzip(travis_zip_path, list = TRUE)
  json_name <- manifest$Name[[1]]
  jq_filter_1 <- "fromstream(1|truncate_stream(inputs))"
  jq_filter_2 <- sprintf(
    "if has(\"%s\") then .pID as $pid | (.%s // [])[] | . + {pID: $pid} else empty end",
    section,
    section
  )
  cmd <- sprintf(
    "unzip -p %s %s | jq -cn --stream %s | jq -c %s",
    shQuote(travis_zip_path),
    shQuote(json_name),
    shQuote(jq_filter_1),
    shQuote(jq_filter_2)
  )

  message("Streaming Travis section '", section, "' ...")
  con <- pipe(cmd, open = "r")
  on.exit(close(con), add = TRUE)
  chunks <- list()
  n_pages <- 0L
  jsonlite::stream_in(
    con,
    handler = function(page) {
      n_pages <<- n_pages + 1L
      chunks[[n_pages]] <<- page
      message("  ", section, " page ", n_pages, " processed")
    },
    pagesize = PAGE_SIZE,
    verbose = FALSE
  )
  df <- dplyr::bind_rows(chunks)
  if (!nrow(df)) {
    df <- tibble::tibble()
  } else {
    names(df) <- ifelse(names(df) == "pID", paste0(prefix, "_pID"), paste0(prefix, "_", names(df)))
    df <- serialize_list_columns(df)
  }
  readr::write_csv(df, cache_path)
  df
}

# -----------------------------
# Travis
# -----------------------------

build_travis <- function() {
  message("\nBuilding Travis parcel-year data ...")
  if (!file.exists(travis_situs_path) || !file.exists(travis_coords_path)) {
    stop("Travis cached situses/coords are required: ", travis_situs_path, ", ", travis_coords_path, call. = FALSE)
  }

  situs <- readr::read_csv(travis_situs_path, show_col_types = FALSE, col_types = readr::cols(.default = "c")) |>
    dplyr::mutate(
      situs_pID = as.character(.data$situs_pID),
      situs_zip = sub("-.*", "", as.character(.data$situs_zip)),
      situs_city = dplyr::coalesce(blank_to_na(.data$situs_city), "AUSTIN"),
      situs_state = dplyr::coalesce(blank_to_na(.data$situs_state), "TX"),
      situs_address = clean_string(paste(.data$situs_streetNum, .data$situs_streetPrefix, .data$situs_streetName, .data$situs_streetSuffix, .data$situs_city, .data$situs_state, .data$situs_zip))
    ) |>
    dplyr::arrange(dplyr::desc(suppressWarnings(as.integer(.data$situs_primarySitus)))) |>
    dplyr::distinct(.data$situs_pID, .keep_all = TRUE)

  coords <- readr::read_csv(travis_coords_path, show_col_types = FALSE, col_types = readr::cols(.default = "c")) |>
    dplyr::transmute(
      source_property_id = as.character(.data$coord_pID),
      lat = suppressWarnings(as.numeric(.data$lat)),
      lon = suppressWarnings(as.numeric(.data$lon)),
      coord_source = dplyr::coalesce(.data$coord_source, "travis_cached_coord")
    )

  parcel_df <- situs |>
    dplyr::transmute(
      county = "Travis",
      parcel_id = paste0("TRAVIS:", .data$situs_pID),
      source_property_id = as.character(.data$situs_pID),
      situs_address = .data$situs_address,
      situs_city = .data$situs_city,
      situs_state = .data$situs_state,
      situs_zip = .data$situs_zip
    ) |>
    dplyr::left_join(coords, by = "source_property_id", relationship = "one-to-one") |>
    filter_points_to_austin(label = "Travis parcels")

  valuations <- stream_tcad_section("valuations", "valuation", travis_valuations_cache_path)
  land_value_col <- choose_first_existing(valuations, c(
    "valuation_landMarketValue", "valuation_landMktValue", "valuation_landValue",
    "valuation_land", "valuation_landMarket", "valuation_totalLandMktValue",
    "valuation_landHS", "valuation_landNHS", "valuation_landNonHS",
    "valuation_marketLandValue", "valuation_totalLandValue"
  ))
  if (is.na(land_value_col)) land_value_col <- choose_by_regex(valuations, "land.*(mkt|market|value)|land.*val")
  tax_year_col <- choose_first_existing(valuations, c("valuation_year", "valuation_taxYear", "valuation_appraisalYear", "valuation_rollYear"))
  if (is.na(tax_year_col)) tax_year_col <- choose_by_regex(valuations, "year")

  if (!is.na(land_value_col) && "valuation_pID" %in% names(valuations)) {
    land_values <- valuations |>
      dplyr::transmute(
        source_property_id = as.character(.data$valuation_pID),
        current_land_value = suppressWarnings(as.numeric(.data[[land_value_col]])),
        land_value_tax_year = if (!is.na(tax_year_col)) suppressWarnings(as.integer(.data[[tax_year_col]])) else NA_integer_
      ) |>
      dplyr::arrange(.data$source_property_id, dplyr::desc(.data$land_value_tax_year)) |>
      dplyr::group_by(.data$source_property_id) |>
      dplyr::summarise(
        current_land_value = first_non_missing(.data$current_land_value),
        land_value_tax_year = suppressWarnings(as.integer(first_non_missing(.data$land_value_tax_year))),
        .groups = "drop"
      )
    parcel_df <- parcel_df |>
      dplyr::left_join(land_values, by = "source_property_id", relationship = "one-to-one") |>
      dplyr::mutate(land_value_source = paste0("tcad_valuations.", land_value_col))
  } else {
    warning("Could not identify Travis valuation land-value field; Travis land values will be NA.")
    parcel_df <- parcel_df |>
      dplyr::mutate(current_land_value = NA_real_, land_value_tax_year = NA_integer_, land_value_source = "tcad_valuations_field_not_found")
  }

  deeds <- stream_tcad_section("deeds", "deed", travis_deeds_cache_path)
  date_col <- choose_first_existing(deeds, c("deed_deedDt", "deed_deedDate", "deed_date", "deed_recordedDate"))
  buyer_col <- choose_first_existing(deeds, c("deed_buyerLine", "deed_buyerline", "deed_buyer"))
  seller_col <- choose_first_existing(deeds, c("deed_sellerLine", "deed_sellerline", "deed_seller"))

  readr::write_csv(
    tibble::tibble(
      county = "Travis",
      valuation_land_value_col = land_value_col,
      valuation_tax_year_col = tax_year_col,
      deed_date_col = date_col,
      deed_buyer_col = buyer_col,
      deed_seller_col = seller_col
    ),
    travis_selected_fields_path
  )

  if (!is.na(date_col) && "deed_pID" %in% names(deeds)) {
    deed_rows <- deeds |>
      dplyr::mutate(
        transaction_year = lubridate::year(parse_any_date(.data[[date_col]])),
        buyer_is_corporate = if (!is.na(buyer_col)) is_corporate_name(.data[[buyer_col]]) else NA,
        seller_is_corporate = if (!is.na(seller_col)) is_corporate_name(.data[[seller_col]]) else NA
      ) |>
      dplyr::filter(!is.na(.data$transaction_year)) |>
      dplyr::transmute(
        parcel_id = paste0("TRAVIS:", as.character(.data$deed_pID)),
        transaction_year = as.integer(.data$transaction_year),
        buyer_is_corporate = .data$buyer_is_corporate,
        seller_is_corporate = .data$seller_is_corporate
      )

    transactions <- deed_rows |>
      dplyr::group_by(.data$parcel_id, .data$transaction_year) |>
      dplyr::summarise(
        transaction_count = dplyr::n(),
        corporate_buyer_transaction_count = sum(.data$buyer_is_corporate, na.rm = TRUE),
        corporate_seller_transaction_count = sum(.data$seller_is_corporate, na.rm = TRUE),
        corporate_party_transaction_count = sum(.data$buyer_is_corporate | .data$seller_is_corporate, na.rm = TRUE),
        transaction_source = "tcad_deeds",
        .groups = "drop"
      )
  } else {
    warning("Could not identify Travis deed date field; Travis transaction counts will be zero/NA.")
    transactions <- tibble::tibble()
  }

  append_transactions(parcel_df, transactions, "tcad_deeds_no_transaction")
}

# -----------------------------
# Williamson
# -----------------------------

build_williamson <- function() {
  message("\nBuilding Williamson parcel-year data ...")
  if (!file.exists(wcad_property_path) || !file.exists(wcad_parcels_path)) {
    warning("Williamson WCAD files missing; skipping Williamson.")
    return(tibble::tibble())
  }

  wcad_parcels <- readRDS(wcad_parcels_path) |>
    dplyr::mutate(
      PropertyID = as.character(.data$propertyid),
      wcad_parcel_id = as.character(.data$parcelid)
    )
  wcad_points <- sf::st_point_on_surface(wcad_parcels)
  point_xy <- sf::st_coordinates(wcad_points)
  wcad_lookup <- wcad_parcels |>
    dplyr::mutate(lon = point_xy[, "X"], lat = point_xy[, "Y"]) |>
    sf::st_drop_geometry() |>
    dplyr::transmute(
      PropertyID = .data$PropertyID,
      wcad_parcel_id = .data$wcad_parcel_id,
      parcel_siteaddress = .data$siteaddress,
      lon = .data$lon,
      lat = .data$lat
    ) |>
    dplyr::group_by(.data$PropertyID) |>
    dplyr::summarise(
      wcad_parcel_id = first_non_missing(.data$wcad_parcel_id),
      parcel_siteaddress = first_non_missing(.data$parcel_siteaddress),
      lon = suppressWarnings(as.numeric(first_non_missing(.data$lon))),
      lat = suppressWarnings(as.numeric(first_non_missing(.data$lat))),
      .groups = "drop"
    )

  property <- readr::read_csv(wcad_property_path, show_col_types = FALSE, col_types = readr::cols(.default = "c")) |>
    dplyr::distinct(.data$PropertyID, .keep_all = TRUE)

  parcel_df <- property |>
    dplyr::mutate(PropertyID = as.character(.data$PropertyID)) |>
    dplyr::left_join(wcad_lookup, by = "PropertyID", relationship = "one-to-one") |>
    dplyr::transmute(
      county = "Williamson",
      parcel_id = paste0("WILLIAMSON:", dplyr::coalesce(.data$wcad_parcel_id, .data$QuickRefID, paste0("WCAD-", .data$PropertyID))),
      source_property_id = .data$PropertyID,
      situs_address = dplyr::coalesce(.data$SitusAddress, .data$Address, .data$PropertyAddress, .data$parcel_siteaddress),
      situs_city = .data$City,
      situs_state = dplyr::coalesce(.data$State, "TX"),
      situs_zip = sub("-.*", "", .data$Zip),
      lat = suppressWarnings(as.numeric(.data$lat)),
      lon = suppressWarnings(as.numeric(.data$lon)),
      coord_source = dplyr::if_else(!is.na(.data$lat) & !is.na(.data$lon), "wcad_parcel_point_on_surface", NA_character_),
      current_land_value = suppressWarnings(as.numeric(.data$TotalLandMktValue)),
      land_value_tax_year = suppressWarnings(as.integer(.data$`Tax Year`)),
      land_value_source = "wcad_property_certified.TotalLandMktValue"
    ) |>
    filter_points_to_austin(label = "Williamson parcels")

  parcel_df |>
    dplyr::mutate(
      transaction_year = NA_integer_,
      transaction_count = NA_integer_,
      corporate_buyer_transaction_count = NA_integer_,
      corporate_seller_transaction_count = NA_integer_,
      corporate_party_transaction_count = NA_integer_,
      transaction_source = "not_available_in_current_wcad_exports"
    ) |>
    write_empty_if_missing()
}

# -----------------------------
# Hays
# -----------------------------

read_hays_export_records <- function(zip_path) {
  extract_dir <- file.path(data_dir, "hays", "property_export_unzipped")
  dir.create(extract_dir, showWarnings = FALSE, recursive = TRUE)
  utils::unzip(zip_path, exdir = extract_dir)
  nested_zips <- list.files(extract_dir, pattern = "\\.zip$", full.names = TRUE, recursive = TRUE, ignore.case = TRUE)
  if (length(nested_zips)) {
    nested_dir <- file.path(extract_dir, "nested")
    dir.create(nested_dir, showWarnings = FALSE, recursive = TRUE)
    purrr::walk(nested_zips, function(path) {
      target_dir <- file.path(nested_dir, tools::file_path_sans_ext(basename(path)))
      dir.create(target_dir, showWarnings = FALSE, recursive = TRUE)
      utils::unzip(path, exdir = target_dir)
    })
  }
  data_files <- list.files(extract_dir, pattern = "\\.(csv|txt)$", full.names = TRUE, recursive = TRUE, ignore.case = TRUE)
  purrr::map_dfr(data_files, function(path) {
    readr::read_csv(path, col_types = readr::cols(.default = "c"), trim_ws = TRUE, progress = FALSE) |>
      dplyr::mutate(source_file = basename(path))
  })
}

split_hays_records <- function(rows) {
  pick <- function(df, name) {
    if (name %in% names(df)) df[[name]] else rep(NA_character_, nrow(df))
  }
  property_raw <- rows |> dplyr::filter(.data$RecordType == "1")
  sales_raw <- rows |> dplyr::filter(.data$RecordType == "6")
  list(
    property = tibble::tibble(
      PropertyID = pick(property_raw, "PropertyID"),
      QuickRefID = pick(property_raw, "QuickRefID"),
      PropertyNumber = pick(property_raw, "PropertyNumber"),
      SitusAddress = pick(property_raw, "Situs"),
      SitusCity = pick(property_raw, "SitusCity"),
      SitusState = pick(property_raw, "SitusState"),
      SitusZip = pick(property_raw, "SitusZip"),
      CurrLandValue = pick(property_raw, "CurrLandValue"),
      LandValue = pick(property_raw, "LandValue")
    ),
    sales = tibble::tibble(
      PropertyID = pick(sales_raw, "PropertyID"),
      QuickRefID = pick(sales_raw, "QuickRefID"),
      SaleDate = pick(sales_raw, "SaleDate"),
      DeedDate = pick(sales_raw, "DeedDate"),
      PrevOwnerName = pick(sales_raw, "PrevOwnerName")
    )
  )
}

build_hays <- function() {
  message("\nBuilding Hays parcel-year data ...")
  if (!file.exists(hays_property_zip_path) || !file.exists(hays_parcels_path)) {
    warning("Hays files missing; skipping Hays.")
    return(tibble::tibble())
  }

  rows <- read_hays_export_records(hays_property_zip_path)
  tables <- split_hays_records(rows)

  parcels <- readRDS(hays_parcels_path)
  parcel_points <- sf::st_point_on_surface(parcels)
  point_xy <- sf::st_coordinates(parcel_points)
  parcel_lookup <- parcels |>
    dplyr::mutate(lon = point_xy[, "X"], lat = point_xy[, "Y"]) |>
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

  parcel_df <- tables$property |>
    dplyr::distinct(.data$PropertyID, .keep_all = TRUE) |>
    dplyr::left_join(parcel_lookup, by = "QuickRefID", relationship = "many-to-one") |>
    dplyr::transmute(
      county = "Hays",
      parcel_id = paste0("HAYS:", dplyr::coalesce(.data$QuickRefID, .data$PropertyID)),
      source_property_id = .data$PropertyID,
      situs_address = .data$SitusAddress,
      situs_city = .data$SitusCity,
      situs_state = dplyr::coalesce(.data$SitusState, "TX"),
      situs_zip = sub("-.*", "", .data$SitusZip),
      lat = suppressWarnings(as.numeric(.data$lat)),
      lon = suppressWarnings(as.numeric(.data$lon)),
      coord_source = dplyr::if_else(!is.na(.data$lat) & !is.na(.data$lon), "hays_parcel_point_on_surface", NA_character_),
      current_land_value = coalesce_numeric(.data$CurrLandValue, .data$LandValue),
      land_value_tax_year = 2025L,
      land_value_source = "hays_property_export.CurrLandValue"
    ) |>
    filter_points_to_austin(label = "Hays parcels")

  sales <- tables$sales |>
    dplyr::mutate(
      transaction_date = parse_any_date(coalesce_character(.data$DeedDate, .data$SaleDate)),
      transaction_year = lubridate::year(.data$transaction_date),
      seller_is_corporate = is_corporate_name(.data$PrevOwnerName)
    ) |>
    dplyr::filter(!is.na(.data$transaction_year)) |>
    dplyr::transmute(
      parcel_id = paste0("HAYS:", dplyr::coalesce(.data$QuickRefID, .data$PropertyID)),
      transaction_year = as.integer(.data$transaction_year),
      seller_is_corporate = .data$seller_is_corporate
    )

  transactions <- sales |>
    dplyr::group_by(.data$parcel_id, .data$transaction_year) |>
    dplyr::summarise(
      transaction_count = dplyr::n(),
      corporate_buyer_transaction_count = NA_integer_,
      corporate_seller_transaction_count = sum(.data$seller_is_corporate, na.rm = TRUE),
      corporate_party_transaction_count = sum(.data$seller_is_corporate, na.rm = TRUE),
      transaction_source = "hays_sales",
      .groups = "drop"
    )

  append_transactions(parcel_df, transactions, "hays_sales_no_transaction")
}

# -----------------------------
# Build and write
# -----------------------------

county_builders <- list(
  Travis = build_travis,
  Williamson = build_williamson,
  Hays = build_hays
)

unknown_counties <- setdiff(COUNTIES_TO_BUILD, names(county_builders))
if (length(unknown_counties)) {
  stop("Unknown AUSTIN_LAND_TX_COUNTIES value(s): ", paste(unknown_counties, collapse = ", "), call. = FALSE)
}

county_outputs <- purrr::map(COUNTIES_TO_BUILD, function(county) county_builders[[county]]())

austin_parcel_year_land_transactions <- dplyr::bind_rows(county_outputs) |>
  write_empty_if_missing() |>
  dplyr::arrange(.data$county, .data$parcel_id, .data$transaction_year, .data$transaction_source)

duplicate_keys <- austin_parcel_year_land_transactions |>
  dplyr::count(.data$parcel_id, .data$transaction_year, .data$transaction_source) |>
  dplyr::filter(.data$n > 1)
if (nrow(duplicate_keys) > 0L) {
  warning("Duplicate parcel_id + transaction_year + transaction_source rows found: ", nrow(duplicate_keys))
}

readr::write_csv(austin_parcel_year_land_transactions, out_path)

qa_summary <- austin_parcel_year_land_transactions |>
  dplyr::group_by(.data$county) |>
  dplyr::summarise(
    n_rows = dplyr::n(),
    n_parcels = dplyr::n_distinct(.data$parcel_id),
    n_with_land_value = dplyr::n_distinct(.data$parcel_id[!is.na(.data$current_land_value)]),
    n_with_transaction_year = dplyr::n_distinct(.data$parcel_id[!is.na(.data$transaction_year)]),
    min_transaction_year = suppressWarnings(min(.data$transaction_year, na.rm = TRUE)),
    max_transaction_year = suppressWarnings(max(.data$transaction_year, na.rm = TRUE)),
    transaction_count = sum(.data$transaction_count, na.rm = TRUE),
    corporate_buyer_transaction_count = sum(.data$corporate_buyer_transaction_count, na.rm = TRUE),
    corporate_seller_transaction_count = sum(.data$corporate_seller_transaction_count, na.rm = TRUE),
    corporate_party_transaction_count = sum(.data$corporate_party_transaction_count, na.rm = TRUE),
    .groups = "drop"
  ) |>
  dplyr::mutate(
    min_transaction_year = dplyr::if_else(is.infinite(.data$min_transaction_year), NA_real_, .data$min_transaction_year),
    max_transaction_year = dplyr::if_else(is.infinite(.data$max_transaction_year), NA_real_, .data$max_transaction_year)
  )

readr::write_csv(qa_summary, qa_path)

message("\nWrote: ", normalizePath(out_path))
message("QA summary: ", normalizePath(qa_path))
print(qa_summary)
