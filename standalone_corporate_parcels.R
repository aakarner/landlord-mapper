# standalone_corporate_parcels.R
#
# Purpose
# -------
# Downloads the TCAD Special Export (JSON) from traviscad.org and identifies
# residential parcels in Travis County that are likely owned by corporate or
# "financialized" interests.
#
# Output
# ------
# corporate_owned_parcels.csv  – one row per matching parcel, written to the
#                                 current working directory.
#
# How to run
# ----------
# 1. Open this file in RStudio (or any R console).
# 2. Run it section-by-section with Ctrl+Enter, or all at once with
#    source("standalone_corporate_parcels.R").
#
# Requirements
# ------------
# System tool (must be on PATH):
#   jq  https://jqlang.github.io/jq/download/
#       macOS:  brew install jq
#       Linux:  sudo apt-get install jq
#
# R packages (installed automatically if missing):
#   rvest, dplyr, jsonlite, readr, sf, tigris
#
# Memory note
# -----------
# This script uses a chunked/jq-based approach: each nested section of the
# TCAD JSON is streamed out of the ZIP via `unzip -p | jq` and fed into R
# page-by-page with jsonlite::stream_in().  Only one page (PAGE_SIZE rows) is
# held in memory at a time, so a MacBook Air with 8 GB of RAM can run this
# comfortably without loading the full multi-GB file at once.
#
# No Python, no Selenium, no Census API key, no ijson needed.
# ──────────────────────────────────────────────────────────────────────────────

# ── 0. Install / load required packages ──────────────────────────────────────

required_pkgs <- c("rvest", "dplyr", "jsonlite", "readr", "sf", "tigris")
missing_pkgs  <- required_pkgs[!required_pkgs %in% installed.packages()[, "Package"]]
if (length(missing_pkgs)) {
  message("Installing missing packages: ", paste(missing_pkgs, collapse = ", "))
  install.packages(missing_pkgs)
}

library(rvest)
library(dplyr)
library(jsonlite)
library(readr)
library(sf)
library(tigris)

# Check for jq (required for low-memory streaming extraction)
if (nchar(Sys.which("jq")) == 0L) {
  stop(
    "'jq' is required but was not found on your PATH.\n",
    "  macOS:  brew install jq\n",
    "  Linux:  sudo apt-get install jq\n",
    "  See:    https://jqlang.github.io/jq/download/"
  )
}

# ── 1. Configuration ──────────────────────────────────────────────────────────

options(timeout = 7200)  # allow up to 2 hours for large downloads

zip_path  <- "tcad_special_export.zip"   # where to save / look for the ZIP
out_path  <- "corporate_owned_parcels.csv"
PAGE_SIZE <- 10000L   # rows per page passed to stream_in() — tune down if RAM is tight

parcel_poly_zip_path <- "data/Parcel_poly.zip"
address_zip_path <- "data/Addresses.zip"
address_alias_path <- "data/address_aliases.csv"
arcgis_geocode_lookup_path <- "output/geocoding_arcgis_accepted_lookup.csv"
austin_jurisdiction_boundary_path <- "data/BOUNDARIES_jurisdictions_20260429.geojson"
AUSTIN_JURISDICTION_TYPES <- c("FULL")
USE_PARCEL_POLYGON <- TRUE       # preferred shapefile fallback: geoID joins to parcel PID_10
USE_ADDRESS_SHAPEFILE <- TRUE    # looser fallback by exact situs address point
USE_NEAREST_ADDRESS_POINT <- TRUE # approximate fallback by nearby address points on same street/ZIP
ADDRESS_NEAREST_MAX_DELTA <- 20   # max house-number distance for nearest-address fallback

options(tigris_use_cache = TRUE)  # cache tigris downloads between runs
dir.create("output", showWarnings = FALSE)

# ── 2. Download TCAD data ─────────────────────────────────────────────────────
# Scrapes traviscad.org/publicinformation for the latest "Special Export (JSON)"
# ZIP link and downloads it.  Skips download if the file already exists.

if (file.exists(zip_path)) {
  message("ZIP already present — skipping download: ", zip_path)
} else {
  message("Scraping download link from traviscad.org/publicinformation ...")
  page  <- rvest::read_html("https://traviscad.org/publicinformation")
  links <- page |> html_elements(".fusion-li-item-content a")
  url   <- links[grepl("Special.*export.*JSON", links, ignore.case = TRUE)] |>
             html_attr("href")

  if (!length(url) || is.na(url[1])) {
    stop(
      "Could not find the 'Special Export (JSON)' link on traviscad.org.\n",
      "  Visit https://traviscad.org/publicinformation, copy the ZIP URL,\n",
      "  download it manually, and save it as: ", zip_path
    )
  }

  # Use the first matching link
  url <- url[1]
  message("Downloading: ", url)
  message("(This may take several minutes on a slow connection.)")
  download.file(url, zip_path, mode = "wb", quiet = FALSE)
  message("Download complete.")
}

# ── 3. Identify the JSON file inside the ZIP ──────────────────────────────────

manifest  <- unzip(zip_path, list = TRUE)
json_name <- manifest$Name[1]
size_gb   <- round(manifest$Length[1] / 1e9, 2)

message("JSON file in ZIP: ", json_name, " (", size_gb, " GB uncompressed)")
message("Using chunked jq streaming — each section is read ", PAGE_SIZE,
        " rows at a time.")

# ── 4. Stream each nested section from the ZIP via jq ─────────────────────────
# The TCAD JSON is a top-level array of parcel objects.  Each parcel has nested
# arrays for owners, situses, propertyProfile, and propertyCharacteristics.
#
# For each section we build a shell pipeline:
#   unzip -p <zip> <json> | jq -c '<filter>'
# jq expands the nested array into one NDJSON object per row and attaches the
# parent parcel's pID.  jsonlite::stream_in() reads the NDJSON output PAGE_SIZE
# rows at a time, so the peak in-process memory is roughly:
#   PAGE_SIZE * <columns> * <bytes/value>  ≈  a few MB per page.

ALLOWED_SECTIONS <- c("owners", "situses", "propertyProfile",
                      "propertyCharacteristics", "links")

stream_section <- function(zip_path, json_name, section, prefix,
                           page_size = PAGE_SIZE) {
  # Whitelist the section name to prevent command injection.
  if (!section %in% ALLOWED_SECTIONS) {
    stop("Unknown section: ", section,
         "\n  Must be one of: ", paste(ALLOWED_SECTIONS, collapse = ", "))
  }

  # jq filter: for each parcel, expand the nested array and attach pID.
  jq_filter_1 <- "fromstream(1|truncate_stream(inputs))"
  jq_filter_2 <- sprintf(
  'if has("%s") then .pID as $pid | (.%s // [])[] | . + {pID: $pid} else empty end',
  section, section
)
  cmd <- sprintf(
    "unzip -p %s %s | jq -cn --stream %s | jq -c %s",
    shQuote(zip_path),
    shQuote(json_name),
    shQuote(jq_filter_1),
    shQuote(jq_filter_2)
  )
  
  message("  Streaming section '", section, "' ...")
  con    <- pipe(cmd, open = "r")
  on.exit(close(con), add = TRUE)   # always close even if stream_in() errors

  chunks  <- list()
  n_pages <- 0L
  jsonlite::stream_in(
    con,
    handler = function(page) {
      n_pages <<- n_pages + 1L
      chunks[[n_pages]] <<- page   # pre-index avoids repeated length() calls
      message("  Page ", n_pages, " — ", format(n_pages * page_size, big.mark = ","), " rows processed")

    },
    pagesize = page_size,
    verbose  = FALSE
  )

  df <- dplyr::bind_rows(chunks)
  if (nrow(df) == 0L) return(df)

  # Prefix all column names; keep pID as <prefix>_pID for later joins.
  names(df) <- ifelse(
    names(df) == "pID",
    paste0(prefix, "_pID"),
    paste0(prefix, "_", names(df))
  )
  df
}

message("Streaming parcel sections from ZIP ...")
owners_df <- stream_section(zip_path, json_name, "owners",                  "owner")
situs_df  <- stream_section(zip_path, json_name, "situses",                 "situs")
prof_df   <- stream_section(zip_path, json_name, "propertyProfile",         "propertyProf")
char_df   <- stream_section(zip_path, json_name, "propertyCharacteristics", "propertyChar")
links_df  <- stream_section(zip_path, json_name, "links",                   "link")

# ── 4b. Stream parcel-level latitude / longitude ───────────────────────────
# TCAD stores geographic coordinates at the top level of each parcel object
# (fields: lat, lon).  These are NOT nested inside a sub-array, so we use a
# simpler jq filter that selects each parcel and emits only {pID, lat, lon}.

stream_parcel_coords <- function(zip_path, json_name, page_size = PAGE_SIZE) {
  jq_filter_1 <- "fromstream(1|truncate_stream(inputs))"
  jq_filter_2 <- paste(
    'select(has("pID"))',
    '| (try (.geometry | fromjson) catch null) as $g',
    '| {',
    '    pID: .pID,',
    '    geoID: (.propertyIdentification[0]?.geoID // null),',
    '    lat: (if ($g|type)=="array" then ($g[0] // null) else null end),',
    '    lon: (if ($g|type)=="array" then ($g[1] // null) else null end)',
    '  }',
    collapse = " "
  )

  cmd <- sprintf(
    "unzip -p %s %s | jq -cn --stream %s | jq -c %s",
    shQuote(zip_path),
    shQuote(json_name),
    shQuote(jq_filter_1),
    shQuote(jq_filter_2)
  )

  message("  Streaming parcel coordinates ...")
  con     <- pipe(cmd, open = "r")
  on.exit(close(con), add = TRUE)
  chunks  <- list()
  n_pages <- 0L

  jsonlite::stream_in(
    con,
    handler = function(page) {
      n_pages <<- n_pages + 1L
      chunks[[n_pages]] <<- page
      message("  Page ", n_pages, " — ",
              format(n_pages * page_size, big.mark = ","), " rows processed")
    },
    pagesize = page_size,
    verbose  = FALSE
  )

  df <- dplyr::bind_rows(chunks)
  if (nrow(df) == 0L) return(df)
  names(df)[names(df) == "pID"] <- "coord_pID"
  df$coord_source <- ifelse(is.na(df$lat) | is.na(df$lon), NA_character_, "json_geometry")
  df
}

coords_df <- stream_parcel_coords(zip_path, json_name)
gc()

# ── 4c. Coordinate recovery helpers ─────────────────────────────────────────
# The raw JSON coordinate pull is intentionally narrow: one row per parcel/account
# with pID, geoID, lat, lon, and a coord_source label.  A sizeable share of TCAD
# accounts have geometry = "[null, null]", especially condo/unit/common-area style
# records where the account does not map cleanly to a single parcel point.
#
# The helper functions below support the staged fill strategy used immediately
# after their definitions:
#   1. coord_summary() prints and stores recovery counts after each stage.
#   2. fill_missing_coords() applies a lookup table only to rows still missing
#      lat/lon and records the stage in coord_source.
#   3. street_number_numeric() normalizes house numbers for address-point matching.
#   4. nearest_address_lookup() handles the final approximate fallback by finding
#      nearby address points on the same street/type/ZIP.
#
# Keeping these helpers here makes the coordinate pipeline self-contained while
# avoiding repeated mutate/join/coalesce blocks for each recovery source.

coord_summary <- function(df, stage) {
  out <- df |>
    summarise(
      stage = stage,
      n_total = n(),
      n_with_coords = sum(!is.na(lat) & !is.na(lon)),
      n_still_missing = sum(is.na(lat) | is.na(lon))
    )
  message(
    "  ", stage, ": ",
    format(out$n_with_coords, big.mark = ","), " / ",
    format(out$n_total, big.mark = ","), " parcels have coordinates; ",
    format(out$n_still_missing, big.mark = ","), " still missing."
  )
  out
}

fill_missing_coords <- function(coords_df, lookup, by, source_name) {
  if (!"coord_source" %in% names(coords_df)) {
    coords_df$coord_source <- ifelse(
      is.na(coords_df$lat) | is.na(coords_df$lon),
      NA_character_,
      "existing_coord"
    )
  }

  before_missing <- sum(is.na(coords_df$lat) | is.na(coords_df$lon))
  coords_df <- coords_df |>
    left_join(lookup, by = by) |>
    mutate(
      coord_was_missing = is.na(lat) | is.na(lon),
      lat = dplyr::coalesce(lat, lat_fill),
      lon = dplyr::coalesce(lon, lon_fill),
      coord_source = ifelse(
        coord_was_missing & !is.na(lat) & !is.na(lon),
        source_name,
        coord_source
      )
    ) |>
    select(-lat_fill, -lon_fill, -coord_was_missing)
  after_missing <- sum(is.na(coords_df$lat) | is.na(coords_df$lon))
  message(
    "  Filled ", format(before_missing - after_missing, big.mark = ","),
    " parcels from ", source_name, "."
  )
  coords_df
}

street_number_numeric <- function(x) {
  x <- trimws(as.character(x))
  out <- suppressWarnings(as.numeric(sub("^([0-9]+).*", "\\1", x)))
  out[!grepl("^[0-9]+", x)] <- NA_real_
  out
}

blank_to_na <- function(x) {
  x <- trimws(as.character(x))
  x[x == "" | toupper(x) == "NA"] <- NA_character_
  x
}

normalize_street_token <- function(x) {
  x <- toupper(blank_to_na(x))
  x <- gsub("[[:punct:]]", " ", x)
  x <- gsub("[[:space:]]+", " ", x)
  trimws(x)
}

normalize_street_type <- function(x) {
  x <- normalize_street_token(x)
  dplyr::case_when(
    is.na(x) ~ NA_character_,
    x %in% c("HIGHWAY", "HWY", "HIWY", "HY") ~ "HWY",
    x %in% c("EXPRESSWAY", "EXPRESS", "EXPY", "EXWY") ~ "EXPY",
    x %in% c("BOULEVARD", "BLVD") ~ "BLVD",
    x %in% c("AVENUE", "AVE") ~ "AVE",
    x %in% c("STREET", "ST") ~ "ST",
    x %in% c("ROAD", "RD") ~ "RD",
    x %in% c("DRIVE", "DR") ~ "DR",
    x %in% c("LANE", "LN") ~ "LN",
    x %in% c("TRAIL", "TRL") ~ "TRL",
    x %in% c("CIRCLE", "CIR") ~ "CIR",
    x %in% c("PARKWAY", "PKWY") ~ "PKWY",
    x %in% c("BEND", "BND") ~ "BND",
    x %in% c("RIDGE", "RDG", "RG") ~ "RDG",
    TRUE ~ x
  )
}

load_address_aliases <- function(path = address_alias_path) {
  if (!file.exists(path)) {
    message("  Address alias table not found; skipping aliases: ", path)
    return(tibble(
      source_street_name = character(),
      source_street_suffix = character(),
      source_zip = character(),
      target_street_name = character(),
      target_street_suffix = character(),
      target_zip = character(),
      confidence = numeric()
    ))
  }

  readr::read_csv(path, show_col_types = FALSE, col_types = readr::cols(.default = "c")) |>
    mutate(
      source_street_name = normalize_street_token(source_street_name),
      source_street_suffix = normalize_street_type(source_street_suffix),
      source_zip = blank_to_na(sub("-.*", "", as.character(source_zip))),
      target_street_name = normalize_street_token(target_street_name),
      target_street_suffix = normalize_street_type(target_street_suffix),
      target_zip = blank_to_na(sub("-.*", "", as.character(target_zip))),
      confidence = suppressWarnings(as.numeric(confidence))
    ) |>
    filter(
      !is.na(source_street_name),
      !is.na(target_street_name),
      is.na(confidence) | confidence >= 0.90
    ) |>
    distinct(source_street_name, source_street_suffix, source_zip, .keep_all = TRUE)
}

apply_address_aliases <- function(df, name_col, suffix_col, zip_col, aliases) {
  if (nrow(aliases) == 0L) return(df)

  exact_aliases <- aliases |>
    filter(!is.na(source_zip)) |>
    select(
      source_street_name, source_street_suffix, source_zip,
      alias_name_exact = target_street_name,
      alias_suffix_exact = target_street_suffix,
      alias_zip_exact = target_zip
    )

  wildcard_aliases <- aliases |>
    filter(is.na(source_zip)) |>
    select(
      source_street_name, source_street_suffix,
      alias_name_any = target_street_name,
      alias_suffix_any = target_street_suffix,
      alias_zip_any = target_zip
    )

  out <- df |>
    left_join(
      exact_aliases,
      by = setNames(
        c("source_street_name", "source_street_suffix", "source_zip"),
        c(name_col, suffix_col, zip_col)
      ),
      relationship = "many-to-one"
    ) |>
    left_join(
      wildcard_aliases,
      by = setNames(
        c("source_street_name", "source_street_suffix"),
        c(name_col, suffix_col)
      ),
      relationship = "many-to-one"
    ) |>
    mutate(
      "{name_col}" := coalesce(alias_name_exact, alias_name_any, .data[[name_col]]),
      "{suffix_col}" := coalesce(alias_suffix_exact, alias_suffix_any, .data[[suffix_col]]),
      "{zip_col}" := coalesce(alias_zip_exact, alias_zip_any, .data[[zip_col]])
    ) |>
    select(
      -alias_name_exact, -alias_suffix_exact, -alias_zip_exact,
      -alias_name_any, -alias_suffix_any, -alias_zip_any
    )

  n_applied <- sum(
    !is.na(df[[name_col]]) &
      (
        dplyr::coalesce(df[[name_col]], "") != dplyr::coalesce(out[[name_col]], "") |
          dplyr::coalesce(df[[suffix_col]], "") != dplyr::coalesce(out[[suffix_col]], "") |
          dplyr::coalesce(df[[zip_col]], "") != dplyr::coalesce(out[[zip_col]], "")
      ),
    na.rm = TRUE
  )
  message("  Applied address aliases to ", format(n_applied, big.mark = ","), " situs rows.")
  out
}

nearest_address_lookup <- function(missing_situs_df, address_points_df,
                                   max_delta = ADDRESS_NEAREST_MAX_DELTA) {
  empty_lookup <- tibble(
    coord_pID = character(),
    lat_fill = numeric(),
    lon_fill = numeric(),
    nearest_address_method = character(),
    nearest_address_delta = numeric()
  )

  missing_situs_df <- missing_situs_df |>
    mutate(
      situs_addr_num = street_number_numeric(situs_streetNum),
      situs_streetName = normalize_street_token(situs_streetName),
      situs_streetSuffix = normalize_street_type(situs_streetSuffix),
      situs_zip = blank_to_na(sub("-.*", "", as.character(situs_zip)))
    ) |>
    filter(
      !is.na(coord_pID),
      !is.na(situs_addr_num),
      !is.na(situs_streetName),
      !is.na(situs_streetSuffix),
      !is.na(situs_zip)
  )

  if (nrow(missing_situs_df) == 0L || nrow(address_points_df) == 0L) {
    return(empty_lookup)
  }

  address_groups <- split(
    address_points_df,
    paste(
      address_points_df$STREET_NAM,
      address_points_df$STREET_TYP,
      address_points_df$ZIPCODE,
      sep = "\r"
    )
  )

  missing_groups <- split(
    missing_situs_df,
    paste(
      missing_situs_df$situs_streetName,
      missing_situs_df$situs_streetSuffix,
      missing_situs_df$situs_zip,
      sep = "\r"
    )
  )

  out <- lapply(names(missing_groups), function(key) {
    candidates <- address_groups[[key]]
    missing_rows <- missing_groups[[key]]
    if (is.null(candidates) || nrow(candidates) == 0L) return(NULL)

    candidates <- candidates |>
      arrange(addr_num) |>
      distinct(addr_num, .keep_all = TRUE)

    nums <- candidates$addr_num
    result_rows <- lapply(seq_len(nrow(missing_rows)), function(i) {
      target_num <- missing_rows$situs_addr_num[i]
      lower_idx <- findInterval(target_num, nums)
      upper_idx <- lower_idx + 1L

      lower <- if (lower_idx >= 1L) candidates[lower_idx, ] else NULL
      upper <- if (upper_idx <= nrow(candidates)) candidates[upper_idx, ] else NULL

      lower_dist <- if (is.null(lower)) Inf else abs(target_num - lower$addr_num)
      upper_dist <- if (is.null(upper)) Inf else abs(upper$addr_num - target_num)

      if (is.finite(lower_dist) && lower_dist == 0) {
        lat_fill <- lower$addr_lat
        lon_fill <- lower$addr_lon
        method <- "exact_numeric_address_point"
      } else if (is.finite(lower_dist) && is.finite(upper_dist) &&
                 lower_dist <= max_delta && upper_dist <= max_delta) {
        lat_fill <- median(c(lower$addr_lat, upper$addr_lat), na.rm = TRUE)
        lon_fill <- median(c(lower$addr_lon, upper$addr_lon), na.rm = TRUE)
        method <- "interpolated_between_address_points"
      } else if (min(lower_dist, upper_dist) <= max_delta) {
        nearest <- if (lower_dist <= upper_dist) lower else upper
        lat_fill <- nearest$addr_lat
        lon_fill <- nearest$addr_lon
        method <- "nearest_address_point"
      } else {
        return(NULL)
      }

      tibble(
        coord_pID = missing_rows$coord_pID[i],
        lat_fill = lat_fill,
        lon_fill = lon_fill,
        nearest_address_method = method,
        nearest_address_delta = min(lower_dist, upper_dist)
      )
    })

    bind_rows(result_rows)
  })

  out <- bind_rows(out)
  if (nrow(out) == 0L || !"coord_pID" %in% names(out)) {
    return(empty_lookup)
  }

  out |>
    group_by(coord_pID) |>
    summarise(
      lat_fill = median(lat_fill),
      lon_fill = median(lon_fill),
      nearest_address_method = paste(sort(unique(nearest_address_method)), collapse = ";"),
      nearest_address_delta = min(nearest_address_delta),
      .groups = "drop"
    )
}

coords_df <- coords_df |>
  mutate(
    coord_pID = as.character(coord_pID),
    geoID = as.character(geoID),
    lat = suppressWarnings(as.numeric(lat)),
    lon = suppressWarnings(as.numeric(lon))
  )

situs_df <- situs_df |>
  mutate(situs_pID = as.character(situs_pID))

if (all(c("link_pID", "link_linkedPID") %in% names(links_df))) {
  links_df <- links_df |>
    mutate(
      link_pID = as.character(link_pID),
      link_linkedPID = as.character(link_linkedPID)
    )
}

coords_summary <- coord_summary(coords_df, "raw JSON geometry")

# JSON-only fill 1: shared geoID.  This helps when one account in a split/stacked
# property has a coordinate and sibling accounts do not.
geoid_lookup <- coords_df |>
  filter(!is.na(lat), !is.na(lon), !is.na(geoID)) |>
  group_by(geoID) |>
  summarise(
    lat_fill = median(lat),
    lon_fill = median(lon),
    .groups = "drop"
  )

coords_df <- fill_missing_coords(
  coords_df,
  geoid_lookup,
  by = "geoID",
  source_name = "json_geoID"
)
coords_summary <- bind_rows(coords_summary, coord_summary(coords_df, "after geoID fill"))

# JSON-only fill 2: TCAD links.  Linked parcel IDs often point from a unit/account
# record to a related parent or sibling account.
if (all(c("link_pID", "link_linkedPID") %in% names(links_df))) {
  link_lookup <- links_df |>
    select(link_pID, link_linkedPID) |>
    inner_join(
      coords_df |> filter(!is.na(lat), !is.na(lon)),
      by = c("link_linkedPID" = "coord_pID")
    ) |>
    group_by(link_pID) |>
    summarise(
      lat_fill = median(lat),
      lon_fill = median(lon),
      .groups = "drop"
    )

  coords_df <- fill_missing_coords(
    coords_df,
    link_lookup,
    by = c("coord_pID" = "link_pID"),
    source_name = "json_linkedPID"
  )
} else {
  message("  No TCAD links were present in this export; skipping linkedPID fill.")
}
coords_summary <- bind_rows(coords_summary, coord_summary(coords_df, "after linkedPID fill"))

# JSON-only fill 3: repeated situs address.  This is less exact than geoID/links,
# but remains inside TCAD JSON and recovers many condo/apartment-style records.
address_aliases <- load_address_aliases(address_alias_path)

primary_situs_df <- situs_df |>
  mutate(
    situs_zip = blank_to_na(sub("-.*", "", as.character(situs_zip))),
    situs_streetNum = trimws(as.character(situs_streetNum)),
    situs_streetName = normalize_street_token(situs_streetName),
    situs_streetSuffix = normalize_street_type(situs_streetSuffix)
  ) |>
  arrange(desc(situs_primarySitus)) |>
  distinct(situs_pID, .keep_all = TRUE) |>
  select(situs_pID, situs_streetNum, situs_streetName, situs_streetSuffix, situs_zip) |>
  apply_address_aliases(
    name_col = "situs_streetName",
    suffix_col = "situs_streetSuffix",
    zip_col = "situs_zip",
    aliases = address_aliases
  )

address_lookup <- primary_situs_df |>
  inner_join(
    coords_df |> filter(!is.na(lat), !is.na(lon)),
    by = c("situs_pID" = "coord_pID")
  ) |>
  filter(!is.na(situs_streetNum), !is.na(situs_streetName), !is.na(situs_zip)) |>
  group_by(situs_streetNum, situs_streetName, situs_zip) |>
  summarise(
    lat_fill = median(lat),
    lon_fill = median(lon),
    .groups = "drop"
  )

coords_df <- coords_df |>
  left_join(primary_situs_df, by = c("coord_pID" = "situs_pID")) |>
  fill_missing_coords(
    address_lookup,
    by = c("situs_streetNum", "situs_streetName", "situs_zip"),
    source_name = "json_situs_address"
  ) |>
  select(coord_pID, geoID, lat, lon, coord_source)

coords_summary <- bind_rows(coords_summary, coord_summary(coords_df, "after JSON address fill"))

# Preferred shapefile fallback: join to TCAD parcel polygons by geoID/PID_10.
# This is much tighter than address matching because it uses the parcel identifier
# already present in the JSON.
parcel_polygon_keys <- character()
if (USE_PARCEL_POLYGON && file.exists(parcel_poly_zip_path)) {
  message("  Using parcel polygon fallback: ", parcel_poly_zip_path)
  parcel_layer <- paste0("/vsizip/", normalizePath(parcel_poly_zip_path), "/Parcel_poly.shp")
  parcel_points <- sf::st_read(parcel_layer, quiet = TRUE) |>
    sf::st_point_on_surface() |>
    sf::st_transform(4326)
  parcel_xy <- sf::st_coordinates(parcel_points)
  parcel_lookup <- parcel_points |>
    sf::st_drop_geometry() |>
    mutate(
      PID_10 = as.character(PID_10),
      lat_fill = parcel_xy[, "Y"],
      lon_fill = parcel_xy[, "X"]
    ) |>
    filter(!is.na(PID_10), !is.na(lat_fill), !is.na(lon_fill)) |>
    group_by(PID_10) |>
    summarise(
      lat_fill = median(lat_fill),
      lon_fill = median(lon_fill),
      .groups = "drop"
    )
  parcel_polygon_keys <- parcel_lookup$PID_10

  coords_df <- fill_missing_coords(
    coords_df,
    parcel_lookup,
    by = c("geoID" = "PID_10"),
    source_name = "parcel_polygon"
  )

  coords_summary <- bind_rows(coords_summary, coord_summary(coords_df, "after parcel polygon fill"))
} else if (USE_PARCEL_POLYGON) {
  warning("Parcel polygon fallback requested but not found: ", parcel_poly_zip_path)
}

# Last-resort fallback: address point shapefile.  The exact match is useful for
# remaining records whose parcel IDs do not line up but whose situs address does.
if (USE_ADDRESS_SHAPEFILE && file.exists(address_zip_path)) {
  message("  Using address shapefile fallback: ", address_zip_path)
  address_layer <- paste0("/vsizip/", normalizePath(address_zip_path), "/Addresses.shp")
  address_points <- sf::st_read(address_layer, quiet = TRUE) |>
    sf::st_transform(4326)
  address_xy <- sf::st_coordinates(address_points)

  address_points_for_matching <- address_points |>
    sf::st_drop_geometry() |>
    mutate(
      STREET_NUM = trimws(as.character(ADDRESS)),
      STREET_NAM = normalize_street_token(STREET_NAM),
      STREET_TYP = normalize_street_type(STREET_TYP),
      ZIPCODE = blank_to_na(sub("-.*", "", as.character(ZIPCODE))),
      ADDR_PID = dplyr::coalesce(as.character(PID), as.character(PARCEL_ID)),
      addr_num = street_number_numeric(ADDRESS),
      addr_lon = address_xy[, "X"],
      addr_lat = address_xy[, "Y"]
    ) |>
    filter(!is.na(STREET_NUM), !is.na(STREET_NAM), !is.na(ZIPCODE), !is.na(addr_lat), !is.na(addr_lon))

  addr_lookup <- address_points_for_matching |>
    group_by(STREET_NUM, STREET_NAM, STREET_TYP, ZIPCODE) |>
    summarise(
      lat_fill = median(addr_lat),
      lon_fill = median(addr_lon),
      .groups = "drop"
    )

  coords_df <- coords_df |>
    left_join(primary_situs_df, by = c("coord_pID" = "situs_pID")) |>
    fill_missing_coords(
      addr_lookup,
      by = c(
        "situs_streetNum" = "STREET_NUM",
        "situs_streetName" = "STREET_NAM",
        "situs_streetSuffix" = "STREET_TYP",
        "situs_zip" = "ZIPCODE"
      ),
      source_name = "address_point_exact"
    ) |>
    select(coord_pID, geoID, lat, lon, coord_source)

  coords_summary <- bind_rows(coords_summary, coord_summary(coords_df, "after exact address point fill"))

  # Some TCAD situs rows have enough street information to identify a unique
  # address point but are missing ZIP.  Fill only when street number/name/type
  # maps to exactly one address point in Addresses.zip.
  unique_no_zip_lookup <- address_points_for_matching |>
    group_by(STREET_NUM, STREET_NAM, STREET_TYP) |>
    summarise(
      lat_fill = median(addr_lat),
      lon_fill = median(addr_lon),
      n_points = n(),
      n_zips = n_distinct(ZIPCODE),
      .groups = "drop"
    ) |>
    filter(n_points == 1L, n_zips == 1L) |>
    select(STREET_NUM, STREET_NAM, STREET_TYP, lat_fill, lon_fill)

  unique_no_zip_by_pid <- coords_df |>
    filter(is.na(lat) | is.na(lon)) |>
    left_join(primary_situs_df, by = c("coord_pID" = "situs_pID")) |>
    filter(is.na(situs_zip)) |>
    inner_join(
      unique_no_zip_lookup,
      by = c(
        "situs_streetNum" = "STREET_NUM",
        "situs_streetName" = "STREET_NAM",
        "situs_streetSuffix" = "STREET_TYP"
      )
    ) |>
    select(coord_pID, lat_fill, lon_fill)

  readr::write_csv(unique_no_zip_by_pid, "output/address_unique_no_zip_lookup.csv")

  coords_df <- fill_missing_coords(
    coords_df,
    unique_no_zip_by_pid,
    by = "coord_pID",
    source_name = "address_point_unique_no_zip"
  )

  coords_summary <- bind_rows(coords_summary, coord_summary(coords_df, "after unique missing-ZIP address point fill"))

  # Addresses.zip also has parcel/account identifiers.  Use them conservatively:
  # require geoID == address PID/PARCEL_ID plus agreement on street number/name,
  # and treat suffix/ZIP as optional disambiguators when TCAD omitted them.
  address_pid_lookup <- coords_df |>
    filter(is.na(lat) | is.na(lon), !is.na(geoID)) |>
    left_join(primary_situs_df, by = c("coord_pID" = "situs_pID")) |>
    mutate(
      situs_addr_num = street_number_numeric(situs_streetNum),
      situs_zip = blank_to_na(situs_zip)
    ) |>
    inner_join(
      address_points_for_matching |>
        filter(!is.na(ADDR_PID), !is.na(addr_num)) |>
        select(
          ADDR_PID, STREET_NUM, STREET_NAM, STREET_TYP, ZIPCODE,
          addr_num, addr_lat, addr_lon
        ),
      by = c(
        "geoID" = "ADDR_PID",
        "situs_streetName" = "STREET_NAM",
        "situs_addr_num" = "addr_num"
      ),
      relationship = "many-to-many"
    ) |>
    filter(
      is.na(situs_streetSuffix) | is.na(STREET_TYP) | situs_streetSuffix == STREET_TYP,
      is.na(situs_zip) | is.na(ZIPCODE) | situs_zip == ZIPCODE
    ) |>
    group_by(coord_pID) |>
    summarise(
      lat_fill = median(addr_lat),
      lon_fill = median(addr_lon),
      n_address_pid_matches = n(),
      .groups = "drop"
    ) |>
    filter(n_address_pid_matches <= 3L)

  readr::write_csv(address_pid_lookup, "output/address_pid_lookup.csv")

  coords_df <- fill_missing_coords(
    coords_df,
    address_pid_lookup |> select(coord_pID, lat_fill, lon_fill),
    by = "coord_pID",
    source_name = "address_point_pid"
  )

  coords_summary <- bind_rows(coords_summary, coord_summary(coords_df, "after address point PID fill"))

  if (USE_NEAREST_ADDRESS_POINT) {
    message("  Using nearest-address-point fallback within ",
            ADDRESS_NEAREST_MAX_DELTA, " house numbers ...")

    address_points_for_nearest <- address_points_for_matching |>
      filter(
        !is.na(STREET_NAM),
        !is.na(STREET_TYP),
        !is.na(ZIPCODE),
        !is.na(addr_num),
        !is.na(addr_lat),
        !is.na(addr_lon)
      )

    missing_situs_for_nearest <- coords_df |>
      filter(is.na(lat) | is.na(lon)) |>
      left_join(primary_situs_df, by = c("coord_pID" = "situs_pID"))

    nearest_lookup <- nearest_address_lookup(
      missing_situs_for_nearest,
      address_points_for_nearest,
      max_delta = ADDRESS_NEAREST_MAX_DELTA
    )

    readr::write_csv(nearest_lookup, "output/nearest_address_point_lookup.csv")

    coords_df <- fill_missing_coords(
      coords_df,
      nearest_lookup |> select(coord_pID, lat_fill, lon_fill),
      by = "coord_pID",
      source_name = "address_point_nearest"
    )

    coords_summary <- bind_rows(coords_summary, coord_summary(coords_df, "after nearest address point fill"))
  }
} else if (USE_ADDRESS_SHAPEFILE) {
  warning("Address shapefile fallback requested but not found: ", address_zip_path)
}

if (file.exists(arcgis_geocode_lookup_path)) {
  message("  Using accepted ArcGIS geocoder lookup: ", arcgis_geocode_lookup_path)
  arcgis_geocode_lookup <- readr::read_csv(
    arcgis_geocode_lookup_path,
    show_col_types = FALSE,
    col_types = readr::cols(.default = "c")
  ) |>
    transmute(
      coord_pID = as.character(coord_pID),
      lat_fill = suppressWarnings(as.numeric(lat_fill)),
      lon_fill = suppressWarnings(as.numeric(lon_fill))
    ) |>
    filter(!is.na(coord_pID), !is.na(lat_fill), !is.na(lon_fill)) |>
    distinct(coord_pID, .keep_all = TRUE)

  coords_df <- fill_missing_coords(
    coords_df,
    arcgis_geocode_lookup,
    by = "coord_pID",
    source_name = "arcgis_geocoder"
  )

  coords_summary <- bind_rows(coords_summary, coord_summary(coords_df, "after ArcGIS geocoder fill"))
}

missing_coord_diagnostics <- coords_df |>
  filter(is.na(lat) | is.na(lon)) |>
  left_join(primary_situs_df, by = c("coord_pID" = "situs_pID")) |>
  mutate(
    missing_coord_reason = case_when(
      is.na(geoID) ~ "missing_geoID",
      USE_PARCEL_POLYGON & length(parcel_polygon_keys) > 0L &
        !geoID %in% parcel_polygon_keys ~ "geoID_not_in_parcel_polygon",
      TRUE ~ "unmatched_after_available_fills"
    )
  ) |>
  select(
    coord_pID, geoID, missing_coord_reason,
    situs_streetNum, situs_streetName, situs_streetSuffix, situs_zip
  )

# Write the results to .csvs so that we avoid having to re-run the streaming extraction 
# while developing the rest of the script.
readr::write_csv(owners_df, "output/owners.csv")
readr::write_csv(situs_df, "output/situses.csv")
readr::write_csv(prof_df, "output/property_profile.csv")
readr::write_csv(char_df, "output/property_characteristics.csv")
readr::write_csv(links_df, "output/links.csv")
readr::write_csv(coords_df, "output/coords.csv")
readr::write_csv(coords_summary, "output/coords_summary.csv")
readr::write_csv(missing_coord_diagnostics, "output/missing_coord_diagnostics.csv")

# Read in all csv files back into data frames (this simulates the state after the streaming extraction is done, 
# and allows us to iterate faster on the rest of the script without re-running the jq streaming each time).
owners_df <- readr::read_csv("output/owners.csv", show_col_types = FALSE, col_types = readr::cols(.default = "c"))
situs_df  <- readr::read_csv("output/situses.csv", show_col_types = FALSE, col_types = readr::cols(.default = "c"))
prof_df   <- readr::read_csv("output/property_profile.csv", show_col_types = FALSE, col_types = readr::cols(.default = "c"))
char_df   <- readr::read_csv("output/property_characteristics.csv", show_col_types = FALSE, col_types = readr::cols(.default = "c"))
links_df  <- readr::read_csv("output/links.csv", show_col_types = FALSE)
coords_df <- readr::read_csv("output/coords.csv", show_col_types = FALSE)


if (!"coord_source" %in% names(coords_df)) {
  coords_df$coord_source <- ifelse(
    is.na(coords_df$lat) | is.na(coords_df$lon),
    NA_character_,
    "existing_coord"
  )
}

# ── 4d. Diagnose still-missing but potentially geocodable addresses ──────────
# At this point, any remaining NA lat/lon survived all exact and approximate
# coordinate recovery stages.  Many of those records are not meaningfully
# geocodable from address data because TCAD gives placeholder situs text such as
# "VARIOUS LOCATIONS", omits the street number, or omits the street name.
#
# This block does not remove records from coords_df.  It writes diagnostics that
# separate:
#   * non-geocodable placeholders / incomplete situs addresses, and
#   * still-missing records with enough address structure for future geocoding.
#
# The cluster output groups those still-geocodable records by street/type/ZIP so
# we can see where approximate block-face or neighboring-address logic might be
# worth improving next.

if (!exists("street_number_numeric")) {
  street_number_numeric <- function(x) {
    x <- trimws(as.character(x))
    out <- suppressWarnings(as.numeric(sub("^([0-9]+).*", "\\1", x)))
    out[!grepl("^[0-9]+", x)] <- NA_real_
    out
  }
}

if (!exists("address_aliases")) {
  address_aliases <- load_address_aliases(address_alias_path)
}

missing_address_diagnostics <- coords_df |>
  filter(is.na(lat) | is.na(lon)) |>
  left_join(
    situs_df |>
      mutate(
        situs_zip = blank_to_na(sub("-.*", "", as.character(situs_zip))),
        situs_streetNum = trimws(as.character(situs_streetNum)),
        situs_streetName = normalize_street_token(situs_streetName),
        situs_streetSuffix = normalize_street_type(situs_streetSuffix)
      ) |>
      arrange(desc(situs_primarySitus)) |>
      distinct(situs_pID, .keep_all = TRUE) |>
      select(situs_pID, situs_streetNum, situs_streetName, situs_streetSuffix, situs_zip) |>
      apply_address_aliases(
        name_col = "situs_streetName",
        suffix_col = "situs_streetSuffix",
        zip_col = "situs_zip",
        aliases = address_aliases
      ),
    by = c("coord_pID" = "situs_pID")
  ) |>
  mutate(
    situs_addr_num = street_number_numeric(situs_streetNum),
    has_placeholder_street = grepl(
      "VARIOUS|UNKNOWN|NO SITUS|MULTIPLE|COMMON AREA|TBD",
      situs_streetName,
      ignore.case = TRUE
    ),
    geocodable_missing_address =
      !is.na(situs_addr_num) &
      !is.na(situs_streetName) &
      situs_streetName != "" &
      !has_placeholder_street,
    missing_address_reason = case_when(
      has_placeholder_street ~ "placeholder_street_name",
      is.na(situs_streetNum) | situs_streetNum == "" ~ "missing_street_number",
      is.na(situs_addr_num) ~ "non_numeric_street_number",
      is.na(situs_streetName) | situs_streetName == "" ~ "missing_street_name",
      is.na(situs_zip) | situs_zip == "" ~ "missing_zip",
      TRUE ~ "geocodable_address_still_missing_coords"
    )
  )

missing_address_summary <- missing_address_diagnostics |>
  count(geocodable_missing_address, missing_address_reason, sort = TRUE)

missing_geocodable_clusters <- missing_address_diagnostics |>
  filter(geocodable_missing_address) |>
  group_by(situs_streetName, situs_streetSuffix, situs_zip) |>
  summarise(
    n_missing = n(),
    n_unique_numbers = n_distinct(situs_addr_num),
    min_street_number = min(situs_addr_num, na.rm = TRUE),
    median_street_number = median(situs_addr_num, na.rm = TRUE),
    max_street_number = max(situs_addr_num, na.rm = TRUE),
    .groups = "drop"
  ) |>
  arrange(desc(n_missing), situs_streetName, situs_streetSuffix, situs_zip)

readr::write_csv(missing_address_summary, "output/missing_address_geocodability_summary.csv")
readr::write_csv(
  missing_address_diagnostics |> filter(geocodable_missing_address),
  "output/missing_coords_geocodable_addresses.csv"
)
readr::write_csv(
  missing_address_diagnostics |> filter(!geocodable_missing_address),
  "output/missing_coords_non_geocodable_addresses.csv"
)
readr::write_csv(
  missing_geocodable_clusters,
  "output/missing_coords_geocodable_clusters.csv"
)

message("Remaining missing-coordinate address diagnostics:")
print(missing_address_summary)



# ── 5. Clean and build address strings ────────────────────────────────────────
# Mirrors the address_clean() logic in target_helper_functions.R.

clean_address <- function(x) {
  x <- toupper(as.character(x))
  x <- gsub("SUITE|STE|CONDO|UNIT|APT|BLDG|[[:punct:]]", "", x)
  x <- gsub("[[:space:]]+NA[[:space:]]+|[[:space:]]+NO[[:space:]]+", " ", x)
  x <- gsub("^NA*[[:space:]]+|[[:space:]]+NA*$", "", x)
  x <- gsub("[[:space:]]{2,}", " ", x)
  x <- gsub("RANCH ROAD", "RR",   x); x <- gsub("DRIVE",    "DR",   x)
  x <- gsub("INTERSTATE", "IH",   x); x <- gsub("LANE",     "LN",   x)
  x <- gsub("ROAD",       "RD",   x); x <- gsub("TRAIL",    "TRL",  x)
  x <- gsub("STREET",     "ST",   x); x <- gsub("FREEWAY",  "FRWY", x)
  x <- gsub("AVENUE",     "AVE",  x); x <- gsub("CIRCLE",   "CIR",  x)
  x <- gsub("PARKWAY",    "PKWY", x); x <- gsub("BOULEVARD","BLVD", x)
  x <- gsub("MOUNTAIN",   "MTN",  x); x <- gsub("PLAZA",    "PLZ",  x)
  x <- gsub("NORTH(?=[[:space:]]|$)", "N", x, perl = TRUE)
  x <- gsub("SOUTH(?=[[:space:]]|$)", "S", x, perl = TRUE)
  x <- gsub("EAST(?=[[:space:]]|$)",  "E", x, perl = TRUE)
  x <- gsub("WEST(?=[[:space:]]|$)",  "W", x, perl = TRUE)
  x <- gsub("[[:space:]]{2,}", " ", x)
  trimws(x)
}

# Situs (property) address
situs_df$situs_city[is.na(situs_df$situs_city)] <- "AUSTIN"
situs_df$situs_country[is.na(situs_df$situs_country) |
                          situs_df$situs_country == ""] <- "USA"
situs_df$situs_zip <- sub("-.*", "", situs_df$situs_zip)

situs_df$situs_address <- clean_address(
  paste(situs_df$situs_streetNum,    situs_df$situs_streetPrefix,
        situs_df$situs_streetName,   situs_df$situs_streetSuffix,
        situs_df$situs_city,         situs_df$situs_state,
        situs_df$situs_zip)
)

# Owner mailing address
owners_df$owner_addrCountry[is.na(owners_df$owner_addrCountry) |
                               owners_df$owner_addrCountry == ""] <- "USA"
owners_df$owner_addrZip <- sub("-.*", "", owners_df$owner_addrZip)

owners_df$owner_address <- clean_address(
  paste(owners_df$owner_addrDeliveryLine, owners_df$owner_addrUnitDesignator,
        owners_df$owner_addrCity,         owners_df$owner_addrState,
        owners_df$owner_addrZip)
)

# Normalise owner name (remove punctuation / extra spaces)
owners_df$owner_name <- gsub(
  "[[:punct:]]", "",
  gsub("[[:space:]]{2,}", " ", as.character(owners_df$owner_name))
)

# ── 6. Merge all sections by parcel ID ────────────────────────────────────────
# Each nested section carries a pID that corresponds to the TCAD parcel ID.

message("Merging parcel sections ...")
situs_for_merge <- situs_df |>
  mutate(situs_primarySitus_sort = suppressWarnings(as.integer(situs_primarySitus))) |>
  arrange(desc(situs_primarySitus_sort), situs_situsAddressID) |>
  distinct(situs_pID, .keep_all = TRUE) |>
  select(-situs_primarySitus_sort)

message(
  "  Using ", format(nrow(situs_for_merge), big.mark = ","),
  " primary situs rows for ",
  format(dplyr::n_distinct(situs_for_merge$situs_pID), big.mark = ","),
  " parcels."
)

parcels <- situs_for_merge |>
  dplyr::left_join(
    char_df,
    by = c("situs_pID" = "propertyChar_pID"),
    relationship = "one-to-one"
  ) |>
  dplyr::left_join(
    owners_df,
    by = c("situs_pID" = "owner_pID"),
    relationship = "one-to-many"
  ) |>
  dplyr::left_join(
    prof_df,
    by = c("situs_pID" = "propertyProf_pID"),
    relationship = "many-to-one"
  ) |>
  dplyr::left_join(
    coords_df,
    by = c("situs_pID" = "coord_pID"),
    relationship = "many-to-one"
  )

rm(situs_df, situs_for_merge, char_df, owners_df, prof_df, coords_df)
gc()

# ── 7. Classify parcels ───────────────────────────────────────────────────────

# 7a. Residential flag
# State codes starting with A (single-family) or B (multi-family), or SF/MF zoning.
parcels$is_residential <-
  grepl("^A|^B", parcels$propertyProf_imprvStateCd, ignore.case = FALSE) |
  grepl("^A|^B", parcels$propertyProf_landStateCd,  ignore.case = FALSE) |
  grepl("SF|MF",  parcels$propertyChar_zoning,       ignore.case = FALSE)

# 7b. Owner-occupied flag
# An owner is considered occupying the property when their mailing address
# matches the situs address OR a homestead (HS) exemption is present.

# The exemptions field may arrive from JSON as a list column; convert to string
# for a simple grep.
exemptions_str <- tryCatch(
  as.character(parcels$owner_exemptions),
  error = function(e) rep(NA_character_, nrow(parcels))
)

has_hs <- grepl("HS", exemptions_str, fixed = TRUE)

addr_match <- mapply(
  function(owner_add, situs_add) {
    if (is.na(owner_add) || is.na(situs_add)) return(FALSE)
    oa <- trimws(owner_add)
    if (nchar(oa) == 0L) return(FALSE)
    grepl(oa, situs_add, fixed = TRUE)
  },
  parcels$owner_address,
  parcels$situs_address,
  USE.NAMES = FALSE
)

parcels$is_owner_occupied <- addr_match | has_hs

# 7c. Financialization / corporate-ownership flag
# Matches entity-type suffixes (LLC, LP, LTD, INC, LC) and real-estate sector
# keywords against the owner name — mirrors financial_marker_string in the
# original target_helper_functions.R.

financial_markers <- paste(
  # Formal entity type markers — require word boundaries to avoid false matches
  "\\bLTD\\b", "\\bL T D\\b", "\\bL\\.?T\\.?D\\.?\\b",
  "\\bLLC\\b", "\\bL L C\\b", "\\bL\\.?L\\.?C\\.?\\b",
  "\\bLP\\b",  "\\bL P\\b",   "\\bL\\.?P\\.?\\b",
  "\\bLLLP\\b","\\bL L L P\\b","\\bL\\.?L\\.?L\\.?P\\.?\\b",
  "\\bINC\\b", "\\bI N C\\b", "\\bI\\.?N\\.?C\\.?\\b",
  "\\bLC\\b",  "\\bL C\\b",   "\\bL\\.?C\\.?\\b",
  # Real-estate / corporate sector keywords — longer stems are less ambiguous
  # but still benefit from left word boundary
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

parcels$is_financialized <- grepl(financial_markers, parcels$owner_name)

# 7d. Primary target flag: non-owner-occupied + financialized + residential
parcels$is_target <-
  !parcels$is_owner_occupied &
  parcels$is_financialized   &
  parcels$is_residential

# ── 8. Estimate unit count (optional, but useful context) ─────────────────────
# Derived from improvement state codes and total floor area.
# 900 sq ft is used as a rough proxy for one residential unit (matches the
# original pipeline heuristic in target_helper_functions.R).
SQ_FT_PER_UNIT <- 900
parcels$property_units <- as.numeric(parcels$propertyProf_imprvTotalArea) / SQ_FT_PER_UNIT
single_fam_codes <- c("A1", "A2", "A3")
two_unit_codes   <- "B2"
three_unit_codes <- "B3"
four_unit_codes  <- "B4"
commercial_codes <- c("C1","C2","C3","D1","D2","E1","F1","F2")

parcels$property_units[
  parcels$propertyProf_imprvStateCd %in% single_fam_codes |
  parcels$propertyProf_landStateCd  %in% single_fam_codes] <- 1
parcels$property_units[
  parcels$propertyProf_imprvStateCd %in% two_unit_codes |
  parcels$propertyProf_landStateCd  %in% two_unit_codes] <- 2
parcels$property_units[
  parcels$propertyProf_imprvStateCd %in% three_unit_codes |
  parcels$propertyProf_landStateCd  %in% three_unit_codes] <- 3
parcels$property_units[
  parcels$propertyProf_imprvStateCd %in% four_unit_codes |
  parcels$propertyProf_landStateCd  %in% four_unit_codes] <- 4
parcels$property_units[
  parcels$propertyProf_imprvStateCd %in% commercial_codes |
  parcels$propertyProf_landStateCd  %in% commercial_codes] <- 0

# ── 9. Filter and write output ─────────────────────────────────────────────────

message("Filtering for corporate-owned parcels ...")

output_cols <- c(
  "situs_pID",
  "situs_address", "situs_city", "situs_state", "situs_zip",
  "owner_name", "owner_address",
  "owner_addrCity", "owner_addrState", "owner_addrZip",
  "propertyChar_zoning",
  "propertyProf_imprvStateCd", "propertyProf_landStateCd",
  "propertyProf_imprvTotalArea", "propertyProf_imprvActualYearBuilt",
  "property_units",
  "lat", "lon", "coord_source",
  "is_residential", "is_owner_occupied", "is_financialized", "is_target"
)

# Keep only columns that actually exist after the merge (guards against
# structural changes in future TCAD exports).
output_cols <- intersect(output_cols, names(parcels))

corporate_parcels <- parcels |>
  dplyr::filter(is_target) |>
  dplyr::select(all_of(output_cols)) |>
  dplyr::rename(parcel_id = situs_pID) |>
  distinct()

# ── 9b. Filter to parcels within the City of Austin boundary ──────────────
# Prefer the City of Austin jurisdiction layer when available.  `FULL` is the
# normal city-limits boundary.  `LTD` is Austin limited-purpose jurisdiction and
# should only be added if the analysis intentionally includes limited-purpose
# areas.  ETJ areas are excluded by default.

if (file.exists(austin_jurisdiction_boundary_path)) {
  message(
    "Using local Austin jurisdiction boundary: ",
    austin_jurisdiction_boundary_path,
    " [", paste(AUSTIN_JURISDICTION_TYPES, collapse = ", "), "]"
  )
  austin_boundary <- sf::st_read(austin_jurisdiction_boundary_path, quiet = TRUE) |>
    sf::st_make_valid() |>
    dplyr::filter(
      city_name == "CITY OF AUSTIN",
      jurisdiction_type %in% AUSTIN_JURISDICTION_TYPES
    ) |>
    dplyr::summarise(.groups = "drop") |>
    sf::st_transform(4326)
} else {
  message("Fetching City of Austin boundary from the Census Bureau (via tigris) ...")
  austin_boundary <- tigris::places(state = "TX", cb = TRUE, year = 2023) |>
    dplyr::filter(NAME == "Austin") |>
    sf::st_transform(4326)
}

if (nrow(austin_boundary) == 0L) {
  stop(
    "Austin boundary filter produced no polygons. Check ",
    "AUSTIN_JURISDICTION_TYPES and the local jurisdiction boundary file."
  )
}

# Build sf point layer from parcels that have valid coordinates.
has_coords <- !is.na(corporate_parcels$lat) & !is.na(corporate_parcels$lon)

if (sum(has_coords) == 0L) {
  warning(
    "No parcels have lat/lon coordinates — Austin boundary filter skipped.\n",
    "  Check that the TCAD JSON export contains top-level 'lat'/'lon' fields."
  )
} else {
  message(
    "  ", format(sum(has_coords), big.mark = ","),
    " of ", format(nrow(corporate_parcels), big.mark = ","),
    " parcels have coordinates and will be spatially filtered."
  )

  parcels_sf <- sf::st_as_sf(
    corporate_parcels[has_coords, ],
    coords = c("lon", "lat"),
    crs    = 4326,
    remove = FALSE   # keep lat/lon columns in the data frame
  )

  # Intersect with Austin boundary.
  in_austin <- lengths(sf::st_intersects(parcels_sf, austin_boundary)) > 0L

  # Replace corporate_parcels with only those inside Austin.
  n_before <- nrow(corporate_parcels)
  corporate_parcels <- sf::st_drop_geometry(parcels_sf[in_austin, ])
  n_after  <- nrow(corporate_parcels)
  message(
    "  Retained ", format(n_after, big.mark = ","),
    " parcels inside Austin city limits (",
    format(n_before - n_after, big.mark = ","), " outside / no coords dropped)."
  )
}

message("Writing ", nrow(corporate_parcels), " rows to: ", out_path)
readr::write_csv(corporate_parcels, out_path)
message("Done! Output saved to: ", normalizePath(out_path))
