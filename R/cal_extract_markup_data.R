#' @title Extract Calibration Data from HTML Markup
#' @export
#'
#' @description
#' Extracts calibration coefficients and drift correction information from HTML
#' calibration report files. This function processes both field and benchtop
#' calibration files, extracting slope/offset parameters and pre/post measurement
#' data for each sensor. Organizes the extracted data into a nested list structure
#' by year and site-parameter combinations, with field calibrations taking
#' priority over benchtop calibrations when both are available.
#'
#' @param field_cal_dir Character string specifying the directory path containing
#'   field calibration HTML files (default: data/calibration_reports/)
#' @param benchtop_cal_dir Character string specifying the directory path
#'   containing benchtop calibration HTML files (default:
#'   data/calibration_reports/benchtop_calibrations/)
#'
#' @seealso [load_calibration_data()]
#' @seealso [join_sensor_calibration_data()]

cal_extract_markup_data <- function(field_cal_dir = here::here("data", "calibration_reports"),
                                    benchtop_cal_dir = here::here("data", "calibration_reports", "benchtop_calibrations")){

  # Input validation
  if (!is.character(field_cal_dir) || length(field_cal_dir) != 1) {
    stop("field_cal_dir must be a single character string")
  }

  if (!is.character(benchtop_cal_dir) || length(benchtop_cal_dir) != 1) {
    stop("benchtop_cal_dir must be a single character string")
  }

  # Directory validation
  if (!dir.exists(field_cal_dir)) {
    stop("field_cal_dir does not exist: ", field_cal_dir)
  }

  if (!dir.exists(benchtop_cal_dir)) {
    stop("benchtop_cal_dir does not exist: ", benchtop_cal_dir)
  }

  # Prepare field calibrations for extraction ====
  # Identify and filter field calibration HTML files
  f_cal_paths <- list.files(field_cal_dir, pattern = ".html", full.names = T)
  f_cal_paths <- purrr::discard(f_cal_paths, ~grepl("vulink|virridy", .x, ignore.case = T))

  if (length(f_cal_paths) == 0){
    stop("No HTML calibration files found in field_cal_dir: ", field_cal_dir)
  }

  # Extract site names and datetime information from field calibration file paths
  # No error handling internally in this map -- assuming that
  f_cal_info <- f_cal_paths %>%
    purrr::map(function(path_str){
      str_list <- basename(path_str) %>%
        stringr::str_split_1("_|\\.") %>%
        stringr::str_squish()

      # Parse site name from filename
      site <- str_list[1]

      # Parse and convert datetime to UTC
      date <- str_list[2:3] %>%
        stringr::str_flatten(collapse = " ") %>%
        stringr::str_squish() %>%
        lubridate::ymd_hm(tz = "America/Denver") %>%
        lubridate::with_tz(tzone = "UTC")

      cal_info <- tibble::tibble(site = site, date = date)
      return(cal_info)
    })

  # Prepare benchtop calibrations for extraction ====
  # Identify and filter benchtop calibration HTML files
  b_cal_paths <- list.files(benchtop_cal_dir, pattern = ".html", full.names = T)
  b_cal_paths <- purrr::discard(b_cal_paths, ~grepl("vulink|virridy", .x, ignore.case = T))

  # Extract datetime information from benchtop calibration file paths
  b_cal_info <- b_cal_paths %>%
    purrr::map(function(path_str){
      str_list <- basename(path_str) %>%
        stringr::str_split_1("_|\\.") %>%
        stringr::str_squish() %>%
        purrr::discard(~grepl("VuSitu|Calibration|html", .x, ignore.case = T))

      # Handle different datetime formats in benchtop filenames
      if (length(str_list) == 2){
        date <- lubridate::with_tz(lubridate::ymd(str_list[2]), tzone = "UTC")
      } else if (length(str_list) == 3){
        unix_dt <- as.numeric(str_list[3])/1000
        date <- lubridate::with_tz(lubridate::as_datetime(unix_dt, tz = "America/Denver", origin = origin), tzone = "UTC")
      }

      cal_info <- tibble::tibble(site = "benchtop", date = date)
      return(cal_info)
    })

  # Extract calibration information from all HTML files ====
  # Combine field and benchtop calibration file paths and metadata
  cal_paths <- c(f_cal_paths, b_cal_paths)
  cal_info <- c(f_cal_info, b_cal_info)

  # Load HTML markup from all calibration files
  cal_html <- purrr::map(cal_paths, rvest::read_html)

  # Extract calibration data from each HTML file
  cal_data <- purrr::map2_dfr(
    cal_html, cal_info,
    function(html_markup, calibration_information){

      # Extract overall calibration metadata from first table
      file_information <- html_markup %>%
        rvest::html_elements("table") %>%
        rvest::html_table() %>%
        purrr::pluck(1) %>%
        tidyr::pivot_wider(names_from = X1, values_from = X2) %>%
        janitor::clean_names() %>%
        dplyr::mutate(instrument = janitor::make_clean_names(instrument)) %>%
        dplyr::rename(sonde_serial = serial_number)

      # Extract sensor-specific calibration data from each div element
      html_divs <- html_markup %>%
        rvest::html_elements("div")

      # Process each sensor div for calibration coefficients and drift data
      html_div_info <- html_divs %>%
        purrr::map_dfr(function(div){
          # Identify sensor type from div metadata
          sensor <- div %>%
            rvest::html_elements("table") %>%
            rvest::html_table() %>%
            purrr::pluck(1) %>%
            tidyr::pivot_wider(names_from = X1, values_from = X2, names_repair = janitor::make_clean_names) %>%
            dplyr::mutate(sensor = janitor::make_clean_names(sensor)) %>%
            dplyr::pull(sensor)

          # Extract sensor-specific calibration data using appropriate extraction function
          if (sensor %in% c("chlorophyll_a", "conductivity", "fdom", "p_h_orp", "pressure", "rdo", "turbidity")){
            div_table_data <- switch(
              EXPR = sensor,
              "conductivity" = cal_extract_conductivity_data(div),
              "rdo" = cal_extract_rdo_data(div),
              "p_h_orp" = cal_extract_ph_orp_data(div),
              "pressure" = cal_extract_pressure_data(div),
              "turbidity" = cal_extract_turbidity_data(div),
              "fdom" = cal_extract_fdom_data(div),
              "chlorophyll_a" = cal_extract_chla_data(div)
            )
          } else {
            return()
          }
          return(div_table_data)
        })

      # Combine metadata with extracted sensor data
      markup_data <- tibble::tibble(
        calibration_information,
        file_information,
        html_div_info
      ) %>%
        # Standardize site names using fix_sites() function
        fix_sites() %>%
        # Standardize sensor parameter names to match sensor data conventions
        dplyr::mutate(
          sensor = dplyr::case_when(
            sensor == "chlorophyll_a" ~ "Chl-a Fluorescence",
            sensor == "conductivity" ~ "Specific Conductivity",
            sensor == "fdom" ~ "FDOM Fluorescence",
            sensor == "p_h" ~ "pH",
            sensor == "orp" ~ "ORP",
            sensor == "pressure" ~ "Pressure",
            sensor == "rdo" ~ "RDO",
            sensor == "turbidity" ~ "Turbidity",
            .default = sensor
          )
        ) %>%
        # Rename datetime columns for clarity
        dplyr::rename(file_date = date, sonde_date = created, sensor_date = last_calibrated)

      return(markup_data)
    })

  # Organize extracted calibration information ====
  # Separate field and benchtop calibration data for merging
  f_cal_data <- dplyr::filter(cal_data, site != "benchtop")
  b_cal_data <- dplyr::filter(cal_data, site == "benchtop") %>%
    dplyr::select(sensor, sensor_serial, sensor_date, calibration_coefs, driftr_input)

  # Merge field and benchtop calibrations, prioritizing field calibrations
  calibrations <- dplyr::left_join(f_cal_data, b_cal_data,
                                   by = c("sensor", "sensor_serial", "sensor_date"),
                                   suffix = c(".field", ".benchtop"),
                                   relationship = "many-to-many") %>%
    # Coalesce calibration data, preferring field calibrations over benchtop
    dplyr::mutate(
      calibration_coefs = purrr::map2(calibration_coefs.field, calibration_coefs.benchtop,
                                      ~ if(!is.null(.x)) .x else .y),
      drift_input = purrr::map2(driftr_input.field, driftr_input.benchtop,
                                ~ if(!is.null(.x)) .x else .y),
      calibration_coefs = purrr::map(calibration_coefs, ~ if(is.null(.x)) NA else .x),
      drift_input = purrr::map(drift_input, ~ if(is.null(.x)) NA else .x)
    ) %>%
    # Clean up merged columns and filter valid calibrations
    dplyr::select(-dplyr::ends_with(".field"), -dplyr::ends_with(".benchtop")) %>%
    dplyr::filter(!is.na(calibration_coefs) | !is.na(drift_input)) %>%
    # Organize columns by data type
    dplyr::select(
      # Site and sensor identification
      site, sensor,
      # Datetime information
      file_date, sonde_date, sensor_date,
      # Instrument identification
      sonde_serial, sensor_serial,
      # Calibration data
      calibration_coefs, drift_input
    )

  # Structure calibration data by year and site-parameter combinations
  calibrations_list <- calibrations %>%
    # Parse datetime columns
    dplyr::mutate(
      sonde_date = lubridate::mdy(sonde_date),
      sensor_date = lubridate::mdy(dplyr::na_if(sensor_date, "Factory Defaults"))
    ) %>%
    # Split by year
    split(f = lubridate::year(.$file_date)) %>%
    purrr::map(\(year_data){
      # Split each year by site-parameter combinations
      site_param_split_list <- year_data %>%
        split(f = list(.$site, .$sensor), sep = "-", drop = TRUE) %>%
        purrr::discard(\(site_param_df) nrow(site_param_df) == 0) %>%
        purrr::map(function(site_param_df){
          site_param_df %>%
            # Deduplicate calibrations by selecting most recent per day
            dplyr::group_by(sensor, sonde_date, sensor_serial) %>%
            dplyr::slice_max(file_date, n = 1, with_ties = F) %>%
            dplyr::ungroup() %>%
            # Select most recent calibration per sensor
            dplyr::group_by(sensor, sensor_date, sensor_serial) %>%
            dplyr::slice_min(file_date, n = 1, with_ties = F) %>%
            dplyr::ungroup() %>%
            # Remove any remaining duplicates
            dplyr::distinct()
        })
    })

  # Return organized calibration data structure
  return(calibrations_list)
}
