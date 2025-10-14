#' @title Parse In-Situ log HTML File
#' @export
#'
#' @description
#' Parses an In-Situ HTML file exported from HydroVu, VuLink, or Troll devices.
#' Detects file type based on metadata table structure, extracts sensor data,
#' handles timezone conversions, and returns standardized long-format data.
#'
#' @param html_markup HTML markup from an In Situ Log file created by VuSitu.
#' NOTE: This HTML markup must be extracted with xml2 or there will be 
#'
#' @return A tibble with columns: DT_round (datetime floored to 15 min), site,
#'   parameter, value, unit, DT (original datetime), DT_join (character datetime),
#'   and sensor_sn. All datetimes are converted to UTC.
#'
#' @examples
#' \dontrun{
#' # Example requires HTML file
#' html_content <- xml2::read_html("path/to/file.html")
#' result <- parse_insitu_html_log(html_content)
#' }

parse_insitu_html_log <- function(html_markup) {
  
  # Detect File Type ----
  # HydroVu: 1 table (LocationProperties)
  # Troll: LocationProperties + 1 InstrumentProperties table
  # VuLink: LocationProperties + 2 InstrumentProperties tables
  
  section_groups <- html_markup %>%
    xml2::xml_find_all(".//*[@isi-group]") %>%
    xml2::xml_attr("isi-group") %>%
    purrr::keep(~ .x %in% c("LocationProperties", "InstrumentProperties"))
  
  if (!"LocationProperties" %in% section_groups) {
    stop("'LocationProperties' table not in HTML markup. Unable to parse.")
  }
  
  file_type <- NULL
  
  if ("LocationProperties" %in% section_groups && length(section_groups) == 1) {
    file_type <- "hydrovu"
  }
  
  if ("InstrumentProperties" %in% section_groups && sum(grepl("InstrumentProperties", section_groups)) == 1) {
    file_type <- "troll"
  }
  
  if ("InstrumentProperties" %in% section_groups && sum(grepl("InstrumentProperties", section_groups)) == 2) {
    file_type <- "vulink"
  }
  
  if (is.null(file_type)) {
    stop("File type (HydroVu, Vulink, or Troll) of HTML markup cannot be determined.")
  }
  
  # Extract Metadata ----
  
  section_data <- section_groups %>%
    purrr::map(function(group_string) {
      group_xpath <- paste0(".//*[@isi-group-member='", group_string, "']")
      
      group_member_markup <- html_markup %>%
        xml2::xml_find_all(group_xpath)
      
      group_cols <- group_member_markup %>%
        xml2::xml_find_all(".//*[@isi-label]") %>%
        xml2::xml_text()
      
      group_vals <- group_member_markup %>%
        xml2::xml_find_all(".//*[@isi-value]") %>%
        xml2::xml_text()
      
      # Only keep labels that have corresponding values
      min_len <- min(length(group_cols), length(group_vals))
      
      group_df <- tibble::tibble(
        cols = group_cols[1:min_len],
        vals = group_vals[1:min_len]
      ) %>%
        tidyr::pivot_wider(names_from = cols, values_from = vals, values_fn = list) %>%
        tidyr::unnest(cols = dplyr::everything()) %>%
        janitor::clean_names()
      
      return(group_df)
      
    }) %>%
    purrr::set_names(section_groups)
  
  # Validate Metadata ----
  
  if (file_type == "hydrovu") {
    table_info <- section_data[["LocationProperties"]]
    if (nrow(table_info) == 0) return(NULL)
    
    col_name_check <- any(colnames(table_info) %in% c("location_name"))
    if (!col_name_check) return(NULL)
    
  } else if (file_type == "troll" || file_type == "vulink") {
    table_info <- section_data[["LocationProperties"]]
    if (nrow(table_info) == 0) return(NULL)
    
    col_name_check <- any(colnames(table_info) %in% c("location_name"))
    if (!col_name_check) return(NULL)
    
    table_info <- section_data[["InstrumentProperties"]]
    if (nrow(table_info) == 0) return(NULL)
    
    col_name_check <- any(colnames(table_info) %in% c("device_model", "device_sn"))
    if (!col_name_check) return(NULL)
    
  } else {
    stop("Table structures are not as the function expects them to be.")
  }
  
  site <- section_data[["LocationProperties"]][["location_name"]] %>%
    stringr::str_remove_all("\\d+") %>%
    stringr::str_trim()
  
  # Extract Data Table ----
  
  data_table_headers <- html_markup %>%
    xml2::xml_find_all(".//*[@isi-data-column-header]") %>%
    xml2::xml_text()
  
  # Get all data row values at once
  all_data_values <- html_markup %>%
    xml2::xml_find_all(".//tr[@class='data'][@isi-data-row]/td") %>%
    xml2::xml_text()
  
  # Convert to data frame
  n_cols <- length(data_table_headers)
  data_table_data <- matrix(all_data_values, ncol = n_cols, byrow = TRUE) %>%
    as.data.frame(stringsAsFactors = FALSE)
  
  colnames(data_table_data) <- data_table_headers
  
  # Handle Datetime Conversion ----
  # Troll/VuLink files have local time with offset metadata
  # HydroVu files use unix timestamps and are always UTC
  
  if (file_type == "troll" || file_type == "vulink") {
    time_offset_str <- html_markup %>%
      xml2::xml_find_all(".//tr[@class='sectionMember']") %>%
      xml2::xml_text() %>%
      stringr::str_subset("Time Offset") %>%
      stringr::str_extract("-?\\d{2}:\\d{2}:\\d{2}")
    
    local_datetimes <- html_markup %>%
      xml2::xml_find_all(".//tr[@class='data']/td[@class='dateTime']") %>%
      xml2::xml_text()
    
    # Convert local time to UTC by subtracting the offset
    utc_dt <- tibble::tibble(
      local_dt_str = local_datetimes,
      time_offset = time_offset_str,
      utc_dt = lubridate::ymd_hms(local_dt_str) - lubridate::hms(time_offset)
    ) %>%
      dplyr::pull(utc_dt)
  }
  
  if (file_type == "hydrovu") {
    utc_dt <- html_markup %>%
      xml2::xml_find_all(".//tr[@class='data']") %>%
      xml2::xml_attr("isi-hv-timestamp") %>%
      tibble::tibble(isi_hv_timestamp = .) %>%
      dplyr::mutate(
        # HydroVu uses millisecond unix timestamps
        utc_dt = lubridate::as_datetime(as.numeric(isi_hv_timestamp) / 1000,
                                        tz = "UTC")
      ) %>%
      dplyr::pull(utc_dt)
  }
  
  data_table_data <- data_table_data %>%
    dplyr::mutate(`Date Time` = utc_dt)
  
  # Remove VuLink Device Columns ----
  # VuLink has its own columns that we need to remove
  
  if (file_type == "vulink") {
    vulink_sn <- section_data[["InstrumentProperties"]] %>%
      dplyr::filter(grepl("vulink", device_model, ignore.case = TRUE)) %>%
      dplyr::pull(device_sn)
    
    data_table_data <- data_table_data %>%
      dplyr::select(-dplyr::contains(paste0("(", vulink_sn, ")")))
  }
  
  # Clean up large objects
  rm(html_markup, section_data, section_groups, all_data_values, data_table_headers)
  
  # Transform to Long Format ----
  
  data <- data_table_data %>%
    tidyr::pivot_longer(cols = -`Date Time`, values_to = "value", names_to = "parameter") %>%
    # Regex extracts "Parameter Name (unit) (serial_number)" into three capture groups:
    # 1. parameter name (everything before first parenthesis)
    # 2. unit (content between first pair of parentheses)
    # 3. (optional) sensor serial number (content between second pair of parentheses) (HydroVu files do not have this information)
    tidyr::extract(
      parameter,
      into = c("parameter", "unit", "sensor_sn"),
      regex = "^([^(]+)\\(([^)]+)\\)(?:\\s*\\(([^)]+)\\))?$",
      remove = TRUE
    ) %>%
    dplyr::mutate(
      parameter = stringr::str_trim(parameter),
      sensor_sn = ifelse(sensor_sn == "", NA, sensor_sn)
    ) %>%
    janitor::clean_names() %>%
    dplyr::mutate(
      site = site,
      # Standardize parameter names
      parameter = dplyr::case_when(
        stringr::str_detect(parameter, "pH mV") ~ NA,
        stringr::str_detect(parameter, "Saturation") ~ NA,
        stringr::str_detect(parameter, "Temperature") ~ "Temperature",
        stringr::str_detect(parameter, "Turbidity") ~ "Turbidity",
        stringr::str_detect(parameter, "Specific Conductivity") ~ "Specific Conductivity",
        stringr::str_detect(parameter, "RDO Concentration") ~ "DO",
        stringr::str_detect(parameter, "pH") ~ "pH",
        stringr::str_detect(parameter, "Depth") ~ "Depth",
        stringr::str_detect(parameter, "ORP") ~ "ORP",
        stringr::str_detect(parameter, "Chlorophyll-a") ~ "Chl-a Fluorescence",
        stringr::str_detect(parameter, "FDOM") ~ "FDOM Fluorescence",
        TRUE ~ NA
      ),
      # Convert units: feet to meters for Depth, mV to V for ORP
      value = dplyr::case_when(
        parameter == "Depth" ~ as.numeric(value) * 0.3048,
        (parameter == "ORP" & unit == "mV") ~ as.numeric(value) / 1000,
        TRUE ~ as.numeric(value)
      ),
      unit = dplyr::case_when(
        parameter == "Depth" ~ "m",
        parameter == "ORP" ~ "V",
        TRUE ~ unit
      ),
      DT = date_time,
      DT_round = lubridate::floor_date(DT, "15 minutes"),
      DT_join = as.character(DT_round)
    ) %>%
    dplyr::filter(!is.na(parameter)) %>%
    dplyr::select(DT_round, site, parameter, value, unit, DT, DT_join, sensor_sn) %>%
    dplyr::group_by(site, parameter) %>%
    dplyr::arrange(DT_round, .by_group = T) %>%
    dplyr::ungroup() %>%
    dplyr::distinct()
  
  return(data)
}