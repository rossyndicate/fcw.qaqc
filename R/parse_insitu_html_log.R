#' @title Parse In-Situ log HTML File
#' @export
#'
#' @description
#' Parses an Insitu HTML file to extract sensor data with HTML extraction methods.
#'
#' @param html_markup HTML markup from an In Situ Log file created by VuSitu. 
#'
#' @return A data frame containing the parsed sensor data for the HTML markup.
#' @examples
#' \dontrun{
#' # Example requires HTML file
#' html_content <- rvest::read_html("path/to/file.html")
#' result <- parse_insitu_html_log(html_content)
#' }

parse_insitu_html_log <- function(html_markup) {
  
  # Extract information ----
  # Find the logical groups for the sections in the HTML (metadata stuff)
  section_groups <- html_markup %>%
    rvest::html_elements("[isi-group]") %>%
    rvest::html_attr("isi-group") %>%
    unique(.) %>%
    keep(~ .x %in% c("LocationProperties",
                     "InstrumentProperties"))
  
  # For each of the section groups grab the sectionMember information
  section_data <- section_groups %>%
    purrr::map(function(group_string){
      
      group_string <- paste0("[isi-group-member='", group_string, "']")
      
      group_member_markup <- html_markup %>%
        rvest::html_elements(group_string)
      
      group_cols <- group_member_markup %>%
        rvest::html_elements("[isi-label]") %>%
        rvest::html_text()
      
      group_vals <- group_member_markup %>%
        rvest::html_elements("[isi-value]") %>%
        rvest::html_text()
      
      group_df <- tibble::tibble(cols = group_cols, vals = group_vals) %>%
        tidyr::pivot_wider(names_from = cols, values_from = vals, values_fn = list) %>%
        tidyr::unnest(cols = everything()) %>%
        janitor::clean_names()
      
      return(group_df)
      
    }) %>%
    purrr::set_names(section_groups)
  
  # Free up space
  rm(section_groups)
  
  # Check that we got the tables that we expected, in the form that we expected,
  # and end function if we did not. If we did get what we expected set the variables
  # needed for the data extraction
  
  for (group in c("LocationProperties", "InstrumentProperties")){
    
    # Check if table exists
    table_check <- purrr::pluck_exists(section_data[[group]])
    if(!table_check) return(NULL)
    
    # Check if table information exists
    table_info <- section_data[[group]]
    if(nrow(table_info) == 0) return(NULL)
    
    # Check if the column names we expect are in the table
    col_name_check <- any(colnames(table_info) %in% c("location_name",
                                                      "device_model",
                                                      "device_sn"))
    if(!col_name_check) return(NULL)
    
    # Free up space 
    rm(table_check, table_info, col_name_check)
    
  }
  
  # All checks passed, set variables for data table extraction
  
  # Set site name
  site <- section_data[["LocationProperties"]][["location_name"]]
  
  # Check if this is a vulink or troll log
  troll_sn <- section_data[["InstrumentProperties"]] %>%
    dplyr::filter(grepl("TROLL", device_model, ignore.case = T)) %>%
    dplyr::pull(device_sn)
  
  troll_log_check <- nrow(section_data[["InstrumentProperties"]]) == 1
  
  if (!troll_log_check){
    vulink_sn <- section_data[["InstrumentProperties"]] %>%
      dplyr::filter(!grepl("TROLL", device_model, ignore.case = T)) %>%
      dplyr::pull(device_sn)
  }
  
  # Free up space
  rm(section_data)
  
  # Extract isi-data-table header information
  data_table_headers <- html_markup %>%
    rvest::html_elements(".dataHeader[isi-data-table]") %>%
    rvest::html_text() %>%
    stringr::str_split_1("\n") %>%
    dplyr::tibble(headers = .) %>%
    dplyr::filter(headers != "") %>%
    dplyr::pull(headers)
  
  # Extract isi-data-table data information
  # Get all data row values at once (exclude headers and metadata)
  all_data_values <- html_markup %>%
    rvest::html_elements("tr.data[isi-data-row] td") %>%
    rvest::html_text()
  
  # Free up space 
  rm(html_markup)
  
  # Convert to matrix 
  n_cols <- length(data_table_headers)
  data_table_data <- matrix(all_data_values, ncol = n_cols, byrow = TRUE) %>%
    as.data.frame(stringsAsFactors = FALSE)
  
  # Set column names
  colnames(data_table_data) <- data_table_headers
  
  # Format the data so that it is in line with the API pull
  if (!troll_log_check){
    data_table_data <- data_table_data %>%
      dplyr::select(-contains(paste0("(", vulink_sn, ")")))
  }
  
  # Free up memory
  rm(all_data_values, data_table_headers)
  
  data <- data_table_data %>%
    tidyr::pivot_longer(cols = -`Date Time`, values_to = "value", names_to = "parameter") %>%
    tidyr::extract(parameter, into = c("parameter", "unit", "sensor_sn"),
            regex = "^([^(]+)\\(([^)]+)\\)\\s*\\(([^)]+)\\)$") %>%
    janitor::clean_names() %>%
    dplyr::mutate(site = site,
           #simplify parameter names
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
             TRUE ~ NA # Filter this out later
           ), 
           # Transform Depth and ORP values
           value = dplyr::case_when(
             parameter == "Depth" ~ as.numeric(value) * 0.3048, # Convert feet to meters
             (parameter == "ORP" & unit == "mV") ~ as.numeric(value) / 1000, # Convert mV to V
             TRUE ~ as.numeric(value)
           ),
           # Update Depth and ORP units according to transformations
           unit = dplyr::case_when(
             parameter == "Depth" ~ "m",
             parameter == "ORP" ~ "V",
             TRUE ~ unit
           ),
           # Removed timestamp because it is just a duplicated DT. 
           # Here there is a America/Denver -> UTC DT conversion, assuming this data always comes in with Denver time
           DT = lubridate::with_tz(lubridate::ymd_hms(date_time, tz = "America/Denver"), "UTC"),
           DT_round = lubridate::floor_date(DT, "15 minutes") , 
           DT_join = as.character(DT_round)
    ) %>%
    # Filter out irrelevant parameters
    dplyr::filter(!is.na(parameter)) %>%
    dplyr::select(DT_round, site, parameter, value, unit, DT, DT_join, sensor_sn) %>% 
    dplyr::group_by(site, parameter) %>%
    dplyr::arrange(DT_round, .by_group = T) %>%
    dplyr::ungroup() %>%
    dplyr::distinct()
  
  # Free up memory
  rm(data_table_data)
  
  return(data)
}
