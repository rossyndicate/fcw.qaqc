#' @title Parse In-Situ log HTML File
#'
#' @description
#' Parses an Insitu HTML file to extract sensor data with HTML extraction methods.
#'
#' @param html_file_path Path to the HTML file to be parsed. Must be In Situ Log
#' file created by VuSitu. File must be in the format: site_startdate_enddate_type.html
#' type can be either vulink or troll, start and end dates must be in the format
#' YYYYMMDD and site is lower case.
#'
#' @return A data frame containing the parsed sensor data.
#'
#' #' @examples
#' #Reading in one file
#'  pbr_data <- parse_insitu_html_log("data/sensor_data/2025/pbr_20250613_20250701_vulink.html")

# parse_insitu_html_log_update <- function(html_file_path){

# Read the HTML file
html_markup <- rvest::read_html(html_file_path)

# Extract information ----

# Find the logical groups for the sections in the HTML (metadata stuff)
section_groups <- html_markup %>%
  html_elements("[isi-group]") %>%
  html_attr("isi-group") %>%
  unique(.) %>%
  keep(~ .x %in% c("LocationProperties",
                   "InstrumentProperties"))

# For each of the section groups grab the sectionMember information
section_data <- section_groups %>%
  map(function(group_string){

    group_string <- paste0("[isi-group-member='", group_string, "']")

    group_member_markup <- html_markup %>%
      html_elements(group_string)

    group_cols <- group_member_markup %>%
      html_elements("[isi-label]") %>%
      html_text()

    group_vals <- group_member_markup %>%
      html_elements("[isi-value]") %>%
      html_text()

    group_df <- tibble(cols = group_cols, vals = group_vals) %>%
      pivot_wider(names_from = cols, values_from = vals, values_fn = list) %>%
      unnest(cols = everything()) %>%
      clean_names()

    return(group_df)

  }) %>%
  set_names(section_groups)

# Check that we got the tables that we expected, in the form that we expected,
# and end function if we did not. If we did get what we expected set the variables
# needed for the data extraction

for (group in c("LocationProperties", "InstrumentProperties")){

  # Check if table exists
  table_check <- pluck_exists(section_data[[group]])
  if(!table_check) return(FALSE)

  # Check if table information exists
  table_info <- section_data[[group]]
  if(nrow(table_info) == 0) return(FALSE)

  # Check if the column names we expect are in the table
  col_name_check <- any(colnames(table_info) %in% c("location_name",
                                                    "device_model",
                                                    "device_sn"))
  if(!col_name_check) return(FALSE)

}

# All checks passed, set variables for data table extraction

# Set site name
site <- section_data[["LocationProperties"]][["location_name"]]

# Check if this is a vulink or troll log
troll_log_check <- nrow(section_data[["InstrumentProperties"]]) == 1

troll_sn <- section_data[["InstrumentProperties"]] %>%
  filter(grepl("TROLL", device_model, ignore.case = T)) %>%
  pull(device_sn)

if (!troll_log_check){
 vulink_sn <- section_data[["InstrumentProperties"]] %>%
  filter(!grepl("TROLL", device_model, ignore.case = T)) %>%
  pull(device_sn)
}

# Extract isi-data-table header information
data_table_headers <- html_markup %>%
  html_elements(".dataHeader[isi-data-table]") %>%
  html_text() %>%
  str_split_1("\n") %>%
  tibble(headers = .) %>%
  filter(headers != "") %>%
  pull(headers)

# Extract isi-data-table data information
data_table_data <- html_markup %>%
  html_elements(".data[isi-data-row]") %>%
  map_dfr(~ tibble(values = html_elements(.x, "td") %>% html_text()) %>%
            mutate(col = data_table_headers) %>%
            pivot_wider(names_from = col, values_from = values))

# Format the data so that it is in line with the API pull
if (!troll_log_check){
  data_table_data <- data_table_data %>%
    select(-contains(paste0("(", vulink_sn, ")")))
}

data_table_data <- data_table_data %>%
  pivot_longer(cols = -`Date Time`, values_to = "value", names_to = "parameter") %>%
  extract(parameter, into = c("parameter", "unit", "sensor_sn"),
          regex = "^([^(]+)\\(([^)]+)\\)\\s*\\(([^)]+)\\)$")

# Extract the data
data <- tibble(
  site = site,
  device_id = troll_sn,
  timestamp = "date_time col from data table",
  parameter,
  value,
  units
)

# Get rid of the vulink data
if (!troll_log_check){
  data_table_headers <- data_table_headers %>%
    filter(serial != vulink_sn)  %>%
    filter(!grepl("Date Time", headers, ignore.case = T),
           headers != "") %>%
    extract(headers, into = c("parameter", "unit", "serial"),
            regex = "^([^(]+)\\(([^)]+)\\)\\s*\\(([^)]+)\\)$")
}
# }
