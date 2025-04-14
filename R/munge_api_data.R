#' @title Process raw API data for water quality monitoring workflow
#' @export
#'
#' @description
#' Transforms raw CSV files downloaded from the HydroVu API into a standardized
#' format suitable for further quality control processing. This function handles
#' data from multiple monitoring networks, applies site name standardization,
#' performs timezone conversion, and manages special cases where monitoring
#' equipment was relocated between sites. It serves as a crucial preprocessing
#' step that bridges the gap between raw API data and the structured format
#' required by downstream quality control functions.
#'
#' @param api_path Character string specifying the directory path containing the
#' raw CSV files downloaded from the HydroVu API.
#'
#' @param network Character string indicating which monitoring network to process.
#' Options include "CSU", "FCW" (Fort Collins Watershed), or "all". Different
#' networks may have different processing requirements.
#'
#' @param summarize_interval Character string specifying the time interval to
#' round timestamps to. Default is "15 minutes". Accepts any interval format
#' compatible with lubridate::round_date().
#' 
#' @param fs Logical, whether to use the file system functions
#'
#' @return A dataframe containing processed water quality monitoring data with
#' standardized columns:
#' - site: Standardized site name (lowercase, no spaces)
#' - DT: Original timestamp (MST timezone)
#' - DT_round: Rounded timestamp for consistent time intervals
#' - DT_join: Character representation of rounded timestamp for joining
#' - parameter: Measurement type (e.g., "Temperature", "DO")
#' - value: Measured value
#' - units: Measurement units (e.g., "°C", "mg/L")
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [api_puller()]
#' @seealso [tidy_api_data()]

munge_api_data <- function(api_path, network, summarize_interval = "15 minutes", 
                           synapse_env = FALSE, fs = NULL) {
  
  if (synapse_env) {
    
    # List files in ADLS
    file_list <- AzureStor::list_adls_files(fs, api_path, info = "name")
    
    # check if file list is a list that is not empty
    
    # Map files into a dataframe
    api_data <- map_dfr(file_list, function(adls_path){
      # set temp file
      temp_file <- tempfile(fileext = '.csv')
      
      # download file from folder into temp file
      AzureStor::download_adls_file(fs, adls_path, temp_file)
      
      # read the temp file with read.csv
      site_df <- data.table::fread(temp_file) %>% 
        dplyr::select(-id) 
      
      return(site_df)
    }) %>% 
      dplyr::mutate(units = as.character(units)) %>%
      dplyr::distinct()
    
  } else {
    
    api_data <- map_dfr(list.files(api_path, full.names = TRUE), 
                        function(file_path) {
                          site_df <- data.table::fread(file_path) %>% 
                            dplyr::select(-id)
                        }) %>% 
      dplyr::mutate(units = as.character(units)) %>%
      dplyr::distinct()
    
  }
  
  # Apply network-specific processing for CSU/FCW networks
  # do we need to fix site names here?
  if(network %in% c("csu", "CSU", "FCW", "fcw")){
    api_data <- api_data %>%
      # Filter out VuLink data (not used in CSU/FCW networks)
      dplyr::filter(!grepl("vulink", name, ignore.case = TRUE)) %>%
      # Filter out Virridy sondes (not part of CSU/FCW networks)
      dplyr::filter(!grepl("virridy", name, ignore.case = TRUE)) %>%
      # Remove the equipment name column
      dplyr::select(-name) %>%
      
      # Convert timestamps from UTC (as provided by HydroVu API) to MST
      dplyr::mutate(DT = lubridate::as_datetime(timestamp, tz = "UTC")) %>%
      dplyr::mutate(
        # Apply timezone conversion to Mountain Standard Time
        DT = lubridate::with_tz(DT, tzone = "MST"),
        # Round timestamps to specified interval for consistent time series
        DT_round = lubridate::round_date(DT, summarize_interval),
        # Create string version of timestamp for joining operations
        DT_join = as.character(DT_round),
        # Ensure site names are lowercase for consistency
        site = tolower(site)
      ) %>%
      
      # Apply site name standardization and handle historical equipment relocations
      dplyr::mutate(
        # Map alternative site names to standard names
        site = dplyr::case_when(
          site == "rist" ~ "tamasag",
          site == "elc" ~ "boxelder",
          TRUE ~ site
        )
      ) %>%
      # Handle a specific case where equipment was moved between sites
      dplyr::mutate(
        site = ifelse(site == "tamasag" & 
                        DT > lubridate::ymd("2022-09-20", tz = "MST") & 
                        DT < lubridate::ymd("2023-01-01", tz = "MST"), 
                      "boxelder", site)
      ) %>%
      # Standardize "river bluffs" to "riverbluffs" (remove spaces)
      dplyr::mutate(
        site = ifelse(grepl("river bluffs", site, ignore.case = TRUE), 
                      "riverbluffs", site)
      ) %>%
      # Ensure no duplicates after all transformations
      dplyr::distinct(.keep_all = TRUE)
  }
  
  # Apply more inclusive processing for "all" networks option
  if(network %in% c("all", "All")){
    api_data <- api_data %>%
      # Filter out VuLink data (still excluded from "all" networks)
      dplyr::filter(!grepl("vulink", name, ignore.case = TRUE)) %>%
      
      # Remove the equipment name column
      dplyr::select(-name) %>%
      
      # Apply the same timestamp and site name standardization
      dplyr::mutate(DT = lubridate::as_datetime(timestamp, tz = "UTC")) %>%
      dplyr::mutate(
        DT = lubridate::with_tz(DT, tzone = "MST"),
        DT_round = lubridate::round_date(DT, summarize_interval),
        DT_join = as.character(DT_round),
        site = tolower(site)
      ) %>%
      
      # Apply the same site name standardization and equipment relocation handling
      dplyr::mutate(
        site = ifelse(site == "rist", "tamasag",
                      ifelse(site == "elc", "boxelder", site))
      ) %>%
      dplyr::mutate(
        site = ifelse(site == "tamasag" & 
                        DT > lubridate::ymd("2022-09-20", tz = "MST") & 
                        DT < lubridate::ymd("2023-01-01", tz = "MST"), 
                      "boxelder", site)
      ) %>%
      # Ensure no duplicates after all transformations
      dplyr::distinct(.keep_all = TRUE)
  }
  
  return(api_data)
}
