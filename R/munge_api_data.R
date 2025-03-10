#' @title Process raw API data for water quality monitoring workflow
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
#' # Process data for FCW network with 15-minute intervals
#' fcw_data <- munge_api_data(api_path = "data/api",
#'                          network = "FCW",
#'                          summarize_interval = "15 minutes")
#'
#' @seealso [api_puller()]
#' @seealso [tidy_api_data()]

munge_api_data <- function(api_path, network, summarize_interval = "15 minutes") {
  
  # Read and combine all CSV files in the specified directory
  # This creates a single dataframe from potentially many site-specific files
  api_data <- list.files(path = api_path, full.names = TRUE, 
                        recursive = TRUE, pattern = "*.csv") %>%
    purrr::map_dfr(~data.table::fread(.) %>%
                  dplyr::select(-id)) %>%  # Remove location ID column
    dplyr::mutate(units = as.character(units)) %>%  # Ensure units is character type
    # Remove any duplicate rows that might exist from overlapping API pulls
    dplyr::distinct()
  
  # Apply network-specific processing for CSU/FCW networks
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
      # These adjustments ensure data continuity despite physical changes
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
      # But retain Virridy sondes in the dataset
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