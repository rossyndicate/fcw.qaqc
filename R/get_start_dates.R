#' @title Get the start dates for the HydroVu API pull from historically flagged data
#' @export
#'
#' @description
#' Determines optimal starting timestamps for retrieving new water quality data from the HydroVu API
#' based on historical records. Temperature is used as the reference parameter
#' because it is consistently tracked by all sondes, providing the most reliable
#' indication of when data was last collected from each site.
#' 
#' The function incorporates a tracking system to monitor which site-parameter combinations
#' are eligible for processing, helping manage problematic data sources without disrupting
#' the entire pipeline. It also includes a weekly reset capability during Sunday midnight
#' executions, when temporarily disabled parameters can be attempted again.
#' 
#' The resulting timestamps serve as starting points for new API data requests,
#' ensuring continuous data collection without gaps or duplication.
#'
#' @param incoming_historically_flagged_data_list A list of dataframes containing 
#' historical water quality data that has been processed through the QAQC 
#' workflow. Each list element should represent a site-parameter combination and 
#' be named using the "site-parameter" convention.
#' 
#' @param hv_api_tracking_file_path File path to the parquet file that tracks API query
#' status for site-parameter combinations, including eligibility and success metrics.
#' 
#' @param synapse_env Logical, indicating whether function is running in Azure Synapse
#' environment (TRUE) or local environment (FALSE). Default is FALSE.
#' 
#' @param fs Optional filesystem object for Azure Data Lake Storage operations when
#' running in Synapse environment.
#'
#' @return A dataframe with three columns:
#'   - site: The monitoring location identifier
#'   - start_DT: The timestamp (in UTC) of the most recent reading for each site
#'   - end_DT: The current system time (in UTC)
#'
#' @examples
#' # Examples are temporarily disabled
#' 
#' @seealso [api_puller()]
#' @seealso [munge_api_data()]
#' @seealso [update_hv_api_tracker()]

get_start_dates <- function(incoming_historically_flagged_data_list,
                            hv_api_tracking_file_path,
                            sys_time_arg = Sys.time(),
                            synapse_env = FALSE,
                            fs = NULL) {
  
  # Generate default start date and return if historical data list is empty ====
  # Create the start date directly in Denver time
  default_denver_date <- as.POSIXct(paste0(lubridate::year(sys_time_arg), "-03-01"), tz = "America/Denver")
  
  # Convert the start date to UTC for API use
  default_converted_start_DT <- lubridate::with_tz(default_denver_date, "UTC")
  
  # Generate the default start dates tibble
  default_start_dates <- tibble(
    site = c("bellvue",
             "salyer",
             "udall",
             "riverbend",
             "cottonwood",
             "elc",
             "archery",
             "riverbluffs"),
    start_DT = default_converted_start_DT, 
    end_DT = as.POSIXct(sys_time_arg, tz = "UTC") 
  )
  
  # If an empty historical data list is provided, return default dates
  if (length(incoming_historically_flagged_data_list) == 0) {
    return(default_start_dates)
  }
  
  # Read in the HV API Tracking DF =============================================
  
  # Handle cloud-based storage scenario when running in Synapse environment
  if (synapse_env) {
    # Download the file from ADLS into a temporary file and read it in
    read_temp_file <- tempfile(fileext = '.parquet')
    AzureStor::download_adls_file(fs, hv_api_tracking_file_path, read_temp_file)
    hv_api_tracking_df <- arrow::read_parquet(read_temp_file, as_data_frame = TRUE) 
  } else {
    # Handle local storage scenario when working outside of Synapse
    hv_api_tracking_df <- arrow::read_parquet(hv_api_tracking_file_path, as_data_frame = TRUE)
  }
  
  # Pre-process HV API Tracking DF (hv_api_tracking_df) ====
  
  # Check if the current execution corresponds to the Sunday midnight pull in America/Denver timezone
  denver_time <- as.POSIXct(sys_time_arg, tz = "America/Denver")
  sunday_check <- wday(denver_time) == 1 
  midnight_check <- lubridate::hour(lubridate::floor_date(denver_time, unit = "hour")) == 0
  sunday_midnight_check <- if (sunday_check && midnight_check) TRUE else FALSE
  
  if (!sunday_midnight_check) {
    hv_api_tracking_df <- hv_api_tracking_df %>% 
      # Keep sites that are not disabled and don't have to try again
      dplyr::mutate(eligible_for_processing = dplyr::if_else(!auto_disabled & !try_again, TRUE, FALSE)) %>% 
      select(site, parameter, last_success, eligible_for_processing)
  } else { # sunday midnight check passes (try to change if any of the sites pass later)
    hv_api_tracking_df <- hv_api_tracking_df %>% 
      # Keep sites that are not disabled and do have to try again
      dplyr::mutate(eligible_for_processing = dplyr::if_else(!auto_disabled | try_again, TRUE, FALSE)) %>% 
      select(site, parameter, last_success, eligible_for_processing)
  }
  
  # Determine the global minimum ===============================================
  
  # Extract only Temperature parameter dataframes
  temperature_subset <- grep("Temperature", names(incoming_historically_flagged_data_list))
  
  # Extract each site's most recent timestamp based on temperature data
  temp_start_dates_df <- incoming_historically_flagged_data_list[temperature_subset] %>% 
    dplyr::bind_rows() %>% 
    dplyr::mutate(DT_round = lubridate::with_tz(DT_round, "UTC")) %>% 
    dplyr::group_by(site) %>% 
    dplyr::summarize(start_DT = max(DT_round, na.rm = TRUE), .groups = "keep") %>% 
    dplyr::select(start_DT, site)
  
  # Extract each site's most recent timestamp across all parameters, considering those 
  # combinations that are still in contention
  all_params_start_dates_df <- incoming_historically_flagged_data_list %>% 
    dplyr::bind_rows() %>% 
    dplyr::mutate(DT_round = lubridate::with_tz(DT_round, "UTC")) %>% 
    dplyr::group_by(site, parameter) %>% 
    dplyr::summarize(start_DT = max(DT_round, na.rm = TRUE), .groups = "keep") %>% 
    dplyr::left_join(hv_api_tracking_df, by = c("site", "parameter")) %>%
    dplyr::filter(eligible_for_processing) %>% 
    dplyr::ungroup() %>% 
    dplyr::group_by(site) %>% 
    dplyr::summarize(start_DT = max(start_DT, na.rm = TRUE), .groups = "keep") %>% 
    dplyr::select(start_DT, site)
  
  # Check if temperature dates equal all parameter dates
  time_equality_test_dates <- dplyr::full_join(temp_start_dates_df, all_params_start_dates_df, by = "site") %>% 
    dplyr::mutate(dates_equal = start_DT.x == start_DT.y)
  
  all_true <- all(time_equality_test_dates$dates_equal, na.rm = TRUE)
  
  earlier_temp <- min(temp_start_dates_df$start_DT) <= min(all_params_start_dates_df$start_DT)
  
  # Choose which dates to use based on equality test
  selected_dates_df <- if (!all_true & earlier_temp) {
    warning("`get_start_dates()` is using start dates from parameter data.")
    # Here we are assuming that there will never be any issues with retrieving the temperature data.
    all_params_start_dates_df
  } else {
    message("`get_start_dates()` is using start dates from temperature data.")
    temp_start_dates_df
  }
  
  # Establish global minimum for the data
  global_min_start_DT <- min(selected_dates_df$start_DT)
  
  # Update default dataframe with historical data where available
  final_start_dates_df <- default_start_dates %>% 
    dplyr::left_join(selected_dates_df, by = "site") %>% 
    dplyr::mutate(
      # Ensure we're using the earliest date as the start date
      start_DT = global_min_start_DT,
      end_DT = as.POSIXct(sys_time_arg, tz = "UTC")
    ) %>% 
    dplyr::select(-c(start_DT.y, start_DT.x)) %>% 
    dplyr::relocate(site, start_DT, end_DT)
  
  return(final_start_dates_df)
}
