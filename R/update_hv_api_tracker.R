#' @title Update the HydroVu API tracking system with success/failure information
#' @export
#'
#' @description
#' Maintains a tracking system that monitors the success or failure of data retrieval attempts
#' for each site-parameter combination from the HydroVu API. This function analyzes the results 
#' of the most recent data retrieval operation (`api_puller()`) and updates tracking metrics accordingly.
#' 
#' The function implements a tracking mechanism that:
#' - Counts consecutive failures for each site-parameter combination
#' - Records timestamps of successful retrievals
#' - Automatically disables problematic combinations after a threshold of failures
#' - Schedules periodic retry attempts for disabled combinations
#' 
#' This tracking system helps maintain pipeline stability by intelligently managing 
#' problematic data sources without disrupting data collection for functioning sensors.
#'
#' @param post_munged_data_list A list of dataframes containing the results of the most
#' recent HydroVu API data retrieval attempt. Each list element should represent a 
#' site-parameter combination and be named using the "site-parameter" convention.
#' 
#' @param hv_api_tracking_file_path File path to the parquet file that stores the current
#' tracking information for all site-parameter combinations.
#' 
#' @param hv_api_tracking_archive_dir_path Directory path where historical tracking files
#' should be archived.
#' 
#' @param auto_disable_threshold Numeric value specifying the number of consecutive failures
#' required before a site-parameter combination is automatically disabled. Default is 56.
#' A failure would be a failed `api_puller()` call for that site, which would be a 3-hour
#' chunk of data. 56 consecutive failures would align with a week of failed HydroVu
#' API requests.
#' 
#' @param try_again_threshold Numeric value specifying the number of days after which a
#' disabled site-parameter combination should be attempted again. Default is 7.
#' 
#' @param sys_time_arg POSIXct datetime object used for timestamp calculations. Default is
#' the current system time from Sys.time().
#' 
#' @param synapse_env Logical, indicating whether function is running in Azure Synapse
#' environment (TRUE) or local environment (FALSE). Default is FALSE.
#' 
#' @param fs Optional filesystem object for Azure Data Lake Storage operations when
#' running in Synapse environment.
#'
#' @return No direct return value. The function updates the tracking file in the specified
#' location and archives the previous version. A message is printed indicating the
#' location of the new tracking file.
#'
#' @examples
#' # Examples are temporarily disabled
#' 
#' @seealso [get_start_dates()]
#' @seealso [api_puller()]
#' @seealso [move_api_data()]

update_hv_api_tracker <- function(post_munged_data_list, 
                                  hv_api_tracking_file_path, # Raw/HydroVu/HV_Tracking/HVAPITracker20250501-T193734Z.parquet 
                                  hv_api_tracking_archive_dir_path, # /Raw/HydroVu/HV_tracking/hv_tracking_archive
                                  auto_disable_threshold = 56, # consecutive failures in a week to shut down a site-parameter    
                                  try_again_threshold = 7, # days since last failure
                                  sys_time_arg = Sys.time(),
                                  synapse_env = FALSE,
                                  fs = NULL) { 
  
  # Establish file and file path strings =======================================
  
  # Directory that stores the HV API Tracking file
  hv_api_tracking_dir_path <- dirname(hv_api_tracking_file_path)
  
  # File name for the new, updated HV API Tracker file
  timestamp <- format(sys_time_arg, "%Y%m%d-T%H%M%SZ", tz = "UTC")
  updated_hv_api_tracking_file_name <- paste0("HVAPITracker", timestamp, ".parquet")
  
  # Updated file path for the updated HV API Tracker file
  updated_hv_api_tracking_dir_path <- file.path(hv_api_tracking_dir_path, updated_hv_api_tracking_file_name)
  
  # Reading in the data ========================================================
  
  # Pull in the HV API tracker df
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
  
  # Parse the munged API list to track if the HV API pulls were a success or not
  received_data_tracker_df <- purrr::imap_dfr(post_munged_data_list, function(df, idx){
    # Check if the data was received and how much
    success <- !is.null(df) && nrow(df) > 0
    data_count <- if (success) nrow(df) else 0
    
    tibble(site_parameter = idx,
           success, 
           data_count)
  }) %>% 
    filter(site_parameter %in% hv_api_tracking_df$site_parameter)
  
  # Updating the api tracker ===================================================
  updated_tracker <- left_join(hv_api_tracking_df, received_data_tracker_df, by = "site_parameter") %>%
    mutate(
      # Fix success and data_count columns
      success = dplyr::if_else(is.na(success), FALSE, success),
      data_count = dplyr::if_else(is.na(data_count), 0, data_count),
      # If success is TRUE, reset counter, otherwise increment
      failure_count = dplyr::if_else(success == TRUE, 0, failure_count + 1),
      # Update last success date if successful
      last_success = dplyr::if_else(success == TRUE, sys_time_arg, last_success),
      # Mark as auto-disabled if failure count exceeds threshold
      auto_disabled = failure_count >= auto_disable_threshold) %>% 
    rowwise() %>% 
    mutate(
      # Set try_again to true if last success was more than a week ago
      try_again = dplyr::if_else(
        difftime(sys_time_arg, last_success, units = "days")[[1]] >= try_again_threshold,
        TRUE,
        try_again
      )) %>% 
    ungroup() %>% 
    # Clean up after the join
    select(-c(success, data_count))
  
  # Save the updated tracker ===================================================
  
  # Handle cloud-based storage scenario when running in Synapse environment
  if (synapse_env) {
    # Move the previous tracking df to the archive folder
    move_api_data(src_dir = hv_api_tracking_dir_path, 
                  dest_dir = hv_api_tracking_archive_dir_path,
                  synapse_env = TRUE,
                  fs = fs)
    # Save the current tracking df to the hv_api_tracking_file_path
    write_temp_file <- tempfile(fileext = '.parquet')
    arrow::write_parquet(updated_tracker, write_temp_file)
    AzureStor::upload_adls_file(filesystem = fs, 
                                src = write_temp_file,
                                dest = updated_hv_api_tracking_dir_path)
    
    message("New HV API tracking file in ", updated_hv_api_tracking_dir_path)
  } else {
    # Handle local storage scenario when working outside of Synapse
    # Move the previous tracking df to the archive folder
    move_api_data(src_dir = hv_api_tracking_dir_path, 
                  dest_dir = hv_api_tracking_archive_dir_path,
                  synapse_env = FALSE,
                  fs = NULL)
    
    # Upload the data locally
    arrow::write_parquet(updated_tracker, updated_hv_api_tracking_dir_path)
    
    message("New HV API tracking file in ", updated_hv_api_tracking_dir_path)
  }
}
