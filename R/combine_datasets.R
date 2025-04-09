#' @title Merge historical and new water quality monitoring data
#'
#' @description
#' Combines newly collected water quality data with recent historical data to
#' create continuous time series for each site-parameter combination. This function
#' is critical for maintaining data continuity across data collection cycles.
#'
#' The function preserves quality flags from historical data, adds a marker to
#' distinguish historical from new observations, and handles several special cases
#' including missing data scenarios. By integrating the most recent 24 hours of
#' historical data with new readings, it creates an overlap period that helps
#' identify sensor drift and ensures smooth transitions between data collection cycles.
#'
#' @param incoming_data_list A list of dataframes containing newly collected water
#' quality data, typically the output from tidy_api_data(). Each list element
#' should be named with the "site-parameter" naming convention.
#'
#' @param historical_data_list A list of dataframes containing previously processed
#' and flagged historical data. Each list element should follow the same
#' "site-parameter" naming convention as the incoming_data_list.
#'
#' @return A list of dataframes containing merged historical and incoming data.
#' Each dataframe includes:
#' - All columns from the original dataframes
#' - A "historical" boolean column indicating data source (TRUE for historical data)
#' - Quality flags preserved from historical data
#' The result contains only site-parameter combinations present in both input lists,
#' unless one list is empty, in which case all elements from the non-empty list
#' are returned.
#'
#' @examples
#' # Combine new site data with historical records
#' combined_data <- combine_datasets(incoming_data_list = new_data_tidied_list,
#'                                 historical_data_list = historical_data)
#'
#' @seealso [tidy_api_data()]
#' @seealso [add_field_notes()]
#' @seealso [generate_summary_statistics()]

combine_datasets <- function(incoming_data_list, historical_data_list) {
  
  # Check if both datasets are empty/null and stop if so
  # TODO: Returna and log a warning instead of stopping
  if ((is.null(historical_data_list) || length(historical_data_list) == 0) &
      (is.null(incoming_data_list) || length(incoming_data_list) == 0)) {
    stop("No data!")
  }
  
  # Handle case 1: No incoming data, only historical data
  # This can happen if no new data was collected since the last processing cycle
  if (is.null(incoming_data_list) || length(incoming_data_list) == 0) {
    print("No new incoming data.")
    
    # Return just the most recent 24 hours of historical data
    # This provides continuity for the next processing cycle
    last_24_hours <- purrr::map(historical_data_list,
                               function(data) {
                                 data %>%
                                   # Extract the most recent 24 hours of data
                                   # Note: Previous approach with ymd_hms had issues
                                   dplyr::filter(DT_round >= max(DT_round) - lubridate::hours(24)) %>%
                                   # Mark as historical and preserve quality flags
                                   dplyr::mutate(historical = TRUE,
                                               # Copy auto_flag to flag column for continuity
                                               flag = auto_flag)
                               })
    return(last_24_hours)
  }
  
  # Handle case 2: No historical data, only incoming data
  # This can happen when monitoring a new site or after a system reset
  if (is.null(historical_data_list) || length(historical_data_list) == 0) {
    print("No historical data.")
    
    # Mark all incoming data as non-historical and return
    new_data <- purrr::map(incoming_data_list,
                          function(data){
                            data %>%
                              dplyr::mutate(historical = FALSE)
                          })
    return(new_data)
  }
  
  # Standard case: Both historical and incoming data exist
  # Find site-parameter combinations that exist in both datasets
  matching_indexes <- intersect(names(incoming_data_list), names(historical_data_list))
  
  # Extract the most recent 24 hours of historical data for each site-parameter combo
  # This creates an overlap period that helps identify sensor drift
  last_24_hours <- purrr::map(historical_data_list,
                             function(data) {
                               data %>%
                                 # Get the last 24 hours of data
                                 # Note: This timestamp handling requires validation (marked as "SCARY DT")
                                 dplyr::filter(DT_round >= max(DT_round) - lubridate::hours(24)) %>%
                                 # Mark as historical and preserve quality flags
                                 dplyr::mutate(historical = TRUE,
                                             # Copy auto_flag to flag column for continuity
                                             flag = auto_flag) %>%
                                 # Remove the sonde_moved column so it can be recalculated
                                 # based on combined historical and new data
                                 dplyr::select(-sonde_moved)
                             })
  
  # Combine historical and incoming data for each matching site-parameter combination
  combined_hist_inc_data <- purrr::map(matching_indexes, function(index) {
    last_24_hours[[index]] %>%
      # Combine with incoming data, skipping the first row to avoid duplication
      # (the first row of new data likely overlaps with the last row of historical data)
      dplyr::bind_rows(., dplyr::mutate(incoming_data_list[[index]], historical = FALSE)[-1,])
  }) %>%
    # Preserve the site-parameter naming convention in the result
    purrr::set_names(matching_indexes)
  
  return(combined_hist_inc_data)
}