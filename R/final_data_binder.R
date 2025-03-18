#' @title Combine new and historical water quality data
#'
#' @description
#' Merges newly processed water quality data with the existing historical dataset,
#' handling the overlap period appropriately. This function ensures continuity in 
#' the long-term dataset while incorporating the latest quality control flags.
#'
#' The function handles a 24-hour overlap period specifically, where new data may
#' contain updated flags for historical data based on context that wasn't available
#' during previous processing runs.
#'
#' @param new_flagged_data A list of dataframes containing newly processed and flagged 
#' water quality data. Each list element should be named with a site-parameter combination
#' (e.g., "riverbluffs-Temperature") and include a `historical` column indicating which
#' records overlap with the historical dataset.
#'
#' @param historical_flagged_data A list of dataframes containing previously processed
#' water quality data. Each list element should be named with a site-parameter combination
#' matching elements in `new_flagged_data`.
#'
#' @return A list of dataframes containing the merged data, with dataframes named by 
#' site-parameter combinations. Records in the 24-hour overlap period will retain flags
#' from the new data.
#'
#' @examples
#' # Combine new flagged data with historical records
#' updated_dataset <- final_data_binder(
#'   new_flagged_data = final_flags,
#'   historical_flagged_data = historical_data
#' )
#'
#' # Save the updated dataset
#' saveRDS(updated_dataset, "data/pwqn_data.RDS")

final_data_binder <- function(new_flagged_data, historical_flagged_data){
  
  # Handle case when both inputs are empty or null
  if ((is.null(historical_flagged_data) || length(historical_flagged_data) == 0) & 
      (is.null(new_flagged_data) || length(new_flagged_data) == 0)) {
    stop("No data!")
  }
  
  # Handle case when only historical data exists
  if (is.null(new_flagged_data) || length(new_flagged_data) == 0) {
    print("No new incoming data.")
    return(historical_data)
  }
  
  # Handle case when only new data exists
  if (is.null(historical_flagged_data) || length(historical_flagged_data) == 0) {
    print("No historical data.")
    return(new_flagged_data)
  }
  
  # Find site-parameter combinations that exist in both datasets
  matching_indexes <- dplyr::intersect(names(new_flagged_data), names(historical_flagged_data))
  
  # Process each matching site-parameter combination
  updated_historical_flag_list <- purrr::map(matching_indexes, function(index) {
    
    # Get historical data excluding the 24-hour overlap period
    old <- historical_flagged_data[[index]] %>%
      dplyr::filter(DT_round < max(DT_round) - lubridate::hours(24))
    
    # Get historical data for the 24-hour overlap period
    old_to_update <- historical_flagged_data[[index]] %>%
      dplyr::filter(DT_round >= max(DT_round) - lubridate::hours(24))
    
    # Get newly processed data marked as historical (from overlap period)
    new_to_update <- new_flagged_data[[index]] %>%
      dplyr::filter(historical == TRUE) %>%
      dplyr::select(-historical)
    
    # Check if overlap data has changed and notify if it has
    if(identical(old_to_update, new_to_update) == FALSE){
      print(paste0(index, " historical data has been updated based on the latest data."))
    }
    
    # Combine older historical data with all new data and sort chronologically
    old_and_new <- dplyr::bind_rows(old, new_flagged_data[[index]]) %>%
      arrange(DT_round) 
    
  }) %>%
    purrr::set_names(matching_indexes) %>%
    purrr::discard(~ is.null(.))
  
  return(updated_historical_flag_list)
}