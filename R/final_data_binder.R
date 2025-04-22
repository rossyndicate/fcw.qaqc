#' @title Combine new and historical water quality data
#' @export
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
#' @param new_flagged_data_list A list of dataframes containing newly processed and flagged 
#' water quality data. Each list element should be named with a site-parameter combination
#' (e.g., "riverbluffs-Temperature") and include a `historical` column indicating which
#' records overlap with the historical dataset.
#'
#' @param historical_flagged_data_list A list of dataframes containing previously processed
#' water quality data. Each list element should be named with a site-parameter combination
#' matching elements in `new_flagged_data_list`.
#'
#' @return A list of dataframes containing the merged data, with dataframes named by 
#' site-parameter combinations. Records in the 24-hour overlap period will retain flags
#' from the new data.
#'
#' @examples
#' # Examples are temporarily disabled

final_data_binder <- function(new_flagged_data_list, historical_flagged_data_list){
  
  # Handle cases where one or both data sources are unavailable:
  # ========================
  
  ## Handle case 0: Both inputs are empty or null.
  if ((is.null(historical_flagged_data_list) || length(historical_flagged_data_list) == 0) & 
      (is.null(new_flagged_data_list) || length(new_flagged_data_list) == 0)) {
    stop("No data provided to `final_data_binder()`.")
  }
  
  ## Handle case 1: Only historical data, no incoming data
  ## This can happen if no new data was collected since the last processing cycle
  if (is.null(new_flagged_data_list) || length(new_flagged_data_list) == 0) {
    stop("No new incoming data list provided to `final_data_binder()`.")
  }
  
  ## Handle case 2: Only incoming data, no historical data
  ## This can happen when monitoring a new site or after a system reset
  if (is.null(historical_flagged_data_list) || length(historical_flagged_data_list) == 0) {
    warning("No historical data list provided to `final_data_binder()`.")
    return(new_flagged_data_list) # set historical as TRUE here
  }
  
  # Handle the standard case: Both historical and incoming data exist
  # ========================
  
  # Find site-parameter combinations that exist in both datasets
  matching_indexes <- dplyr::intersect(names(new_flagged_data_list), names(historical_flagged_data_list))
  
  # Process each matching site-parameter combination
  updated_historical_flag_list <- purrr::map(matching_indexes, function(index) {
    
    # Get historical data excluding the 24-hour overlap period
    old <- historical_flagged_data_list[[index]] %>%
      dplyr::filter(DT_round < max(DT_round) - lubridate::hours(24))
    
    # Get historical data for the 24-hour overlap period
    old_to_update <- historical_flagged_data_list[[index]] %>%
      dplyr::filter(DT_round >= max(DT_round) - lubridate::hours(24))
    
    # Get newly processed data marked as historical (from overlap period)
    new_to_update <- new_flagged_data_list[[index]] %>%
      dplyr::filter(historical == TRUE) 
    
    # Check if overlap data has changed and notify if it has
    if(identical(old_to_update, new_to_update) == FALSE){
      print(paste0(index, " historical data has been updated based on the latest data."))
    }
    
    # Combine older historical data with all new data and sort chronologically
    old_and_new <- dplyr::bind_rows(old, new_flagged_data_list[[index]]) %>%
      dplyr::arrange(DT_round) 
    
    return(old_and_new)
    
  }) %>%
    purrr::set_names(matching_indexes) %>%
    purrr::discard(~ is.null(.))
  
  # Preserve data is in one data set, but not the other, (i.e. a site-Parameter
  # combination that is not found in the historical data, but is in the new data, 
  # or vice versa.)
  
  # Find site-parameter combinations that only exist in historical data
  historical_only_indexes <- setdiff(names(historical_flagged_data_list), names(new_flagged_data_list))
  
  # Add these historical-only combinations to the result
  if(length(historical_only_indexes) > 0) {
    historical_only_data <- historical_flagged_data_list[historical_only_indexes] %>% 
      purrr::set_names(historical_only_indexes)
    updated_historical_flag_list <- c(updated_historical_flag_list, historical_only_data)
    print(paste0("Preserved historical site-parameter combinations: ", 
                 paste(historical_only_indexes, collapse = ", ")))
  }
  
  # Find site-parameter combinations that only exist in new data
  new_only_indexes <- setdiff(names(new_flagged_data_list), names(historical_flagged_data_list))
  
  # Add these new combinations to the result
  if(length(new_only_indexes) > 0) {
    new_only_data <- new_flagged_data_list[new_only_indexes] %>% 
      purrr::set_names(new_only_indexes)
    updated_historical_flag_list <- c(updated_historical_flag_list, new_only_data)
    print(paste0("Added new site-parameter combinations: ", 
                 paste(new_only_indexes, collapse = ", ")))
  }

  return(updated_historical_flag_list)
}
