#' @title Merge historical and new water quality monitoring data
#' @export
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
#' # Examples are temporarily disabled
#' @seealso [tidy_api_data()]
#' @seealso [add_field_notes()]
#' @seealso [generate_summary_statistics()]

combine_datasets <- function(incoming_data_list, historical_data_list) {
  
  # Handle cases where one or both data sources are unavailable:
  # ========================
  
  ## Handle case 0: Both inputs are empty or null.
  if ((is.null(historical_data_list) || length(historical_data_list) == 0) &
      (is.null(incoming_data_list) || length(incoming_data_list) == 0)) {
    stop("No data provided to `combine_datasets()`.") 
  }
  
  ## Handle case 1: Only historical data, no incoming data
  ## This pipeline should never reach this step. The pipeline should stop if there
  ## is no new incoming data.
  if (is.null(incoming_data_list) || length(incoming_data_list) == 0) {
    stop("No new incoming data list provided to `combine_datasets()`.")
  }
  
  ## Handle case 2: Only incoming data, no historical data
  ## This can happen after a system reset 
  if (is.null(historical_data_list) || length(historical_data_list) == 0) {
    warning("No historical data list provided to `combine_datasets()`.")
    
    # Mark all incoming data as non-historical and return
    new_data <- purrr::map(incoming_data_list,
                           function(data){
                             data %>%
                               dplyr::mutate(historical = FALSE,
                                             flag = as.character(flag)) 
                           })
    return(new_data)
  }
  
  # Handle the standard case: Both historical and incoming data exist
  # ========================
  
  # Find site-parameter combinations that exist in both datasets
  matching_indexes <- dplyr::intersect(names(incoming_data_list), names(historical_data_list))
  
  # Combine historical and incoming data for each matching site-parameter combination
  combined_hist_inc_data <- purrr::map(matching_indexes, function(index) {
    
    # Extract the most recent 24 hours of historical data for each site-parameter combo
    # This creates an overlap period that helps identify sensor drift
    hist_data <- historical_data_list[[index]] %>%
      # Get the last 24 hours of data
      dplyr::filter(DT_round >= max(DT_round) - lubridate::hours(24)) %>%
      # Mark as historical and preserve quality flags
      dplyr::mutate(historical = TRUE,
                    # Copy auto_flag to flag column for continuity
                    flag = as.character(auto_flag)) %>%
      # Remove the sonde_moved column so it can be recalculated
      # based on combined historical and new data
      dplyr::select(-sonde_moved)
    
    # Prepare the incoming data
    inc_data <- incoming_data_list[[index]] %>% 
      dplyr::mutate(historical = FALSE,
                    flag = as.character(flag))
    
    # Find unique timestamps in incoming data
    unique_inc <- inc_data %>%
      dplyr::anti_join(hist_data, by = "DT_round")
    
    # Combine historical with unique incoming data
    dplyr::bind_rows(hist_data, unique_inc) %>% 
      # Ensure chronological order
      dplyr::arrange(DT_round)
  }) %>%
    # Preserve the site-parameter naming convention in the result
    purrr::set_names(matching_indexes)
  
  # Preserve data that only exists in the incoming data
  ## This can happen after a system reset or when monitoring a new site
  
  # Find site-parameter combinations that only exist in new data
  new_only_indexes <- setdiff(names(incoming_data_list), names(historical_data_list))
  
  # Add these new combinations to the result
  if(length(new_only_indexes) > 0) {
    # Process new site-parameter combinations
    new_only_data <- purrr::map(new_only_indexes,
                                function(index){
                                  incoming_data_list[[index]] %>% 
                                    dplyr::mutate(historical = FALSE,
                                                  flag = as.character(flag)) 
                                }) %>% 
      purrr::set_names(new_only_indexes)
    
    # Add these combinations to the results
    combined_hist_inc_data <- c(combined_hist_inc_data, new_only_data)
    print(paste0("Added new site-parameter combinations: ", 
                 paste(new_only_indexes, collapse = ", ")))
  }
  
  # Return the combined dataset (historical data is not needed in output if not in incoming data)
  return(combined_hist_inc_data)
}
