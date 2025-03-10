#' @title Update Historical Flag List
#'
#' @description
#' A function that updates the historical flag list with new flagged data.
#'
#' @param new_flagged_data A list of data frames that have been flagged.
#'
#' @param historical_flagged_data A list of data frames that have been flagged.
#'
#' @return A list of data frames that have been updated with new flagged data.
final_data_binder <- function(new_flagged_data, historical_flagged_data){
  
  if ((is.null(historical_flagged_data) || length(historical_flagged_data) == 0) & 
      (is.null(new_flagged_data) || length(new_flagged_data) == 0)) {
    stop("No data!")
  }
  
  if (is.null(new_flagged_data) || length(new_flagged_data) == 0) {
    
    print("No new incoming data.")
    
    
    
    return(historical_data)
  }
  
  
  if (is.null(historical_flagged_data) || length(historical_flagged_data) == 0) {
    print("No historical data.")
    
    return(new_flagged_data)
    
  }
  
  
  # Get the matching index names
  matching_indexes <- dplyr::intersect(names(new_flagged_data), names(historical_flagged_data))
  
  # bind new_flagged_data and historical_flagged_data together
  updated_historical_flag_list <- purrr::map(matching_indexes, function(index) {
    
    old <- historical_flagged_data[[index]] %>%
      dplyr::filter(DT_round < max(DT_round) - lubridate::hours(24))  # this is the antijoin step (but i thought we were doing 24 hours?) -jd changing this to 24 removes the duplicate problem
    
    old_to_update <- historical_flagged_data[[index]] %>%
      dplyr::filter(DT_round >= max(DT_round) - lubridate::hours(24))  # this is the antijoin step (but i thought we were doing 24 hours?) -jd changing this to 24 removes the duplicate problem
    
    new_to_update <- new_flagged_data[[index]] %>%
      dplyr::filter(historical == TRUE) %>%
      dplyr::select(-historical)
    
    if(identical(old_to_update, new_to_update) == FALSE){
      
      print(paste0(index, " historical data has been updated based on the latest data."))
    }
    
    old_and_new <- dplyr::bind_rows(old, new_flagged_data[[index]]) %>%
      arrange(DT_round) 
    
  }) %>%
    purrr::set_names(matching_indexes) %>%
    purrr::keep(~ !is.null(.))
  
  # need to make sure that when we are doing this we are not getting rid of dfs in the RDS list, still needs to be checked -jd
  # if we are matching up the historical list(88) with the incoming list(72) and only returning the matches then we will miss
  # be returning a list that is being written that is shorter than the list that we started with... Definitely needs to be fixed.
  
  return(updated_historical_flag_list)
  
}

# error 1: tz discrepancy at the join between incoming data and HFD
# this messes up the future pulls because the start times df will be wrong

# time_diffs <- diff(test_data$DT_round)
#
# time_diffs # these should all be 15 and there is one that is not (where we joined incoming data to the HFD)
