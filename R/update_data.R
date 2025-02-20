#' @title Update master data list
#' 
#' @description
#' A function that updates the historical flag list with new flagged data.
#' 
#' @param new_flagged_data A list of data frames that have been flagged.
#' 
#' @param historical_flagged_data A list of data frames that have been flagged.
#' 
#' @return A list of data frames that have been updated with new flagged data.
update_data <- function(new_flagged_data, historical_flagged_data){
  
  # Get the matching index names
  matching_indexes <- intersect(names(new_flagged_data), names(historical_flagged_data))
  
  # bind new_flagged_data and historical_flagged_data together
  updated_historical_flag_list <- map(matching_indexes, function(index) {
    
    old <- historical_flagged_data[[index]] %>%
      filter(DT_round < ymd_hms(max(DT_round) - hours(24), tz = "MST")) %>% # this is the antijoin step (but i thought we were doing 24 hours?) -jd changing this to 24 removes the duplicate problem
      mutate(last_site_visit = force_tz(last_site_visit, tzone = "MST"))
    
    bind_rows(old, new_flagged_data[[index]]) %>%
      arrange(DT_round) %>%
      select(-historical) # if we are removing this then I think we should remove that step from combined_data -jd
  }) %>%
    set_names(matching_indexes) %>%
    keep(~ !is.null(.))
  
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