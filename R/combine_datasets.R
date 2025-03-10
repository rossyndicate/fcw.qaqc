#' @title Combine historical and incoming data
#' 
#' @description
#' A function that combines the subset of historically flagged data and creates historical columns.
#' NOTE: historical columns should be a part of the historically flagged data, but it is not at this point.
#' 
#' @param incoming_data_list A list of dataframes with the incoming data.
#' 
#' @param historical_data_list A list of dataframes with the historical data.
#' 
#' @return A list of dataframes with the combined historical and incoming data.
#' 
#' @examples
#' combine_hist_inc_data(incoming_data_list = incoming_data_list, historical_data_list = historical_data_list)

combine_datasets <- function(incoming_data_list, historical_data_list) {
  
  if ((is.null(historical_data_list) || length(historical_data_list) == 0) & 
      (is.null(incoming_data_list) || length(incoming_data_list) == 0)) {
    stop("No data!")
  }
  
  
  if (is.null(incoming_data_list) || length(incoming_data_list) == 0) {
    
    print("No new incoming data.")
    
    last_24_hours <- purrr::map(historical_data_list,
                                function(data) {
                                  data %>%
                                    # most recent day of already-flagged data... original line of code below didn't work:
                                    # dplyr::filter(DT_round >= lubridate::ymd_hms(max(DT_round) - lubridate::hours(24), tz = "MST")) %>%
                                    dplyr::filter(DT_round >= max(DT_round) - lubridate::hours(24)) %>%
                                    # mark it as "historic"
                                    dplyr::mutate(historical = TRUE,
                                                  # ensure last_site_visit column has proper DT:
                                                  # last_site_visit = lubridate::force_tz(last_site_visit, tzone = "MST"),
                                                  # preserved_flag = flag,
                                                  flag = auto_flag)
                                })
    
    return(last_24_hours)
  }
  
  if (is.null(historical_data_list) || length(historical_data_list) == 0) {
    print("No historical data.")
    
    new_data <- purrr::map(incoming_data_list,
                           function(data){
                             data %>%
                               dplyr::mutate(historical = FALSE)
                           })
    
    return(new_data)
    
  }
  
  # Get the matching index names
  matching_indexes <- intersect(names(incoming_data_list), names(historical_data_list))
  
  # Get the last 24 hours of the historically flagged data based on earliest
  # DT listed in the api pull for that site/data combo
  last_24_hours <- purrr::map(historical_data_list,
                              function(data) {
                                data %>%
                                  # KW THIS iS ANOTHER SCARY DT THAT NEEDS CONFIRMED!
                                  dplyr::filter(DT_round >= max(DT_round) - lubridate::hours(24)) %>%
                                  # mark it as "historic"
                                  dplyr::mutate(historical = TRUE,
                                                # create new column flag to preserve historical auto_flag data,
                                                flag = auto_flag) %>%
                                  # remove the sonde_moved column so that this gets updated across both historical and new data:
                                  dplyr::select(-sonde_moved)
                              })
  
  # bind summarized_incoming_data and last_24_hours together
  combined_hist_inc_data <- purrr::map(matching_indexes, function(index) {
    last_24_hours[[index]] %>%
      dplyr::bind_rows(., dplyr::mutate(incoming_data_list[[index]], historical = FALSE)[-1,])
  }) %>%
    purrr::set_names(matching_indexes)
  
  return(combined_hist_inc_data)
}

