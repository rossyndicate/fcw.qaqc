#' @title Get the start dates for the HydroVu API pull from the historically flagged data
#' @export
#'
#' @description
#' This function finds the most recent timestamp (max datetime) in the Temperature 
#' parameter data frames for each monitoring site's historically flagged data. 
#' Temperature is used as the reference parameter because it is consistently 
#' tracked by all sondes and therefore provides the most reliable indication of 
#' when data was last collected from each site.
#' 
#' The resulting timestamps serve as starting points for new API data requests,
#' ensuring continuous data collection without gaps or unnecessary duplication.
#'
#' @param incoming_historically_flagged_data_list A list of dataframes containing 
#' historical water quality data that has already been processed through the QAQC 
#' workflow. Each list element should represent a site-parameter combination and 
#' be named with the "site-parameter" naming convention.
#'
#' @return A dataframe containing two columns:
#' - site: The monitoring location identifier
#' - DT_round: The timestamp (in MST timezone) of the most recent Temperature 
#' reading for each site, which will be used as the start date for new API pulls
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [api_puller()]
#' @seealso [munge_api_data()]

get_start_dates <- function(incoming_historically_flagged_data_list) {
  
  # Create the start date directly in Denver time
  denver_date <- as.POSIXct(paste0(lubridate::year(Sys.time()), "-03-01"), tz = "America/Denver")
  
  # Convert the start date to UTC for API use
  converted_start_DT <- lubridate::with_tz(denver_date, "UTC")
  
  # Generate the default start dates tibble
  default_start_dates <- tibble(
    site = c("bellvue", # rist
             "salyer",
             "udall",
             "riverbend",
             "cottonwood",
             "elc", # elc
             "archery",
             "riverbluffs"),
    start_DT = converted_start_DT, 
    end_DT = as.POSIXct(Sys.time(), tz = "UTC") 
  )
  
  # Extract only the Temperature parameter dataframes from the historical data list
  temperature_subset <- grep("Temperature", names(incoming_historically_flagged_data_list))
  
  
  if (length(incoming_historically_flagged_data_list) > 0) {
    
    # Extract each sites most recent timestamp based on their temperature data
    historical_start_dates_df <- incoming_historically_flagged_data_list[temperature_subset] %>% 
      dplyr::bind_rows(.) %>% 
      dplyr::mutate(DT_round = lubridate::with_tz(DT_round, "UTC")) %>% 
      dplyr::group_by(site) %>% 
      dplyr::slice_max(DT_round) %>% 
      dplyr::select(start_DT = DT_round, site) %>% 
      dplyr::ungroup()
    
    # update default df
    final_start_dates_df <- default_start_dates %>% 
      left_join(historical_start_dates_df, by = "site") %>% 
      mutate(start_DT = coalesce(start_DT.y, start_DT.x),
             end_DT = as.POSIXct(Sys.time(), tz = "UTC") ) %>% 
      select(-c(start_DT.y, start_DT.x)) %>% 
      relocate(site, start_DT, end_DT)
    
    return(final_start_dates_df)
    
  } else { 
    
    # If the historical data is empty, default to default_start_dates
    final_start_dates_df <- default_start_dates
    
    return(final_start_dates_df)
  }
}
