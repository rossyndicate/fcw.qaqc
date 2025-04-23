#' @title Get the start dates for the HydroVu API pull from historically flagged data
#' @export
#'
#' @description
#' Attempts to find the most recent timestamp in Temperature parameter data for each monitoring 
#' site's historically flagged data. Temperature is used as the reference parameter 
#' because it is consistently tracked by all sondes, providing the most reliable 
#' indication of when data was last collected from each site. This function will compare
#' the temperature data's most recent timestamp to that of all the other parameters.
#' If it finds that these are not equal, it will prefer to use the earliest timestamp,
#' regardless of its source, with the assumption that the earlier start dates should be
#' attributed to parameters that are not Temperature. 
#' 
#' The resulting timestamps serve as starting points for new API data requests,
#' ensuring continuous data collection without gaps or duplication.
#'
#' @param incoming_historically_flagged_data_list A list of dataframes containing 
#'   historical water quality data that has been processed through the QAQC 
#'   workflow. Each list element should represent a site-parameter combination and 
#'   be named using the "site-parameter" convention.
#'
#' @return A dataframe with three columns:
#'   - site: The monitoring location identifier
#'   - start_DT: The timestamp (in UTC) of the most recent reading for each site
#'   - end_DT: The current system time (in UTC)
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [api_puller()]
#' @seealso [munge_api_data()]

get_start_dates <- function(incoming_historically_flagged_data_list) {
  
  # Create the start date directly in Denver time
  default_denver_date <- as.POSIXct(paste0(lubridate::year(Sys.time()), "-03-01"), tz = "America/Denver")
  
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
    end_DT = as.POSIXct(Sys.time(), tz = "UTC") 
  )
  
  # If no historical data is provided, return default dates
  if (length(incoming_historically_flagged_data_list) == 0) {
    return(default_start_dates)
  }
  
  # Extract only Temperature parameter dataframes
  temperature_subset <- grep("Temperature", names(incoming_historically_flagged_data_list))
  
  # Extract each site's most recent timestamp based on temperature data
  temp_start_dates_df <- incoming_historically_flagged_data_list[temperature_subset] %>% 
    dplyr::bind_rows() %>% 
    dplyr::mutate(DT_round = lubridate::with_tz(DT_round, "UTC")) %>% 
    dplyr::group_by(site) %>% 
    dplyr::summarize(start_DT = max(DT_round, na.rm = TRUE)) %>% 
    dplyr::select(start_DT, site)
  
  # Extract each site's most recent timestamp across all parameters
  all_params_start_dates_df <- incoming_historically_flagged_data_list %>% 
    dplyr::bind_rows() %>% 
    dplyr::mutate(DT_round = lubridate::with_tz(DT_round, "UTC")) %>% 
    dplyr::group_by(site) %>% 
    dplyr::summarize(start_DT = max(DT_round, na.rm = TRUE)) %>% 
    dplyr::select(start_DT, site)
  
  # Check if temperature dates equal all parameter dates
  test_dates <- dplyr::full_join(temp_start_dates_df, all_params_start_dates_df, by = "site") %>% 
    dplyr::mutate(dates_equal = start_DT.x == start_DT.y)
  
  all_true <- all(test_dates$dates_equal, na.rm = TRUE)
  
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
      end_DT = as.POSIXct(Sys.time(), tz = "UTC")
    ) %>% 
    dplyr::select(-c(start_DT.y, start_DT.x)) %>% 
    dplyr::relocate(site, start_DT, end_DT)
  
  return(final_start_dates_df)
}
