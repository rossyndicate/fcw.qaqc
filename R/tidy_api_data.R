#' @title Process and summarize site-parameter combinations from API data
#'
#' @description
#' Transforms raw water quality monitoring data for a specific site-parameter
#' combination into a standardized time series with consistent intervals and
#' summary statistics. This function handles the essential task of converting
#' potentially irregular raw sensor readings into evenly-spaced time series
#' data required for effective quality control and analysis. The function
#' calculates mean values, data spread, and observation counts for each time
#' interval, then ensures a complete time series by filling in any missing
#' intervals with NA values.
#'
#' @param api_data A dataframe containing processed data for a single
#' site-parameter combination, typically a single element from the list
#' returned by splitting the munge_api_data() output.
#'
#' @param summarize_interval Character string specifying the time interval for
#' aggregating and rounding data points. Default is "15 minutes". Accepts any
#' interval format compatible with padr::pad() like "1 hour", "30 mins", etc.
#'
#' @return A dataframe containing processed time series data with consistent
#' time intervals and summary statistics:
#' - DT_round: Rounded timestamp defining each time interval
#' - site: Monitoring location identifier
#' - parameter: Measurement type (e.g., "Temperature", "DO")
#' - units: Measurement units (e.g., "°C", "mg/L")
#' - mean: Average value for the interval
#' - spread: Range of values within the interval (max - min)
#' - n_obs: Number of observations in each interval
#' - DT_join: Character representation of DT_round for joining operations
#' - flag: Empty column for subsequent quality control flagging
#'
#' @examples
#' # Process Temperature data for riverbluffs site with 15-minute intervals
#' riverbluffs_temp <- new_data[["riverbluffs-Temperature"]] %>%
#'   tidy_api_data(summarize_interval = "15 minutes")
#'
#' @seealso [munge_api_data()]
#' @seealso [combine_datasets()]

tidy_api_data <- function(api_data, summarize_interval = "15 minutes") {
  
  # Standardize "minutes" to "mins" for compatibility with padr::pad()
  if(grepl("minutes", summarize_interval)){
    summarize_interval <- gsub("minutes", "mins", summarize_interval, 
                             ignore.case = TRUE)
  }
  
  # Extract site and parameter information from the input data
  # This assumes api_data contains a single site-parameter combination
  site_arg <- unique(api_data$site)
  parameter_arg <- unique(api_data$parameter)
  
  # Process the data within a tryCatch to handle potential errors gracefully
  summary <- tryCatch({
    api_data %>%
      # Remove any duplicate records that might have been introduced
      dplyr::distinct() %>%
      
      # Group data by time interval and calculate summary statistics
      # This transforms potentially multiple readings per interval into a single row
      dplyr::group_by(DT_round, site, parameter, units) %>%
      dplyr::summarize(
        # Calculate the mean value for this time interval
        mean = as.numeric(mean(value, na.rm = T)),
        # Calculate the range of values within this time interval (max - min)
        spread = abs(min(value, na.rm = T) - max(value, na.rm = T)),
        # Count how many observations occurred in this time interval
        n_obs = n()
      ) %>%
      dplyr::ungroup() %>%
      dplyr::arrange(DT_round) %>%
      
      # Ensure a complete time series by padding with NA values for missing intervals
      # This is critical for consistent time series analysis
      padr::pad(by = "DT_round", interval = summarize_interval) %>%
      
      # Add columns needed for downstream processing
      dplyr::mutate(
        # Create string version of timestamp for joining operations
        DT_join = as.character(DT_round),
        # Ensure site and parameter values are preserved after padding
        site = site_arg,
        parameter = parameter_arg,
        # Add empty flag column for future quality control procedures
        flag = NA
      ) %>%
      
      # Remove any potential duplicates that might have been introduced
      dplyr::distinct(.keep_all = TRUE) %>%
      
      # Filter out any rows where site is NA (can happen with padding)
      dplyr::filter(!is.na(site))
  },
  error = function(err) {
    # Provide informative error message if processing fails
    cat("An error occurred with site ", site_arg, " parameter ", parameter_arg, ".\n")
    cat("Error message:", conditionMessage(err), "\n")
    flush.console() # Immediately print the error messages
    NULL # Return NULL in case of an error
  })
  
  return(summary)
}