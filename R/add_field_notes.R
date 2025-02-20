#' @title Summarize site parameter data from the API and field notes data frames.
#' @description A function that summarizes and joins site parameter data from the API with the field notes data frames.
#' @param site_arg A site name.
#' @param parameter_arg A parameter name.
#' @param df A dataframe with the tidied time series data
#' @param summarize_interval At what time interval the user would like the data set to be aggregated and rounded to. Default is 15 minutes
#' @param notes The munged field notes
#' @return A dataframe with summary statistics for a given site parameter data frame, plus field notes data

add_field_notes <- function(df, notes) {
  
  site_arg <- unique(df$site)
  parameter_arg <- unique(df$parameter)
  
  # filter deployment records for the full join
  site_field_notes <- notes %>%
    dplyr::filter(grepl(paste(unlist(stringr::str_split(site_arg, " ")), collapse = "|"), site, ignore.case = TRUE))
  
  # filtering the data and generating results
  summary <- tryCatch({
    df %>%
      # safety step of removing any erroneous dupes
      dplyr::distinct() %>%
      # join our tidied data frame with our field notes data:
      dplyr::full_join(., dplyr::select(site_field_notes, sonde_employed, sonde_moved,
                                        last_site_visit, DT_join, visit_comments,
                                        sensor_malfunction, cals_performed),
                       by = c('DT_join')) %>%
      arrange((DT_join)) %>%
      # make sure DT_join is still correct:
      dplyr::mutate(DT_round = lubridate::as_datetime(DT_join, tz = "MST")) %>%
      # Use fill() to determine when sonde was in the field, and when the last site visit was
      # Necessary step for FULL dataset only (this step occurs in combine_hist_inc_data.R for auto QAQC)
      dplyr::mutate(sonde_employed = ifelse(is.na(sonde_employed), 0, sonde_employed)) %>%
      tidyr::fill(c(sonde_employed, last_site_visit, sensor_malfunction)) %>%
      # for instances at the top of df's where log was running ahead of deployment:
      dplyr::mutate(sonde_employed = ifelse(is.na(last_site_visit), 1, sonde_employed)) %>%
      dplyr::distinct(.keep_all = TRUE) %>%
      dplyr::filter(!is.na(site))
    
  },
  
  error = function(err) {
    # error message
    cat("An error occurred with site ", site_arg, " parameter ", parameter_arg, ".\n")
    cat("Error message:", conditionMessage(err), "\n")
    flush.console() # Immediately print the error messages
    NULL  # Return NULL in case of an error
  })
  
  return(summary)
  
}
