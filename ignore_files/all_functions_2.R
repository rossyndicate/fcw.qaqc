# Combined R Functions
# Auto-generated on 2025-04-09 16:31:48



# ===================================
# File: R/add_burial_flag.R
# ===================================

#' @title Flag all parameters within sonde when sonde burial is detected by DO sensor
#'
#' @description
#' Identifies and flags all sensor parameters when evidence of sonde burial is detected
#' by the dissolved oxygen (DO) sensor. When a monitoring sonde becomes buried in sediment,
#' all measurements are affected, but the DO sensor often provides the clearest indication
#' of burial events.
#'
#' This function propagates burial flags detected by the `find_do_noise()` function to all
#' other parameters measured at the same time points. This ensures consistent flagging
#' across all measurements during burial events.
#'
#' Unlike most flagging functions, this function operates on site-level dataframes
#' containing multiple parameters rather than individual site-parameter combinations.
#'
#' @param df A dataframe containing all parameters for a single site. Must include columns:
#' - `parameter`: Measurement type (function looks for "DO" parameter)
#' - `DT_join`: Character timestamp used for joining measurements across parameters
#' - `flag`: Existing quality flags, with "Possible burial" flags already applied to DO
#'   measurements by the `find_do_noise()` function
#'
#' @return A dataframe with the same structure as the input, but with the `flag`
#' column updated to include "Possible burial" flags for all parameters when burial
#' is detected by the DO sensor.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [add_flag()]
#' @seealso [find_do_noise()]

add_burial_flag <- function(df){  
  # Extract DO readings and their flag status for the site
  # This creates a reference table of timestamps where burial was detected
  do <- df %>%
    data.table::data.table() %>%
    dplyr::filter(parameter == "DO") %>%
    dplyr::select(DT_join, do_flag = flag)
  
  
  do_checked <- df %>%
    # Process all parameters except DO first to avoid duplicate flagging
    dplyr::filter(!parameter %in% c("DO")) %>%
    # Join with DO flags using timestamp to align readings across parameters
    dplyr::left_join(., do, by = "DT_join") %>%
    # Add burial flag to all parameters at timestamps where DO indicates burial
    add_flag(grepl("Possible burial", do_flag), "Possible burial") %>%
    # Remove the temporary DO flag column we added
    dplyr::select(-do_flag) %>%
    # Add back the original DO parameter data (already properly flagged)
    dplyr::bind_rows(df %>% filter(parameter == "DO")) %>%
    # Remove any duplicate rows that might have been created
    dplyr::distinct()
    
  return(do_checked)
  
}


# ===================================
# File: R/add_depth_shift_flag.R
# ===================================

#' @title Flag periods when sonde depth was changed
#'
#' @description
#' Identifies and flags time periods when sondes were physically 
#' moved or repositioned in their housings. Sonde depth changes can affect multiple 
#' parameters and cause discontinuities in the data that aren't related to actual 
#' environmental changes.
#'
#' The function handles two different data processing workflows:
#' - Pre-2024 data: Uses a separate table that tracks when sondes were moved
#' - Post-2024 data: Uses information already integrated into field notes 
#'   through the `sonde_moved` boolean field
#'
#' @param df A data frame containing water quality measurements. Must include columns:
#' - `site`: Standardized site name
#' - `DT_join`: Character timestamp in a format compatible with joining to field notes
#' - `flag`: Existing quality flags (will be updated by this function)
#' - `sonde_moved`: Boolean field indicating if sonde was moved (only required when `post2024 = TRUE`)
#'
#' @param level_shift_table A data frame containing field notes with information about 
#' when sondes were physically moved. When `post2024 = FALSE`, must include columns:
#' - `site`: Site name matching the df site column
#' - `DT_join`: Timestamp of the sonde movement
#' - `type`: Field identifying records where type = "sonde moved"
#'
#' @param post2024 Logical value indicating whether the data is from before (FALSE) or 
#' after (TRUE) the 2024 field season. Default is TRUE.
#'
#' @return A data frame with the same structure as the input, plus an additional
#' `depth_change` column containing "sonde moved" flags where applicable.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [add_field_flag()]
#' @seealso [add_flag()]

add_depth_shift_flag <- function(df, level_shift_table, post2024 = TRUE){
  if(post2024 == FALSE){
    # For pre-2024 data, extract sonde movement events from the shift table
    # and join them with the measurement data
    depth_shifts <- level_shift_table %>%
      dplyr::filter(type == "sonde moved") %>%
      dplyr::mutate(DT_join = as.character(lubridate::ymd_hms(DT_join)))
    
    add_shifts <- df %>%
      dplyr::left_join(., depth_shifts, by = c("site", "DT_join")) %>%
      dplyr::mutate(depth_change = ifelse(!is.na(type), "sonde moved", NA)) %>%
      dplyr::select(-type)
    
    return(add_shifts)
  } else {
    # For post-2024 data, the sonde_moved field is already in the dataset
    # from the integrated field notes
    df <- df %>%
      dplyr::mutate(depth_change = ifelse(sonde_moved == TRUE, "sonde moved", NA))
    
    return(df)
    
  }
}


# ===================================
# File: R/add_drift_flag.R
# ===================================

#' @title Flag sensor drift in optical measurements
#'
#' @description
#' Identifies and flags periods when optical sensors show evidence of progressive drift,
#' which often indicates biofouling or calibration issues. The function analyzes time series
#' patterns to detect when measurements show a steady linear trend over time that is
#' unlikely to represent natural environmental conditions.
#'
#' The function only processes optical parameters prone to biofouling:
#' - FDOM Fluorescence
#' - Turbidity 
#' - Chl-a Fluorescence
#'
#' For other parameters, the function returns the dataframe unchanged.
#'
#' @param df A data frame containing water quality measurements. Must include columns:
#' - `parameter`: The measurement type (function checks if it's an optical parameter)
#' - `mean`: The calculated mean value of measurements
#' - `flag`: Existing quality flags (will be updated by this function)
#' - `DT_round`: Timestamp for rolling window analysis
#'
#' @return A data frame with the same structure as the input, but with the `flag`
#' column updated to include "drift" for periods showing evidence of sensor drift.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [add_flag()]

add_drift_flag <- function(df){
  # Only apply drift detection to optical parameters prone to biofouling
  if(unique(df$parameter) %in% c("FDOM Fluorescence", "Turbidity", "Chl-a Fluorescence")){
 
    # Define function to detect linear trends in rolling windows
    # Returns R-squared value of linear fit between measurements and time
    progressive_drift <- function(x) {
      # Only assess time series with less than 10% missing data in the rolling window
      if(length(x[!is.na(x)]) > (length(x) - (length(x)*0.1))){
        # Fit linear model of measurements vs. time index
        model <- lm(x ~ c(1:length(x)), na.action = na.omit)
        # Extract R-squared value indicating strength of linear trend
        r_squared <- summary(model)$r.squared
        # Return R-squared value
        return(r_squared)
      } else {
        # If too much missing data, set R-squared to 0 (no trend)
        no_slope <- 0
        return(no_slope)
      }
    }
    
    # Function to check if the mean R-squared in a window exceeds threshold
    # Returns TRUE if mean R-squared ≥ 0.60, indicating consistent drift
    check_too_steady <- function(x) {
      mean(x) >= 0.60
    }
    
    # Apply drift detection analysis to the time series
    df <- df %>%
      data.table::data.table() %>%
      # Calculate R-squared values for different window sizes and alignments
      # 96 observations = 1 day at 15-minute intervals
      # 288 observations = 3 days at 15-minute intervals
      dplyr::mutate(
        # 1-day windows with different alignments
        r2_s_right = data.table::frollapply(mean, n = 96, FUN = progressive_drift, align = "right", fill = NA),
        r2_s_center = data.table::frollapply(mean, n = 96, FUN = progressive_drift, align = "left", fill = NA),
        # 3-day windows with different alignments
        r2_l_right = data.table::frollapply(mean, n = 288, FUN = progressive_drift, align = "right", fill = NA),
        r2_l_center = data.table::frollapply(mean, n = 288, FUN = progressive_drift, align = "left", fill = NA),
        # Take the maximum R-squared from any window configuration
        tightest_r = pmax(r2_s_center, r2_s_right, r2_l_center, r2_l_right, na.rm = TRUE),
        # Check if any 1-day period has consistently high R-squared values
        failed = data.table::frollapply(tightest_r, n = 96, FUN = check_too_steady, align = "right", fill = NA)) %>%
      # Add drift flag for periods with consistent linear trends
      # Only add if drift flag doesn't already exist
      add_flag(failed == 1, "drift")
    
    return(df)
  } else {
    # Return unmodified dataframe for non-optical parameters
    return(df)
  }
}


# ===================================
# File: R/add_field_flag.R
# ===================================

#' @title Flag data periods affected by field activities
#'
#' @description
#' Identifies and flags time periods in water quality data that are affected by
#' field activities such as sensor maintenance, calibration, or deployment changes.
#' This function marks three distinct conditions related to field operations:
#'
#' 1. "sonde not employed" - Periods when the sensor was physically removed from
#'    the water body (indicated by sonde_employed = 1 in field notes)
#'
#' 2. "site visit" - Exact timestamps when field technicians were actively
#'    working with the equipment on site
#'
#' 3. "sv window" - A buffer period around site visits (15 minutes before and
#'    60 minutes after) when data may be affected by field activities
#'
#' These flags identify periods when readings may not reflect natural environmental 
#' conditions due to human interference. The function requires that field notes 
#' have already been joined to the water quality data.
#'
#' @param df A dataframe containing water quality data with field notes already
#' joined. Must include columns:
#' - flag: Existing quality flags
#' - sonde_employed: Binary indicator of sonde deployment status (1 = not in water)
#' - last_site_visit: Timestamp of the most recent site visit
#' - DT_round: Rounded timestamp for each data record
#'
#' @return A dataframe with the same structure as the input, but with the flag
#' column updated to include "sonde not employed", "site visit", and/or "sv window"
#' flags as appropriate.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [add_field_notes()]
#' @seealso [add_flag()]

add_field_flag <- function(df) {
  
  # First, flag periods when the sonde was physically removed from the water
  # This identifies data that doesn't represent in-situ water conditions
  df <- df %>%
    add_flag(sonde_employed == 1, "sonde not employed") %>%
    
    # Next, flag the exact timestamps when technicians were at the site
    # These represent direct human interference with the equipment
    add_flag(as.character(last_site_visit) == as.character(DT_round), "site visit")
  
  # Add flags for the post-visit window (60 minutes after a site visit)
  # This accounts for the recovery period after equipment handling
  # For 15-minute data, this means checking the next 4 timestamps after a site visit
  for (i in 1:4) {
    df <- df %>%
      # Look back i steps to see if there was a site visit flag
      # If found, mark this timestamp as within the recovery window
      add_flag(lag(stringr::str_detect(flag, "site visit"), n = i), "sv window")
  }
  
  # Add flags for the pre-visit window (15 minutes before a site visit)
  # This accounts for potential disturbance before logging the visit time
  df <- df %>%
    # Look ahead 1 step to see if there will be a site visit flag
    # If found, mark this timestamp as within the preparation window
    add_flag(lead(stringr::str_detect(flag, "site visit"), n = 1), "sv window")
  
  return(df)
}


# ===================================
# File: R/add_field_notes.R
# ===================================

#' @title Integrate field notes with water quality monitoring data
#'
#' @description
#' This function merges sensor readings with important field
#' observations about equipment status, maintenance activities, calibration events,
#' and site conditions.
#'
#' The function handles the complexity of matching timestamped field notes to the
#' continuous time series data, ensuring that important information about sensor
#' deployment, maintenance, and observed issues is properly associated with all
#' relevant data points. It also performs forward-filling of certain field attributes
#' to maintain continuity of status information (like deployment status) between
#' field visits.
#'
#' @param df A dataframe containing processed time series data for a single
#' site-parameter combination, typically an element generated by `combine_datasets`.
#'
#' @param notes A dataframe containing processed field notes from mWater, typically
#' the output from `grab_mWater_sensor_notes()`. These notes include information
#' about sensor deployment, maintenance, and observed issues.
#'
#' @return A dataframe containing the original time series data enriched with
#' field note information:
#' - All columns from the original time series data
#' - sonde_employed: Indicator of sensor deployment status
#' - sonde_moved: Indicator of sensor position changes
#' - last_site_visit: Timestamp of most recent site visit
#' - visit_comments: Technician observations during site visits
#' - sensor_malfunction: Noted sensor issues
#' - cals_performed: Calibration events
#'
#' Field status information is forward-filled to maintain continuity between
#' site visits.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [grab_mWater_sensor_notes()]
#' @seealso [tidy_api_data()]
#' @seealso [combine_datasets()]
#' @seealso [generate_summary_statistics()]
#' @seealso [add_field_flag()]

add_field_notes <- function(df, notes) {
  
  # Extract site and parameter information from the input data
  site_arg <- unique(df$site)
  parameter_arg <- unique(df$parameter)
  
  # Filter field notes to include only those relevant to this site
  # This uses a flexible matching approach that handles variations in site naming
  site_field_notes <- notes %>%
    dplyr::filter(grepl(paste(unlist(stringr::str_split(site_arg, " ")), 
                             collapse = "|"), site, ignore.case = TRUE))
  
  # Process the data within a tryCatch to handle potential errors gracefully
  summary <- tryCatch({
    df %>%
      # Remove any duplicate records that might have been introduced
      dplyr::distinct() %>%
      
      # Join the time series data with relevant field note information
      # This adds human observations to the sensor readings
      dplyr::full_join(., dplyr::select(site_field_notes, 
                                      sonde_employed, sonde_moved,
                                      last_site_visit, DT_join, visit_comments,
                                      sensor_malfunction, cals_performed),
                       by = c('DT_join')) %>%
      
      # Ensure proper temporal ordering of the combined data
      arrange((DT_join)) %>%
      
      # Ensure timestamps remain in correct datetime format after joining
      dplyr::mutate(DT_round = lubridate::as_datetime(DT_join, tz = "MST")) %>%
      
      # Set default sonde_employed status (0 = deployed/in water)
      # and forward-fill deployment status and site visit information
      # This maintains status continuity between discrete field observations
      dplyr::mutate(sonde_employed = ifelse(is.na(sonde_employed), 0, sonde_employed)) %>%
      tidyr::fill(c(sonde_employed, last_site_visit, sensor_malfunction)) %>%
      
      # Handle special case: If no site visit information exists at the beginning
      # of the record, assume sonde was not yet deployed (sonde_employed = 1)
      dplyr::mutate(sonde_employed = ifelse(is.na(last_site_visit), 1, sonde_employed)) %>%
      
      # Final cleanup of any duplicates and rows with missing site information
      dplyr::distinct(.keep_all = TRUE) %>%
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


# ===================================
# File: R/add_flag.R
# ===================================

#' @title Core function for data quality flagging system
#'
#' @description
#' This function serves as the foundation of the water quality monitoring flagging
#' system, providing a consistent mechanism for adding quality indicators to data.
#' It evaluates a user-specified condition against each row of the dataframe and
#' adds the corresponding flag text to matching rows. The function is designed to
#' handles multiple flags by preserving existing flags and avoid duplication.
#'
#' This design allows specialized flagging functions to focus on their specific
#' detection logic while delegating the actual flag application to this centralized
#' function, ensuring consistent flag formatting throughout the system.
#'
#' @param df A dataframe containing water quality monitoring data with a `flag`
#' column that may contain existing quality flags or NA values.
#'
#' @param condition_arg A logical expression that will be evaluated for each row
#' of the dataframe. This expression uses tidyverse-style non-standard evaluation
#' allowing direct reference to column names (e.g., mean > threshold).
#'
#' @param description_arg Character string containing the flag text to add when
#' the condition is TRUE (e.g., "outside spec range", "sensor drift").
#'
#' @returns A dataframe identical to the input, except with the `flag` column
#' updated to include the new flag description for rows where the condition was
#' TRUE. The function preserves existing flags using a semicolon-newline separator
#' and avoids adding duplicate flags.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [add_burial_flag()]
#' @seealso [add_depth_shift_flag()]
#' @seealso [add_drift_flag()]
#' @seealso [add_field_flag()]
#' @seealso [add_frozen_flag()]
#' @seealso [add_malfunction_flag()]
#' @seealso [add_na_flag()]
#' @seealso [add_repeat_flag()]
#' @seealso [add_seasonal_flag()]
#' @seealso [add_spec_flag()]
#' @seealso [add_suspect_flag()]
#' @seealso [add_unsubmerged_flag()]

add_flag <- function(df, condition_arg, description_arg) {
  
  # Update the flag column based on the provided condition
  # This uses tidyverse programming techniques to evaluate the condition
  # within the context of the dataframe
  df <- df %>% mutate(flag = case_when(
    # For rows where the condition is TRUE:
    {{condition_arg}} ~ if_else(
      # If there's no existing flag, use just the new description
      is.na(flag), paste(description_arg),
      # If there are existing flags, check if this flag already exists
      ifelse(
        # Only add the flag if it doesn't already exist (prevent duplicates)
        !grepl(description_arg, flag), 
        # Append the new flag with a semicolon+newline separator for readability
        paste(flag, description_arg, sep = ";\n"), 
        # If flag already exists, keep the original flag unchanged
        flag
      )
    ),
    # For rows where condition is FALSE, preserve the existing flag value
    TRUE ~ flag
  ))
  
  return(df)
}


# ===================================
# File: R/add_frozen_flag.R
# ===================================

#' @title Flag measurements taken during freezing conditions
#'
#' @description
#' Identifies and flags water quality measurements collected when water temperature
#' was at or below freezing (0°C). This function helps identify periods when ice formation
#' may affect sensor readings across all parameters at a site.
#'
#' Unlike most flagging functions that operate on individual parameters, this function
#' requires a site-level dataframe containing temperature data and applies freezing
#' flags across all parameters.
#'
#' @param df A data frame containing all parameters for a single site. Must include columns:
#' - `DT_round`: Timestamp for each measurement
#' - `DT_join`: Character timestamp used for joining
#' - `parameter`: Measurement type (function looks for "Temperature")
#' - `mean`: The calculated mean value of measurements
#' - `flag`: Existing quality flags (will be updated by this function)
#'
#' @return A data frame with the same structure as the input, but with the `flag`
#' column updated to include "frozen" for all parameters when water temperature
#' is at or below 0°C.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [add_flag()]
add_frozen_flag <- function(df){
  # Extract temperature data for the site
  temperature <- df %>%
    data.table::data.table() %>%
    dplyr::select(DT_round, DT_join, parameter, mean) %>%
    dplyr::filter(parameter == "Temperature") %>%
    dplyr::select(DT_join, Temperature = mean)
  
  # Join temperature data to all parameters and apply freezing flag
  temperature_checked <- df %>%
    dplyr::left_join(., temperature, by = "DT_join") %>%
    # Flag all parameters when water temperature is at or below freezing
    add_flag(., Temperature <= 0, "frozen") %>%
    # Remove the temporary temperature column to maintain original structure
    dplyr::select(-Temperature)
  
  return(temperature_checked)
}


# ===================================
# File: R/add_malfunction_flag.R
# ===================================

#' @title Flag known sensor malfunctions from field notes
#'
#' @description
#' Adds malfunction flags to water quality data based on field technician observations
#' and notes. This function identifies periods when sensors were known to be malfunctioning
#' and categorizes these into specific types of malfunctions.
#'
#' The function uses a separate `mal_flag` column rather than the standard `flag` column,
#' allowing malfunction information to be treated differently in downstream processing.
#'
#' @param df A data frame containing water quality measurements. Must include columns:
#' - `site`: Standardized site name
#' - `parameter`: The measurement type
#' - `DT_round`: Timestamp for each measurement
#' - `flag`: Existing quality flags
#'
#' @param malfunction_records A data frame containing records of known sensor malfunctions 
#' with columns:
#' - `site`: Site name matching the df site column
#' - `parameter`: Parameter name (NA indicates the entire sonde was malfunctioning)
#' - `start_DT`: Start timestamp of the malfunction period
#' - `end_DT`: End timestamp of the malfunction period (NA indicates ongoing)
#' - `notes`: Text description of the malfunction
#'
#' @return A data frame with the same structure as the input, plus a `mal_flag` column
#' containing descriptions of malfunction types for affected measurements.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [grab_mWater_malfunction_notes()]
add_malfunction_flag <- function(df, malfunction_records){
  
  # Extract unique site and parameter from input dataframe
  df_site <- unique(df$site)
  df_parameter <- unique(df$parameter)
  
  # Create mal_flag column if it doesn't exist
  df <- df %>%
    dplyr::mutate(mal_flag = ifelse("mal_flag" %in% names(.), mal_flag, NA))
  
  # Helper function to add malfunction flags while preserving existing ones
  add_mal_flag <- function(df, condition_arg, description_arg) {
    df <- df %>% dplyr::mutate(mal_flag = dplyr::case_when(
      {{condition_arg}} ~ if_else(is.na(mal_flag), paste(description_arg),
                                  ifelse(!grepl(description_arg, mal_flag), paste(mal_flag, description_arg, sep = ";\n"), mal_flag)),
      TRUE ~ mal_flag))
    return(df)
  }
  
  # Filter malfunction records relevant to this site and parameter
  malfunction_records_filtered <- malfunction_records %>%
    dplyr::filter(site == df_site) %>%
    # Include records for this parameter, sonde-wide issues, and burial/submersion issues
    dplyr::filter(is.na(parameter) | parameter == df_parameter | 
                    grepl("buried|burial|bury|sediment|roots", notes, ignore.case = TRUE) | 
                    grepl("not submerged", notes, ignore.case = TRUE)) %>%
    # Handle ongoing malfunctions (no end date)
    dplyr::mutate(end_DT = ifelse(is.na(end_DT), lubridate::ymd_hms("9999-12-31 23:59:59", tz = "MST"), end_DT)) %>%
    dplyr::mutate(end_DT = as.POSIXct(end_DT, tz = "MST", origin = "1970-01-01")) %>%
    tibble::rowid_to_column()
  
  # Special handling for ORP parameter, which is affected by pH sensor issues
  if(df_parameter == "ORP"){
    malfunction_records_filtered <- malfunction_records %>%
      dplyr::filter(site == df_site) %>%
      # For ORP, also include pH issues
      dplyr::filter(is.na(parameter) | parameter == df_parameter | parameter == "pH" |
                      grepl("buried|burial|bury|sediment|roots", notes, ignore.case = TRUE) | 
                      grepl("not submerged", notes, ignore.case = TRUE)) %>%
      dplyr::mutate(end_DT = ifelse(is.na(end_DT), lubridate::ymd_hms("9999-12-31 23:59:59", tz = "MST"), end_DT)) %>%
      dplyr::mutate(end_DT = as.POSIXct(end_DT, tz = "MST", origin = "1970-01-01")) %>%
      tibble::rowid_to_column()
  }
  
  # Only process if there are relevant malfunction records
  if (nrow(malfunction_records_filtered) > 0) {
    
    # Categorize malfunctions into specific types based on notes
    
    # Sensor drift and biofouling
    drift <- malfunction_records_filtered %>%
      dplyr::filter(grepl("grime|gunk|drift|biofoul|biofilm", notes, ignore.case = TRUE))
    
    # Sensor burial
    burial <- malfunction_records_filtered %>%
      dplyr::filter(grepl("buried|burial|bury|sediment|roots", notes, ignore.case = TRUE))
    
    # Depth calibration issues
    depth_funk <- malfunction_records_filtered %>%
      dplyr::filter(grepl("improper level calibration", notes, ignore.case = TRUE))
    
    # Sensors not in water
    unsubmerged <- malfunction_records_filtered %>%
      dplyr::filter(grepl("not submerged", notes, ignore.case = TRUE))
    
    # Any other sensor malfunctions
    general_malfunction <- malfunction_records_filtered %>%
      dplyr::filter(!rowid %in% drift$rowid & !rowid %in% burial$rowid &
                      !rowid %in% depth_funk$rowid & !rowid %in% unsubmerged$rowid)
    
    # Create time intervals for each malfunction type
    drift_interval_list <- map2(
      .x = drift$start_DT,
      .y = drift$end_DT,
      .f = ~lubridate::interval(.x, .y, tz = "MST"))
    
    burial_interval_list <- map2(
      .x = burial$start_DT,
      .y = burial$end_DT,
      .f = ~lubridate::interval(.x, .y, tz = "MST"))
    
    depth_interval_list <- map2(
      .x = depth_funk$start_DT,
      .y = depth_funk$end_DT,
      .f = ~lubridate::interval(.x, .y, tz = "MST"))
    
    unsubmerged_interval_list <- map2(
      .x = unsubmerged$start_DT,
      .y = unsubmerged$end_DT,
      .f = ~lubridate::interval(.x, .y, tz = "MST"))
    
    general_interval_list <- map2(
      .x = general_malfunction$start_DT,
      .y = general_malfunction$end_DT,
      .f = ~lubridate::interval(.x, .y, tz = "MST"))
    
    # Apply specific flags for each malfunction type
    try(df <- df %>%
          add_mal_flag(DT_round %within% burial_interval_list, "reported sonde burial"))
    
    try(df <- df %>%
          add_mal_flag(DT_round %within% drift_interval_list, "reported sensor biofouling"))
    
    try(df <- df %>%
          add_mal_flag(DT_round %within% depth_interval_list, "reported depth calibration malfunction"))
    
    try(df <- df %>%
          add_mal_flag(DT_round %within% unsubmerged_interval_list, "reported sonde unsubmerged"))
    
    try(df <- df %>%
          add_mal_flag(DT_round %within% general_interval_list, "reported sensor malfunction"))
  }
  
  return(df)
}


# ===================================
# File: R/add_na_flag.R
# ===================================

#' @title Flag missing data in water quality measurements
#'
#' @description
#' Identifies and flags rows in water quality data where measurements are missing (NA values).
#' This function helps maintain data quality by explicitly marking gaps in the continuous
#' monitoring record, allowing downstream analysis to properly handle these missing values.
#'
#' @param df A data frame containing water quality measurements. Must include columns:
#' - mean: The calculated mean value of measurements (function checks this for NA values)
#' - flag: Existing quality flags (will be updated by this function)
#'
#' @return A data frame with the same structure as the input, but with the flag
#' column updated to include "missing data" for any rows where the mean value is NA.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [add_flag()]

add_na_flag <- function(df){
  df <- df %>%
    add_flag(is.na(mean), "missing data")
}


# ===================================
# File: R/add_repeat_flag.R
# ===================================

#' @title Flag sequentially repeated measurements
#'
#' @description
#' Identifies and flags water quality measurements that have the exact same value
#' as either the preceding or following measurement. Repeated values can indicate
#' sensor malfunction, signal processing issues, or data transmission problems.
#'
#' @param df A data frame containing water quality measurements. Must include columns:
#' - `mean`: The calculated mean value of measurements
#' - `front1`: The next measurement value
#' - `back1`: The previous measurement value
#' - `flag`: Existing quality flags (will be updated by this function)
#'
#' @return A data frame with the same structure as the input, but with the `flag`
#' column updated to include "repeated value" for any measurements that match
#' either the preceding or following value.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [add_flag()]

add_repeat_flag <- function(df){
  df <- df %>%
    add_flag((mean == front1 | mean == back1), "repeated value")
}


# ===================================
# File: R/add_seasonal_flag.R
# ===================================

#' @title Flag data outside seasonal ranges and with abnormal slopes
#'
#' @description
#' Identifies and flags water quality measurements that fall outside expected seasonal 
#' patterns based on historical data. This function applies two distinct quality flags:
#'
#' 1. "outside of seasonal range" - Applied when a measurement falls outside the 1st-99th
#' percentile range of historical measurements for that site, parameter, and season
#'
#' 2. "slope violation" - Applied when the rate of change (slope) between consecutive
#' measurements exceeds historical thresholds, potentially indicating sensor malfunction
#' or unusual environmental events
#'
#' These flags help distinguish between natural seasonal variation and potentially
#' problematic measurements requiring further investigation.
#'
#' @param df A data frame containing water quality measurements. Must include columns:
#' - `site`: Standardized site name
#' - `parameter`: The measurement type
#' - `mean`: The calculated mean value of measurements
#' - `season`: Season categorization for the measurement
#' - `slope_ahead`: Rate of change to the next measurement
#' - `slope_behind`: Rate of change from the previous measurement
#' - `flag`: Existing quality flags (will be updated by this function)
#'
#' @param threshold_table A dataframe containing seasonal threshold values with columns:
#' - `site`: Site name matching the df site column
#' - `parameter`: Parameter name matching the df parameter column
#' - `season`: Season categorization (e.g., "spring", "summer")
#' - `t_mean01`: 1st percentile threshold for the mean value
#' - `t_mean99`: 99th percentile threshold for the mean value
#' - `t_slope_behind_01`: 1st percentile threshold for slope values
#' - `t_slope_behind_99`: 99th percentile threshold for slope values
#'
#' @return A data frame with the same structure as the input, but with the flag
#' column updated to include "outside of seasonal range" and/or "slope violation"
#' flags as appropriate.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [add_spec_flag()]
#' @seealso [add_flag()]

add_seasonal_flag <- function(df, threshold_table) {
  # Extract the unique site name from the dataframe
  # This assumes a single site per dataframe
  site_name <- unique(na.omit(df$site))
  
  # Extract the unique parameter name from the dataframe
  # This assumes a single parameter type per dataframe
  parameter_name <- unique(na.omit(df$parameter))
  
  # Filter threshold table to get only values relevant to this site-parameter combination
  # and remove the site and parameter columns as they're no longer needed for joining
  lookup <- threshold_table %>%
    filter(site == site_name & parameter == parameter_name) %>%
    select(!c(site, parameter))
  
  df <- df %>%
    # Join seasonal thresholds to the data using the season column as the key
    left_join(lookup, by = "season") %>%
    
    # Flag observations outside the seasonal 1st-99th percentile range
    # These may represent unusual environmental conditions or sensor issues
    add_flag((mean < t_mean01 | mean > t_mean99), "outside of seasonal range") %>%
    
    # Flag observations with abnormal rates of change (slopes)
    # Unusually rapid changes may indicate sensor malfunction or data artifacts
    add_flag(((slope_ahead >= t_slope_behind_99 | slope_behind >= t_slope_behind_99) |
                (slope_ahead <= t_slope_behind_01 | slope_behind <= t_slope_behind_01)), "slope violation")
  
  return(df)
}


# ===================================
# File: R/add_spec_flag.R
# ===================================

#' @title Flag data outside sensor specification ranges
#'
#' @description
#' Identifies and flags water quality measurements that fall outside the manufacturer's 
#' specified operating ranges for each sensor type. These flags help distinguish between 
#' extreme but valid environmental conditions and readings that exceed the sensor's 
#' technical capabilities, which may be less reliable.
#'
#' The function references predefined minimum and maximum threshold values stored in 
#' a configuration file (by default in `'data/qaqc/sensor_spec_thresholds.yml'`). When
#' measurements in the `mean` column fall below the minimum or above the maximum 
#' specification for that particular parameter, the function adds an appropriate quality flag.
#'
#' @param df A data frame containing water quality measurements. Must include columns:
#' - `parameter`: The measurement type (e.g., "Temperature", "DO", "Actual Conductivity")
#' - `mean`: The calculated mean value of measurements
#' - `flag`: Existing quality flags (will be updated by this function)
#'
#' @param spec_table The path to a YAML file containing sensor specification thresholds,
#' or a pre-loaded list of thresholds. Default is `"data/qaqc/sensor_spec_thresholds.yml"`.
#' The file should be structured with parameter names as top-level keys, each containing
#' 'min' and 'max' subkeys defining the acceptable range.
#'
#' @return A data frame with the same structure as the input, but with the flag
#' column updated to include "outside of sensor specification range" for any
#' measurements that exceed manufacturer specifications.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [add_flag()] For the underlying function that adds flags

add_spec_flag <- function(df, spec_table = yaml::read_yaml("data/qaqc/sensor_spec_thresholds.yml")){
  # TODO: make this a non yaml solution and add it to the threshold table
  
  # Extract the parameter name from the dataframe
  # This assumes a single parameter type per dataframe
  parameter_name <- unique(na.omit(df$parameter))
  
  # Pull the sensor specification range from the yaml file
  # Using eval(parse()) to handle any R expressions in the threshold values
  sensor_min <- eval(parse(text = spec_table[[parameter_name]]$min))
  sensor_max <- eval(parse(text = spec_table[[parameter_name]]$max))
  
  # Apply flags to values outside the specification range
  # Only add new flags if they don't already exist for that measurement
  df <- df %>%
    # adding sensor range flags
    add_flag(parameter == parameter_name & (mean < sensor_min | mean > sensor_max) & !grepl("outside of sensor specification range", flag),
             "outside of sensor specification range") %>%
    return(df)
}


# ===================================
# File: R/add_suspect_flag.R
# ===================================

#' @title Flag suspect data based on context patterns
#'
#' @description
#' Identifies and flags potentially problematic measurements that appear valid in isolation 
#' but are suspect due to surrounding data patterns. The function applies the "suspect data" 
#' flag in two scenarios:
#' 
#' 1. When an unflagged measurement falls within a 2-hour window where ≥50% of surrounding 
#'    data points have quality flags
#' 
#' 2. When an isolated measurement appears within a 2-hour window where ≥90% of surrounding 
#'    data is missing
#'
#' @param df A dataframe containing water quality measurements. Must include columns:
#' - `mean`: The measurement value (NA indicates missing data)
#' - `auto_flag`: Existing quality flags
#'
#' @return A dataframe with the same structure as the input, but with the `auto_flag` 
#' column updated to include "suspect data" flags for measurements that match the 
#' specified conditions.
#'
#' @examples
#' # Examples are temporarily disabled

add_suspect_flag <- function(df) {
  # Define flags that should be excluded from the suspect data analysis
  auto_flag_string <- "sonde not employed|missing data|site visit|sv window|reported sonde burial|reported sensor biofouling|reported depth calibration malfunction|reported sensor malfunction"
  
  # Function to check if ≥50% of points in a window have flags
  check_2_hour_window_fail <- function(x) {
    sum(x) / length(x) >= 0.5
  }
  
  # First pass: Flag isolated good data points within mostly flagged regions
  df_test <- df %>%
    # Create binary indicator for relevant flags (1 = flagged, 0 = not flagged or excluded flag type)
    dplyr::mutate(auto_flag_binary = ifelse(is.na(auto_flag) | grepl(auto_flag_string, auto_flag) | auto_flag == "suspect data", 0, 1)) %>%
    # Check if ≥50% of data in 2-hour windows (8 observations) has flags
    dplyr::mutate(over_50_percent_fail_window_right = zoo::rollapply(auto_flag_binary, width = 8, FUN = check_2_hour_window_fail, fill = NA, align = "right")) %>%
    dplyr::mutate(over_50_percent_fail_window_center = zoo::rollapply(auto_flag_binary, width = 8, FUN = check_2_hour_window_fail, fill = NA, align = "center")) %>%
    # Flag unflagged observations within heavily flagged windows as "suspect data"
    dplyr::mutate(auto_flag = as.character(ifelse(is.na(auto_flag) &
                                                    (over_50_percent_fail_window_right == TRUE | over_50_percent_fail_window_center == TRUE),
                                                  "suspect data", auto_flag)))
  
  # Function to check if ≥90% of points in a window are missing
  check_2_hour_window_missing <- function(x) {
    sum(x) / length(x) >= (8/9)
  }
  
  # Second pass: Flag isolated measurements within mostly missing data regions
  df_test <- df_test %>%
    # Create binary indicator for missing data (1 = missing, 0 = present)
    dplyr::mutate(auto_flag_binary = ifelse(is.na(mean), 1, 0)) %>%
    # Check if ≥90% of data in 2-hour windows is missing
    dplyr::mutate(over_90_percent_missing_window_right = zoo::rollapply(auto_flag_binary, width = 8, FUN = check_2_hour_window_missing, fill = NA, align = "right")) %>%
    dplyr::mutate(over_90_percent_missing_window_center = zoo::rollapply(auto_flag_binary, width = 8, FUN = check_2_hour_window_missing, fill = NA, align = "center")) %>%
    # Flag valid but isolated measurements as "suspect data"
    dplyr::mutate(auto_flag = as.character(ifelse(is.na(auto_flag) & !is.na(mean) &
                                                    (over_90_percent_missing_window_right == TRUE & over_90_percent_missing_window_center == TRUE),
                                                  "suspect data", auto_flag)))
  
  return(df_test)
}


# ===================================
# File: R/add_unsubmerged_flag.R
# ===================================

#' @title Flag measurements when sonde was not fully submerged
#'
#' @description
#' Identifies and flags water quality measurements collected when the monitoring
#' sonde was not fully submerged in water. This function uses depth sensor readings
#' to determine submersion status and applies flags across all parameters measured
#' at the same time. The data will be flagged if the value in the `relative_depth`
#' column is less than or equal to 0.
#'
#' @param df A dataframe containing all parameters for a single site. Must include columns:
#' - `parameter`: Measurement type (function requires "Depth" parameter to be present)
#' - `DT_join`: Character timestamp used for joining
#' - `mean`: The calculated mean value of measurements
#' - `flag`: Existing quality flags (will be updated by this function)
#'
#' @return A dataframe with the same structure as the input, but with the `flag`
#' column updated to include "sonde unsubmerged" for all parameters when depth
#' readings indicate the sonde was not fully underwater.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [add_flag()]

add_unsubmerged_flag <- function(df){
  # Extract depth measurements for the site
  depth <- df %>%
    data.table::data.table() %>%
    dplyr::select(DT_round, DT_join, parameter, mean) %>%
    dplyr::filter(parameter == "Depth") %>%
    dplyr::select(DT_join, Depth = mean)
  
  # Join depth measurements with all parameters and apply flag
  depth_checked <- df %>%
    dplyr::left_join(., depth, by = "DT_join") %>%
    # Flag all parameters when depth is 0 or negative
    add_flag(., Depth <= 0, "sonde unsubmerged") %>%
    # Remove temporary depth column to maintain original structure
    dplyr::select(-Depth)
  
  return(depth_checked)
}


# ===================================
# File: R/api_puller.R
# ===================================

#' @title Download water quality data from HydroVu API
#'
#' @description
#' Downloads raw water quality monitoring data from the HydroVu platform for 
#' specified sites and time periods. This function handles the connection to 
#' HydroVu, processes the API responses, and saves the raw data as CSV files 
#' in a specified directory.
#' 
#' The function can retrieve data from different networks (FCW, CSU, or Virridy), 
#' with appropriate filtering applied depending on the network. It also handles 
#' special cases such as sites with multiple sondes from different networks.
#'
#' @param site Character string or vector specifying the site name(s) to download 
#' data for. Site names are matched case-insensitively against the HydroVu 
#' location names.
#'
#' @param network Character string specifying which network of sensors to query. 
#' Options include "FCW", "CSU", "virridy", or "all". Different networks may have 
#' different data processing requirements.
#'
#' @param start_dt POSIXct timestamp indicating the starting point for data 
#' retrieval. Usually derived from the most recent timestamp in existing 
#' historical data.
#'
#' @param end_dt POSIXct timestamp indicating the endpoint for data retrieval. 
#' Default is the current system time (Sys.time()).
#'
#' @param api_token OAuth client object obtained from hv_auth() function, used 
#' for authentication with the HydroVu API.
#'
#' @param dump_dir Character string specifying the directory path where 
#' downloaded CSV files should be saved.
#' 
#' @param fs Logical, whether to use the file system functions
#'
#' @return No direct return value. The function writes CSV files to the specified 
#' dump_dir, with filenames formatted as "sitename_timestamp.csv".
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [get_start_dates()]
#' @seealso [hv_auth()]
#' @seealso [munge_api_data()]

api_puller <- function(site, network, start_dt, end_dt = Sys.time(), api_token, dump_dir, 
                       synapse_env = FALSE, fs = NULL) {
  
  # Retrieve appropriate sensor locations based on the requested network
  if(network %in% c("all", "All", "virridy", "Virridy")){
    locs <- hv_locations_all(hv_token)
  } else if(network %in% c("csu", "fcw", "CSU", "FCW")){
    locs <- hv_locations_all(hv_token) %>%
      dplyr::filter(!grepl("virridy", name, ignore.case = TRUE))
  }
  
  # Suppress scientific notation to ensure consistent formatting
  options(scipen = 999)
  
  # Loop through each site to retrieve and save data
  for(i in 1:length(site)){
    
    # Special handling for "River Bluffs" site
    if(tolower(site[i]) == "riverbluffs" | tolower(site[i]) == "river bluffs"){
      site_loc <- locs %>%
        dplyr::mutate(name = tolower(name)) %>%
        dplyr::filter(grepl("River Bluffs|RiverBluffs", name, ignore.case = TRUE))
    } else {
      # For other sites, filter locations that contain the site name
      site_loc <- locs %>%
        dplyr::mutate(name = tolower(name)) %>%
        dplyr::filter(grepl(site[i], name, ignore.case = TRUE))
    }
    
    # Extract the HydroVu location IDs for API requests
    site_loc_list <- site_loc$id
    
    # Convert timestamps to UTC format for API compatibility
    utc_start_date <- format(as.POSIXct(start_dt, tz = "UTC") + lubridate::hours(0), format = "%Y-%m-%d %H:%M:%S")
    utc_end_date <- format(as.POSIXct(end_dt, tz = "UTC") + lubridate::hours(0), format = "%Y-%m-%d %H:%M:%S")
    timezone <- "UTC"
    
    # Request data for each location ID within the specified time period
    alldata <- site_loc_list %>% purrr::map(~hv_data_id(.,
                                                        start_time = utc_start_date,
                                                        end_time = utc_end_date,
                                                        token = api_token,
                                                        tz = timezone))
    
    # Filter out error responses (404s) and keep only valid data frames
    filtered <- purrr::keep(alldata, is.data.frame)
    
    # If no data was found for this site during the time period, report and continue
    if(length(filtered) == 0){
      print(paste0("No data at ", site[i], " during this time frame"))
    } else {
      # Combine all dataframes, standardize column names, and join with location metadata
      one_df <- dplyr::bind_rows(filtered) %>%
        data.table::data.table() %>%
        dplyr::rename(id = Location,
                      parameter = Parameter,
                      units = Units) %>%
        dplyr::left_join(., site_loc, by = "id") %>%
        dplyr::mutate(site = tolower(site[i])) %>%
        dplyr::select(site, id, name, timestamp, parameter, value, units)
      
      # Format the timestamp string for filenames
      timestamp_str <- stringr::str_replace(stringr::str_replace(substr(end_dt, 1, 16), "[:\\s]", "_"), ":", "")
      
      # Create clean path (remove any double slashes)
      file_name <- paste0(site[i], "_", timestamp_str, ".csv")
      file_path <- file.path(dump_dir, file_name)
      
      
      # ADLS-compatible write procedure
      if(network %in% c("csu", "CSU", "fcw", "FCW")){
        # For FCW/CSU networks, exclude Virridy sensors and FDOM parameter
        filtered_df <- one_df %>% 
          dplyr::filter(!grepl("virridy", name, ignore.case = TRUE),
                        parameter != "FDOM Fluorescence")
        
        if (synapse_env){
          # Then upload to ADLS
          tryCatch({
            # First write to a temporary file
            temp_file <- tempfile(fileext = ".csv")
            print(paste("Writing temp file:", temp_file))
            readr::write_csv(filtered_df, temp_file)
            print(paste("Temp file exists:", file.exists(temp_file)))
            print(paste("Uploading to:", file_path))
            AzureStor::upload_adls_file(
              filesystem = fs, 
              src = temp_file,
              dest = file_path)
            print(paste("Upload complete for:", site[i]))
          }, error = function(e) {
            print(paste("Error in upload process:", e$message))
          })
        } else {
          write_csv(filtered_df, file_path)
          print(paste("Upload into", dump_dir, "complete for:", site[i]))
        }
        
        
      } else if(network %in% c("all","All","Virridy","virridy")){
        # For sites with only one type of sonde, save all data together
        # First write to a temporary file
        temp_file <- tempfile(fileext = ".csv")
        readr::write_csv(one_df, temp_file)
        
        # Then upload to ADLS
        try({
          upload_adls_file(fs, clean_path, temp_file)
          print(paste0("Successfully uploaded data for ", site[i], " to ", clean_path))
        })
      }
    }
  }
}


# ===================================
# File: R/combine_datasets.R
# ===================================

#' @title Merge historical and new water quality monitoring data
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
  
  # Check if both datasets are empty/null and stop if so
  if ((is.null(historical_data_list) || length(historical_data_list) == 0) &
      (is.null(incoming_data_list) || length(incoming_data_list) == 0)) {
    warning("No data provided to combine_datasets")
    return(list())  # Return empty list instead of stopping
  }
  
  # Handle case 1: No incoming data, only historical data
  # This can happen if no new data was collected since the last processing cycle
  if (is.null(incoming_data_list) || length(incoming_data_list) == 0) {
    warning("No new incoming data.")
    
    # Return just the most recent 24 hours of historical data
    # This provides continuity for the next processing cycle
    last_24_hours <- purrr::map(historical_data_list,
                                function(data) {
                                  data %>%
                                    # Extract the most recent 24 hours of data
                                    # Note: Previous approach with ymd_hms had issues
                                    dplyr::filter(DT_round >= max(DT_round) - lubridate::hours(24)) %>%
                                    # Mark as historical and preserve quality flags
                                    dplyr::mutate(historical = TRUE,
                                                  flag = as.character(flag),
                                                  # Copy auto_flag to flag column for continuity
                                                  flag = auto_flag)
                                })
    return(last_24_hours)
  }
  
  # Handle case 2: No historical data, only incoming data
  # This can happen when monitoring a new site or after a system reset
  if (is.null(historical_data_list) || length(historical_data_list) == 0) {
    warning("No historical data.")
    
    # Mark all incoming data as non-historical and return
    new_data <- purrr::map(incoming_data_list,
                           function(data){
                             data %>%
                               dplyr::mutate(historical = FALSE,
                                             flag = as.character(flag)) 
                           })
    return(new_data)
  }
  
  # Standard case: Both historical and incoming data exist
  # Find site-parameter combinations that exist in both datasets
  matching_indexes <- intersect(names(incoming_data_list), names(historical_data_list))
  
  # Extract the most recent 24 hours of historical data for each site-parameter combo
  # This creates an overlap period that helps identify sensor drift
  last_24_hours <- purrr::map(historical_data_list,
                              function(data) {
                                data %>%
                                  # Get the last 24 hours of data
                                  # Note: This timestamp handling requires validation (marked as "SCARY DT")
                                  dplyr::filter(DT_round >= max(DT_round) - lubridate::hours(24)) %>%
                                  # Mark as historical and preserve quality flags
                                  dplyr::mutate(historical = TRUE,
                                                # Copy auto_flag to flag column for continuity
                                                flag = auto_flag) %>%
                                  # Remove the sonde_moved column so it can be recalculated
                                  # based on combined historical and new data
                                  dplyr::select(-sonde_moved)
                              })
  
  # Combine historical and incoming data for each matching site-parameter combination
  combined_hist_inc_data <- purrr::map(matching_indexes, function(index) {
    
    hist_data <- last_24_hours[[index]] %>% 
      dplyr::mutate(flag = as.character(flag))
    
    inc_data <- incoming_data_list[[index]] %>% 
      dplyr::mutate(historical = FALSE,
                    flag = as.character(flag))
    
    # Find unique timestamps in incoming data
    unique_inc <- inc_data %>%
      dplyr::anti_join(hist_data, by = "DT_round")
    
    # Combine historical with unique incoming data
    dplyr::bind_rows(hist_data, unique_inc)
  }) %>%
    # Preserve the site-parameter naming convention in the result
    purrr::set_names(matching_indexes)
  
  return(combined_hist_inc_data)
}


# ===================================
# File: R/fcw.qaqc-package.R
# ===================================

#' FCWQAQC: Quality Assurance and Quality Control for Fort Collins Poudre Sonde Network Data
#'
#' Tools for processing, flagging, and analyzing water quality data
#' from the Fort Collins Watershed monitoring network. This package provides
#' functions for data validation, quality control flagging, and summary statistics.
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr filter mutate arrange select left_join desc n case_when lead
#' @importFrom purrr map2
#' @importFrom stats lm na.omit lag
#' @importFrom utils flush.console
#' @importFrom data.table :=
#' @importFrom rlang sym
#' @importFrom lubridate %within%
#'
#' @name fcw.qaqc
#' @keywords internal
"_PACKAGE"


# ===================================
# File: R/final_data_binder.R
# ===================================

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
#' # Examples are temporarily disabled

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


# ===================================
# File: R/find_do_noise.R
# ===================================

#' @title Flag dissolved oxygen (DO) sensor noise and potential sonde burial
#' 
#' @description 
#' This function identifies two types of issues with DO sensors:
#' 1. Short-term interference: Flagged when sudden fluctuations occur or DO is 
#' abnormally low.
#' 2. Possible burial: Flagged when persistent interference is detected over a 
#' 24-hour period.
#' This function processes site-parameter dataframes, but only modifies those 
#' containg DO data. For those dataframes without DO data, the function returns
#' the original dataframe unmodified.
#' 
#' @param df A site-parameter dataframe containing water quality measurements. Must include:
#' - `parameter`: Measurement type (function checks for "DO")
#' - `mean`: The calculated mean value of measurements
#' - `back1`: The previous measurement value
#' - `front1`: The next measurement value
#' - `flag`: Existing quality flags (will be updated by this function)
#' 
#' @return A dataframe with the same structure as the input, but with `flag` column
#' updated to include "DO interference" and/or "Possible burial" flags where applicable.
#' 
#' @examples
#' # Examples are temporarily disabled
#' @seealso [add_flag()]
 
find_do_noise <- function(df){
  
  # Only process dataframes containing dissolved oxygen data
  if("DO" %in% df$parameter){
    
    # Flag short-term DO interference when either:
    # 1. There's a spike in both directions (before and after current reading) of ≥ 0.5 mg/L
    # 2. DO is abnormally low (≤ 5 mg/L)
    df <- df %>%
      add_flag((back1 - mean >= 0.5 & front1 - mean >= 0.5) | (mean <= 5), "DO interference")
    
    
    # Define a function to check if a given window has at least 24 instances of interference
    check_day_hour_window_fail <- function(x) {
      sum(x) >= 24
    }
    
    # Flag possible sensor burial when DO interference persists for 24+ hours
    df <- df %>%
      data.table::data.table() %>%
      # Create binary indicator for DO interference flag
      dplyr::mutate(DO_drift_binary = ifelse(grepl("DO interference", flag), 1, 0)) %>%
      # Apply rolling window check in both right-aligned and center-aligned modes
      dplyr::mutate(right = data.table::frollapply(DO_drift_binary, n = 96, FUN = check_day_hour_window_fail, align = "right", fill = NA),
                    center = data.table::frollapply(DO_drift_binary, n = 96, FUN = check_day_hour_window_fail, align = "center", fill = NA)) %>%
      # Flag possible burial if either window check indicates persistent interference
      add_flag(right == 1 | center == 1, "Possible burial")
    
    
    return(df)
    
  } else {
    
    # Return unmodified dataframe for non-DO parameters
    return(df)
    
  }
  
}


# ===================================
# File: R/fix_calibration.R
# ===================================

#' @title Correct data with underlying bad calibrations using In-Situ calibration reports
#'
#' @description This function uses calibration reports collected by field crews to "correct"
#' instances of poor calibration. These are infrequent, and this function is not yet complete.
#'
#' @param df Site-parameter data frame
#' @param cal_errors A data frame containing a list of known instances of poor calibration
#'
#' @return A data frame with modified `mean` values corrected for improper calibration. These observations are also identified in the `cal_fix` column.

fix_calibration <- function(df, cal_errors){

  # Filter records for relevant site-param information
  df_site <- unique(df$site)
  df_parameter <- unique(df$parameter)

  # Depth calibration requires its own fix. It's hacky and I don't like it, but
  # the way that In-Situ's calibration report reports pressure calibrations makes it
  # impossible to actually back calibrate. Therefore, I'm just assuming that level
  # doesn't actually change after bad calibrations, and hard code it so that the
  # first "bad" depth is forced to equal the last "good" depth and I offset all
  # subsequent "bad" depth values by the difference between them.

    if(!"Depth" %in% df$parameter){

      nope <- df %>% dplyr::mutate(cal_fix = NA)

      return(nope)

    }

  if(df_parameter == "Depth"){


    df <- df %>%
      dplyr::mutate(raw = mean)

    if("Depth" %in% df$parameter & "archery" %in% df$site){

      #df <- all_data_flagged[["archery-Depth"]] # for testing

      site_depth <- df %>%
        dplyr::mutate(relative_depth = ifelse(DT_round >= lubridate::as_datetime('2022-05-21 15:45:00', "MST") & DT_round <= lubridate::as_datetime('2022-05-24 15:45:00', "MST"),
                                              mean +
                                                abs(
                                                  dplyr::filter(df, as.character(DT_round) == ('2022-05-24 15:15:00'))$mean -
                                                    dplyr::filter(df, as.character(DT_round) == ('2022-05-24 15:45:00'))$mean
                                                ),
                                              mean))

    } else if("Depth" %in% df$parameter & "timberline" %in% df$site){

      # df <- all_data_flagged[["timberline-Depth"]]

      site_depth <- df %>%
        dplyr::mutate(relative_depth = ifelse(year == "2022" & DT_round <= lubridate::as_datetime('2022-04-07 17:00:00', "MST"),
                                              as.numeric(mean) +
                                                abs(
                                                  dplyr::filter(df, as.character(DT_round) == ('2022-04-07 16:15:00'))$mean -
                                                    dplyr::filter(df, as.character(DT_round) == ('2022-04-07 17:15:00'))$mean
                                                ),
                                              as.numeric(mean)))

    } else if ("Depth" %in% df$parameter & "legacy" %in% df$site) {

      # df <- all_data_flagged[["legacy-Depth"]]

      site_depth <- df %>%
        dplyr::mutate(relative_depth = ifelse(DT_round >= lubridate::as_datetime('2022-04-06 06:00:00', "MST") & DT_round <= lubridate::as_datetime('2022-04-12 09:15:00', "MST"),
                                              as.numeric(mean) +
                                                abs(
                                                  dplyr::filter(df, as.character(DT_round) == ('2022-04-12 09:15:00'))$mean -
                                                    dplyr::filter(df, as.character(DT_round) == ('2022-04-12 09:30:00'))$mean
                                                ),
                                              mean))

      site_depth <-  site_depth %>%
        dplyr::mutate(relative_depth = ifelse(DT_round >= lubridate::as_datetime('2022-07-08 17:00:00', "MST") & DT_round <= lubridate::as_datetime('2022-07-12 09:00:00', "MST"),
                                              relative_depth +
                                                abs(
                                                  dplyr::filter(site_depth, as.character(DT_round) == ('2022-07-08 14:15:00'))$relative_depth -
                                                    dplyr::filter(site_depth, as.character(DT_round) == ('2022-07-08 17:00:00'))$relative_depth
                                                ),
                                              relative_depth))

      site_depth <-  site_depth %>%
        dplyr::mutate(relative_depth = ifelse(DT_round >= lubridate::as_datetime('2022-07-22 11:30:00', "MST") & DT_round <= lubridate::as_datetime('2022-07-25 14:15:00', "MST"),
                                              relative_depth +
                                                abs(
                                                  dplyr::filter(site_depth, as.character(DT_round) == ('2022-07-22 09:45:00'))$relative_depth -
                                                    dplyr::filter(site_depth, as.character(DT_round) == ('2022-07-22 11:30:00'))$relative_depth
                                                ),
                                              relative_depth))

    } else if ("Depth" %in% df$parameter & "tamasag" %in% df$site) {

      # df <- all_data_flagged[["tamasag-Depth"]]

      site_depth <- df %>%
        dplyr::mutate(relative_depth = ifelse(DT_round <= "2022-04-24 07:15:00" & year == "2022",
                                              as.numeric(mean) +
                                                abs(
                                                  dplyr::filter(df, as.character(DT_round) == ('2022-04-24 07:15:00'))$mean -
                                                    dplyr::filter(df, as.character(DT_round) == ('2022-04-24 07:30:00'))$mean
                                                ),
                                              mean))

      # return(tamasag_depth)

    } else {

      site_depth <- df %>%
        dplyr::mutate(relative_depth = mean,
                      cal_fix = NA)
    }

    depth_flagged <- site_depth %>%
      dplyr::mutate(mean = relative_depth,
                    cal_fix = ifelse(raw != mean, "calibration fix", NA)) %>%
      dplyr::select(-relative_depth)

    return(depth_flagged)

  }

  # PLACEHOLDER UNTIL OTHER CALIBRATIONS ARE FULLY DEVELOPED:
  return(df %>%
           dplyr::mutate(cal_fix = NA))

}

# # For non-depth parameters, we can refer to the calibration report:
# bad_cal <- cal_errors %>%
#   dplyr::mutate(start_DT = as.character(lubridate::as_datetime(start_DT)),
#                 end_DT = as.character(lubridate::as_datetime(end_DT)),
#                 #report_to_correct = as.character(lubridate::as_datetime(report_to_correct))
#   )
#
# bad_cal_records_filtered <- bad_cal %>%
#    dplyr::filter(site == df_site) %>%
#    dplyr::filter(grepl(df_parameter, parameter, ignore.case = TRUE)) %>%
#    dplyr::mutate(end_DT = ifelse(is.na(end_DT), ymd_hms("9999-12-31 23:59:59", tz = "MST"), end_DT)) %>%
#    dplyr::mutate(end_DT = as.character(as.POSIXct(end_DT, tz = "MST"))) %>%
#   tibble::rowid_to_column()
#
# # If there are no bad calibrations listed for that site-param, return original
# # dataframe, filling our updated "mean" column with the old unmodified values:
# if(nrow(bad_cal_records_filtered == 0)){
#
#   df <- df %>%
#     mutate(raw = mean,
#            mean = mean)
#
#   return(df)
#
# } else {
#
#   cal_tabler <- function(cal_files){
#
#     #cal_files <- list.files("data/calibration_reports")[3] # for testing
#
#     cal <- read_html(paste0(getwd(), "/data/calibration_reports/", cal_files)) %>%
#       html_nodes("div") %>%
#       html_text() %>%
#       as_tibble()
#
#     rdo <- cal %>% filter(grepl("RDO", value)) %>% pull() %>% str_replace_all(., " ", "") %>% tolower()
#
#     ph_orp <- cal %>% filter(grepl("pH/ORP", value)) %>% pull() %>% str_replace_all(., " ", "") %>% tolower()
#
#     conductivity <- cal %>% filter(grepl("Conductivity",value)) %>% pull() %>% str_replace_all(., " ", "") %>% tolower()
#
#     if(length(cal %>% filter(grepl("Turbidity",value)) %>% pull() %>% str_replace_all(., " ", "") %>% tolower()) != 0){
#
#       turbidity <- cal %>% filter(grepl("Turbidity",value)) %>% pull() %>% str_replace_all(., " ", "") %>% tolower()
#
#     } else {
#
#       turbidity <- "No Turbidity Sensor"
#
#     }
#
#     # Always the last sensor when depth is available:
#     depth <- ifelse(str_detect(cal %>% .[nrow(.),] %>% pull() %>% str_replace_all(., " ", "") %>% tolower(), "pressure"),#"psireferencedepth"),
#                     cal %>% .[nrow(.),] %>% pull() %>% str_replace_all(., " ", "") %>% tolower(),
#                     "No Depth Sensor")
#
#     time_mst <- paste0(str_sub(str_match(cal_files, "(\\d+)_mst")[, 2:1][1], 1, 2), ":",
#                        str_sub(str_match(cal_files, "(\\d+)_mst")[, 2:1][1], 3, 4))
#     #str_sub(cal_files, -13, -12),":", str_sub(cal_files, -11, -10))
#
#     date <- str_match(cal_files, "^[^_]+_([0-9]{8})_")[, 2]
#
#     #str_sub(cal_files, -22, -19),"-", str_sub(cal_files, -18, -17),"-", str_sub(cal_files, -16, -15))
#
#     cal_table <- tibble(site = sub("\\_.*", "", cal_files) %>% tolower(),
#
#                         DT = ymd_hm(paste(date, time_mst, tz = "MST")),
#
#                         # Dissolved Oxygen
#                         rdo_cal_date = as.character(mdy(str_match(rdo, "lastcalibrated\\s*(.*?)\\s*calibrationdetails")[,2])),
#                         rdo_slope = str_match(rdo, "slope\\s*(.*?)\\s*offset")[,2],
#                         rdo_offset = str_match(rdo, "offset\\s*(.*?)\\s*mg/l")[,2],
#                         rdo_100 = str_match(rdo, "premeasurement\\s*(.*?)\\s*%satpost")[,2],
#                         rdo_conc = str_match(rdo, "concentration\\s*(.*?)\\s*mg/lpremeasurement")[,2],
#                         rdo_temp = str_match(rdo, "temperature\\s*(.*?)\\s*°c")[,2],
#                         rdo_pressure = str_match(rdo, "pressure\\s*(.*?)\\s*mbar")[,2],
#
#                         # pH
#                         ph_cal_date = as.character(mdy(str_match(ph_orp, "lastcalibrated\\s*(.*?)\\s*calibrationdetails")[,2])),
#                         ph_slope_pre = str_match(ph_orp, "offset1slope\\s*(.*?)\\s*mv/ph")[,2],
#                         ph_offset_pre = str_match(ph_orp, "mv/phoffset\\s*(.*?)\\s*mvslopeandoffset2")[,2],
#                         ph_slope_post = str_match(ph_orp, "offset2slope\\s*(.*?)\\s*mv/ph")[,2],
#                         ph_offset_post = str_match(ph_orp, paste0(ph_slope_post,"mv/phoffset\\s*(.*?)\\s*mvorporp"))[,2],
#                         # Sometimes, the post value can actually be in the high 6 pH... therefore the post measurement regex matching text is conditional
#                         ph_7_nice = str_sub(str_match(ph_orp, "postmeasurementph7\\s*(.*?)\\s*mvcal")[,2], 10, nchar(str_match(ph_orp, "postmeasurementph7\\s*(.*?)\\s*mvcal")[,2])),
#                         ph_7_high = str_sub(str_match(ph_orp, "postmeasurementph8\\s*(.*?)\\s*mvcal")[,2], 10, nchar(str_match(ph_orp, "postmeasurementph8\\s*(.*?)\\s*mvcal")[,2])),
#                         ph_7_low = str_sub(str_match(ph_orp, "postmeasurementph6\\s*(.*?)\\s*mvcal")[,2], 10, nchar(str_match(ph_orp, "postmeasurementph6\\s*(.*?)\\s*mvcal")[,2])),
#                         ph_7 = ifelse(!is.na(ph_7_nice), ph_7_nice,
#                                       ifelse(!is.na(ph_7_high), ph_7_high, ph_7_low)),
#
#                         # ORP
#                         #Newly encountered thing: sometimes the calibration report calls the ORP standard Zobell's, sometimes it's just called "ORP Standard":
#                         orp_offset = ifelse(is.na(str_match(ph_orp, "zobell'soffset\\s*(.*?)\\s*mvtemperature")[,2]) & is.na(str_match(ph_orp, "quickcal\\s*(.*?)\\s*mvtemperature")[,2]),
#                                             str_match(ph_orp, "orpstandardoffset\\s*(.*?)\\s*mvtemperature")[,2],
#                                             ifelse(is.na(str_match(ph_orp, "zobell'soffset\\s*(.*?)\\s*mvtemperature")[,2]) & is.na(str_match(ph_orp, "orpstandardoffset\\s*(.*?)\\s*mvtemperature")[,2]),
#                                                    str_match(ph_orp, "quickcal\\s*(.*?)\\s*mvtemperature")[,2],
#                                                    str_match(ph_orp, "zobell'soffset\\s*(.*?)\\s*mvtemperature")[,2])),
#
#                         # Conductivity
#                         cond_cal_date = as.character(mdy(str_match(conductivity, "lastcalibrated\\s*(.*?)\\s*calibrationdetails")[,2])),
#                         tds_conversion_ppm = str_sub(str_match(conductivity, "tdsconversionfactor\\s*(.*?)\\s*cellconstant")[,2], 6, nchar(str_match(conductivity, "tdsconversionfactor\\s*(.*?)\\s*cellconstant")[,2])),
#
#                         # calibration report formatting has changed in 2024 for this variable. Therefore a post-2024 correction must occur
#                         cond_cell_constant = ifelse(year(DT) < 2024, str_match(conductivity, "cellconstant\\s*(.*?)\\s*referencetemperature")[,2],
#                                                     str_match(conductivity, "cellconstant\\s*(.*?)\\s*offset")[,2]),
#
#                         cond_offset = ifelse(year(DT) < 2024, NA,
#                                              str_match(conductivity, "offset\\s*(.*?)\\s*µs/cm")[,2]),
#
#                         cond_pre = str_match(conductivity,paste0(str_match(conductivity,
#                                                                            "premeasurementactual\\s*(.*?)\\s*specificconductivity")[,2],"specificconductivity\\s*(.*?)\\s*µs/cmpost"))[,2],
#                         cond_post = str_match(conductivity,paste0(str_match(conductivity,
#                                                                             "postmeasurementactual\\s*(.*?)\\s*specificconductivity")[,2],"specificconductivity\\s*(.*?)\\s*µs/cm"))[,2]) %>%
#                         # if(turbidity == "No Turbidity Sensor"){
#                         # # Turbidity
#                         # turb_cal_date = "None",
#                         # ntu_slope = "None",
#                         # ntu_offset = "None",
#                         # ntu_10 = "None",
#                         # ntu_100 = "None") %>%
#
#     select(-c(ph_7_nice, ph_7_high, ph_7_low))
#
#     # Not all sondes have depth.
#     if(!is.na(str_match(depth, "lastcalibrated"))){#\\s*(.*?)\\s*calibrationdetails")[,2])){
#       cal_table <- cal_table %>%
#         mutate(
#           # Depth
#           depth_cal_date = as.character(mdy(str_match(depth, "lastcalibrated\\s*(.*?)\\s*calibrationdetails")[,2])),
#           depth_offset = str_match(depth, "zerooffset\\s*(.*?)\\s*psireferencedepth")[,2],
#           depth_ref_depth = str_match(depth, "psireferencedepth\\s*(.*?)\\s*ftreferenceoffset")[,2],
#           depth_ref_offset = str_match(depth, "ftreferenceoffset\\s*(.*?)\\s*psipremeasurement")[,2],
#           depth_pre_psi = str_match(depth, "psipremeasurement\\s*(.*?)\\s*psipostmeasurement")[,2],
#           depth_post_psi = str_match(depth, "psipostmeasurement\\s*(.*?)\\s*psi")[,2])
#     }
#
#     if(depth == "No Depth Sensor"){
#
#       cal_table <- cal_table %>%
#         mutate(# Depth
#       depth_cal_date = "No Depth Sensor",
#       depth_offset = "No Depth Sensor",
#       depth_ref_depth = "No Depth Sensor",
#       depth_ref_offset = "No Depth Sensor",
#       depth_pre_psi = "No Depth Sensor",
#       depth_post_psi = "No Depth Sensor")
#     }
#
#
#     if(!is.na(str_match(turbidity, "lastcalibrated"))){#calibrationpoint1premeasurement\\s*(.*?)\\s*ntupost")[,2])){
#       # Not all sondes have turbidity.
#       cal_table <- cal_table %>%
#         mutate(
#           # Turbidity
#           turb_cal_date = as.character(mdy(str_match(turbidity, "lastcalibrated\\s*(.*?)\\s*calibrationdetails")[,2])),
#           ntu_slope = str_match(turbidity, "slope\\s*(.*?)\\s*offset")[,2],
#           ntu_offset = str_match(turbidity, "offset\\s*(.*?)\\s*ntu")[,2],
#           ntu_10 = str_match(turbidity, "calibrationpoint1premeasurement\\s*(.*?)\\s*ntupost")[,2],
#           ntu_100 = str_match(turbidity, "calibrationpoint2premeasurement\\s*(.*?)\\s*ntupost")[,2])
#     }
#
#     if(turbidity == "No Turbidity Sensor"){
#       cal_table <- cal_table %>%
#         mutate(
#           # Turbidity
#           turb_cal_date = "No Turbidity Sensor",
#           ntu_slope = "No Turbidity Sensor",
#           ntu_offset = "No Turbidity Sensor",
#           ntu_10 = "No Turbidity Sensor",
#           ntu_100 = "No Turbidity Sensor")
#
#
#
#
#     }
#
#
#     cal_table <- cal_table %>%
#       mutate(
#         #Factory Defaults
#         factory_defaults = paste0(ifelse(grepl("factorydefault", turbidity), "Turbidity ", ""),
#                                   ifelse(grepl("factorydefault", rdo), "RDO ", ""),
#                                   ifelse(is.na(ph_slope_post), "pH ", ""),
#                                   ifelse(is.na(orp_offset), "ORP ", ""),
#                                   ifelse(grepl("factorydefault", conductivity), "Conductivity ", ""),
#                                   ifelse(grepl("factorydefaults", depth), "Depth ", ""))) %>%
#       # convert all columns to character values to preserve info
#       mutate(across(.cols = everything(), .fns = as.character)) %>%
#       # remove "," from big numbers
#       mutate(across(everything(), ~str_replace_all(., ",", "")))
#
#   }
#
#   bad_cal_interval_list <- map2(
#     .x = bad_cal_records_filtered$start_DT,
#     .y = bad_cal_records_filtered$end_DT,
#     .f = ~interval(.x, .y, tz = "MST"))
#
#   if(df_parameter == "DO"){
#
#     cal_table <- list.files("data/calibration_reports", pattern=".html") %>%
#       .[grepl(df_site, ., ignore.case = TRUE)] %>%
#       map_dfr(., cal_tabler) %>%
#       distinct(.keep_all = TRUE) %>%
#       mutate(DT = as.character(round_date(ymd_hms(DT, tz = "MST"), "15 minutes"))) %>%
#       # mutate(across(-matches("date|site|DT|factory"), as.numeric)) %>%
#       dplyr::select(DT, site, rdo_slope, rdo_offset)
#
#     df_mod <- df %>%
#       left_join(., cal_table, by = c("DT_join" = "DT", "site")) %>%
#       fill(names(cal_table)[!grepl("\\b(site|DT)\\b", names(cal_table))], .direction = "down") %>%
#       mutate(rdo_slope_pre = as.numeric(rdo_slope), rdo_offset_pre = as.numeric(rdo_offset)) %>%
#       select(names(df), contains(c("pre"))) %>%
#       left_join(., cal_table, by = c("DT_join" = "DT", "site")) %>%
#       fill(names(cal_table)[!grepl("\\b(site|DT)\\b", names(cal_table))], .direction = "up") %>%
#       mutate(rdo_slope_post = as.numeric(rdo_slope), rdo_offset_post = as.numeric(rdo_offset)) %>%
#       select(names(df), contains(c("pre", "post"))) %>%
#       #mutate(raw = (mean -rdo_offset_pre)/rdo_offset_pre)
#       mutate(raw = case_when(DT_round %within% bad_cal_interval_list & is.na(rdo_slope_pre) ~ mean,
#                              DT_round %within% bad_cal_interval_list & !is.na(rdo_slope_pre) ~ ((mean - rdo_offset_pre)/rdo_slope_pre),
#                              .default = mean),
#              cal_fix = case_when(DT_round %within% bad_cal_interval_list ~ (raw*rdo_slope_post) + rdo_offset_post,
#                                  .default = mean)) %>%
#       add_flag(mean != cal_fix, "calibration fix") %>%
#       mutate(raw = mean,
#              mean = cal_fixed)
#
#
#   }
#
#   if(df_parameter == "pH"){
#
#     cal_table <- list.files("data/calibration_reports", pattern=".html") %>%
#       .[grepl(df_site, ., ignore.case = TRUE)] %>%
#       map_dfr(., cal_tabler) %>%
#       distinct(.keep_all = TRUE) %>%
#       mutate(DT = as.character(round_date(ymd_hms(DT, tz = "MST"), "15 minutes"))) %>%
#       # mutate(across(-matches("date|site|DT|factory"), as.numeric)) %>%
#       dplyr::select(DT, site, ph_slope_pre, ph_offset_pre, ph_slope_post, ph_offset_post, factory_defaults)
#
#     df_mod <- df %>%
#       left_join(., cal_table, by = c("DT_join" = "DT", "site")) %>%
#       fill(names(cal_table)[!grepl("\\b(site|DT)\\b", names(cal_table))], .direction = "down") %>%
#       mutate(ph_slope_pre = as.numeric(ph_slope_pre), ph_offset_pre = as.numeric(ph_offset_pre),
#              ph_slope_post = as.numeric(ph_slope_post), ph_offset_post = as.numeric(ph_offset_post)) %>%
#       mutate(raw = case_when(DT_round %within% bad_cal_interval_list & is.na(ph_slope_pre) & grepl("pH", factory_defaults, ignore.case = TRUE) ~ mean,
#                              DT_round %within% bad_cal_interval_list & !is.na(ph_slope_pre) & !grepl("pH", factory_defaults, ignore.case = TRUE) ~ ((mean - ph_offset_pre)/ph_slope_pre),
#                              .default = mean),
#              cal_fix = case_when(DT_round %within% bad_cal_interval_list ~ (raw*ph_slope_post) + ph_offset_post,
#                                  .default = mean)) %>%
#       add_flag(mean != cal_fix, "calibration fix")
#
#   }
#
#   if(df_parameter == "")
#
# }
# }


# ===================================
# File: R/fix_turbidity.R
# ===================================

fix_turbidity <- function(df){

# Filter records for relevant site-param information
df_site <- unique(df$site)
df_parameter <- unique(df$parameter)

# Function to add a column if it doesn't already exist
add_column_if_not_exists <- function(df, column_name, default_value = NA) {
  if (!column_name %in% colnames(df)) {
    df <- df %>% dplyr::mutate(!!sym(column_name) := default_value)
  }
  return(df)
}

df <- df %>%
  add_column_if_not_exists(column_name = "raw") %>%
  mutate(raw = ifelse(is.na(raw), mean, raw))

if(df_parameter == "Turbidity"){

  df <- df %>%
    mutate(mean = ifelse(mean >= 1000, 1000, mean))

  }

return(df)

}


# ===================================
# File: R/generate_summary_statistics.R
# ===================================

#' @title Calculate contextual statistics for water quality time series
#'
#' @description
#' Enhances water quality time series data with a comprehensive set of statistical
#' context metrics that support anomaly detection and pattern analysis. This function
#' calculates both temporal relationships (how each reading relates to adjacent points)
#' and rolling statistics (how each reading fits within recent trends).
#'
#' The function generates several types of contextual information:
#' - Adjacent value comparison: Values immediately before and after each point
#' - Rolling statistics: 7-point rolling median, mean, and standard deviation
#' - Rate-of-change metrics: Slope calculations for trend analysis
#' - Temporal context: Month, year, and seasonal classification
#'
#' These statistics provide the foundation for subsequent quality control processes
#' by establishing the expected behavior patterns and natural variability of each
#' parameter at each site. The 7-point window (covering ~90 minutes for 15-minute
#' data) captures short-term patterns while accommodating brief anomalies.
#'
#' @param site_param_df A dataframe containing time series data for a single
#' site-parameter combination. Must include:
#' - DT_round: Timestamp for each observation
#' - mean: The measured parameter value
#'
#' @return A dataframe containing all original columns plus the following added
#' statistical context columns:
#' - front1/back1: Values immediately ahead of and behind each point
#' - rollmed/rollavg/rollsd: 7-point rolling median, mean, and standard deviation
#' - slope_ahead/slope_behind: Rate of change to adjacent points
#' - rollslope: 7-point rolling slope average
#' - month/year/y_m: Temporal classification components
#' - season: Hydrological season classification (winter_baseflow, snowmelt,
#'   monsoon, fall_baseflow)
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [combine_datasets()]
#' @seealso [add_field_notes()]

generate_summary_statistics <- function(site_param_df) {

  # Helper function to safely add a new column if it doesn't already exist
  # This preserves historical data when present and adds placeholder columns when needed
  add_column_if_not_exists <- function(df, column_name, default_value = NA) {
    if (!column_name %in% colnames(df)) {
      df <- df %>% dplyr::mutate(!!sym(column_name) := default_value)
    }
    return(df)
  }
  
  summary_stats_df <- site_param_df %>%
    # Initialize or preserve columns for statistical context
    # This approach maintains continuity with historical data when it exists
    add_column_if_not_exists(column_name = "front1") %>%
    add_column_if_not_exists(column_name = "back1") %>%
    add_column_if_not_exists(column_name = "rollmed") %>%
    add_column_if_not_exists(column_name = "rollavg") %>%
    add_column_if_not_exists(column_name = "rollsd") %>%
    add_column_if_not_exists(column_name = "slope_ahead") %>%
    add_column_if_not_exists(column_name = "slope_behind") %>%
    add_column_if_not_exists(column_name = "rollslope") %>%
    
    # Calculate temporal and statistical context metrics
    dplyr::mutate(
      # Adjacent values: Measurements immediately before and after this point
      # These help identify sudden changes and isolated anomalies
      front1 = ifelse(is.na(front1), dplyr::lead(mean, n = 1), front1),
      back1 = ifelse(is.na(back1), dplyr::lag(mean, n = 1), back1),
      
      # Rolling statistics with 7-point windows (this point + 6 preceding points)
      # These establish the recent behavioral pattern for each parameter
      
      # Rolling median: Robust measure of central tendency less affected by outliers
      rollmed = ifelse(is.na(rollmed), 
                     RcppRoll::roll_median(mean, n = 7, align = 'right', 
                                         na.rm = F, fill = NA_real_), 
                     rollmed),
      
      # Rolling mean: Average value over the recent window
      rollavg = ifelse(is.na(rollavg), 
                     RcppRoll::roll_mean(mean, n = 7, align = 'right', 
                                       na.rm = F, fill = NA_real_), 
                     rollavg),
      
      # Rolling standard deviation: Measure of recent variability
      rollsd = ifelse(is.na(rollsd), 
                    RcppRoll::roll_sd(mean, n = 7, align = 'right', 
                                    na.rm = F, fill = NA_real_), 
                    rollsd),
      
      # Rate-of-change metrics for trend analysis
      # For 15-minute data, dividing by 15 converts to units per minute
      
      # Slope to the next point (looking forward)
      slope_ahead = ifelse(is.na(slope_ahead), (front1 - mean)/15, slope_ahead),
      
      # Slope from the previous point (looking backward)
      slope_behind = ifelse(is.na(slope_behind), (mean - back1)/15, slope_behind),
      
      # Rolling average slope over the recent window
      rollslope = ifelse(is.na(rollslope), 
                       RcppRoll::roll_mean(slope_behind, n = 7, align = 'right', 
                                         na.rm = F, fill = NA_real_), 
                       rollslope),
      
      # Temporal context for seasonal pattern analysis
      month = lubridate::month(DT_round),
      year = lubridate::year(DT_round),
      y_m = paste(year, '-', month),
      
      # Hydrological season classification based on month
      # This reflects the distinct flow regimes of the watershed
      season = dplyr::case_when(
        month %in% c(12, 1, 2, 3, 4) ~ "winter_baseflow",  # Winter base flow period
        month %in% c(5, 6) ~ "snowmelt",                   # Spring snowmelt period
        month %in% c(7, 8, 9) ~ "monsoon",                 # Summer monsoon period
        month %in% c(10, 11) ~ "fall_baseflow",            # Fall base flow period
        TRUE ~ NA)
    )
  
  return(summary_stats_df)
}


# ===================================
# File: R/get_start_dates.R
# ===================================

#' @title Get the start dates for the HydroVu API pull from the historically
#' flagged data
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
  
  # Establish a default df to keep trying sites even if we haven't been able to retrieve their
  # temperature data
  default_start_dates <- tibble(
    site = c("pbd",
             "bellvue", # rist
             "salyer",
             "udall",
             "riverbend",
             "cottonwood",
             "elc", # elc
             "archery",
             "riverbluffs"),
    start_DT = as.POSIXct("2025-03-27", tz = "MST"),
    end_DT = as.POSIXct("2025-03-27", tz = "MST") + days(1)
  )
  
  # Extract only the Temperature parameter dataframes from the historical data list
  temperature_subset <- grep("Temperature", names(incoming_historically_flagged_data_list))
  
  
  if (length(incoming_historically_flagged_data_list) > 0) {
    
    # Extract each sites most recent timestamp based on their temperature data
    historical_start_dates_df <- incoming_historically_flagged_data_list[temperature_subset] %>% 
      dplyr::bind_rows(.) %>% 
      dplyr::mutate(DT_round = lubridate::with_tz(DT_round, "MST")) %>% 
      dplyr::group_by(site) %>% 
      dplyr::slice_max(DT_round) %>% 
      dplyr::select(start_DT = DT_round, site) %>% 
      dplyr::ungroup()
    
    # update default df
    final_start_dates_df <- default_start_dates %>% 
      left_join(historical_start_dates_df, by = "site") %>% 
      mutate(start_DT = coalesce(start_DT.y, start_DT.x),
             end_DT = max(start_DT) + hours(3)) %>% # CHANGE TO Sys.time()
      select(-c(start_DT.y, start_DT.x)) %>% 
      relocate(site, start_DT, end_DT)
    
    return(final_start_dates_df)
    
  } else { 
    
    # If the historical data is empty, default to default_start_dates
    final_start_dates_df <- default_start_dates
    
    return(final_start_dates_df)
  }
}


# ===================================
# File: R/globals.R
# ===================================

# Global variable declarations
utils::globalVariables(c(
  "parameter", "site", "DT_round", "flag", "Temperature", "DT_join",
  "sonde_moved", "auto_flag", "front1", "back1", "Depth", "value",
  # Add all the other variables from your check output here
  "year", "month"
))


# ===================================
# File: R/grab_mWater_malfunction_notes.R
# ===================================

#' @title Extract sensor malfunction records from mWater field data
#'
#' @description
#' Processes mWater field data to extract and format records specifically related to
#' sensor malfunctions. This function filters for visits tagged as "Sensor malfunction",
#' standardizes parameter names to match the system's naming conventions, and
#' reformats the data for integration with the QAQC workflow. When field technicians
#' identify sensor issues, these records can be used to flag periods of known
#' malfunctions in the dataset.
#'
#' @param mWater_api_data A dataframe containing field notes from mWater, typically
#' the output from load_mWater().
#'
#' @return A dataframe containing only sensor malfunction records with standardized columns:
#' - start_DT: When the malfunction was first observed (timestamp)
#' - end_DT: When the malfunction was resolved or expected to be resolved (timestamp)
#' - site: Location identifier (standardized format)
#' - parameter: Affected sensor parameter (standardized name)
#' - notes: Technician observations about the malfunction
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [load_mWater()]
#' @seealso [add_malfunction_flag()]

grab_mWater_malfunction_notes <- function(mWater_api_data){

  # Filter mWater data to extract only records tagged as sensor malfunction visits
  # These represent instances where field technicians identified sensor problems
  malfunction_records <- mWater_api_data %>%
    dplyr::filter(grepl("Sensor malfunction", visit_type, ignore.case = TRUE)) %>%
    dplyr::select(start_DT, site, crew, which_sensor_malfunction, malfunction_end_dt, notes = visit_comments)
  
  # Define the standard parameter names used in the QAQC system
  # This ensures consistent naming when matching malfunction records to sensor data
  parameters <- c("Battery Level",
                  "Baro",
                  "Chl-a Fluorescence",
                  "Depth",
                  "DO",
                  "External Voltage",
                  "ORP",
                  "pH",
                  "Specific Conductivity",
                  "Temperature",
                  "Turbidity")
  
  # Process and standardize the malfunction records
  malfunction_records <- malfunction_records %>%
    
    # Select and rename columns to match expected format for QAQC workflow
    dplyr::select(start_DT, end_DT = malfunction_end_dt, site, parameter = which_sensor_malfunction, notes) %>%
    
    # Handle cases where multiple parameters are affected by splitting comma-separated values
    # This creates separate rows for each affected parameter
    tidyr::separate_rows(parameter, sep = ", ") %>%
    
    # Standardize parameter names to match the system's naming conventions
    # Field technicians may use different terminology for the same sensors
    dplyr::mutate(
      parameter = dplyr::case_when(
        parameter == "Chlorophyll a" ~ "Chl-a Fluorescence",
        parameter == "RDO" ~ "DO",
        parameter == "Conductivity" ~ "Specific Conductivity",
        .default = parameter
      ),
      site = dplyr::case_when(
        site == "river bluffs" ~ "riverbluffs",
        .default = site
      )
    ) %>%
    
    # Filter to include only parameters relevant to the QAQC analysis
    dplyr::filter((is.na(parameter)) | (parameter %in% parameters))
  
  return(malfunction_records)
}


# ===================================
# File: R/grab_mWater_sensor_notes.R
# ===================================

#' @title Extract sensor maintenance field notes from mWater data
#'
#' @description
#' Processes mWater field data to extract and format records specifically related to 
#' sensor maintenance, calibration, and deployment activities. This function filters 
#' for relevant visit types, formats equipment change information, and standardizes 
#' the data structure to match downstream QAQC processing requirements.
#'
#' This function focuses on routine sensor operations (cleaning, calibration, deployment)
#' and explicitly excludes sensor malfunction records, which are handled separately by
#' `grab_mWater_malfunction_notes()`.
#'
#' @param mWater_api_data A dataframe containing field notes from mWater, typically
#' the output from `load_mWater()`.
#'
#' @return A dataframe containing only sensor maintenance records with standardized columns:
#' - site: Location identifier
#' - DT_round/DT_join: Timestamps for joining with sensor data
#' - sonde_employed: Binary indicator of deployment status
#' - sensor_swapped_notes: Formatted notes about equipment changes
#' - Various maintenance fields (sensors_cleaned, cals_performed, etc.)
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [load_mWater()]
#' @seealso [add_field_notes()]

grab_mWater_sensor_notes <- function(mWater_api_data){

  # Extract and format sensor maintenance notes from mWater data
  # These notes record routine sensore operations like cleaning, calibration, 
  # and deployment

  mWater_field_notes <- mWater_api_data %>%
    # Filter for sensor-related activities but exclude malfunction reports
    # (malfunction reports are handled separately in `grab_mWater_malfunction_notes()`)
    dplyr::filter(grepl("Sensor", visit_type, ignore.case = TRUE) & 
                  !grepl("Sensor malfunction", visit_type, ignore.case = TRUE)) %>%

    # Create derived fields to track equipment status and changes
    dplyr::mutate(
      # Track sonde deployment status based on field technician actions:
      # 0 = deployed, 1 = pulled (removed), NA = other actions or unknown
      sonde_employed = dplyr::case_when(
        is.na(sensor_change)  ~ NA,
        sensor_change == "Swapped" ~ NA,
        sensor_change == "Pulled" ~ 1,
        sensor_change == "Deployed" ~ 0),

      # Create formatted notes about equipment changes with serial numbers
      sensor_swapped_notes = dplyr::case_when(is.na(sensor_change)  ~ NA,
                                              sensor_change == "Pulled" & !is.na(sensor_pulled) ~ paste0("SN Removed: ", sensor_pulled),
                                              sensor_change == "Swapped" ~ paste0("SN Removed: ", sensor_pulled, " SN Deployed: ", sensor_deployed),
                                              sensor_change == "Deployed" ~ sensor_deployed),
      
      # Create standardized date/time fields for joining with sensor data
      DT_join = as.character(DT_round),
      field_season = lubridate::year(DT_round),
      last_site_visit = DT_round,
      date = as.character(date)) %>%
    
    # Sort by timestamp (most recent first)
    dplyr::arrange(desc(DT_round))%>%
    
    # Select and reorder columns to match expected format for QAQC workflow
    dplyr::select(
      site, crew, DT_round, sonde_employed, sensors_cleaned, wiper_working, 
      rdo_cap_condition, rdo_cap_replaced, ph_junction_replaced, cals_performed, 
      cal_report_collected, sonde_moved, sensor_malfunction, sensor_pulled, 
      sensor_deployed, sensor_swapped_notes, visit_type, start_time_mst, DT_join, 
      start_DT, end_dt, date, visit_comments, photos_downloaded, field_season, 
      last_site_visit
    )
  
  return(mWater_field_notes)

}


# ===================================
# File: R/HydroVuR/flatten_page_params.R
# ===================================

#' @title Process paginated parameter readings from HydroVu API responses
#'
#' @description
#' Transforms the complex nested structure of a HydroVu API response page into a
#' flat dataframe suitable for analysis. This function handles the intricate
#' JSON structure returned by the API, extracting all parameter readings while
#' preserving their associated metadata.
#'
#' @param page_data A list containing a single page of results from the HydroVu
#' API, typically one element from the list returned by httr2::resp_body_json()
#'
#' @return A dataframe containing all parameter readings from the input page
#' with columns for parameter ID, unit ID, custom parameter flag, timestamp,
#' and value.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [hv_data_id()]

flatten_page_params <- function(page_data) {
  # Create an empty list to hold processed parameter data
  out <- list()
  
  # Extract the parameters list from the page data
  # This contains readings for multiple parameters (DO, temp, etc.)
  d <- page_data[["parameters"]]
  
  # Loop through each parameter in the parameters list
  for (i in seq_along(d)) {
    # For each parameter:
    # 1. Extract all its readings
    # 2. Convert to a dataframe
    # 3. Add parameter and unit metadata as columns
    x <- d[[i]][["readings"]] %>%
      # Collapse the readings for this parameter into a dataframe
      purrr::map_dfr(as.data.frame) %>%
      # Add parameter ID, unit ID, and custom parameter flag as columns
      dplyr::mutate(parameterId = d[[i]][["parameterId"]],
                   unitId = d[[i]][["unitId"]],
                   customParameter = d[[i]][["customParameter"]], 
                   .before = timestamp)
    
    # Add this parameter's data to our output list
    out <- c(out, list(x))
  }
  
  # Combine all parameter dataframes into a single dataframe
  df <- purrr::map_dfr(out, as.data.frame)
  
  return(df)
}


# ===================================
# File: R/HydroVuR/hv_auth.R
# ===================================

#' @title Authenticate with the HydroVu API
#'
#' @description
#' Creates an OAuth client for authenticating with the HydroVu API service. This 
#' function establishes the connection credentials needed to access water quality 
#' monitoring data from HydroVu-connected sondes and sensors. The authentication 
#' uses the OAuth 2.0 protocol to ensure secure access to the API.
#'
#' This function is typically called once at the beginning of each data collection 
#' cycle to obtain authorization for subsequent API requests.
#'
#' @param client_id A character string containing the client ID provided by HydroVu 
#' for API access. This serves as the public identifier for your application.
#'
#' @param client_secret A character string containing the client secret provided by 
#' HydroVu for API access. This serves as the password for your application and 
#' should be kept secure, typically stored in a credentials file.
#'
#' @param url A character string specifying the OAuth token endpoint URL for the 
#' HydroVu API. Default is "https://www.hydrovu.com/public-api/oauth/token".
#'
#' @return An OAuth client object from the httr2 package that can be used in 
#' subsequent API requests to retrieve water quality data.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [api_puller()]

hv_auth <- function(client_id, client_secret, url = "https://www.hydrovu.com/public-api/oauth/token") {
  
  # Create an OAuth client object for the HydroVu API
  client <- httr2::oauth_client(client_id, token_url = url,
                               secret = client_secret)
  
  # Return the client object for use in subsequent API requests
  return(client)

}


# ===================================
# File: R/HydroVuR/hv_data_id.R
# ===================================

#' @title Retrieve time series data for a specific HydroVu location
#'
#' @description
#' Fetches water quality monitoring data for a specific location ID from the 
#' HydroVu API for a given time period. This function handles the complexity of 
#' working with the HydroVu API, including authentication, pagination, timestamp 
#' conversions, and joining parameter and unit metadata to make the data more 
#' readable and meaningful.
#' 
#' The function converts human-readable timestamps to Unix timestamps for the API 
#' request, then converts the response data back to the specified timezone for 
#' consistency with other system data. It also manages pagination to ensure all 
#' data points within the specified time range are retrieved, even when they span 
#' multiple API response pages.
#'
#' @param loc_id Character string containing the unique identifier for a HydroVu 
#' location (monitoring site). This ID is obtained from the hv_locations_all() 
#' function.
#'
#' @param start_time Character string specifying the start of the time range for 
#' data retrieval in "YYYY-MM-DD HH:MM:SS" format. Should be in the timezone 
#' specified by the tz parameter.
#'
#' @param end_time Character string specifying the end of the time range for data 
#' retrieval in "YYYY-MM-DD HH:MM:SS" format. Should be in the timezone specified 
#' by the tz parameter.
#'
#' @param tz Character string specifying the timezone for input timestamps and 
#' returned data. Default is determined by the calling environment's timezone 
#' variable.
#'
#' @param token OAuth client object obtained from hv_auth() function, used for 
#' authentication with the HydroVu API.
#'
#' @return A dataframe containing water quality time series data with columns:
#' - timestamp: Time of measurement in the specified timezone
#' - value: The measured value
#' - Location: The location ID (same as input loc_id)
#' - Parameter: Human-readable parameter name (e.g., "Temperature", "DO")
#' - Units: Measurement units (e.g., "°C", "mg/L")
#' The data is arranged by Parameter and timestamp for easy analysis.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [hv_auth()]
#' @seealso [hv_locations_all()]
#' @seealso [api_puller()]
#' @seealso [flatten_page_params()]
#' @seealso [hv_names()]

hv_data_id <- function(loc_id, start_time = startdate, end_time = enddate, tz = timezone, token) {
  
  # Convert input timestamps from specified timezone to Unix timestamps (seconds since epoch) in UTC
  # This is required for HydroVu API requests which use Unix timestamps
  start <- as.numeric(lubridate::with_tz(lubridate::ymd_hms(start_time, tz = tz), tzone = "UTC"))
  end <- as.numeric(lubridate::with_tz(lubridate::ymd_hms(end_time, tz = tz), tzone = "UTC"))
  
  # Construct the API URL with location ID and time parameters
  url = "https://www.hydrovu.com/public-api/v1/locations/"
  url <- paste0(url, loc_id, "/data?endTime=", end, '&startTime=', start)
  
  # Initialize the API request
  req <- httr2::request(url)
  
  # Log which site we're attempting to query (helpful for debugging)
  print(paste0('Trying site ', loc_id))
  
  try({
    # Perform the initial API request with OAuth authentication
    resp <- req %>% 
      httr2::req_oauth_client_credentials(token) %>% 
      httr2::req_perform()
    
    # Extract the JSON response body and store in a list
    data <- list(resp %>% httr2::resp_body_json())
    
    # Get the response headers to check for pagination
    h <- resp %>% httr2::resp_headers()
    
    # Handle pagination - continue fetching pages while the X-ISI-Next-Page header exists
    while (!is.null(h[["X-ISI-Next-Page"]]))
    {
      # Request the next page using the page marker from the previous response
      resp <- req %>% 
        httr2::req_headers("X-ISI-Start-Page" = h[["X-ISI-Next-Page"]]) %>%
        httr2::req_oauth_client_credentials(token) %>% 
        httr2::req_perform()
      
      # Add this page's results to our collection
      data <- c(data, list(resp %>% httr2::resp_body_json()))
      
      # Update headers to check if there are more pages
      h <- resp %>% httr2::resp_headers()
    }
    
    # Retrieve parameter and unit metadata for translating IDs to human-readable names
    params <- hv_names(token, return = "params")
    units <- hv_names(token, return = "units")
    
    # Process the collected data:
    # 1. Flatten the nested data structure into a dataframe
    # 2. Convert timestamps back to the specified timezone
    # 3. Join with parameter and unit metadata for readability
    # 4. Arrange data by parameter and timestamp for easy analysis
    df <- purrr::map_dfr(data, flatten_page_params) %>%
      dplyr::mutate(
        timestamp = lubridate::with_tz(lubridate::as_datetime(timestamp, tz = "UTC"), tzone = tz),
        Location = loc_id
      ) %>%
      dplyr::inner_join(params, by = "parameterId") %>%
      dplyr::inner_join(units, by = "unitId") %>%
      dplyr::select(-parameterId, -unitId) %>%
      dplyr::arrange(Parameter, timestamp)
    
    return(df)
  })
}


# ===================================
# File: R/HydroVuR/hv_locations_all.R
# ===================================

#' @title Retrieve all monitoring locations from HydroVu
#'
#' @description
#' Fetches the complete list of water quality monitoring locations accessible to
#' the authenticated client from the HydroVu API. This function handles pagination
#' in the API response to ensure all locations are retrieved, even when the total
#' number of locations exceeds the API's per-page limit.
#' 
#' The function uses OAuth2 client credentials for authentication and manages the
#' complex task of requesting and combining multiple pages of location data into a
#' single, clean dataframe.
#'
#' @param client An OAuth2 client object as returned by the hv_auth() function,
#' containing the necessary authentication credentials.
#'
#' @param url Character string specifying the HydroVu API endpoint for retrieving
#' location data. Default is "https://www.hydrovu.com/public-api/v1/locations/list".
#'
#' @return A dataframe containing all monitoring locations accessible to the
#' authenticated client, with metadata such as:
#' - id: Unique identifier for each location
#' - name: Location display name
#' - Various other location metadata fields
#' 
#' The GPS coordinates are removed from the result, and duplicate entries are
#' filtered out.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [hv_auth()]
#' @seealso [api_puller()]
#' @seealso [hv_data_id()]

hv_locations_all <- function(client,
                             url = "https://www.hydrovu.com/public-api/v1/locations/list") {
  # Initialize the API request
  req <- httr2::request(url)
  
  try({
    # Perform the initial API request with OAuth authentication
    resp <- req %>% 
      httr2::req_oauth_client_credentials(client) %>% 
      httr2::req_perform()
    
    # Extract the JSON response body and store in a list
    locs <- list(resp %>% httr2::resp_body_json())
    
    # Get the response headers to check for pagination
    h <- resp %>% httr2::resp_headers()
    
    # Handle pagination - continue fetching pages while the X-ISI-Next-Page header exists
    # This ensures we get ALL locations, not just the first page
    while (!is.null(h[["X-ISI-Next-Page"]]))
    {
      # Request the next page using the page marker from the previous response
      resp2 <- req %>%
        httr2::req_headers("X-ISI-Start-Page" = h[["X-ISI-Next-Page"]]) %>%
        httr2::req_oauth_client_credentials(client) %>%
        httr2::req_perform()
      
      # Add this page's results to our collection
      locs <- c(locs, list(resp2 %>% httr2::resp_body_json()))
      
      # Update headers to check if there are more pages
      h <- resp2 %>% httr2::resp_headers()
    }
    
    # Process the collected data:
    # 1. Flatten the nested list structure into a dataframe
    # 2. Remove the GPS coordinates for privacy/security
    # 3. Remove any duplicate location entries
    df <- purrr::flatten_df(locs) %>%
      dplyr::select(-gps) %>%
      dplyr::filter(!duplicated(.))
    
    return(df)
  })
}


# ===================================
# File: R/HydroVuR/hv_names.R
# ===================================

#' @title Retrieve friendly names for HydroVu parameters and units
#'
#' @description
#' Fetches human-readable names for parameter IDs and unit IDs from the HydroVu
#' API. The raw data from HydroVu uses numeric identifiers for parameters and
#' units, which are not intuitive. This function retrieves the mapping between
#' these IDs and their corresponding friendly names (e.g., parameter ID "2" 
#' might map to "Temperature" and unit ID "7" to "°C").
#'
#' @param client An OAuth2 client object as returned by the hv_auth() function,
#' containing the necessary authentication credentials.
#'
#' @param return Character string specifying what data to return. Options are:
#' "both" (default) returns a list with both parameter and unit mappings,
#' "params" returns only parameter mappings, and "units" returns only unit
#' mappings.
#'
#' @param url Character string specifying the HydroVu API endpoint for
#' retrieving friendly names. Default is "https://www.hydrovu.com/public-api/
#' v1/sispec/friendlynames".
#'
#' @return Depending on the 'return' parameter:
#' - "params": A dataframe mapping parameter IDs to friendly parameter names
#' - "units": A dataframe mapping unit IDs to friendly unit names
#' - "both": A list containing both dataframes
#' - If an invalid 'return' value is provided, returns NULL with an error message
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [hv_data_id()]

hv_names <- function(client, return = "both", 
                   url = "https://www.hydrovu.com/public-api/v1/sispec/friendlynames") {
  # Initialize the API request
  req <- httr2::request(url)
  
  # Perform the API request with OAuth authentication
  resp <- req %>% 
    httr2::req_oauth_client_credentials(client) %>%
    httr2::req_perform()
  
  # Extract the response body containing parameter and unit mappings
  names <- resp %>% httr2::resp_body_json()
  
  # Process parameter mappings - convert from JSON to dataframe
  # This creates a dataframe with parameterId and Parameter (friendly name) columns
  p <- names[["parameters"]] %>% 
    purrr::map_dfr(as.data.frame, .id = "parameterId")
  names(p) <- c("parameterId", "Parameter")
  
  # Process unit mappings - convert from JSON to dataframe
  # This creates a dataframe with unitId and Units (friendly name) columns
  u <- names[["units"]] %>% 
    purrr::map_dfr(as.data.frame, .id = "unitId")
  names(u) <- c("unitId", "Units")
  
  # Return the requested data based on the 'return' parameter
  if (return == "params") {
    return(p)
  }
  else if (return == "units") {
    return(u)
  }
  else if (return == "both") {
    b <- list(params = p, units = u)
    return(b)
  }
  else {
    print("Error: return must be one of c('both', 'params', 'units')")
    return(NULL)
  }
}


# ===================================
# File: R/intersensor_check.R
# ===================================

#' @title Remove redundant slope violation flags across parameters
#'
#' @description
#' Reduces overflagging by identifying and removing "slope violation" flags that likely 
#' represent real environmental changes rather than sensor malfunctions. When rapid changes 
#' occur simultaneously in multiple parameters, especially in temperature or depth, these 
#' changes typically reflect actual hydrologic events rather than sensor problems.
#'
#' This function processes all parameters from a single site and:
#' 1. Identifies concurrent slope violations across parameters
#' 2. Removes "slope violation" flags from non-temperature, non-depth parameters when they 
#'    coincide with similar flags in temperature or depth
#' 3. Removes all "slope violation" flags from temperature and depth parameters, as these 
#'    parameters rarely exhibit false spikes
#'
#' @param df A data frame containing all parameters for a single site. Must include columns:
#' - `DT_round`: Timestamp for each measurement
#' - `DT_join`: Character timestamp used for joining
#' - `parameter`: Measurement type
#' - `mean`: The calculated mean value of measurements
#' - `flag`: Existing quality flags containing "slope violation" flags to be checked
#'
#' @return A data frame with the same structure as the input, but with the `flag`
#' column updated to remove "slope violation" flags that coincide with temperature 
#' or depth changes, and with all "slope violation" flags removed from temperature 
#' and depth parameters.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [add_flag()]

intersensor_check <- function(df){
  # Extract temperature data and flags for the site
  # Include flags from adjacent timestamps for context
  temperature <- df %>%
    data.table::data.table() %>%
    dplyr::select(DT_round, DT_join, parameter, mean, flag) %>%
    dplyr::filter(parameter == "Temperature") %>%
    dplyr::select(DT_join, Temperature = parameter, Temperature_flag = flag) %>%
    dplyr:: mutate(Temperature_front1 = dplyr::lead(Temperature_flag, n = 1),
                   Temperature_back1 = dplyr::lag(Temperature_flag, n = 1))
  
  # Extract depth data and flags for the site
  # Include flags from adjacent timestamps for context
  depth <- df %>%
    data.table::data.table() %>%
    dplyr::select(DT_round, DT_join, parameter, mean, flag) %>%
    dplyr::filter(parameter == "Depth") %>%
    dplyr::select(DT_join, Depth = parameter, Depth_flag = flag) %>%
    dplyr::mutate(Depth_front1 = dplyr::lead(Depth_flag, n = 1),
                  Depth_back1 = dplyr::lag(Depth_flag, n = 1))
  
  # Process non-temperature, non-depth parameters to remove redundant flags
  intersensors_checked <- df %>%
    dplyr::filter(!parameter %in% c("Depth", "Temperature")) %>%
    dplyr::left_join(., temperature, by = "DT_join") %>%
    dplyr::left_join(., depth, by = "DT_join") %>%
    # Identify slope violations that coincide with similar flags in depth or temperature
    # (including timestamps immediately before or after)
    dplyr::mutate(intersensored = dplyr::case_when(grepl("slope violation", flag) &
                                                     (grepl("slope violation", Depth_flag)   | grepl("slope violation", Temperature_flag)   |
                                                        grepl("slope violation", Depth_front1) | grepl("slope violation", Temperature_front1) |
                                                        grepl("slope violation", Depth_back1)  | grepl("slope violation", Temperature_back1)
                                                     ) ~ TRUE)) %>%
    # Remove the "slope violation" flag when it coincides with depth/temperature changes
    dplyr::mutate(flag = ifelse(is.na(intersensored), flag, stringr::str_replace(flag, "slope violation", "")))
  
  # Process temperature and depth parameters
  final_checked_data <- df %>%
    dplyr::filter(parameter %in% c("Depth", "Temperature")) %>%
    # Remove all "slope violation" flags from temperature and depth
    # These parameters rarely exhibit false spikes
    dplyr::mutate(flag = stringr::str_replace(flag, "slope violation", "")) %>%
    # Combine with the processed non-temperature, non-depth parameters
    dplyr::bind_rows(., intersensors_checked) %>%
    # Remove temporary columns added during processing
    dplyr::select(-c(Depth, Depth_flag, Temperature, Temperature_flag))
  
  return(final_checked_data)
}


# ===================================
# File: R/load_mWater.R
# ===================================

#' @title Load and tidy mWater field notes
#'
#' @description A function that downloads and cleasn field notes from mWater. This
#' funciton handles time zone conversion, standardizes text fields, and prepares
#' the data for integration with sonde readings. 
#'
#' @param creds A .yml file with necessary credentials for accessing the field 
#' notes. Must contain a 'url' field. 
#' 
#' @param summarize_interval Character string specifying the time interval to round timestamps to.
#' Default is "15 minutes". Accepts any interval format compatible with 
#' lubridate::floor_date() like "1 hour", "30 mins", etc.
#'
#' @return A dataframe containing processed field notes with standardized columns:
#' - site: Standardized site name (lowercase, no spaces)
#' - DT_round: Rounded timestamp for joining with sensor data
#' - start_DT/end_dt: Start and end times of field visits (MST timezone)
#' - visit_type: Type of field visit (standardized)
#' - sensor_pulled/sensor_deployed: Serial numbers of equipment
#' - And various other field observation columns
#' 
#' @examples
#' # Examples are temporarily disabled
#' @seealso [grab_mWater_sensor_notes()]
#' @seealso [grab_mWater_malfunction_notes()]

load_mWater <- function(creds = yaml::read_yaml("creds/mWaterCreds.yml"), summarize_interval = "15 minutes"){

  # Retrieve the API URL from the credentials file
  api_url <- as.character(creds)
  # TODO: how do we want to handle access when this package is public? (in relation to 
  # creds file)

  # Download field notes from mWater API and perform data cleaning operations
  all_notes_cleaned <- readr::read_csv(url(api_url), show_col_types = FALSE) %>%
    dplyr::mutate(
      # Convert timestamps from UTC to Mountain Standard Time (MST)
      # Handle multiple possible date-time formats using lubridate
      start_DT = lubridate::with_tz(lubridate::parse_date_time(start_dt, orders = c("%Y%m%d %H:%M:%S", "%m%d%y %H:%M", "%m%d%Y %H:%M", "%b%d%y %H:%M" )), tz = "MST"),
      end_dt = lubridate::with_tz(lubridate::parse_date_time(end_dt, orders = c("%Y%m%d %H:%M:%S", "%m%d%y %H:%M", "%m%d%Y %H:%M", "%b%d%y %H:%M" )), tz = "MST"),
      malfunction_end_dt = lubridate::with_tz(lubridate::parse_date_time(malfunction_end_dt, orders = c("%Y%m%d %H:%M:%S", "%m%d%y %H:%M", "%m%d%Y %H:%M", "%b%d%y %H:%M" )), tz = "MST"),
      
      # Extract date and time components for convenience
      date = as.Date(start_DT, tz = "MST"),
      start_time_mst = format(start_DT, "%H:%M"),

      # Ensure sensor serial numbers are character type
      sensor_pulled = as.character(sn_removed),
      sensor_deployed = as.character(sn_deployed),

      # Handle "Other" site names by using the free text response
      # Also standardize by removing spaces and converting to lowercase
      site = ifelse(site == "Other (please specify)", tolower(stringr::str_replace_all(site_other, " ", "")), site),
      
      # Fix a survey design issue where "???" appears in visit_type
      visit_type = dplyr::case_when(stringr::str_detect(visit_type, "\\?\\?\\?") ~ stringr::str_replace(string = visit_type,
                                                                               pattern =  "\\?\\?\\?",
                                                                               replacement = "Sensor Calibration or Check"),
                             TRUE ~ visit_type),
      
      # Replace "Other" in visit_type with the free text response when applicable
      visit_type = dplyr::case_when(stringr::str_detect(visit_type, "Other") ~ stringr::str_replace(string = visit_type,
                                                                           pattern =  "Other \\(please specify\\)",
                                                                           replacement = visit_type_other),
                             TRUE ~ visit_type),
      
      # Replace "Other" in sensor_malfunction with the free text response
      which_sensor_malfunction = dplyr::case_when(stringr::str_detect(which_sensor_malfunction, "Other") ~ stringr::str_replace(string = which_sensor_malfunction,
                                                                                                       pattern =  "Other \\(please specify\\)",
                                                                                                       replacement = as.character(other_which_sensor_malfunction)),
                                           TRUE ~ which_sensor_malfunction),

      # Replace "Other" in photos_downloaded with the free text response
      photos_downloaded = ifelse(photos_downloaded == "Other (please specify)", photos_downloaded_other, photos_downloaded),
      
      # Create a rounded timestamp for joining with sensor data at consistent intervals
      DT_round = lubridate::floor_date(start_DT, unit = summarize_interval)) %>%
    # arrange by timestamp (most recent first)
    dplyr::arrange(DT_round)%>%
    # Remove redundant "other" columns that have been merged into primary columns
    dplyr::select(-c(photos_downloaded_other,visit_type_other, site_other, other_which_sensor_malfunction ))

  return(all_notes_cleaned)

}


# ===================================
# File: R/load_old_field_notes.R
# ===================================

#' @title Load and tidy old field notes
#'
#' @description A function that uploads and cleans the field notes excel file. This function adds datetime
#' columns to the field notes dataframe and filters out field notes where the sensor
#' was not handled.
#' @param filepath A file path to the raw field notes.
#' @param summarize_interval At what time interval the user would like the data set to be aggregated and rounded to. Default is 15 minutes.
#' @return A dataframe with the field notes.

load_old_field_notes <- function(filepath,  summarize_interval = "15 minutes"){
  
  raw_field_notes <- readxl::read_excel(filepath)
  
  field_notes <- raw_field_notes %>%
    dplyr::mutate(start_DT = lubridate::ymd_hm(paste(date, start_time_mst), tz = "MST")) %>%
    dplyr::mutate(
      DT_round = lubridate::floor_date(start_DT, unit = summarize_interval),
      DT_join = as.character(DT_round),
      site = tolower(site),
      field_season = lubridate::year(DT_round),
      last_site_visit = DT_round,
      sonde_moved = NA) %>%
    dplyr::arrange(site, DT_round) %>%
    # rename instances of old names:
    dplyr::mutate(site = ifelse(site == "rist", "tamasag",
                                ifelse(site == "elc", "boxelder", site))) %>%
    # `sonde_employed` determines if the sonde is deployed or not. 0 = sonde deployed, 1 = sonde is not deployed
    dplyr::mutate(sonde_employed = dplyr::case_when(!is.na(sensor_pulled) & !is.na(sensor_deployed) ~ 0,
                                                    !is.na(sensor_pulled) & is.na(sensor_deployed) ~ 1,
                                                    is.na(sensor_pulled) & !is.na(sensor_deployed) ~ 0,
                                                    is.na(sensor_pulled) & is.na(sensor_deployed) ~ 0),
                  end_dt  = as.POSIXct(NA, tz = "MST")) %>%
    # remove field dates where sensor was not handled:
    dplyr::filter(grepl("Sensor Cleaning or Check|Sensor Calibration", visit_type, ignore.case = TRUE))
  
  return(field_notes)
  
}


# ===================================
# File: R/move_api_data.R
# ===================================

#' @title Archive API data files after processing
#'
#' @description
#' Moves raw data files from the working API directory to an archive directory 
#' after they have been processed through the water quality monitoring workflow.
#' This function performs several steps:
#' 
#' 1. Identifies files in the API directory that aren't already in the archive
#' 2. Copies those files to the archive directory
#' 3. Verifies the copy operation
#' 4. Removes all files from the original API directory
#'
#' This function is typically called at the end of the data processing workflow
#' to maintain a clean working environment while preserving raw data files.
#'
#' @param api_dir File path to the working API directory containing processed data files
#' @param archive_dir File path to the archive directory where files will be stored
#'
#' @return No return value, called for side effects (file operations)
#'
#' @examples
#' # Examples are temporarily disabled

move_api_data <- function(api_dir, archive_dir,
                          synapse_env = FALSE, fs = NULL) {
  
  if (synapse_env) {
    # List files in the working and archive directories
    incoming_files <- basename(AzureStor::list_adls_files(fs, api_dir, info = "name"))
    archive_files <- basename(AzureStor::list_adls_files(fs, archive_dir, info = "name"))
    
    # Identify files that aren't already in the archive
    files_to_copy <- setdiff(incoming_files, archive_files)
    
    if (length(files_to_copy) > 0) {
      # Copy new files to the archive
      local_file_list <- map(files_to_copy, function(file_name) {
        # create a temp file 
        temp_file <- tempfile(fileext = ".csv")
        
        # create a file path to the file name
        local_path <- file.path(api_dir, file_name)
        
        # download the data into the temporary file
        AzureStor::download_adls_file(fs, local_path, temp_file)
        
        # return the temporary file
        return(temp_file)
      }) %>% 
        unlist()
      
      # Create a list of new destination paths
      dest_paths <- file.path(archive_dir, files_to_copy)
      
      # Use multiupload the files
      AzureStor::multiupload_adls_file(
        filesystem = fs,
        src = local_file_list,
        dest = dest_paths
      )
      
      for (i in files_to_copy) {
        print(paste0("File ", i, " has been copied to ", archive_dir, " from ", api_dir, "."))
      }
    } else {
      print(paste0("All files from ", api_dir, " are already present in ", archive_dir, ". Nothing to copy."))
    }
    
    # Brief pause to ensure file operations complete
    Sys.sleep(30)
    
    # Refresh list of archive files after copying
    archive_files <- basename(AzureStor::list_adls_files(fs, archive_dir, info = "name"))
    
    # Verify copy operation was successful
    if (all(incoming_files %in% archive_files)) {
      print(paste0("All files from ", api_dir, " have been successfully copied to ", archive_dir, "."))
    } else {
      warning(paste0("Not all files from ", api_dir, " have been successfully copied to ", archive_dir, "."))
      # Note: Pipeline continues despite verification issues
    }
    
    # Remove the copied files from the working directory
    if (length(files_to_copy) > 0) {
      walk(files_to_copy, function(file_name){
        AzureStor::delete_adls_file(filesystem = fs,
                                    file = file.path(api_dir, file_name))
        print(paste0("File ", file_name, " has been deleted from ", api_dir, "."))
      })
      print("Copied files have been removed from the incoming directory.")
    }
    
    # Wait for deletion to complete
    Sys.sleep(30)
    
    # Final cleanup: remove any remaining files from the working directory
    incoming_files <- basename(AzureStor::list_adls_files(fs, api_dir, info = "name"))
    
    if (length(incoming_files) > 0) {
      files_to_delete <- AzureStor::list_adls_files(fs, api_dir, info = "name")
      walk(files_to_delete, function(file_path){
        AzureStor::delete_adls_file(filesystem = fs,
                                    file = file_path)
        print(paste0("File ", file_path, " has been deleted from ", api_dir, "."))
      })
    }
    
    print(paste0("All files removed from ", api_dir, "."))
    
  } else {
    # List files in the working and archive directories
    incoming_files <- list.files(api_dir, full.names = FALSE)
    archive_files <- list.files(archive_dir, full.names = FALSE)
    
    # Identify files that aren't already in the archive
    files_to_copy <- setdiff(incoming_files, archive_files)
    
    # Copy new files to the archive
    if (length(files_to_copy) > 0) {
      for (file in files_to_copy) {
        full_file_name <- file.path(api_dir, file)
        file.copy(full_file_name, archive_dir)
        print(paste0(file, " has been moved to archive API data folder."))
      }
      print("Files have been copied from the incoming directory to the archive directory.")
    } else {
      print("All files are already present in the archive directory. Nothing to copy.")
    }
    
    # Brief pause to ensure file operations complete
    Sys.sleep(5)
    
    # Refresh list of archive files after copying
    archive_files <- list.files(archive_dir, full.names = FALSE)
    
    # Verify copy operation was successful
    if (all(incoming_files %in% archive_files)) {
      print("All files in the incoming directory have been successfully copied to the archive directory.")
    } else {
      print("Not all files from the incoming directory have been successfully copied to the archive directory.")
      # Note: Pipeline continues despite verification issues
    }
    
    # Remove the copied files from the working directory
    if (length(files_to_copy) > 0) {
      for (file in files_to_copy) {
        full_file_name <- file.path(api_dir, file)
        file.remove(full_file_name)
      }
      print("Copied files have been removed from the incoming directory.")
    }
    
    # Final cleanup: remove any remaining files from the working directory
    for (file in list.files(api_dir, full.names = TRUE)) {
      file.remove(file)
    }
    print("All files removed from incoming directory.")
  }
  
  
}


# ===================================
# File: R/munge_api_data.R
# ===================================

#' @title Process raw API data for water quality monitoring workflow
#'
#' @description
#' Transforms raw CSV files downloaded from the HydroVu API into a standardized
#' format suitable for further quality control processing. This function handles
#' data from multiple monitoring networks, applies site name standardization,
#' performs timezone conversion, and manages special cases where monitoring
#' equipment was relocated between sites. It serves as a crucial preprocessing
#' step that bridges the gap between raw API data and the structured format
#' required by downstream quality control functions.
#'
#' @param api_path Character string specifying the directory path containing the
#' raw CSV files downloaded from the HydroVu API.
#'
#' @param network Character string indicating which monitoring network to process.
#' Options include "CSU", "FCW" (Fort Collins Watershed), or "all". Different
#' networks may have different processing requirements.
#'
#' @param summarize_interval Character string specifying the time interval to
#' round timestamps to. Default is "15 minutes". Accepts any interval format
#' compatible with lubridate::round_date().
#' 
#' @param fs Logical, whether to use the file system functions
#'
#' @return A dataframe containing processed water quality monitoring data with
#' standardized columns:
#' - site: Standardized site name (lowercase, no spaces)
#' - DT: Original timestamp (MST timezone)
#' - DT_round: Rounded timestamp for consistent time intervals
#' - DT_join: Character representation of rounded timestamp for joining
#' - parameter: Measurement type (e.g., "Temperature", "DO")
#' - value: Measured value
#' - units: Measurement units (e.g., "°C", "mg/L")
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [api_puller()]
#' @seealso [tidy_api_data()]

munge_api_data <- function(api_path, network, summarize_interval = "15 minutes", 
                           synapse_env = FALSE, fs = NULL) {
  
  if (synapse_env) {
    
    # List files in ADLS
    file_list <- AzureStor::list_adls_files(fs, api_path, info = "name")
    
    # Map files into a dataframe
    api_data <- map_dfr(file_list, function(adls_path){
      # set temp file
      temp_file <- tempfile(fileext = '.csv')
      
      # download file from folder into temp file
      AzureStor::download_adls_file(fs, adls_path, temp_file)
      
      # read the temp file with read.csv
      site_df <- data.table::fread(temp_file) %>% 
        dplyr::select(-id) 
      
      return(site_df)
    }) %>% 
      dplyr::mutate(units = as.character(units)) %>%
      dplyr::distinct()
    
  } else {
    
    api_data <- map_dfr(list.files(api_path, full.names = TRUE), 
                        function(file_path) {
                          site_df <- data.table::fread(file_path) %>% 
                            dplyr::select(-id)
                        }) %>% 
      dplyr::mutate(units = as.character(units)) %>%
      dplyr::distinct()
    
  }
  
  # Apply network-specific processing for CSU/FCW networks
  # do we need to fix site names here?
  if(network %in% c("csu", "CSU", "FCW", "fcw")){
    api_data <- api_data %>%
      # Filter out VuLink data (not used in CSU/FCW networks)
      dplyr::filter(!grepl("vulink", name, ignore.case = TRUE)) %>%
      # Filter out Virridy sondes (not part of CSU/FCW networks)
      dplyr::filter(!grepl("virridy", name, ignore.case = TRUE)) %>%
      # Remove the equipment name column
      dplyr::select(-name) %>%
      
      # Convert timestamps from UTC (as provided by HydroVu API) to MST
      dplyr::mutate(DT = lubridate::as_datetime(timestamp, tz = "UTC")) %>%
      dplyr::mutate(
        # Apply timezone conversion to Mountain Standard Time
        DT = lubridate::with_tz(DT, tzone = "MST"),
        # Round timestamps to specified interval for consistent time series
        DT_round = lubridate::round_date(DT, summarize_interval),
        # Create string version of timestamp for joining operations
        DT_join = as.character(DT_round),
        # Ensure site names are lowercase for consistency
        site = tolower(site)
      ) %>%
      
      # Apply site name standardization and handle historical equipment relocations
      dplyr::mutate(
        # Map alternative site names to standard names
        site = dplyr::case_when(
          site == "rist" ~ "tamasag",
          site == "elc" ~ "boxelder",
          TRUE ~ site
        )
      ) %>%
      # Handle a specific case where equipment was moved between sites
      dplyr::mutate(
        site = ifelse(site == "tamasag" & 
                        DT > lubridate::ymd("2022-09-20", tz = "MST") & 
                        DT < lubridate::ymd("2023-01-01", tz = "MST"), 
                      "boxelder", site)
      ) %>%
      # Standardize "river bluffs" to "riverbluffs" (remove spaces)
      dplyr::mutate(
        site = ifelse(grepl("river bluffs", site, ignore.case = TRUE), 
                      "riverbluffs", site)
      ) %>%
      # Ensure no duplicates after all transformations
      dplyr::distinct(.keep_all = TRUE)
  }
  
  # Apply more inclusive processing for "all" networks option
  if(network %in% c("all", "All")){
    api_data <- api_data %>%
      # Filter out VuLink data (still excluded from "all" networks)
      dplyr::filter(!grepl("vulink", name, ignore.case = TRUE)) %>%
      
      # Remove the equipment name column
      dplyr::select(-name) %>%
      
      # Apply the same timestamp and site name standardization
      dplyr::mutate(DT = lubridate::as_datetime(timestamp, tz = "UTC")) %>%
      dplyr::mutate(
        DT = lubridate::with_tz(DT, tzone = "MST"),
        DT_round = lubridate::round_date(DT, summarize_interval),
        DT_join = as.character(DT_round),
        site = tolower(site)
      ) %>%
      
      # Apply the same site name standardization and equipment relocation handling
      dplyr::mutate(
        site = ifelse(site == "rist", "tamasag",
                      ifelse(site == "elc", "boxelder", site))
      ) %>%
      dplyr::mutate(
        site = ifelse(site == "tamasag" & 
                        DT > lubridate::ymd("2022-09-20", tz = "MST") & 
                        DT < lubridate::ymd("2023-01-01", tz = "MST"), 
                      "boxelder", site)
      ) %>%
      # Ensure no duplicates after all transformations
      dplyr::distinct(.keep_all = TRUE)
  }
  
  return(api_data)
}


# ===================================
# File: R/network_check.R
# ===================================

#' @title Reduce overflagging by comparing across monitoring network
#'
#' @description
#' Identifies and corrects overflagging by comparing data patterns across upstream 
#' and downstream monitoring sites. When quality flags at a site coincide with similar 
#' patterns at neighboring sites, these flags are often indicating real water quality 
#' events rather than sensor malfunctions and can be removed.
#'
#' Different site configurations are handled based on the monitoring network specified.
#' For chlorophyll measurements in the FCW/CSU network, no changes are made as these
#' measurements are site-specific.
#'
#' @param df A site-parameter dataframe that has undergone initial flagging. Must include:
#' - `site`: Standardized site name
#' - `parameter`: The measurement type 
#' - `DT_round`: Timestamp for measurements
#' - `flag`: Existing quality flags
#'
#' @param network Character string specifying the monitoring network to use for site 
#' relationships. Options are "all" (default), "CSU", or "FCW" (case-insensitive).
#'
#' @return A dataframe with the same structure as the input, plus an `auto_flag` column
#' that contains cleaned flags where network-wide events have been accounted for.
#'
#' @examples
#' # Examples are temporarily disabled

network_check <- function(df, network = "all") {
  # Extract site and parameter name from dataframe
  site_name <- unique(na.omit(df$site))
  parameter_name <- unique(na.omit(df$parameter))
  
  # Skip processing for chlorophyll in CSU/FCW network
  if(network  %in% c("csu", "CSU", "fcw", "FCW") & parameter_name == "Chl-a Fluorescence"){
    no_change <- df %>%
      dplyr::mutate(auto_flag = flag)
    return(no_change)
  }
  
  # Define site order based on spatial arrangement along river
  if(network  %in% c("csu", "CSU", "fcw", "FCW")){
    sites_order <- c("pbd",
                     "bellvue", # rist
                     "salyer",
                     "udall",
                     "riverbend",
                     "cottonwood",
                     "elc", # elc
                     "archery",
                     "riverbluffs")
    width_fun = 17 #2 hours before/after
  } else if(network %in% c("all", "All")){
    # More extensive site order for full network
    sites_order <-  c("joei",
                      "cbri",
                      "chd",
                      "pfal",
                      "pbd",
                      "bellvue", # rist
                      "salyer",
                      "udall",
                      "riverbend",
                      "cottonwood",
                      "elc", # elc
                      "archery",
                      "riverbluffs")
    width_fun = 17 # 2 hours before/after
    
    # Special cases for certain site groups
    if(site_name %in% c("penn", "sfm", "lbea")){
      sites_order <- c("penn",
                       "sfm",
                       "lbea")
    }
    if(site_name == "springcreek"){
      sites_order <- c("timberline virridy",
                       "springcreek",
                       "prospect virridy")
    }
    if(site_name == "boxcreek"){
      sites_order <- c("boxelder virridy",
                       "boxcreek",
                       "archery virridy")
    }
  }
  
  # Find the index of current site in ordered list
  site_index <- which(sites_order == sites_order[grep(gsub(" virridy", "", site_name), sites_order, ignore.case = TRUE)])
  
  # Create site-parameter identifier
  site_param <- paste0(site_name, "-", parameter_name)
  
  # Initialize empty dataframes for upstream/downstream sites
  prev_site_df <- tibble::tibble(DT_round = NA)
  next_site_df <- tibble::tibble(DT_round = NA)
  
  # Try to get upstream site data
  tryCatch({
    previous_site <- paste0(sites_order[site_index-1],"-",parameter_name)
    prev_site_df <- intersensor_flags[[previous_site]] %>%
      dplyr::select(DT_round, site_up = site, flag_up = flag) %>%
      data.table::data.table()},
    error = function(err) {
      cat(paste0(site_name," has no upstream site with ", parameter_name, ".\n"))})
  
  # Try to get downstream site data
  tryCatch({
    next_site <- paste0(sites_order[site_index+1],"-",parameter_name)
    next_site_df <- intersensor_flags[[next_site]] %>%
      dplyr::select(DT_round, site_down = site, flag_down = flag) %>%
      data.table::data.table()},
    error = function(err) {
      cat(paste0(site_name, " has no downstream site with ", parameter_name, ".\n"))})
  
  # Join current site data with upstream and downstream data
  join <- df %>%
    dplyr::left_join(., prev_site_df, by = "DT_round") %>%
    dplyr::left_join(., next_site_df, by = "DT_round")
  
  # Add placeholder columns if joining didn't provide them
  if(!("flag_down" %in% colnames(join))) {join$flag_down <- NA}
  if(!("flag_up" %in% colnames(join))) {join$flag_up <- NA}
  if(!("site_down" %in% colnames(join))) {join$site_down <- NA}
  if(!("site_up" %in% colnames(join))) {join$site_up <- NA}
  
  # Function to check if any flags exist in a time window
  check_2_hour_window_fail <- function(x) {
    sum(x) >= 1
  }
  
  # Function to add column if it doesn't exist
  add_column_if_not_exists <- function(df, column_name, default_value = NA) {
    if (!column_name %in% colnames(df)) {
      df <- df %>% dplyr::mutate(!!sym(column_name) := default_value)
    }
    return(df)
  }
  
  # Process flags based on upstream/downstream patterns
  df_test <- join %>%
    # Create binary indicator for upstream/downstream flags
    # 0 = no relevant flags upstream/downstream, 1 = at least one site has relevant flags
    dplyr::mutate(flag_binary = ifelse(
      (is.na(flag_up) | grepl("drift|DO interference|repeat|sonde not employed|frozen|unsubmerged|missing data|site visit|sv window|sensor malfunction|burial|sensor biofouling|improper level cal|sonde moved", flag_up)) &
        (is.na(flag_down) | grepl("drift|DO interference|repeat|sonde not employed|frozen|unsubmerged|missing data|site visit|sv window|sensor malfunction|burial|sensor biofouling|improper level cal|sonde moved", flag_down)), 0, 1)) %>%
    # Check for flags in 2-hour window (17 observations at 15-min intervals)
    dplyr::mutate(overlapping_flag = zoo::rollapply(flag_binary, width = width_fun, FUN = check_2_hour_window_fail, fill = NA, align = "center")) %>%
    add_column_if_not_exists(column_name = "auto_flag") %>%
    # If flag exists but is also present up/downstream, it likely represents a real event
    # In that case, remove the flag (set auto_flag to NA)
    dplyr::mutate(auto_flag = ifelse(!is.na(flag) & !grepl("drift|DO interference|repeat|sonde not employed|frozen|unsubmerged|missing data|site visit|sv window|sensor malfunction|burial|sensor biofouling|improper level cal|sonde moved", flag) & 
                                       (overlapping_flag == TRUE & !is.na(overlapping_flag)), NA, flag)) %>%
    dplyr::select(-c(flag_up, flag_down, site_up, site_down, flag_binary, overlapping_flag))
  
  return(df_test)
}


# ===================================
# File: R/tidy_api_data.R
# ===================================

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
#' # Examples are temporarily disabled
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
        n_obs = n(),
        .groups = "drop"
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


# ===================================
# File: R/tidy_flag_column.R
# ===================================

#' @title Clean and standardize quality flag formatting
#'
#' @description
#' Standardizes and formats the quality flags in the `auto_flag` column to ensure 
#' consistency across the dataset. This function handles several formatting tasks:
#' 
#' 1. Converts various flag formats to standardized versions
#' 2. Prioritizes certain flag types over others
#' 3. Removes trailing and leading semicolons
#' 4. Sorts multiple flags in a consistent order
#' 5. Replaces newlines with spaces
#' 
#' @param df A dataframe containing water quality flags. Must include an `auto_flag`
#' column containing quality flag text.
#'
#' @return A dataframe with the same structure as the input, but with the `auto_flag`
#' column standardized and cleaned for consistency.
#'
#' @examples
#' # Examples are temporarily disabled

tidy_flag_column <- function(df){
  
  # Helper function to sort semicolon-separated flag lists
  sort_semicolon_list <- function(text) {
    text %>%
      stringr::str_split(";") %>%                  # Split the string into a list of words
      purrr::map(~ sort(trimws(.x))) %>%         # Sort the words and trim spaces
      purrr::map_chr(~ paste(.x, collapse = ";")) # Rejoin them with semicolons
  }
  
  # Helper function to clean up semicolons in flag text
  remove_trailing_semicolons <- function(text) {
    text %>%
      stringr::str_replace_all(";\\s*$", "") %>%  # Remove "; " at the end of the string
      stringr::str_replace_all(";$", "") %>%       # Remove ";" at the end of the string
      stringr::str_replace_all(";\\s*;", ";") %>%  # Replace multiple semicolons with a single ";"
      stringr::str_replace_all("^;+", "") %>%      # Remove semicolons at the start of the string
      stringr::str_replace_all(";\\s*$", "")       # Remove trailing semicolons again
  }
  
  # Process and standardize flag values  
  df <- df %>%
    data.table::data.table() %>%
    # Standardize flag values using a case-by-case approach
    dplyr::mutate(auto_flag = dplyr::case_when(
      is.na(auto_flag) ~ NA_character_,  # Keep NAs
      # Replace specific flag patterns with standardized versions
      grepl("site visit", auto_flag) ~ "site visit",
      grepl("sv window", auto_flag) ~ "site visit window",                                                      
      grepl("reported sonde not employed", auto_flag) ~ "sonde not employed",
      grepl("reported sensor malfunction", auto_flag) ~ "reported sensor malfunction",
      grepl("reported sonde burial", auto_flag) ~ "reported sonde burial",
      grepl("reported sensor biofouling", auto_flag) ~ "reported sensor biofouling",
      grepl("frozen", auto_flag) ~ "frozen",
      TRUE ~ auto_flag  # Keep original value if no conditions matched
    )) %>%
    # Apply cleanup functions to format flags consistently
    dplyr::mutate(auto_flag = purrr::map_chr(auto_flag, remove_trailing_semicolons)) %>%
    dplyr::mutate(auto_flag = stringr::str_replace_all(auto_flag, "\\n", " ")) %>%
    dplyr::mutate(auto_flag = stringr::str_trim(auto_flag) %>% sort_semicolon_list(.)) 
  
  return(df)
  
}


# ===================================
# File: R/utils-pipe.R
# ===================================

#' Pipe operator
#'
#' See \code{magrittr::\link[magrittr:pipe]{\%>\%}} for details.
#'
#' @name %>%
#' @rdname pipe
#' @keywords internal
#' @export
#' @importFrom magrittr %>%
#' @usage lhs \%>\% rhs
#' @param lhs A value or the magrittr placeholder.
#' @param rhs A function call using the magrittr semantics.
#' @return The result of calling `rhs(lhs)`.
NULL
