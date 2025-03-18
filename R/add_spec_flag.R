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
#' # Flag conductivity measurements outside sensor specifications
#' archery_conductivity_flagged <- add_spec_flag(df = all_data_flagged$`archery-Actual Conductivity`)
#'
#' # Flag temperature measurements outside sensor specifications
#' boxelder_temp_flagged <- add_spec_flag(df = all_data_flagged$`boxelder-Temperature`)
#'
#' # Use custom specification thresholds
#' custom_specs <- yaml::read_yaml("path/to/custom/thresholds.yml")
#' custom_flagged <- add_spec_flag(df = all_data_flagged$`riverbluffs-DO`, spec_table = custom_specs)
#'
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