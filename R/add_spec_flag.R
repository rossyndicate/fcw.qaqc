# Adding flags related to sensor specification ranges; these are instances where
# a value exceeds the manufacturer specified limits.

#' @title Add a flag if the value of a parameter exceeds its sensor specification range.
#'
#' @description
#' A function designed to append the 'outside of sensor specification range' flag to
#' a row if the `mean` column in that row exceeds the sensor specification ranges that are
#' set in the `src/qaqc/sensor_spec_thresholds.yml` file. These are instances
#' where a value exceeds the expected ranges for the Poudre based on the sensor
#' manufacturer's specifications.
#'
#' @param df A data frame with a `flag` column.
#' @param spec_table file containing In-Situ spec ranges for each sensor
#'
#' @return A data frame with a `flag` column that has been updated with the
#' 'outside of sensor specification range' flag.
#'
#' @examples
#' add_spec_flag(df = all_data_flagged$`archery-Actual Conductivity`)
#' add_spec_flag(df = all_data_flagged$`boxelder-Temperature`)
#'
#' @seealso [flag_all_data()]

add_spec_flag <- function(df, spec_table = yaml::read_yaml("data/qaqc/sensor_spec_thresholds.yml")){

  # make this a non yaml solution and add it to the threshold table
  # get the parameter from the parameter column in the df of interest
  parameter_name <- unique(na.omit(df$parameter))
  # Pull the sensor specification range from the yaml file
  sensor_min <- eval(parse(text = spec_table[[parameter_name]]$min))
  sensor_max <- eval(parse(text = spec_table[[parameter_name]]$max))

  df <- df %>%
    # adding sensor range flags
    add_flag(parameter == parameter_name & (mean < sensor_min | mean > sensor_max) & !grepl("outside of sensor specification range", flag),
             "outside of sensor specification range") %>%

    return(df)

}
