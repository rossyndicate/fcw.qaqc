#' @title Join Sensor and Calibration Data
#'
#' @description
#' Joins sensor time series data with calibration information using temporal
#' proximity matching. This function links each sensor measurement to the most
#' recent applicable calibration by finding the closest calibration date that
#' precedes or equals each sensor observation timestamp. Processes data
#' hierarchically by year and site-parameter combinations to maintain data
#' organization structure.
#'
#' @param sensor_data_list Nested list containing sensor time series data
#'   organized by year and site-parameter combinations
#' @param calibration_data_list Nested list containing calibration data
#'   organized by year and site-parameter combinations from
#'   load_calibration_data()
#'
#' @seealso [load_calibration_data()]
#' @seealso [prepare_sensor_calibration_data()]

# TODO: Fix the join so that it joins on site visits
cal_join_sensor_calibration_data <- function(sensor_data_list, calibration_data_list) {

  # Identify overlapping years between sensor and calibration data
  years <- intersect(names(sensor_data_list), names(calibration_data_list))

  # Process each year of data
  year_data_list <- years %>%
    map(function(year){

      # Extract sensor and calibration data for current year
      year_sensor_data <- sensor_data_list[[year]]
      year_calibration_data <- calibration_data_list[[year]]

      # Identify overlapping site-parameter combinations within the year
      site_params <- intersect(names(sensor_data_list[[year]]), names(calibration_data_list[[year]]))

      # Process each site-parameter combination
      site_param_df_list <- site_params %>%
        map(function(site_param){

          # Extract data for current site-parameter combination
          sensor_data <- year_sensor_data[[site_param]]
          calibration_data <- select(year_calibration_data[[site_param]], -sensor)

          # Join calibration data to sensor data using temporal proximity matching
          # For each sensor timestamp, find the most recent calibration that
          # precedes or equals the sensor measurement time
          by <- join_by(site, closest(DT_round >= file_date))
          joined_sensor_calibration_data <- left_join(sensor_data, calibration_data, by) %>%
            arrange(DT_round)

          return(joined_sensor_calibration_data)
        }) %>%
        set_names(site_params)

      return(site_param_df_list)
    }) %>%
    set_names(years)

  return(year_data_list)
}
