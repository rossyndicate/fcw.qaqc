#' @title Prepare Sensor Calibration Data for Back Calibration
#' @export
#'
#' @description
#' Segments joined sensor-calibration data into calibration windows bounded by
#' consecutive calibrations. This function creates data chunks where each chunk
#' contains sensor data from one calibration period plus the first row from the
#' next calibration, enabling temporal interpolation between calibration
#' parameters. Processes data hierarchically by year and site-parameter
#' combinations to maintain organization structure.
#'
#' @param sensor_calibration_data_list Nested list containing joined sensor and
#'   calibration data from join_sensor_calibration_data(), organized by year
#'   and site-parameter combinations
#'
#' @seealso [join_sensor_calibration_data()]
#' @seealso [back_calibrate()]

cal_prepare_sensor_calibration_data <- function(sensor_calibration_data_list) {

  # Process each year of joined sensor-calibration data
  prepped_yearly_site_param_chunks <- sensor_calibration_data_list %>%
    map(function(year){ # Iterate over each year's site-parameter list

      # Process each site-parameter combination within the year
      prepped_yearly_site_param_chunks <- year %>%
        map(function(site_param_df){

          # Split data by calibration dates to create calibration periods
          calibration_chunks <- site_param_df %>%
            group_by(file_date) %>%
            group_split() %>%
            discard(~all(is.na(.x$sonde_serial)))  # Remove chunks with missing sonde data

          # Handle single calibration case (no temporal interpolation needed)
          if(length(calibration_chunks) == 1) return(calibration_chunks)

          # Create calibration windows by pairing consecutive calibrations
          chunks <- c(1:(length(calibration_chunks) - 1))
          prepped_calibration_chunks <- chunks %>%
            map(function(chunk) {
              # Get current calibration period data
              cal_1 <- calibration_chunks[[chunk]]
              # Get first row of next calibration for temporal boundary
              cal_2 <- slice(calibration_chunks[[chunk + 1]], 1)
              # Combine to create calibration window bounded by two calibrations
              prepped_chunk <- bind_rows(cal_1, cal_2)
              return(prepped_chunk)
            })

          return(prepped_calibration_chunks)
        })
    })

  # Return nested structure: year -> site-parameter -> calibration chunks
  # Each chunk contains sensor data bounded by two consecutive calibrations
  # enabling temporal interpolation of calibration parameters
  return(prepped_yearly_site_param_chunks)
}
