#' @title pH Linear Model Transformation
#' @export
#'
#' @description
#' Converts pH observations to millivolt units using two-segment linear models
#' from three-point pH calibration. This function applies forward linear
#' transformation (pH to mV) to enable subsequent back-calibration processing
#' in the correct instrumental units. Uses pH value thresholds to select
#' between low-range and high-range linear segments based on the sensor's
#' three-point calibration characteristics.
#'
#' @param df Tibble containing sensor data bounded by two calibrations
#' @param obs_col Character string specifying the column name containing pH
#'   observations recorded in HydroVu
#' @param lm_coefs_col Character string specifying the column name containing
#'   linear model coefficients with two slope/offset pairs
#'
#' @seealso [cal_wt()]
#' @seealso [cal_lin_trans_inv_lm_pH()]
#' @seealso [cal_three_point_drift_pH()]

cal_lm_pH <- function(df, obs_col, lm_coefs_col) {

  # Create output column names for pH to mV conversion
  mv_f <- paste0(obs_col, "_mV_f")

  # Extract calibration coefficients from first calibration
  calibration_1 <- df[[lm_coefs_col]][[1]]

  # Handle missing calibration data
  if (!is.data.frame(calibration_1) || nrow(calibration_1) == 0) {
    df <- df %>%
      mutate(!!mv_f := NA_integer_)
    return(df)
  }

  # Extract slope and offset parameters for two-segment calibration
  slopes <- calibration_1 %>% pull(slope)
  offsets <- calibration_1 %>% pull(offset)

  slope_1 <- as.numeric(slopes[1])   # Low pH range slope (mV/pH)
  offset_1 <- as.numeric(offsets[1]) # Low pH range offset (mV)

  slope_2 <- as.numeric(slopes[2])   # High pH range slope (mV/pH)
  offset_2 <- as.numeric(offsets[2]) # High pH range offset (mV)

  # Convert pH to millivolts using two-segment linear models
  df <- df %>%
    mutate(
      # Apply linear transformation for low pH range: y = mx + b
      mv_1 = (slope_1 * .data[[obs_col]]) + offset_1,
      # Apply linear transformation for high pH range: y = mx + b
      mv_2 = (slope_2 * .data[[obs_col]]) + offset_2,
      # Select appropriate mV value based on pH threshold (neutral = 7)
      !!mv_f := ifelse(.data[[obs_col]] >= 7, mv_2, mv_1)
    ) %>%
    select(-c(mv_1, mv_2))  # Remove intermediate calculation columns

  return(df)
}
