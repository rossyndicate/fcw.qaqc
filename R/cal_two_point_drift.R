#' @title Two Point Drift Calibration
#'
#' @description
#' Applies two-point drift correction using low and high calibration standards.
#' This function corrects for sensor drift by calculating linear interpolation
#' between two calibration points, accounting for drift in both standards across
#' the calibration window. Used specifically for turbidity sensors that require
#' two-point drift correction to handle non-linear drift patterns across the
#' measurement range.
#'
#' @param df Tibble containing sensor data bounded by two calibrations
#' @param lm_trans_col Character string specifying the column name containing
#'   linearly transformed data from cal_lin_trans_lm()
#' @param drift_corr_col Character string specifying the column name containing
#'   drift correction information
#' @param wt_col Character string specifying the column name containing temporal
#'   weight parameters
#'
#' @seealso [cal_wt()]
#' @seealso [cal_lin_trans_lm()]
#' @seealso [cal_check()]

cal_two_point_drift <- function(df, lm_trans_col, drift_corr_col, wt_col){

  # Create output column name for drift-corrected values
  transformed_col <- paste0(str_split_1(lm_trans_col, "_")[1], "_drift_trans")

  # Extract drift calibration data from the latest calibration
  drift_back_calibration <- df[[drift_corr_col]][[nrow(df)]]

  # Handle missing calibration data
  if (!is.data.frame(drift_back_calibration) || nrow(drift_back_calibration) == 0) {
    df <- df %>%
      mutate(!!transformed_col := NA_integer_)
    return(df)
  }

  # Extract expected values for calibration standards
  s_e <- drift_back_calibration %>% pull(post_measurement)
  a_e <- as.numeric(s_e[1]) # Low standard expected
  b_e <- as.numeric(s_e[2]) # High standard expected

  # Calculate expected range between standards
  delta_e <- b_e - a_e

  # Extract observed values for calibration standards
  s_o <- drift_back_calibration %>% pull(pre_measurement)
  a_o <- as.numeric(s_o[1])  # Low standard observed
  b_o <- as.numeric(s_o[2])  # High standard observed

  # Calculate drift offsets for each standard
  delta_a <- a_e - a_o  # Low standard drift
  delta_b <- b_e - b_o  # High standard drift

  # Apply temporally-weighted two-point drift correction
  df <- df %>%
    mutate(
      # Calculate temporally-adjusted standard values
      a_t = a_e - (.data[[wt_col]] * delta_a),  # Weighted low standard
      b_t = b_e - (.data[[wt_col]] * delta_b),  # Weighted high standard
      # Apply linear interpolation drift correction: C_t = ((m_t-a_t)/(b_t-a_t))(b_e-a_e)+a_e
      !!transformed_col := (((.data[[lm_trans_col]] - a_t) / (b_t - a_t)) * delta_e) + a_e
    ) %>%
    select(-c(a_t, b_t))  # Remove intermediate calculation columns

  return(df)
}
