#' @title One Point Drift Calibration
#'
#' @description
#' Applies single-point drift correction using pre- and post-measurement
#' calibration standards. This function corrects for sensor drift by calculating
#' the difference between expected and observed standard values, then applying a
#' temporally-weighted correction across the calibration window. Used primarily
#' for Chlorophyll-a, FDOM, ORP, Pressure, Specific Conductivity, and RDO
#' sensors that require single-point drift correction.
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

cal_one_point_drift <- function(df, lm_trans_col, drift_corr_col, wt_col){

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

  # Extract calibration standard values for drift calculation
  expected_standard <- as.numeric(drift_back_calibration %>% pull(post_measurement))
  observed_standard <- as.numeric(drift_back_calibration %>% pull(pre_measurement))

  # Calculate drift offset between expected and observed standards
  standard_delta <- expected_standard - observed_standard

  # Apply temporally-weighted drift correction: C_t = m_t + w_t(delta_S)
  df <- df %>%
    mutate(!!transformed_col := .data[[lm_trans_col]] + (.data[[wt_col]]*standard_delta))

  return(df)
}
