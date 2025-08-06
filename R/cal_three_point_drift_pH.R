#' @title Three Point Drift Calibration (pH Specific)
#'
#' @description
#' Applies three-point drift correction using low, medium, and high pH calibration
#' buffers. This function corrects for sensor drift by performing piecewise linear
#' interpolation across three calibration points, accounting for non-linear drift
#' patterns across the pH measurement range. Used specifically for pH sensors that
#' require three-point calibration due to their multi-segment response
#' characteristics.
#'
#' @param df Tibble containing sensor data bounded by two calibrations
#' @param obs_col Character string specifying the column name containing original
#'   pH observations for drift selection logic
#' @param lm_trans_col Character string specifying the column name containing
#'   linearly transformed data from pH-specific transformation functions
#' @param drift_corr_col Character string specifying the column name containing
#'   drift correction information
#' @param wt_col Character string specifying the column name containing temporal
#'   weight parameters
#'
#' @seealso [cal_wt()]
#' @seealso [cal_lm_pH()]
#' @seealso [cal_lin_trans_inv_lm_pH()]
#' @seealso [cal_check()]

cal_three_point_drift_pH <- function(df, obs_col, lm_trans_col, drift_corr_col, wt_col) {

  # Create output column names for drift calculations
  drift_f <- paste0(str_split_1(lm_trans_col, "_")[1], "_drift_f")

  # Extract drift calibration data from the latest calibration
  drift_back_calibration <- df[[drift_corr_col]][[nrow(df)]]

  # Handle missing drift calibration data
  if (!is.data.frame(drift_back_calibration) || nrow(drift_back_calibration) == 0){
    df <- df %>%
      mutate(!!drift_f := NA_integer_)
    return(df)
  }

  # Filter drift data to relevant columns
  drift_back_calibration <- drift_back_calibration %>%
    select(point, type, p_h)

  # Extract expected values for the three pH standards (type == 2)
  expected_standards <- drift_back_calibration %>%
    group_by(point) %>%
    filter(type == 2) %>%
    pull(p_h)

  a_e <- as.numeric(expected_standards[1])  # Low pH standard expected
  b_e <- as.numeric(expected_standards[2])  # Medium pH standard expected
  c_e <- as.numeric(expected_standards[3])  # High pH standard expected

  # Calculate expected ranges between standards
  delta_e_ab <- b_e - a_e  # Low to medium range
  delta_e_bc <- c_e - b_e  # Medium to high range

  # Extract observed values for the three pH standards (type == 1)
  observed_standards <- drift_back_calibration %>%
    group_by(point) %>%
    filter(type == 1) %>%
    pull(p_h)

  a_o <- as.numeric(observed_standards[1])  # Low pH standard observed
  b_o <- as.numeric(observed_standards[2])  # Medium pH standard observed
  c_o <- as.numeric(observed_standards[3])  # High pH standard observed

  # Calculate drift offsets for each standard
  delta_a <- a_e - a_o  # Low standard drift
  delta_b <- b_e - b_o  # Medium standard drift
  delta_c <- c_e - c_o  # High standard drift

  # Apply temporally-weighted three-point drift correction
  df <- df %>%
    mutate(
      # Calculate temporally-adjusted standard values
      a_t = a_e + (.data[[wt_col]] * delta_a),  # Weighted low standard
      b_t = b_e + (.data[[wt_col]] * delta_b),  # Weighted medium standard
      c_t = c_e + (.data[[wt_col]] * delta_c),  # Weighted high standard
      # Calculate piecewise linear corrections for each pH range
      drift_1 = ((.data[[lm_trans_col]] - a_t) / (b_t - a_t)) * (b_e - a_e) + a_e,  # Low-medium range
      drift_2 = ((.data[[lm_trans_col]] - b_t) / (c_t - b_t)) * (c_e - b_e) + b_e,  # Medium-high range
      # Select appropriate correction based on original pH value
      !!drift_f := ifelse(.data[[obs_col]] >= 7, drift_2, drift_1)
    ) %>%
    select(-c(a_t, b_t, c_t, drift_1, drift_2))  # Remove intermediate columns

  return(df)
}
