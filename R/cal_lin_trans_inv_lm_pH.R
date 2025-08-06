#' @title Linear Transition Between Inverse Linear Models (pH Specific)
#'
#' @description
#' Converts millivolt values back to pH units using temporally-weighted inverse
#' linear transformation between two consecutive calibrations. This function
#' applies two-segment inverse linear models with temporal interpolation to
#' account for sensor drift across the calibration window. Uses pH value
#' thresholds to select between low-range and high-range inverse transformations
#' based on the original pH observations.
#'
#' @param df Tibble containing sensor data bounded by two calibrations
#' @param obs_col Character string specifying the column name containing
#'   original pH observations for range selection logic (default: "mean")
#' @param mv_col Character string specifying the column name containing
#'   millivolt values from pH linear model transformation (default: "mean_mV_f")
#' @param lm_coefs_col Character string specifying the column name containing
#'   linear model coefficients (default: "calibration_coefs")
#' @param wt_col Character string specifying the column name containing temporal
#'   weight parameters (default: "wt")
#'
#' @seealso [cal_wt()]
#' @seealso [cal_lm_pH()]
#' @seealso [cal_three_point_drift_pH()]

cal_lin_trans_inv_lm_pH <- function(df = ., obs_col = "mean", mv_col = "mean_mV_f", lm_coefs_col = "calibration_coefs", wt_col = "wt") {

  # Create output column names for pH conversion
  ph_f <- paste0(str_split_1(mv_col, "_")[1], "_pH_f")

  # Extract calibration coefficients from bounding calibrations
  calibration_1 <- df[[lm_coefs_col]][[1]]
  calibration_2 <- df[[lm_coefs_col]][[nrow(df)]]

  # Handle missing calibration data
  cal_1_check <- (is.data.frame(calibration_1) && nrow(calibration_1) != 0)
  cal_2_check <- (is.data.frame(calibration_2) && nrow(calibration_2) != 0)
  if (!cal_1_check | !cal_2_check){
    df <- df %>%
      mutate(!!ph_f := NA_integer_)
    return(df)
  }

  # Extract parameters from first calibration (earliest)
  cal_1_slopes <- calibration_1 %>% pull(slope)
  cal_1_offsets <- calibration_1 %>% pull(offset)

  cal_1_slope_1 <- as.numeric(cal_1_slopes[1])   # Low pH range slope
  cal_1_offset_1 <- as.numeric(cal_1_offsets[1]) # Low pH range offset
  cal_1_slope_2 <- as.numeric(cal_1_slopes[2])   # High pH range slope
  cal_1_offset_2 <- as.numeric(cal_1_offsets[2]) # High pH range offset

  # Extract parameters from second calibration (latest)
  cal_2_slopes <- calibration_2 %>% pull(slope)
  cal_2_offsets <- calibration_2 %>% pull(offset)

  cal_2_slope_1 <- as.numeric(cal_2_slopes[1])   # Low pH range slope
  cal_2_offset_1 <- as.numeric(cal_2_offsets[1]) # Low pH range offset
  cal_2_slope_2 <- as.numeric(cal_2_slopes[2])   # High pH range slope
  cal_2_offset_2 <- as.numeric(cal_2_offsets[2]) # High pH range offset

  # Calculate parameter differences for temporal interpolation
  slope_1_delta <- cal_1_slope_1 - cal_2_slope_1   # Low range slope difference
  offset_1_delta <- cal_1_offset_1 - cal_2_offset_1 # Low range offset difference
  slope_2_delta <- cal_1_slope_2 - cal_2_slope_2   # High range slope difference
  offset_2_delta <- cal_1_offset_2 - cal_2_offset_2 # High range offset difference

  # Convert millivolts back to pH using temporally-weighted inverse transformation
  df <- df %>%
    mutate(
      # Inverse transformation for low pH range: x = (y-(b_1-wt(b_1-b_2)))/(m_1-wt(m_1-m_2))
      ph_1 = (.data[[mv_col]] - (cal_1_offset_1 - (.data[[wt_col]] * offset_1_delta))) /
        (cal_1_slope_1 - (.data[[wt_col]] * slope_1_delta)),
      # Inverse transformation for high pH range: x = (y-(b_1-wt(b_1-b_2)))/(m_1-wt(m_1-m_2))
      ph_2 = (.data[[mv_col]] - (cal_1_offset_2 - (.data[[wt_col]] * offset_2_delta))) /
        (cal_1_slope_2 - (.data[[wt_col]] * slope_2_delta)),
      # Select appropriate pH value based on original pH threshold (neutral = 7)
      !!ph_f := ifelse(.data[[obs_col]] >= 7, ph_2, ph_1)
    ) %>%
    select(-c(ph_1, ph_2))  # Remove intermediate calculation columns

  return(df)
}
