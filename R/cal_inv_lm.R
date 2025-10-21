#' @title Inverse Linear Model Transformation
#' @export
#'
#' @description
#' Converts sensor observations back to raw instrumental values by applying the
#' inverse of the linear calibration model. This function reverses the
#' slope/offset transformation applied by the sensor to recover the original raw
#' measurements, enabling subsequent re-calibration with updated parameters.
#'
#' @param df Tibble containing sensor data bounded by two calibrations
#' @param obs_col Character string specifying the column name containing
#'   observations recorded in HydroVu
#' @param lm_coefs_col Character string specifying the column name containing
#'   linear model coefficients
#'
#' @seealso [cal_wt()]
#' @seealso [cal_lin_trans_lm()]

cal_inv_lm <- function(df, obs_col, lm_coefs_col){

  # Create output column name for raw values
  raw_col <- paste0(obs_col, "_raw")

  # Extract calibration coefficients from first calibration
  calibration_1 <- df[[lm_coefs_col]][[1]]

  # Handle missing calibration data
  if (!is.data.frame(calibration_1) || nrow(calibration_1) == 0) {
    df <- df %>%
      mutate(!!raw_col := NA_integer_)
    return(df)
  }

  # Extract slope and offset parameters for inverse transformation
  slope_1 <- as.numeric(calibration_1 %>% pull(slope))
  offset_1 <- as.numeric(calibration_1 %>% pull(offset))

  # Apply inverse linear model transformation: x = (y - b) / m
  df <- df %>%
    mutate(!!raw_col := (.data[[obs_col]] - offset_1) / slope_1)

  return(df)
}
