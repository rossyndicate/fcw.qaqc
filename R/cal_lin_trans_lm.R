#' @title Linear Transition Between Linear Models
#' @export
#'
#' @description
#' Applies temporally-weighted linear transformation between two consecutive
#' calibrations. This function interpolates between the slope and offset
#' parameters from calibration 1 (earliest) and calibration 2 (latest) using
#' temporal weights to create a smooth transition across the calibration window.
#' The transformation accounts for gradual sensor drift by blending calibration
#' parameters based on time position.
#'
#' @param df Tibble containing sensor data bounded by two calibrations
#' @param raw_col Character string specifying the column name containing raw
#'   observation values from the inverse linear model transformation
#' @param lm_coefs_col Character string specifying the column name containing
#'   linear model coefficients
#' @param wt_col Character string specifying the column name containing temporal
#'   weight parameters
#'
#' @seealso [cal_wt()]
#' @seealso [cal_inv_lm()]
#' @seealso [cal_one_point_drift()]
#' @seealso [cal_two_point_drift()]

cal_lin_trans_lm <- function(df, raw_col, lm_coefs_col, wt_col){

  # Create output column name for linearly transformed values
  transformed_col <- paste0(str_split_1(raw_col, "_")[1], "_lm_trans")

  # Extract calibration coefficients from bounding calibrations
  calibration_1 <- df[[lm_coefs_col]][[1]]
  calibration_2 <- df[[lm_coefs_col]][[nrow(df)]]

  # Handle missing calibration data
  cal_1_check <- (is.data.frame(calibration_1) && nrow(calibration_1) != 0)
  cal_2_check <- (is.data.frame(calibration_2) && nrow(calibration_2) != 0)
  if (!cal_1_check | !cal_2_check){
    df <- df %>%
      mutate(!!transformed_col := NA_integer_)
    return(df)
  }

  # Extract parameters from first calibration (earliest)
  slope_1 <- as.numeric(calibration_1 %>% pull(slope))
  offset_1 <- as.numeric(calibration_1 %>% pull(offset))

  # Extract parameters from second calibration (latest)
  slope_2 <- as.numeric(calibration_2 %>% pull(slope))
  offset_2 <- as.numeric(calibration_2 %>% pull(offset))

  # Calculate parameter differences for temporal interpolation
  slope_delta <- slope_1 - slope_2
  offset_delta <- offset_1 - offset_2

  # Apply temporally-weighted linear model: y = (m_1-wt(m_1-m_2))x+(b_1-wt(b_1-b_2))
  df <- df %>%
    mutate(!!transformed_col := ((slope_1-(.data[[wt_col]]*slope_delta))*.data[[raw_col]])+(offset_1-(.data[[wt_col]]*offset_delta)))

  return(df)
}
