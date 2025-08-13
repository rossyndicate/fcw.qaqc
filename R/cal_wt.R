#' @title Temporal Weight Factor Calculation
#'
#' @description
#' Calculates temporal weight factors for linear interpolation between consecutive
#' calibrations. This function creates a weight column that assigns values from 0
#' (earliest time) to 1 (latest time) within each calibration window, enabling
#' time-weighted transitions between calibration parameters. Inspired by the
#' driftR package's dr_factor function.
#'
#' @param df Tibble containing sensor data bounded by two calibrations
#' @param dt_col Character string specifying the column name containing POSIXct
#'   datetime information
#' @param wt_col Character string specifying the name for the created weight
#'   column (default: "wt")
#'
#' @seealso [cal_lin_trans_lm()]
#' @seealso [cal_one_point_drift()]
#' @seealso [cal_two_point_drift()]
#' @seealso [cal_three_point_drift_pH()]

cal_wt <- function(df, dt_col, wt_col = "wt"){

  # Extract temporal boundaries for the calibration window
  first_dt <- min(df[[dt_col]])
  last_dt <- max(df[[dt_col]])
  tot_time <- as.numeric(difftime(last_dt, first_dt, units = "mins"))

  # Calculate temporal weight factors (0 to 1) for linear interpolation
  df <- df %>%
    mutate(!!wt_col := as.numeric(difftime(.data[[dt_col]], first_dt, units = "mins"))/tot_time)

  return(df)
}
