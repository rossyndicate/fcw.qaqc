#' @title Calibration Validation and Final Value Selection
#' @export
#'
#' @description
#' Validates calibration results and creates final calibrated values by
#' comparing transformed data against original observations. This function
#' prioritizes linearly transformed values over drift-corrected values and
#' falls back to original observations when calibration fails. Creates a
#' calibration success flag for quality control purposes.
#'
#' @param df Tibble containing sensor data with calibration transformations
#' @param obs_col Character string specifying the column name containing
#'   original sensor observations
#' @param lm_trans_col Character string specifying the column name containing
#'   linearly transformed calibration values (excludes drift correction)
#'
#' @seealso [cal_lin_trans_lm()]
#' @seealso [cal_one_point_drift()]
#' @seealso [cal_two_point_drift()]
#' @seealso [cal_three_point_drift_pH()]

cal_check <- function(df, obs_col, lm_trans_col) {

  # Create output column name for final calibrated values
  transformed_col <- paste0(obs_col, "_cal")

  # Check if all linearly transformed values are missing (calibration failure)
  all_na <- all(is.na(df[[lm_trans_col]]))

  # Create final calibrated values and quality control flag
  df <- df %>%
    mutate(
      # Create calibration success flag for quality control
      cal_check = ifelse(!all_na, TRUE, FALSE),
      # Use linearly transformed values if available, otherwise fall back to original observations
      !!transformed_col := ifelse(cal_check, .data[[lm_trans_col]], .data[[obs_col]])
    ) %>%
    relocate(cal_check, .after = !!transformed_col)

  return(df)
}
