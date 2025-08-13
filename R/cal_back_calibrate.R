#' @title Back Calibration Orchestrator
#'
#' @description
#' Orchestrates the complete back calibration workflow for sensor data chunks
#' bounded by two calibrations. This function determines the appropriate
#' calibration pathway based on sensor parameter type and applies the
#' corresponding sequence of correction functions. Standard sensors follow a
#' five-step process, turbidity uses two-point drift correction, and pH sensors
#' require specialized three-point processing with unit conversions.
#'
#' @param prepped_snsr_cal_df Tibble containing prepared sensor calibration data
#'   chunk from prepare_sensor_calibration_data(), bounded by two calibrations
#'
#' @seealso [cal_wt()]
#' @seealso [cal_inv_lm()]
#' @seealso [cal_lin_trans_lm()]
#' @seealso [cal_one_point_drift()]
#' @seealso [cal_two_point_drift()]
#' @seealso [cal_lm_pH()]
#' @seealso [cal_lin_trans_inv_lm_pH()]
#' @seealso [cal_three_point_drift_pH()]
#' @seealso [cal_check()]

cal_back_calibrate <- function(prepped_snsr_cal_df) {

  # Identify sensor parameter type for calibration pathway selection
  parameter <- unique(prepped_snsr_cal_df$parameter)

  # Standard sensor calibration pathway (five-step process)
  if (parameter %in% c("Chl-a Fluorescence", "FDOM Fluorescence", "ORP", "Pressure", "Specific Conductivity", "RDO")) {
    back_calibrated_chunk <- prepped_snsr_cal_df %>%
        # Calculate temporal weights for calibration interpolation
        cal_wt(df = ., dt_col = "DT_round") %>%
        # Convert observed values back to raw instrumental units
        cal_inv_lm(df = ., obs_col = "mean", lm_coefs_col = "calibration_coefs") %>%
        # Apply temporally-weighted linear transformation between calibrations
        cal_lin_trans_lm(df = ., raw_col = "mean_raw", lm_coefs_col = "calibration_coefs", wt_col = "wt") %>%
        # Apply single-point drift correction
        cal_one_point_drift(df = ., lm_trans_col = "mean_lm_trans", drift_corr_col = "drift_input", wt_col = "wt") %>%
        # Validate calibration results and create final calibrated values
        cal_check(df = ., obs_col = "mean", lm_trans_col = "mean_lm_trans")
    return(head(back_calibrated_chunk, -1))
  }

  # Turbidity sensor calibration pathway (two-point drift correction)
  if (parameter == "Turbidity") {
    back_calibrated_chunk <- prepped_snsr_cal_df %>%
      # Calculate temporal weights for calibration interpolation
      cal_wt(df = ., dt_col = "DT_round") %>%
      # Convert observed values back to raw instrumental units
      cal_inv_lm(df = ., obs_col = "mean", lm_coefs_col = "calibration_coefs") %>%
      # Apply temporally-weighted linear transformation between calibrations
      cal_lin_trans_lm(df = ., raw_col = "mean_raw", lm_coefs_col = "calibration_coefs", wt_col = "wt") %>%
      # Apply two-point drift correction for turbidity-specific drift patterns
      cal_two_point_drift(df = ., lm_trans_col = "mean_lm_trans", drift_corr_col = "drift_input", wt_col = "wt") %>%
      # Validate calibration results and create final calibrated values
      cal_check(df = ., obs_col = "mean", lm_trans_col = "mean_lm_trans")
    return(head(back_calibrated_chunk, -1))
  }

  # pH sensor calibration pathway (specialized three-point processing)
  # pH calibration requires special handling due to:
  # - Slopes in mV/pH units and offsets in mV units (not direct pH units)
  # - Three-point calibration creating two slope transitions
  # - Need for unit conversion from pH to mV for back-calibration
  # - Three-point drift correction with piecewise linear interpolation
  if (parameter == "pH"){
    back_calibrated_chunk <- prepped_snsr_cal_df %>%
      # Calculate temporal weights for calibration interpolation
      cal_wt(df = ., dt_col = "DT_round") %>%
      # Convert pH values to millivolt units for back-calibration processing
      cal_lm_pH(df = ., obs_col = "mean", lm_coefs_col = "calibration_coefs") %>%
      # Apply temporally-weighted inverse transformation back to pH units
      cal_lin_trans_inv_lm_pH(df = ., obs_col = "mean", mv_col = "mean_mV_f", lm_coefs_col = "calibration_coefs", wt_col = "wt") %>%
      # Apply three-point drift correction with piecewise linear interpolation
      cal_three_point_drift_pH(df = ., obs_col = "mean", lm_trans_col = "mean_pH_f", drift_corr_col = "drift_input", wt_col = "wt") %>%
      # Validate calibration results and create final calibrated values
      cal_check(df = ., obs_col = "mean", lm_trans_col = "mean_pH_f")
    return(head(back_calibrated_chunk, -1))
  }

  # Return NULL result if parameter type is not recognized
  return(NULL)
}
