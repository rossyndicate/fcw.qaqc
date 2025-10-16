#' @title Load Calibration Data
#' @export
#'
#' @description
#' Loads or generates calibration coefficient and drift correction data from
#' HTML calibration files. This function manages calibration data by either
#' loading existing processed data or extracting fresh data from source HTML
#' files. Creates a global calibration_data object organized by year and
#' site-parameter combinations for use throughout the calibration workflow.
#'
#' @param cal_data_file_path Character string specifying the file path for
#'   saved calibration data RDS file (default: munged_calibration_data.RDS
#'   in data/calibration_reports/0_cal_data_munge/)
#' @param update Logical flag indicating whether to regenerate calibration data
#'   from source HTML files (default: FALSE)
#' @param ... Additional arguments passed to underlying calibration extraction
#'   functions
#'
#' @seealso [cal_extract_markup_data()]
#' @seealso [join_sensor_calibration_data()]

load_calibration_data <- function(cal_data_file_path = here("data", "calibration_reports", "0_cal_data_munge", "munged_calibration_data.RDS"),
                                  update = FALSE, ...) {

  # Input validation
  if (!is.character(cal_data_file_path) || length(cal_data_file_path) != 1) {
    stop("cal_data_file_path must be a single character string")
  }

  if (!is.logical(update) || length(update) != 1) {
    stop("update must be a single logical value (TRUE or FALSE)")
  }

  # Check if directory exists, create if needed
  cal_data_dir <- dirname(cal_data_file_path)
  if (!dir.exists(cal_data_dir)) {
    message("Creating directory: ", cal_data_dir)
    dir.create(cal_data_dir, recursive = TRUE, showWarnings = FALSE)
  }

  # Check if file exists
  file_exists <- file.exists(cal_data_file_path)

  if (file_exists && !update) {
    # Load existing calibration data (typical usage)
    message("Calibration data exists! Loading calibration_data...")

    tryCatch({
      calibration_data <<- readr::read_rds(cal_data_file_path)
      message("Successfully loaded calibration data from: ", cal_data_file_path)
    }, error = function(e) {
      stop("Failed to load calibration data from ", cal_data_file_path,
           ". Error: ", e$message)
    })

  } else if (update || !file_exists) {
    # Generate calibration data (either forced update or file doesn't exist)
    if (!file_exists) {
      message("Calibration data does not exist!")
    }
    message("Generating the calibration data, this may take a second...")

    # Check if cal_extract_markup_data function exists
    if (!exists("cal_extract_markup_data", mode = "function")) {
      stop("cal_extract_markup_data function not found. Please ensure all calibration functions are sourced.")
    }

    tryCatch({
      calibration_data <<- cal_extract_markup_data(...)

      # Validate generated data
      if (is.null(calibration_data) || length(calibration_data) == 0) {
        stop("cal_extract_markup_data returned empty or NULL data")
      }

      message("Successfully generated calibration data")

    }, error = function(e) {
      stop("Failed to generate calibration data. Error: ", e$message)
    })

    # Save data if update was requested or file didn't exist
    if (update || !file_exists) {
      tryCatch({
        readr::write_rds(calibration_data, cal_data_file_path)
        message("Calibration data saved to: ", cal_data_file_path)
      }, error = function(e) {
        warning("Generated calibration data successfully but failed to save to ",
                cal_data_file_path, ". Error: ", e$message)
      })
    } else {
      message("Generated calibration data not saved (update = FALSE)")
    }
  }

  # Final validation that calibration_data exists in global environment
  if (!exists("calibration_data", envir = .GlobalEnv)) {
    stop("Failed to create calibration_data object in global environment")
  }

  message("Calibration data ready for use")
}
