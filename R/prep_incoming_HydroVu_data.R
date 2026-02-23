#' @title Process and summarize raw API data for water quality monitoring workflow
#' @export
#'
#' @description
#' Reads raw HydroVu API parquet files from either Azure Data Lake Storage or
#' local storage, standardizes the data, then transforms each site-parameter
#' combination into a consistent time series with summary statistics. This
#' function combines the former munge and tidy steps into a single pipeline:
#' raw files are ingested and cleaned first, then each site-parameter
#' combination is summarized into evenly-spaced intervals with mean values,
#' data spread, and observation counts. Missing intervals are filled with NA
#' to ensure a complete time series for downstream quality control.
#'
#' @param api_dir Character string specifying the directory path containing the
#' raw parquet files downloaded from the HydroVu API.
#'
#' @param summarize_interval Character string specifying the time interval to
#' round and aggregate timestamps to. Default is "15 minutes". Accepts any
#' interval format compatible with lubridate::round_date() and padr::pad()
#' (e.g., "1 hour", "30 minutes").
#'
#' @param synapse_env Logical, whether to read files from Azure Data Lake
#' Storage via a Synapse environment. Default is FALSE.
#'
#' @param fs AzureStor filesystem object required when \code{synapse_env = TRUE}.
#' Ignored otherwise.
#'
#' @return A named list of dataframes, one per site-parameter combination
#' (named "site-parameter"), each containing one row per time interval with:
#' - DT_round: Rounded timestamp defining each time interval
#' - site: Standardized site name (lowercase, no spaces)
#' - parameter: Measurement type (e.g., "Temperature", "DO")
#' - units: Measurement units (e.g., "°C", "mg/L")
#' - mean: Average value for the interval
#' - spread: Range of values within the interval (max - min)
#' - n_obs: Number of observations in each interval
#' - DT_join: Character representation of DT_round for joining operations
#' - flag: Empty column for subsequent quality control flagging
#'
#' Site-parameter combinations that fail during the tidy step return NULL and
#' are excluded from the final output with an informative message.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [api_HydroVu_puller()]
#' @seealso [prep_bind_datasets()]

prep_incoming_HydroVu_data <- function(api_dir, summarize_interval = "15 minutes",
                           synapse_env = FALSE, fs = NULL) {

  # Standardize "minutes" to "mins" for compatibility with padr::pad()
  pad_interval <- if (grepl("minutes", summarize_interval, ignore.case = TRUE)) {
    gsub("minutes", "mins", summarize_interval, ignore.case = TRUE)
  } else {
    summarize_interval
  }

  # --- Ingest raw parquet files ---

  if (synapse_env) {

    # Get list of all files in the Azure Data Lake Storage (ADLS) directory
    file_list <- AzureStor::list_adls_files(fs, api_dir, info = "name")

    # Read each file from ADLS into a combined dataframe
    api_data <- purrr::map_dfr(file_list, function(adls_path) {
      temp_file <- tempfile(fileext = ".parquet")
      AzureStor::download_adls_file(fs, adls_path, temp_file)
      arrow::read_parquet(temp_file, as_data_frame = TRUE)
    })

  } else {

    # Read each local parquet file into a combined dataframe
    api_data <- purrr::map_dfr(list.files(api_dir, full.names = TRUE),
                               function(file_path) {
                                 arrow::read_parquet(file_path, as_data_frame = TRUE)
                               })

  }

  # --- Standardize and clean the combined raw data ---

  api_data <- api_data %>%
    dplyr::select(-id) %>%
    dplyr::mutate(units = as.character(units)) %>%
    # Filter out VuLink and Virridy data (not used in CSU/FCW networks)
    dplyr::filter(!grepl("vulink", name, ignore.case = TRUE)) %>%
    dplyr::filter(!grepl("virridy", name, ignore.case = TRUE)) %>%
    # Filter out FDOM (not used in CSU/FCW networks)
    dplyr::filter(!grepl("FDOM Fluorescence", parameter, ignore.case = TRUE)) %>%
    dplyr::select(-name) %>%
    dplyr::mutate(
      DT = timestamp,
      DT_round = lubridate::round_date(DT, summarize_interval),
      DT_join = as.character(DT_round),
      site = tolower(site)
    )

  # --- Summarize each site-parameter combination ---

  # Split into a list of single site-param dataframes named "site-parameter"
  site_param_list <- split(api_data, f = list(api_data$site, api_data$parameter),
                           sep = "-", drop = TRUE)

  result_list <- purrr::map(site_param_list, function(site_param_df) {

    site_arg <- unique(site_param_df$site)
    parameter_arg <- unique(site_param_df$parameter)

    tryCatch({
      site_param_df %>%
        dplyr::distinct() %>%
        dplyr::group_by(DT_round, site, parameter, units) %>%
        dplyr::summarize(
          mean   = as.numeric(mean(value, na.rm = TRUE)),
          spread = abs(min(value, na.rm = TRUE) - max(value, na.rm = TRUE)),
          n_obs  = dplyr::n(),
          .groups = "drop"
        ) %>%
        dplyr::ungroup() %>%
        dplyr::arrange(DT_round) %>%
        padr::pad(by = "DT_round", interval = pad_interval) %>%
        dplyr::mutate(
          DT_join   = as.character(DT_round),
          site      = site_arg,
          parameter = parameter_arg,
          flag      = NA
        ) %>%
        dplyr::filter(!is.na(site)) %>%
        dplyr::distinct(.keep_all = TRUE)
    },
    error = function(e) {
      message("An error occurred with site ", site_arg, " parameter ", parameter_arg)
      message("Error message: ", e$message, "\n")
      return(NULL)
    })
  })

  # Drop any NULLs from site-params that errored out, preserving names
  return(purrr::compact(result_list))
}
