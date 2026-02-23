#' @title Combine incoming and historical water quality monitoring data
#' @export
#'
#' @description
#' Merges incoming water quality data with historical data for each site-parameter
#' combination. This function handles two distinct use cases controlled by the
#' \code{subset} argument:
#'
#' \strong{Pre-flagging mode} (\code{subset = TRUE}): Trims historical data to the
#' most recent \code{subset_period} window and combines it with incoming data to
#' create a working list for the flagging pipeline. Historical records are tagged
#' \code{historical = TRUE} and incoming records are tagged \code{historical = FALSE}.
#' The \code{historical} column is used downstream by \code{prep_bind_datasets(..., subset = FALSE)}
#' to identify the overlap period and replace it with freshly flagged data.
#' Equivalent to the former \code{combine_datasets()}.
#'
#' \strong{Archive write-back mode} (\code{subset = FALSE}): Merges newly flagged
#' data back into the long-term historical archive, replacing the \code{subset_period}
#' overlap window with updated flags from the most recent processing run. All output
#' records are tagged \code{historical = TRUE} since they are being written to the
#' archive. Equivalent to the former \code{final_data_binder()}.
#'
#' @param incoming_data_list A list of dataframes containing newly collected or
#' newly flagged water quality data. Each list element should be named with the
#' "site-parameter" naming convention (e.g., "riverbluffs-Temperature").
#'
#' @param historical_data_list A list of dataframes containing previously processed
#' historical data. Each list element should follow the same "site-parameter" naming
#' convention as \code{incoming_data_list}.
#'
#' @param subset Logical. If \code{TRUE}, trims historical data to the
#' \code{subset_period} window before combining (pre-flagging mode). If \code{FALSE},
#' replaces the \code{subset_period} overlap in the historical archive with incoming
#' data (archive write-back mode). Default is \code{FALSE}.
#'
#' @param subset_period Character string specifying the time window used as the
#' overlap period. Required when \code{subset = TRUE}. Also replaces the hardcoded
#' 24-hour overlap boundary in archive write-back mode. Accepts any duration string
#' compatible with \code{lubridate} (e.g., \code{"24 hours"}, \code{"12 hours"}).
#' Default is \code{NULL}.
#'
#' @return A named list of dataframes, one per site-parameter combination, containing
#' the merged data. All dataframes include a \code{historical} boolean column indicating
#' whether each record originates from the historical archive (\code{TRUE}) or is
#' newly incoming (\code{FALSE}).
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [prep_incoming_HydroVu_data()]
#' @seealso [prep_add_field_notes()]
#' @seealso [prep_summary_statistics()]

prep_bind_datasets <- function(incoming_data_list, historical_data_list,
                             subset = FALSE, subset_period = NULL) {

  # Validate subset_period is provided when subset = TRUE
  if (subset == TRUE && is.null(subset_period)) {
    stop("`subset_period` must be supplied when `subset = TRUE`.")
  }

  # Handle cases where one or both data sources are unavailable: ===============

  ## Handle case 0: Both inputs are empty or null.
  if ((is.null(historical_data_list) || length(historical_data_list) == 0) &
      (is.null(incoming_data_list) || length(incoming_data_list) == 0)) {
    stop("No data provided to `prep_bind_datasets()`.")
  }

  ## Handle case 1: No incoming data.
  ## In pre-flagging mode, the pipeline should never reach this step without new data.
  ## In archive write-back mode, this means no new flags were generated.
  if (is.null(incoming_data_list) || length(incoming_data_list) == 0) {
    stop("No new incoming data list provided to `prep_bind_datasets()`.")
  }

  ## Handle case 2: No historical data.
  ## This can happen after a system reset or when monitoring a new site.
  if (is.null(historical_data_list) || length(historical_data_list) == 0) {
    warning("No historical data list provided to `prep_bind_datasets()`.")

    if (subset == TRUE) {
      # Pre-flagging mode: tag all incoming as non-historical and return
      new_data <- purrr::map(incoming_data_list, function(data) {
        data %>%
          dplyr::mutate(historical = FALSE,
                        flag = as.character(flag))
      })
    } else {
      # Archive write-back mode: tag all incoming as historical and return
      new_data <- purrr::map(incoming_data_list, function(data) {
        data %>%
          dplyr::mutate(historical = TRUE)
      })
    }
    return(new_data)
  }

  # Handle the standard case: Both historical and incoming data exist ==========

  # Find site-parameter combinations present in both datasets
  matching_indexes <- dplyr::intersect(names(incoming_data_list), names(historical_data_list))

  # Parse the subset_period string into a lubridate duration for boundary calculations
  subset_duration <- lubridate::duration(subset_period)

  # Combine historical and incoming data for each matching site-parameter combination
  combined_data <- purrr::map(matching_indexes, function(index) {

    if (subset == TRUE) {

      # --- Pre-flagging mode ---
      # Trim historical data to the subset_period window and tag as historical.
      # This creates an overlap period that provides context for drift detection
      # and ensures smooth transitions between data collection cycles.
      hist_data <- historical_data_list[[index]] %>%
        dplyr::filter(DT_round >= max(DT_round) - subset_duration) %>%
        dplyr::mutate(historical = TRUE,
                      # Copy auto_flag to flag column for continuity into the flagging pipeline
                      flag = as.character(auto_flag)) %>%
        # Remove sonde_moved so it can be recalculated across the combined time series
        dplyr::select(-sonde_moved)

      # Prepare incoming data and tag as non-historical
      inc_data <- incoming_data_list[[index]] %>%
        dplyr::mutate(historical = FALSE,
                      flag = as.character(flag))

      # Keep only incoming timestamps not already present in the historical subset
      # to avoid duplicating the overlap period
      unique_inc <- inc_data %>%
        dplyr::anti_join(hist_data, by = "DT_round")

      # Combine the historical subset with unique incoming records
      dplyr::bind_rows(hist_data, unique_inc) %>%
        dplyr::arrange(DT_round)

    } else {

      # --- Archive write-back mode ---
      # Split historical data at the subset_period boundary.
      # Records before the boundary are kept as-is; the overlap window is replaced
      # by the freshly flagged incoming data.

      # Historical data older than the overlap window — kept unchanged
      old <- historical_data_list[[index]] %>%
        dplyr::filter(DT_round < max(DT_round) - subset_duration)

      # Historical data within the overlap window — to be replaced
      old_to_update <- historical_data_list[[index]] %>%
        dplyr::filter(DT_round >= max(DT_round) - subset_duration)

      # Incoming data corresponding to the overlap window (flagged historical records)
      new_to_update <- incoming_data_list[[index]] %>%
        dplyr::filter(historical == TRUE)

      # Notify if the overlap period flags have changed since the last processing run
      if (identical(old_to_update, new_to_update) == FALSE) {
        print(paste0(index, " historical data has been updated based on the latest data."))
      }

      # Combine pre-overlap historical data with all incoming data, then tag
      # everything as historical since this is the archive write-back
      dplyr::bind_rows(old, incoming_data_list[[index]]) %>%
        dplyr::arrange(DT_round) %>%
        dplyr::mutate(historical = TRUE)
    }

  }) %>%
    purrr::set_names(matching_indexes)

  # Handle site-parameter combinations that only exist in the incoming data =====
  # This can occur after a system reset or when a new site comes online.

  new_only_indexes <- setdiff(names(incoming_data_list), names(historical_data_list))

  if (length(new_only_indexes) > 0) {
    new_only_data <- purrr::map(new_only_indexes, function(index) {
      if (subset == TRUE) {
        # Pre-flagging mode: new site has no history yet, tag as non-historical
        incoming_data_list[[index]] %>%
          dplyr::mutate(historical = FALSE,
                        flag = as.character(flag))
      } else {
        # Archive write-back mode: going into the archive, tag as historical
        incoming_data_list[[index]] %>%
          dplyr::mutate(historical = TRUE)
      }
    }) %>%
      purrr::set_names(new_only_indexes)

    combined_data <- c(combined_data, new_only_data)
    print(paste0("Added new site-parameter combinations: ",
                 paste(new_only_indexes, collapse = ", ")))
  }

  # Handle site-parameter combinations that only exist in historical data =======
  # Only relevant in archive write-back mode — these are sites that had no new
  # data this cycle and should be preserved as-is in the archive.

  if (subset == FALSE) {
    historical_only_indexes <- setdiff(names(historical_data_list), names(incoming_data_list))

    if (length(historical_only_indexes) > 0) {
      historical_only_data <- historical_data_list[historical_only_indexes] %>%
        purrr::set_names(historical_only_indexes)

      combined_data <- c(combined_data, historical_only_data)
      print(paste0("Preserved historical-only site-parameter combinations: ",
                   paste(historical_only_indexes, collapse = ", ")))
    }
  }

  return(combined_data)
}
