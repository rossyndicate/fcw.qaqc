#' @title Flag sequentially repeated measurements
#'
#' @description
#' Identifies and flags water quality measurements that have the exact same value
#' as either the preceding or following measurement. Repeated values can indicate
#' sensor malfunction, signal processing issues, or data transmission problems.
#'
#' @param df A data frame containing water quality measurements. Must include columns:
#' - `mean`: The calculated mean value of measurements
#' - `front1`: The next measurement value
#' - `back1`: The previous measurement value
#' - `flag`: Existing quality flags (will be updated by this function)
#'
#' @return A data frame with the same structure as the input, but with the `flag`
#' column updated to include "repeated value" for any measurements that match
#' either the preceding or following value.
#'
#' @examples
#' # Flag repeated conductivity measurements
#' archery_conductivity_flagged <- add_repeat_flag(df = all_data_flagged$`archery-Actual Conductivity`)
#'
#' # Flag repeated temperature measurements
#' boxelder_temp_flagged <- add_repeat_flag(df = all_data_flagged$`boxelder-Temperature`)
#'
#' @seealso [add_flag()]

add_repeat_flag <- function(df){
  df <- df %>%
    add_flag((mean == front1 | mean == back1), "repeated value")
}