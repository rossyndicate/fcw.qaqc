#' FCWQAQC: Quality Assurance and Quality Control for Fort Collins Poudre Sonde Network Data
#'
#' Tools for processing, flagging, and analyzing water quality data
#' from the Fort Collins Watershed monitoring network. This package provides
#' functions for data validation, quality control flagging, and summary statistics.
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr filter mutate arrange select left_join desc n case_when lead
#' @importFrom purrr map2
#' @importFrom stats lm na.omit lag
#' @importFrom utils flush.console
#' @importFrom data.table :=
#' @importFrom rlang sym
#' @importFrom lubridate %within%
#'
#' @name fcw.qaqc
#' @keywords internal
"_PACKAGE"
