#' @title Add NA flags to a data frame based on `mean` column.
#'
#' @description
#' A function designed to append the 'missing data' flag to a row if the `mean`
#' column in that row contains NA values.
#'
#' @param df A data frame with a `flag` column.
#'
#' @return A data frame with a `flag` column that has been updated with the
#' 'missing data' flag.
#'
#' @examples
#' add_range_flags(df = all_data_flagged$`archery-Actual Conductivity`)
#' add_range_flags(df = all_data_flagged$`boxelder-Temperature`)

add_na_flag <- function(df){
  df <- df %>%
    add_flag(is.na(mean), "missing data")
}
