#' @title Add a flag if the value in the `mean` column repeats sequentially.
#'
#' @description
#' A function designed to append the 'repeated value' flag to a row if the value
#' in the `mean` column is equal to the previous or next value in the `mean`
#' column.
#'
#' @param df A data frame with a `flag` column.
#'
#' @return A data frame with a `flag` column that has been updated with the
#' 'repeated value' flag.
#'
#' add_range_flags(df = all_data_flagged$`archery-Actual Conductivity`)
#' add_range_flags(df = all_data_flagged$`boxelder-Temperature`)

add_repeat_flag <- function(df){
  df <- df %>%
    add_flag((mean == front1 | mean == back1), "repeated value")
}
