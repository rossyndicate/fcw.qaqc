#' @title Add a column to a data frame if it does not exist
#'
#' @description
#' Checks whether a column is present in a data frame.
#' If the column does not exist, it is added with a default value.
#'
#' @param df A data frame or tibble.
#' @param column_name Name of the column to check or add (string).
#' @param default_value Value to fill the column with if created. Defaults to NA.
#'
#' @return A data frame with the column ensured to exist.
#'
#' @examples
#' df <- tibble::tibble(a = 1:3)
#' df <- add_column_if_not_exists(df, "b", 0)
#'
#' @export

add_column_if_not_exists <- function(df, column_name, default_value = NA) {
  
  if (!column_name %in% colnames(df)) {
    df <- df %>% dplyr::mutate(!!sym(column_name) := default_value)
  }
  return(df)
}
