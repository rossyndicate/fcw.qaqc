#' @title Underlying function for flagging data.
#'
#' @description
#' This function adds a flag to the `flag` column of a given data frame based on
#' specified conditions for each row. The name of the flag is provided
#' by the user.
#'
#' @param df A data frame with a `flag` column.
#'
#' @param condition_arg A logical statement that is evaluated in the context of
#' the data frame.
#'
#' @param description_arg Flag added to the `flag` column.
#'
#' @returns
#' An identical data frame with a `flag` column that has been updated with the
#' flag description provided.

add_flag <- function(df, condition_arg, description_arg) {
  df <- df %>% mutate(flag = case_when(
    {{condition_arg}} ~ if_else(is.na(flag), paste(description_arg),
                                ifelse(!grepl(description_arg, flag), paste(flag, description_arg, sep = ";\n"), flag)),
    TRUE ~ flag))
  return(df)
}
