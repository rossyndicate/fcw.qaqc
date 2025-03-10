#' @title Add a flag if the sonde was not fully submerged by water.
#'
#' @description
#' A function designed to append the 'unsubmerged' flag to a row if the value
#' in the `relative_depth` column is less than or equal to 0.
#'
#' @param df A data frame with a `flag` column.
#'
#' @return A data frame with a `flag` column that has been updated with the
#' 'sonde unsubmerged' flag.
#'
#' @examples
#' add_unsubmerged_flag(df = all_data_flagged$`archery-Actual Conductivity`)
#' add_unsubmerged_flag(df = all_data_flagged$`boxelder-Temperature`)
#'
#' @seealso [flag_all_data()]

add_unsubmerged_flag <- function(df){

  # create a df of temperature for each site
  depth <- df %>%
    data.table::data.table() %>%
    dplyr::select(DT_round, DT_join, parameter, mean) %>%
    dplyr::filter(parameter == "Depth") %>%
    dplyr::select(DT_join, Depth = mean)

  # add "depth" column to df:
  depth_checked <- df %>%
    dplyr::left_join(., depth, by = "DT_join") %>%
    # If depth is below 0, flag as "sonde unsubmerged"
    add_flag(., Depth <= 0, "sonde unsubmerged") %>%
    # remove the temp column so df is identical in structure to OG df
    dplyr::select(-Depth)

  return(depth_checked)

}
