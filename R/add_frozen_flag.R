#' @title Add a flag if the water temperature is freezing.
#' 
#' @description
#' A function designed to append the 'frozen' flag to a row if the value
#' in the `mean` column is less than or equal to 0.
#' 
#' @param df A data frame with a `flag` column.
#' 
#' @return A data frame with a `flag` column that has been updated with the
#' 'frozen' flag.
#' 
#' @examples
#' add_frozen_flag(df = all_data_flagged$`archery-Actual Conductivity`)
#' add_frozen_flag(df = all_data_flagged$`boxelder-Temperature`)
#' 
#' @seealso [flag_all_data()]

add_frozen_flag <- function(df){

  # create a df of temperature for each site
  temperature <- df %>%
    data.table::data.table() %>%
    dplyr::select(DT_round, DT_join, parameter, mean) %>%
    dplyr::filter(parameter == "Temperature") %>%
    dplyr::select(DT_join, Temperature = mean)

    # add "temperature" column to df:
    temperature_checked <- df %>%
      dplyr::left_join(., temperature, by = "DT_join") %>%
      # If water temperature is freezing, flag all parameters
      add_flag(., Temperature <= 0, "frozen") %>%
      # remove the temp column so df is identical in structure to OG df
      dplyr::select(-Temperature)

    return(temperature_checked)

}
