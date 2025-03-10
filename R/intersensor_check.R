#' @title intersensor_check
#'
#' @description
#' A function designed to reduce overflagging: if a slope violation occurs at the
#' same time as a slope violation in either depth or temperature, it is likely
#' not a sensor malfunction and instead a real product of the river.
#'
#' @param df An updated data frame with a `flag` column.
#'
#' @return A data frame with a `flag` column that has been updated with the
#' inter-sensor flag reduction step.
#'
#' @seealso [add_flag()]

intersensor_check <- function(df){

  # create a df of temperature for each site
  temperature <- df %>%
    data.table::data.table() %>%
    dplyr::select(DT_round, DT_join, parameter, mean, flag) %>%
    dplyr::filter(parameter == "Temperature") %>%
    dplyr::select(DT_join, Temperature = parameter, Temperature_flag = flag) %>%
    dplyr:: mutate(Temperature_front1 = dplyr::lead(Temperature_flag, n = 1),
                   Temperature_back1 = dplyr::lag(Temperature_flag, n = 1))

  # create a df of depth for each site
  depth <- df %>%
    data.table::data.table() %>%
    dplyr::select(DT_round, DT_join, parameter, mean, flag) %>%
    dplyr::filter(parameter == "Depth") %>%
    dplyr::select(DT_join, Depth = parameter, Depth_flag = flag) %>%
    dplyr::mutate(Depth_front1 = dplyr::lead(Depth_flag, n = 1),
                   Depth_back1 = dplyr::lag(Depth_flag, n = 1))

  # add "temperature" and "depth" data columns to df:
  intersensors_checked <- df %>%
    dplyr::filter(!parameter %in% c("Depth", "Temperature")) %>%
    dplyr::left_join(., temperature, by = "DT_join") %>%
    dplyr::left_join(., depth, by = "DT_join") %>%
    # If either the depth or temperature have the same flag as a given parameter
    # identified at the same time (or one obs before/after), tag it
    dplyr::mutate(intersensored = dplyr::case_when(grepl("slope violation", flag) &
                                                     (grepl("slope violation", Depth_flag)   | grepl("slope violation", Temperature_flag)   |
                                                        grepl("slope violation", Depth_front1) | grepl("slope violation", Temperature_front1) |
                                                        grepl("slope violation", Depth_back1)  | grepl("slope violation", Temperature_back1)
                                                     ) ~ TRUE)) %>%
    dplyr::mutate(flag = ifelse(is.na(intersensored), flag, stringr::str_replace(flag, "slope violation", "")))

  final_checked_data <- df %>%
    dplyr::filter(parameter %in% c("Depth", "Temperature")) %>%
    # After using the temp and depth slope flags, remove that flagging entirely
    # from those parameters. We have yet to find an instance of the slope flag
    # capturing "fake" spikes in either of those data sets:
    dplyr::mutate(flag = stringr::str_replace(flag, "slope violation", "")) %>%
    dplyr::bind_rows(., intersensors_checked) %>%
    dplyr::select(-c(Depth, Depth_flag, Temperature, Temperature_flag))

  return(final_checked_data)

}
