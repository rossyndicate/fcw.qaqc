#' @title add_burial_flag
#'
#' @description
#' A function designed to identify instances of likely sonde burial
#'
#' @param df An updated data frame with a `flag` column.
#'
#' @return A data frame with a `flag` column that has been updated with the
#' DO-related possible burial flag.
#'
#' @seealso [add_flag()]

add_burial_flag <- function(df){  
  # create a df of temperature for each site
  do <- df %>%
    data.table::data.table() %>%
    dplyr::filter(parameter == "DO") %>%
    dplyr::select(DT_join, do_flag = flag)
  
  
  do_checked <- df %>%
    dplyr::filter(!parameter %in% c("DO")) %>%
    dplyr::left_join(., do, by = "DT_join") %>%
    add_flag(grepl("Possible burial", do_flag), "Possible burial") %>%
    dplyr::select(-do_flag) %>%
    dplyr::bind_rows(df %>% filter(parameter == "DO")) %>%
    dplyr::distinct()
  
  
  return(do_checked)
  
}
