tidy_flag_column <- function(df){
  
  sort_semicolon_list <- function(text) {
    text %>%
      stringr::str_split(";") %>%                  # Split the string into a list of words
      purrr::map(~ sort(trimws(.x))) %>%         # Sort the words and trim spaces
      purrr::map_chr(~ paste(.x, collapse = ";")) # Rejoin them with semicolons
  }
  
  remove_trailing_semicolons <- function(text) {
    text %>%
      stringr::str_replace_all(";\\s*$", "") %>%  # Remove "; " at the end of the string
      stringr::str_replace_all(";$", "") %>%       # Remove ";" at the end of the string
    stringr::str_replace_all(";\\s*;", ";") %>%  # Replace multiple semicolons (with or without spaces) with a single ";"
      stringr::str_replace_all("^;+", "") %>%      # Remove semicolons at the start of the string
      stringr::str_replace_all(";\\s*$", "")       # Remove trailing semicolons (with or without spaces)
  }
  
  
  
  df <- df %>%
    data.table::data.table() %>%
    dplyr::mutate(auto_flag = dplyr::case_when(
      is.na(auto_flag) ~ NA_character_,  # Keep NAs
      # grepl("site visit|sv window", auto_flag) ~ stringr::str_remove_all(auto_flag, "DO interference|sonde not employed|repeated value|drift|missing data|outside of seasonal range|slope violation|outside of sensor specification range|outside of sensor realistic range|frozen|suspect data"),
      grepl("site visit", auto_flag) ~ "site visit",#stringr::str_remove_all(auto_flag, 
      grepl("sv window", auto_flag) ~ "site visit window",                                                      #                   
      grepl("reported sonde not employed", auto_flag)  ~ "sonde not employed",
      grepl("reported sensor malfunction", auto_flag)  ~ "reported sensor malfunction",
      grepl("reported sonde burial", auto_flag) ~ "reported sonde burial",
      grepl("reported sensor biofouling", auto_flag) ~ "reported sensor biofouling",
      grepl("frozen", auto_flag)  ~ "frozen",
      TRUE ~ auto_flag  # Keep original value if no conditions matched
    )) %>%
    dplyr::mutate(auto_flag = purrr::map_chr(auto_flag, remove_trailing_semicolons)) %>%
    dplyr::mutate(auto_flag = stringr::str_replace_all(auto_flag, "\\n", " ")) %>%
    dplyr::mutate(auto_flag = stringr::str_trim(auto_flag) %>% sort_semicolon_list(.)) 
  
}

# df <- df %>%
#   data.table::data.table() %>%
#   dplyr::mutate(auto_flag = dplyr::case_when(
#     is.na(auto_flag) ~ NA_character_,  # Keep NAs
#     # grepl("site visit|sv window", auto_flag) ~ stringr::str_remove_all(auto_flag, "DO interference|sonde not employed|repeated value|drift|missing data|outside of seasonal range|slope violation|outside of sensor specification range|outside of sensor realistic range|frozen|suspect data"),
#     grepl("site visit", auto_flag) ~ "site visit",#stringr::str_remove_all(auto_flag, 
#     grepl("sv window", auto_flag) ~ "site visit window",                                                      #                   
#     grepl("sonde not employed", auto_flag) & !grepl("site visit|sv window", auto_flag) ~ "sonde not employed",
#     grepl("sensor malfunction", auto_flag) & !grepl("site visit|sv window", auto_flag) ~ "sensor malfunction",
#     grepl("sonde burial", auto_flag) & !grepl("site visit|sv window", auto_flag) ~ "sonde burial",
#     grepl("sensor biofouling", auto_flag) & !grepl("site visit|sv window", auto_flag) ~ "sensor biofouling",
#     TRUE ~ auto_flag  # Keep original value if no conditions matched
#   )) %>%
#   dplyr::mutate(auto_flag = purrr::map_chr(auto_flag, remove_trailing_semicolons)) %>%
#   dplyr::mutate(auto_flag = stringr::str_replace_all(auto_flag, "\\n", " ")) %>%
#   dplyr::mutate(auto_flag = stringr::str_trim(auto_flag) %>% sort_semicolon_list(.)) 