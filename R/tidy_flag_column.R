#' @title Clean and standardize quality flag formatting
#'
#' @description
#' Standardizes and formats the quality flags in the `auto_flag` column to ensure 
#' consistency across the dataset. This function handles several formatting tasks:
#' 
#' 1. Converts various flag formats to standardized versions
#' 2. Prioritizes certain flag types over others
#' 3. Removes trailing and leading semicolons
#' 4. Sorts multiple flags in a consistent order
#' 5. Replaces newlines with spaces
#' 
#' @param df A dataframe containing water quality flags. Must include an `auto_flag`
#' column containing quality flag text.
#'
#' @return A dataframe with the same structure as the input, but with the `auto_flag`
#' column standardized and cleaned for consistency.
#'
#' @examples
#' # Clean quality flags across all parameters and sites
#' final_flags <- intersensor_flags %>%
#'   dplyr::bind_rows() %>%
#'   tidy_flag_column() %>%
#'   split(f = list(.$site, .$parameter), sep = "-")

tidy_flag_column <- function(df){
  
  # Helper function to sort semicolon-separated flag lists
  sort_semicolon_list <- function(text) {
    text %>%
      stringr::str_split(";") %>%                  # Split the string into a list of words
      purrr::map(~ sort(trimws(.x))) %>%         # Sort the words and trim spaces
      purrr::map_chr(~ paste(.x, collapse = ";")) # Rejoin them with semicolons
  }
  
  # Helper function to clean up semicolons in flag text
  remove_trailing_semicolons <- function(text) {
    text %>%
      stringr::str_replace_all(";\\s*$", "") %>%  # Remove "; " at the end of the string
      stringr::str_replace_all(";$", "") %>%       # Remove ";" at the end of the string
      stringr::str_replace_all(";\\s*;", ";") %>%  # Replace multiple semicolons with a single ";"
      stringr::str_replace_all("^;+", "") %>%      # Remove semicolons at the start of the string
      stringr::str_replace_all(";\\s*$", "")       # Remove trailing semicolons again
  }
  
  # Process and standardize flag values  
  df <- df %>%
    data.table::data.table() %>%
    # Standardize flag values using a case-by-case approach
    dplyr::mutate(auto_flag = dplyr::case_when(
      is.na(auto_flag) ~ NA_character_,  # Keep NAs
      # Replace specific flag patterns with standardized versions
      grepl("site visit", auto_flag) ~ "site visit",
      grepl("sv window", auto_flag) ~ "site visit window",                                                      
      grepl("reported sonde not employed", auto_flag) ~ "sonde not employed",
      grepl("reported sensor malfunction", auto_flag) ~ "reported sensor malfunction",
      grepl("reported sonde burial", auto_flag) ~ "reported sonde burial",
      grepl("reported sensor biofouling", auto_flag) ~ "reported sensor biofouling",
      grepl("frozen", auto_flag) ~ "frozen",
      TRUE ~ auto_flag  # Keep original value if no conditions matched
    )) %>%
    # Apply cleanup functions to format flags consistently
    dplyr::mutate(auto_flag = purrr::map_chr(auto_flag, remove_trailing_semicolons)) %>%
    dplyr::mutate(auto_flag = stringr::str_replace_all(auto_flag, "\\n", " ")) %>%
    dplyr::mutate(auto_flag = stringr::str_trim(auto_flag) %>% sort_semicolon_list(.)) 
  
  return(df)
  
}