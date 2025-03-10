find_do_noise <- function(df){

if("DO" %in% df$parameter){

  df <- df %>%
    add_flag((back1 - mean >= 0.5 & front1 - mean >= 0.5) | (mean <= 5), "DO interference")
  
  
  # Define a function to check if a given window has any instances of the same word
  check_day_hour_window_fail <- function(x) {
    sum(x) >= 24
  }
  
  df <- df %>%
    data.table::data.table() %>%
    dplyr::mutate(DO_drift_binary = ifelse(grepl("DO interference", flag), 1, 0)) %>%
    dplyr::mutate(right = data.table::frollapply(DO_drift_binary, n = 96, FUN = check_day_hour_window_fail, align = "right", fill = NA),
                  center = data.table::frollapply(DO_drift_binary, n = 96, FUN = check_day_hour_window_fail, align = "center", fill = NA)) %>%
    add_flag(right == 1 | center == 1, "Possible burial")
  
  
  return(df)

} else {

  return(df)

}

  }
