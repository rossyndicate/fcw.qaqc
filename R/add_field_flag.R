#' @title Add field related flags to a data frame based on field notes.
#'
#' @description
#' A function that checks 3 different and separate conditions related to the
#' field notes and adds the corresponding flags accordingly. The "sonde not
#' employed" flag is added if the sonde was not employed at the site.
#' "site visit" flag is added if the last site visit date is the same as the
#' date of the data. "sv window" flag is added if site visit flag is detected
#' within a 75 minute window before (15 mins) and after (60 mins). Note that this function will only work on data
#' that has already been joined to field notes.
#'
#' @param df A data frame with a `flag` column.
#'
#' @return A data frame with a `flag` column that has been updated with the
#' relevant field note flags.
#'
#' @examples
#' add_range_flags(df = all_data_flagged$`archery-Actual Conductivity`)
#' add_range_flags(df = all_data_flagged$`boxelder-Temperature`)

add_field_flag <- function(df) {

  df <- df %>%
    # flag when sonde was not employed in the river
    add_flag(sonde_employed == 1, "sonde not employed") %>% # removing the instances where we check for the flag description in the flag column when we are flagging. This is already done in `add_flag()`
    # flag when sonde was handled in a site visit
    add_flag(as.character(last_site_visit) == as.character(DT_round), "site visit")

  # Add flags for the next 60 minutes after a site visit
  for (i in 1:4) {
    df <- df %>%
      add_flag(lag(str_detect(flag, "site visit"), n = i), "sv window")
  }

  # ... and the first 15 minutes before:
  df <- df %>%
    add_flag(lead(str_detect(flag, "site visit"), n = 1), "sv window")

  return(df)

}
