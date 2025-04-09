#' @title Core function for data quality flagging system
#'
#' @description
#' This function serves as the foundation of the water quality monitoring flagging
#' system, providing a consistent mechanism for adding quality indicators to data.
#' It evaluates a user-specified condition against each row of the dataframe and
#' adds the corresponding flag text to matching rows. The function is designed to
#' handles multiple flags by preserving existing flags and avoid duplication.
#'
#' This design allows specialized flagging functions to focus on their specific
#' detection logic while delegating the actual flag application to this centralized
#' function, ensuring consistent flag formatting throughout the system.
#'
#' @param df A dataframe containing water quality monitoring data with a `flag`
#' column that may contain existing quality flags or NA values.
#'
#' @param condition_arg A logical expression that will be evaluated for each row
#' of the dataframe. This expression uses tidyverse-style non-standard evaluation
#' allowing direct reference to column names (e.g., mean > threshold).
#'
#' @param description_arg Character string containing the flag text to add when
#' the condition is TRUE (e.g., "outside spec range", "sensor drift").
#'
#' @returns A dataframe identical to the input, except with the `flag` column
#' updated to include the new flag description for rows where the condition was
#' TRUE. The function preserves existing flags using a semicolon-newline separator
#' and avoids adding duplicate flags.
#'
#' @examples
#' # Flag values over 25 as potentially unreliable
#' df <- add_flag(water_data, mean > 25, "value exceeds typical range")
#'
#' # Flag missing values
#' df <- add_flag(water_data, is.na(mean), "missing data")
#'
#' @seealso [add_burial_flag()]
#' @seealso [add_depth_shit_flag()]
#' @seealso [add_drift_flag()]
#' @seealso [add_field_flag()]
#' @seealso [add_frozen_flag()]
#' @seealso [add_malfunction_flag()]
#' @seealso [add_na_flag()]
#' @seealso [add_repeat_flag()]
#' @seealso [add_seasonal_flag()]
#' @seealso [add_spec_flag()]
#' @seealso [add_suspect_flag()]
#' @seealso [add_unsubmerged_flag()]

add_flag <- function(df, condition_arg, description_arg) {
  
  # Update the flag column based on the provided condition
  # This uses tidyverse programming techniques to evaluate the condition
  # within the context of the dataframe
  df <- df %>% mutate(flag = case_when(
    # For rows where the condition is TRUE:
    {{condition_arg}} ~ if_else(
      # If there's no existing flag, use just the new description
      is.na(flag), paste(description_arg),
      # If there are existing flags, check if this flag already exists
      ifelse(
        # Only add the flag if it doesn't already exist (prevent duplicates)
        !grepl(description_arg, flag), 
        # Append the new flag with a semicolon+newline separator for readability
        paste(flag, description_arg, sep = ";\n"), 
        # If flag already exists, keep the original flag unchanged
        flag
      )
    ),
    # For rows where condition is FALSE, preserve the existing flag value
    TRUE ~ flag
  ))
  
  return(df)
}