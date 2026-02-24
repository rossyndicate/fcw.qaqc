#' @title Apply turbidity data corrections and validations
#' @export
#'
#' @description This function applies turbidity-specific corrections to data values,
#' including capping values at a maximum threshold of 1000. The function preserves
#' the original measurements in a 'raw' column and returns a modified dataset with
#' corrected turbidity values.
#'
#' @param df Site-parameter data frame containing turbidity measurements
#'
#' @return A data frame with turbidity values capped at 1000 units, preserving 
#'         original values in the 'raw' column and updating the 'mean' column 
#'         with corrected values.
#'
#' @examples
#' # Examples are temporarily disabled

standardize_turbidity <- function(df){

# Filter records for relevant site-param information
df_site <- unique(df$site)
df_parameter <- unique(df$parameter)


df <- df %>%
  add_column_if_not_exists(column_name = "raw") %>%
  mutate(raw = ifelse(is.na(raw), mean, raw))

if(df_parameter == "Turbidity"){

  df <- df %>%
    mutate(mean = ifelse(mean >= 1000, 1000, mean))

  }

return(df)

}
