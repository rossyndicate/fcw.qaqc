#' @title Apply low-pass binomial filter to reduce sensor noise
#' @export
#' 
#' @description
#' Reduces high-frequency noise in turbidity measurements using a 5-point binomial kernel
#' low-pass filter applied three times in succession. This function specifically addresses
#' the noise characteristics of optical turbidity sensors, which are prone to rapid 
#' fluctuations due to suspended particles, biofouling, and electronic interference.
#' 
#' The filter uses a binomial kernel with weights `[1,4,6,4,1]` normalized by dividing by 16, which provides
#' a Gaussian-like smoothing effect. This kernel gives the greatest influence to the 
#' current measurement, moderate influence to the measurements 15 minutes before and 
#' after, and least influence to the measurements 30 minutes before and after. The 
#' triple application of the filter creates a more aggressive smoothing effect equivalent
#' to a higher-order binomial filter.
#' 
#' This function currently processes only turbidity data, as turbidity sensors are 
#' particularly susceptible to noise from suspended particles and optical interference.
#' For other parameters, the function returns the input dataframe with
#' `new_value_col` set to `NA_integer_`.
#'
#' @param df A data frame containing water quality measurements. Must include columns:
#' - `parameter`: The measurement type (function checks for "Turbidity")
#' - `DT_round`: Timestamp for temporal alignment (used for rolling window operations)
#' - The column named by `value_col`
#'
#' @param value_col Name of the numeric column to filter. Defaults to `"mean"`.
#' Pass `"mean_filled"` to filter the gap-interpolated values produced by
#' `apply_interpolation_missing_data()`.
#'
#' @param new_value_col Name of the output column that will hold the filtered
#' values. Defaults to `"smoothed_mean"`.
#'
#' @return A data frame with the same structure as the input plus a new column
#' named `new_value_col`. For turbidity, it contains the filtered values drawn
#' from `value_col`. For all other parameters, it is set to `NA_integer_`.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [apply_interpolation_missing_data()]
#' @seealso [add_drift_flag()]

low_pass_filter <- function(df, value_col = "mean", new_value_col = "smoothed_mean") {

  # Only apply low pass filter to turbidity data due to its susceptibility to optical noise
  if (unique(df$parameter) != "Turbidity") {
    return(df %>% mutate(!!sym(new_value_col) := NA_integer_))
  }

  # Define 5-point binomial kernel function with weights [1,4,6,4,1]
  binomial_kernel <- function(int_vec) {
    kernel <- c(1, 4, 6, 4, 1)
    x <- sum(int_vec * kernel) / 16
    return(x)
  }

  # Apply low pass smoothing filter three times in succession for aggressive noise reduction
  # Each application uses a centered 5-point window covering ±30 minutes (at 15-min intervals)
  df <- df %>%
    add_column_if_not_exists(column_name = new_value_col) %>%
    mutate(
      # First pass: read from user-specified input column
      !!sym(new_value_col) := if_else(is.na(.data[[new_value_col]]),
                              data.table::frollapply(.data[[value_col]], n = 5, FUN = binomial_kernel,
                                                     fill = NA, align = "center"),
                              .data[[new_value_col]]),
      # Second pass
      !!sym(new_value_col) := if_else(is.na(.data[[new_value_col]]),
                              data.table::frollapply(.data[[new_value_col]], n = 5, FUN = binomial_kernel,
                                                     fill = NA, align = "center"),
                              .data[[new_value_col]]),
      # Third pass
      !!sym(new_value_col) := if_else(is.na(.data[[new_value_col]]),
                              data.table::frollapply(.data[[new_value_col]], n = 5, FUN = binomial_kernel,
                                                     fill = NA, align = "center"),
                              .data[[new_value_col]])
    )

  return(df)
}
