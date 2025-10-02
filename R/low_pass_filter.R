#' @title Apply low-pass binomial filter to reduce sensor noise
#' @export
#' 
#' @description
#' Reduces high-frequency noise in turbidity measurements using a 5-point binomial kernel
#' low-pass filter applied three times in succession. This function specifically addresses
#' the noise characteristics of optical turbidity sensors, which are prone to rapid 
#' fluctuations due to suspended particles, biofouling, and electronic interference.
#' 
#' The filter uses a binomial kernel with weights [1,4,6,4,1] normalized by dividing by 16, which provides
#' a Gaussian-like smoothing effect. This kernel gives the greatest influence to the 
#' current measurement, moderate influence to the measurements 15 minutes before and 
#' after, and least influence to the measurements 30 minutes before and after. The 
#' triple application of the filter creates a more aggressive smoothing effect equivalent
#' to a higher-order binomial filter.
#' 
#' This function currently processes only turbidity data, as turbidity sensors are 
#' particularly susceptible to noise from suspended particles and optical interference.
#' For other parameters, the function returns the input dataframe unchanged.
#' 
#' @param df A data frame containing water quality measurements. Must include columns:
#' - `parameter`: The measurement type (function checks for "Turbidity")
#' - `mean`: The calculated mean value of measurements to be smoothed
#' - `DT_round`: Timestamp for temporal alignment (used for rolling window operations)
#'
#' @return A data frame with the same structure as the input, but with an additional
#' `smoothed_mean` column containing the filtered turbidity values. For non-turbidity
#' parameters, returns the original dataframe unchanged.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [add_drift_flag()]

low_pass_filter <- function(df) {
  # Only apply low pass filter to turbidity data due to its susceptibility to optical noise
  if (unique(df$parameter) == "Turbidity") {
    
    # Define 5-point binomial kernel function with weights [1,4,6,4,1]
    binomial_kernel <- function(int_vec) {
      kernel <- c(1, 4, 6, 4, 1)  
      x <- sum(int_vec * kernel) / 16  
      return(x)
    }
    
    # Apply low pass smoothing filter three times in succession for aggressive noise reduction
    # Each application uses a centered 5-point window covering ±30 minutes (at 15-min intervals)
    df <- df %>% 
      add_column_if_not_exists(column_name = "smoothed_mean") %>% 
      mutate(
        # First pass
        smoothed_mean = if_else(is.na(smoothed_mean), 
                                data.table::frollapply(mean, n = 5, FUN = binomial_kernel, 
                                                       fill = NA, align = "center"),
                                smoothed_mean),
        # Second pass
        smoothed_mean = if_else(is.na(smoothed_mean), 
                                data.table::frollapply(smoothed_mean, n = 5, FUN = binomial_kernel, 
                                                       fill = NA, align = "center"),
                                smoothed_mean),
        # Third pass
        smoothed_mean = if_else(is.na(smoothed_mean), 
                                data.table::frollapply(smoothed_mean, n = 5, FUN = binomial_kernel, 
                                                       fill = NA, align = "center"),
                                smoothed_mean)
      )
    
    return(df)
  } else {
    # Return unmodified dataframe for non-turbidity parameters
    return(df)
  }
}