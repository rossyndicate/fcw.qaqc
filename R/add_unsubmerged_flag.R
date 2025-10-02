#' @title Flag measurements when sonde was not fully submerged
#' @export
#'
#' @description
#' Identifies and flags water quality measurements collected when the monitoring
#' sonde was not fully submerged in water. This function uses depth and specific conductivity sensor readings
#' to determine submersion status and applies flags across all parameters measured
#' at the same time. The data will be flagged if the value in the `relative_depth`
#' column is less than or equal to 0. This data will also be flagged if `specific conductivity` is less than 10 uS/cm. 
#'
#' @param df A dataframe containing all parameters for a single site. Must include columns:
#' - `parameter`: Measurement type (function requires "Depth" parameter to be present)
#' - `DT_join`: Character timestamp used for joining
#' - `mean`: The calculated mean value of measurements
#' - `flag`: Existing quality flags (will be updated by this function)
#' 
#' @param method Method to determine submersion status. Options are:
#' -`"depth"`: (default) Uses only depth readings to flag data. This method flags data when depth is less than or equal to 0.
#'  This method may be appropriate in situations where specific conductivity data is unreliable or unavailable.
#' - `"depth_sc"`: Uses both depth and specific conductivity readings to flag data.
#'  This method flags data when depth is less than or equal to 0 or when specific conductivity is less than or equal to 10 uS/cm.
#'  This is the recommended method for most scenarios.
#'  -`"sc"`: Uses only specific conductivity readings to flag data. This method flags data when specific conductivity is less than or equal to 10 uS/cm.
#'  This method may be appropriate in situations where depth data is unreliable or unavailable.
#'
#' @return A dataframe with the same structure as the input, but with the `flag`
#' column updated to include "sonde unsubmerged" for all parameters when depth 
#' and/or specific conductivity readings indicate the sonde was not fully underwater.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [add_flag()]

add_unsubmerged_flag <- function(df, method = "depth"){
  
  #check if method is not one of the options
  if(!method %in% c("depth_sc", "depth", "sc")){
    stop("Invalid method. Choose from 'depth_sc', 'depth', or 'sc'.")
  }

    # valid method
    # Extract depth measurements for the site if a depth method is selected
    if(method %in% c("depth_sc", "depth")){
      depth <- df %>%
        data.table::data.table() %>%
        dplyr::select(DT_round, DT_join, parameter, mean) %>%
        dplyr::filter(parameter == "Depth") %>%
        dplyr::select(DT_join, Depth = mean)
    }
    # Extract specific conductivity measurements for the site if a sc method is selected
    if(method %in% c("depth_sc", "sc")){
      sc <- df %>%
        data.table::data.table() %>%
        dplyr::select(DT_round, DT_join, parameter, mean) %>%
        dplyr::filter(parameter == "Specific Conductivity") %>%
        dplyr::select(DT_join, SC = mean)
    }
    #Apply with both depth and SC data
    if(method == "depth_sc"){
    # Join depth measurements with all parameters and apply flag
    depth_checked <- df %>%
      dplyr::left_join(., depth, by = "DT_join") %>%
      dplyr::left_join(., sc, by = "DT_join") %>%
      # Flag all parameters when depth is 0 or negative OR when SC is less than 10 uS/cm (likely not submerged)
      add_flag(., Depth <= 0 | SC <= 10 , "sonde unsubmerged") %>%
      # Remove temporary depth/SC column to maintain original structure
      dplyr::select(-Depth, -SC)
    }
    #Only apply check with depth data
    if(method == "depth"){
      # Join depth measurements with all parameters and apply flag
      depth_checked <- df %>%
        dplyr::left_join(., depth, by = "DT_join") %>%
        # Flag all parameters when depth is 0 or negative
        add_flag(., Depth <= 0, "sonde unsubmerged") %>%
        # Remove temporary depth column to maintain original structure
        dplyr::select(-Depth)
    }
    #Only apply check with SC data
    if(method == "sc"){
      # Join specific conductivity measurements with all parameters and apply flag
      depth_checked <- df %>%
        dplyr::left_join(., sc, by = "DT_join") %>%
        # Flag all parameters when SC is less than 10 uS/cm (likely not submerged)
        add_flag(., SC <= 10, "sonde unsubmerged") %>%
        # Remove temporary SC column to maintain original structure
        dplyr::select(-SC)
    }

 
  return(depth_checked)
}
