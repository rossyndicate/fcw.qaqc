#' @title Process paginated parameter readings from HydroVu API responses
#'
#' @description
#' Transforms the complex nested structure of a HydroVu API response page into a
#' flat dataframe suitable for analysis. This function handles the intricate
#' JSON structure returned by the API, extracting all parameter readings while
#' preserving their associated metadata.
#'
#' @param page_data A list containing a single page of results from the HydroVu
#' API, typically one element from the list returned by httr2::resp_body_json()
#'
#' @return A dataframe containing all parameter readings from the input page
#' with columns for parameter ID, unit ID, custom parameter flag, timestamp,
#' and value.
#'
#' @examples
#' # Process a single page
#' page_df <- flatten_page_params(data[[1]])
#'
#' # Process all pages
#' all_data <- purrr::map_dfr(data, flatten_page_params)
#'
#' @seealso [hv_data_id()]

flatten_page_params <- function(page_data) {
  # Create an empty list to hold processed parameter data
  out <- list()
  
  # Extract the parameters list from the page data
  # This contains readings for multiple parameters (DO, temp, etc.)
  d <- page_data[["parameters"]]
  
  # Loop through each parameter in the parameters list
  for (i in seq_along(d)) {
    # For each parameter:
    # 1. Extract all its readings
    # 2. Convert to a dataframe
    # 3. Add parameter and unit metadata as columns
    x <- d[[i]][["readings"]] %>%
      # Collapse the readings for this parameter into a dataframe
      purrr::map_dfr(as.data.frame) %>%
      # Add parameter ID, unit ID, and custom parameter flag as columns
      dplyr::mutate(parameterId = d[[i]][["parameterId"]],
                   unitId = d[[i]][["unitId"]],
                   customParameter = d[[i]][["customParameter"]], 
                   .before = timestamp)
    
    # Add this parameter's data to our output list
    out <- c(out, list(x))
  }
  
  # Combine all parameter dataframes into a single dataframe
  df <- purrr::map_dfr(out, as.data.frame)
  
  return(df)
}