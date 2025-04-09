#' @title Retrieve friendly names for HydroVu parameters and units
#'
#' @description
#' Fetches human-readable names for parameter IDs and unit IDs from the HydroVu
#' API. The raw data from HydroVu uses numeric identifiers for parameters and
#' units, which are not intuitive. This function retrieves the mapping between
#' these IDs and their corresponding friendly names (e.g., parameter ID "2" 
#' might map to "Temperature" and unit ID "7" to "°C").
#'
#' @param client An OAuth2 client object as returned by the hv_auth() function,
#' containing the necessary authentication credentials.
#'
#' @param return Character string specifying what data to return. Options are:
#' "both" (default) returns a list with both parameter and unit mappings,
#' "params" returns only parameter mappings, and "units" returns only unit
#' mappings.
#'
#' @param url Character string specifying the HydroVu API endpoint for
#' retrieving friendly names. Default is "https://www.hydrovu.com/public-api/
#' v1/sispec/friendlynames".
#'
#' @return Depending on the 'return' parameter:
#' - "params": A dataframe mapping parameter IDs to friendly parameter names
#' - "units": A dataframe mapping unit IDs to friendly unit names
#' - "both": A list containing both dataframes
#' - If an invalid 'return' value is provided, returns NULL with an error message
#'
#' @examples
#' # Get authentication token
#' hv_token <- hv_auth(client_id = "your_client_id", 
#'                    client_secret = "your_client_secret")
#'
#' # Get both parameter and unit mappings
#' all_names <- hv_names(client = hv_token)
#'
#' # Get only parameter mappings
#' params <- hv_names(client = hv_token, return = "params")
#'
#' @seealso [hv_data_id()]

hv_names <- function(client, return = "both", 
                   url = "https://www.hydrovu.com/public-api/v1/sispec/friendlynames") {
  # Initialize the API request
  req <- httr2::request(url)
  
  # Perform the API request with OAuth authentication
  resp <- req %>% 
    httr2::req_oauth_client_credentials(client) %>%
    httr2::req_perform()
  
  # Extract the response body containing parameter and unit mappings
  names <- resp %>% httr2::resp_body_json()
  
  # Process parameter mappings - convert from JSON to dataframe
  # This creates a dataframe with parameterId and Parameter (friendly name) columns
  p <- names[["parameters"]] %>% 
    purrr::map_dfr(as.data.frame, .id = "parameterId")
  names(p) <- c("parameterId", "Parameter")
  
  # Process unit mappings - convert from JSON to dataframe
  # This creates a dataframe with unitId and Units (friendly name) columns
  u <- names[["units"]] %>% 
    purrr::map_dfr(as.data.frame, .id = "unitId")
  names(u) <- c("unitId", "Units")
  
  # Return the requested data based on the 'return' parameter
  if (return == "params") {
    return(p)
  }
  else if (return == "units") {
    return(u)
  }
  else if (return == "both") {
    b <- list(params = p, units = u)
    return(b)
  }
  else {
    print("Error: return must be one of c('both', 'params', 'units')")
    return(NULL)
  }
}