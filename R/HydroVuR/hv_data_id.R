#' @title Retrieve time series data for a specific HydroVu location
#'
#' @description
#' Fetches water quality monitoring data for a specific location ID from the 
#' HydroVu API for a given time period. This function handles the complexity of 
#' working with the HydroVu API, including authentication, pagination, timestamp 
#' conversions, and joining parameter and unit metadata to make the data more 
#' readable and meaningful.
#' 
#' The function converts human-readable timestamps to Unix timestamps for the API 
#' request, then converts the response data back to the specified timezone for 
#' consistency with other system data. It also manages pagination to ensure all 
#' data points within the specified time range are retrieved, even when they span 
#' multiple API response pages.
#'
#' @param loc_id Character string containing the unique identifier for a HydroVu 
#' location (monitoring site). This ID is obtained from the hv_locations_all() 
#' function.
#'
#' @param start_time Character string specifying the start of the time range for 
#' data retrieval in "YYYY-MM-DD HH:MM:SS" format. Should be in the timezone 
#' specified by the tz parameter.
#'
#' @param end_time Character string specifying the end of the time range for data 
#' retrieval in "YYYY-MM-DD HH:MM:SS" format. Should be in the timezone specified 
#' by the tz parameter.
#'
#' @param tz Character string specifying the timezone for input timestamps and 
#' returned data. Default is determined by the calling environment's timezone 
#' variable.
#'
#' @param token OAuth client object obtained from hv_auth() function, used for 
#' authentication with the HydroVu API.
#'
#' @return A dataframe containing water quality time series data with columns:
#' - timestamp: Time of measurement in the specified timezone
#' - value: The measured value
#' - Location: The location ID (same as input loc_id)
#' - Parameter: Human-readable parameter name (e.g., "Temperature", "DO")
#' - Units: Measurement units (e.g., "°C", "mg/L")
#' The data is arranged by Parameter and timestamp for easy analysis.
#'
#' @examples
#' # Authenticate with HydroVu
#' hv_token <- hv_auth(client_id = "your_client_id", client_secret = "your_client_secret")
#'
#' # Get all locations
#' locations <- hv_locations_all(hv_token)
#'
#' # Retrieve data for first location in the list
#' site_data <- hv_data_id(loc_id = locations$id[1],
#'                         start_time = "2023-01-01 00:00:00",
#'                         end_time = "2023-01-02 00:00:00",
#'                         tz = "UTC",
#'                         token = hv_token)
#'
#' @seealso [hv_auth()]
#' @seealso [hv_locations_all()]
#' @seealso [api_puller()]
#' @seealso [flatten_page_params()]
#' @seealso [hv_names()]

hv_data_id <- function(loc_id, start_time = startdate, end_time = enddate, tz = timezone, token) {
  
  # Convert input timestamps from specified timezone to Unix timestamps (seconds since epoch) in UTC
  # This is required for HydroVu API requests which use Unix timestamps
  start <- as.numeric(lubridate::with_tz(lubridate::ymd_hms(start_time, tz = tz), tzone = "UTC"))
  end <- as.numeric(lubridate::with_tz(lubridate::ymd_hms(end_time, tz = tz), tzone = "UTC"))
  
  # Construct the API URL with location ID and time parameters
  url = "https://www.hydrovu.com/public-api/v1/locations/"
  url <- paste0(url, loc_id, "/data?endTime=", end, '&startTime=', start)
  
  # Initialize the API request
  req <- httr2::request(url)
  
  # Log which site we're attempting to query (helpful for debugging)
  print(paste0('Trying site ', loc_id))
  
  try({
    # Perform the initial API request with OAuth authentication
    resp <- req %>% 
      httr2::req_oauth_client_credentials(token) %>% 
      httr2::req_perform()
    
    # Extract the JSON response body and store in a list
    data <- list(resp %>% httr2::resp_body_json())
    
    # Get the response headers to check for pagination
    h <- resp %>% httr2::resp_headers()
    
    # Handle pagination - continue fetching pages while the X-ISI-Next-Page header exists
    while (!is.null(h[["X-ISI-Next-Page"]]))
    {
      # Request the next page using the page marker from the previous response
      resp <- req %>% 
        httr2::req_headers("X-ISI-Start-Page" = h[["X-ISI-Next-Page"]]) %>%
        httr2::req_oauth_client_credentials(token) %>% 
        httr2::req_perform()
      
      # Add this page's results to our collection
      data <- c(data, list(resp %>% httr2::resp_body_json()))
      
      # Update headers to check if there are more pages
      h <- resp %>% httr2::resp_headers()
    }
    
    # Retrieve parameter and unit metadata for translating IDs to human-readable names
    params <- hv_names(token, return = "params")
    units <- hv_names(token, return = "units")
    
    # Process the collected data:
    # 1. Flatten the nested data structure into a dataframe
    # 2. Convert timestamps back to the specified timezone
    # 3. Join with parameter and unit metadata for readability
    # 4. Arrange data by parameter and timestamp for easy analysis
    df <- purrr::map_dfr(data, flatten_page_params) %>%
      dplyr::mutate(
        timestamp = lubridate::with_tz(lubridate::as_datetime(timestamp, tz = "UTC"), tzone = tz),
        Location = loc_id
      ) %>%
      dplyr::inner_join(params, by = "parameterId") %>%
      dplyr::inner_join(units, by = "unitId") %>%
      dplyr::select(-parameterId, -unitId) %>%
      dplyr::arrange(Parameter, timestamp)
    
    return(df)
  })
}