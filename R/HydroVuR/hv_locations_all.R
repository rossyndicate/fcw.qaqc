#' @title Retrieve all monitoring locations from HydroVu
#'
#' @description
#' Fetches the complete list of water quality monitoring locations accessible to
#' the authenticated client from the HydroVu API. This function handles pagination
#' in the API response to ensure all locations are retrieved, even when the total
#' number of locations exceeds the API's per-page limit.
#' 
#' The function uses OAuth2 client credentials for authentication and manages the
#' complex task of requesting and combining multiple pages of location data into a
#' single, clean dataframe.
#'
#' @param client An OAuth2 client object as returned by the hv_auth() function,
#' containing the necessary authentication credentials.
#'
#' @param url Character string specifying the HydroVu API endpoint for retrieving
#' location data. Default is "https://www.hydrovu.com/public-api/v1/locations/list".
#'
#' @return A dataframe containing all monitoring locations accessible to the
#' authenticated client, with metadata such as:
#' - id: Unique identifier for each location
#' - name: Location display name
#' - Various other location metadata fields
#' 
#' The GPS coordinates are removed from the result, and duplicate entries are
#' filtered out.
#'
#' @examples
#' # Authenticate with HydroVu
#' hv_token <- hv_auth(client_id = "your_client_id", client_secret = "your_client_secret")
#'
#' # Retrieve all locations
#' all_locations <- hv_locations_all(client = hv_token)
#'
#' @seealso [hv_auth()]
#' @seealso [api_puller()]
#' @seealso [hv_data_id()]

hv_locations_all <- function(client,
                             url = "https://www.hydrovu.com/public-api/v1/locations/list") {
  # Initialize the API request
  req <- httr2::request(url)
  
  try({
    # Perform the initial API request with OAuth authentication
    resp <- req %>% 
      httr2::req_oauth_client_credentials(client) %>% 
      httr2::req_perform()
    
    # Extract the JSON response body and store in a list
    locs <- list(resp %>% httr2::resp_body_json())
    
    # Get the response headers to check for pagination
    h <- resp %>% httr2::resp_headers()
    
    # Handle pagination - continue fetching pages while the X-ISI-Next-Page header exists
    # This ensures we get ALL locations, not just the first page
    while (!is.null(h[["X-ISI-Next-Page"]]))
    {
      # Request the next page using the page marker from the previous response
      resp2 <- req %>%
        httr2::req_headers("X-ISI-Start-Page" = h[["X-ISI-Next-Page"]]) %>%
        httr2::req_oauth_client_credentials(client) %>%
        httr2::req_perform()
      
      # Add this page's results to our collection
      locs <- c(locs, list(resp2 %>% httr2::resp_body_json()))
      
      # Update headers to check if there are more pages
      h <- resp2 %>% httr2::resp_headers()
    }
    
    # Process the collected data:
    # 1. Flatten the nested list structure into a dataframe
    # 2. Remove the GPS coordinates for privacy/security
    # 3. Remove any duplicate location entries
    df <- purrr::flatten_df(locs) %>%
      dplyr::select(-gps) %>%
      dplyr::filter(!duplicated(.))
    
    return(df)
  })
}