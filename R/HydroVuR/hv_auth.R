#' @title Authenticate with the HydroVu API
#'
#' @description
#' Creates an OAuth client for authenticating with the HydroVu API service. This 
#' function establishes the connection credentials needed to access water quality 
#' monitoring data from HydroVu-connected sondes and sensors. The authentication 
#' uses the OAuth 2.0 protocol to ensure secure access to the API.
#'
#' This function is typically called once at the beginning of each data collection 
#' cycle to obtain authorization for subsequent API requests.
#'
#' @param client_id A character string containing the client ID provided by HydroVu 
#' for API access. This serves as the public identifier for your application.
#'
#' @param client_secret A character string containing the client secret provided by 
#' HydroVu for API access. This serves as the password for your application and 
#' should be kept secure, typically stored in a credentials file.
#'
#' @param url A character string specifying the OAuth token endpoint URL for the 
#' HydroVu API. Default is "https://www.hydrovu.com/public-api/oauth/token".
#'
#' @return An OAuth client object from the httr2 package that can be used in 
#' subsequent API requests to retrieve water quality data.
#'
#' @examples
#' # Load credentials from a YAML file
#' hv_creds <- yaml::read_yaml("creds/HydroVuCreds.yml")
#'
#' # Create the authentication client
#' hv_token <- hv_auth(client_id = as.character(hv_creds["client"]),
#'                    client_secret = as.character(hv_creds["secret"]))
#'
#' @seealso [api_puller()]

hv_auth <- function(client_id, client_secret, url = "https://www.hydrovu.com/public-api/oauth/token") {
  
  # Create an OAuth client object for the HydroVu API
  client <- httr2::oauth_client(client_id, token_url = url,
                               secret = client_secret)
  
  # Return the client object for use in subsequent API requests
  return(client)

}