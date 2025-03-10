hv_names <- function (client, return = "both", url = "https://www.hydrovu.com/public-api/v1/sispec/friendlynames") {
  req <- httr2::request(url)
  resp <- req %>% httr2::req_oauth_client_credentials(client) %>% 
    httr2::req_perform()
  names <- resp %>% httr2::resp_body_json()
  p <- names[["parameters"]] %>% purrr::map_dfr(as.data.frame, 
                                                .id = "parameterId")
  names(p) <- c("parameterId", "Parameter")
  u <- names[["units"]] %>% purrr::map_dfr(as.data.frame, 
                                           .id = "unitId")
  names(u) <- c("unitId", "Units")
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
