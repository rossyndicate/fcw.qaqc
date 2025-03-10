# B Steele's original hv_auth function
hv_auth <- function (client_id, client_secret, url = "https://www.hydrovu.com/public-api/oauth/token") {
  client <- httr2::oauth_client(client_id, token_url = url, 
                                secret = client_secret)
  return(client)
}
