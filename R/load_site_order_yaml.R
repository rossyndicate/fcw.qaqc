#' Load site order definitions from a YAML file
#'
#' @description
#' Reads a YAML file that defines upstream and downstream relationships
#' for each site and returns a named list of site order vectors.
#' 
#' Each site entry in the YAML file should have two keys:
#' - `upstream`: the immediate upstream site name, or an empty string "" if none
#' - `downstream`: the immediate downstream site name, or an empty string "" if none
#'
#' For each site, this function constructs a vector containing the upstream site
#' (if defined), the site itself, and the downstream site (if defined).
#' 
#' #' # Example YAML file format:
#'  sites:
#'    top_site_name:
#'      upstream: ""
#'      downstream: down_one_site_name
#' 
#'    down_one_site_name:
#'      upstream: top_site_name
#'      downstream: bottom_site_name
#' 
#'    bottom_site_name:
#'      upstream: down_one_site_name
#'     downstream: ""
#' 
#'    disconnected_site:
#'      upstream: ""
#'     downstream: ""
#'
#' Expected Outputs: 
#' site_order_list <- load_site_order_yaml("site_orders.yaml")
#' site_order_list$down_one_site_name
#' #> [1] "top_site_name" "down_one_site_name" "bottom_site_name"
#'
#' site_order_list$disconnected_site
#' #> [1] "disconnected_site"
#'
#' @param yaml_path Path to a YAML file containing site relationships.
#'
#' @return A named list where each element corresponds to a site and contains
#' a character vector of that site's upstream, self, and downstream neighbors.
#' 
#' @seealso [network_check()]
#'
#'
load_site_order_yaml <- function(yaml_path){
  # Make sure file exists
  if(!file.exists(yaml_path)){
    stop(paste0("YAML file not found at path: ", yaml_path))
  }
  # Load site order from YAML file
  site_order <- tryCatch({
    yaml::read_yaml(yaml_path)$sites
  },
  error = function(e) {
    stop(paste0("Error reading YAML file: ", e$message))
  })
  
  # Validate structure of loaded YAML
  if(!is.list(site_order) || length(site_order) == 0){
    stop("YAML file does not contain a valid 'sites' list.")
  }
  #Check that each site has upstream and downstream lists
  for(site in names(site_order)){
    if(!all(c("upstream", "downstream") %in% names(site_order[[site]]))){
      stop(paste0("Site '", site, "' is missing 'upstream' or 'downstream' keys in YAML file."))
    }
  }
  
  # site_orders should be a named list where each name is a site and each entry is a list with 'upstream' and 'downstream' keys
  #if up/downstream sites do not exist (ie "") then only return the site itself
  # Start at the site itself
  site_order_list <- purrr::map(names(site_order), function(site_name){
    
    order <- site_name
    upstream <- site_order[[site_name]]$upstream
    downstream <- site_order[[site_name]]$downstream
    
    # Check upstream
    if (upstream != "") {
      order <- c(upstream, order)
    }
    
    # Check downstream
    if (downstream != "") {
      order <- c(order, downstream)
    }
    
    return(order)
    
  })%>%
    purrr::set_names(., names(site_order))
  
  
  return(site_order_list)
}
