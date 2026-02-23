#' Load site order definitions from a YAML file
#' @export
#'
#' @description
#' Reads a YAML or CSV file that defines upstream and downstream relationships
#' for each site and returns a named list of site order vectors. If the site is in an independent network,
#' the name of the list should be the name of that site to avoid confusion/duplicate searches.
#'
#' For each network, this function constructs a vector containing the upstream sites
#' (if defined), the site itself, and the downstream sites (if defined).
#'
#' @details
#' ## Example YAML file format
#' See template in inst/templates/template_site_orders.yaml:
#' \preformatted{
#' network:
#'   clp:
#'     joei, cbri, chd, pfal, pbr, pman, pbd, bellvue, salyer, udall, riverbend, cottonwood, elc, archery, riverbluffs
#'   springcreek:
#'     riverbend, springcreek, cottonwood
#'   boxcreek:
#'     elc, boxcreek, archery
#'   sfm:
#'     sfm
#'   mtn_campus:
#'     mtn_campus
#' }
#'
#' ## Example CSV format
#' See template in inst/templates/template_site_orders.csv:
#' \preformatted{
#' network_name,network
#' clp,"joei, cbri, chd, pfal, pbr, pman, pbd, bellvue, salyer, udall, riverbend, cottonwood, elc, archery, riverbluffs"
#' springcreek,"riverbend, springcreek, cottonwood"
#' boxcreek,"elc, boxcreek, archery"
#' sfm,sfm
#' mtn_campus,mtn_campus
#' }
#'
#' ## Expected Outputs
#' \preformatted{
#' site_order_list <- load_network_order("site_orders.yaml")
#' site_order_list$clp
#' # "joei" "cbri" "chd" "pfal" "pbr" "pman" "pbd" "bellvue" "salyer" "udall" "riverbend" "cottonwood" "elc" "archery" "riverbluffs"
#'
#' site_order_list$sfm
#' # "sfm"
#' }
#'
#' @param file_path Path to a YAML or CSV file containing site relationships.
#'
#' @return A named list where each element corresponds to a network/system and contains
#' a character vector of that network's ordered sites from upstream to downstream.
#'
#' @seealso [network_check()]
#'

load_network_order <- function(file_path){
  # Make sure file exists
  if(!file.exists(file_path)){
    stop(paste0("Site order file not found at path: ", file_path))
  }
  # Check file extension
  file_ext <- stringr::str_split_i(file_path, "\\.", 2)
  if(!(file_ext %in% c("yaml", "yml", "csv"))){
    stop("Site order file must be a YAML (.yaml/.yml) or CSV (.csv) file.")
  }

  # Do yaml parsing
  if(file_ext %in% c("yaml", "yml")){
    # Load site order from YAML file
    site_order_raw <- tryCatch({
      yaml::read_yaml(file_path)$network
    },
    error = function(e) {
      stop(paste0("Error reading YAML file: ", e$message))
    })

    # Validate structure of loaded YAML
    if(!is.list(site_order_raw) || length(site_order_raw) == 0){
      stop("YAML file does not contain a valid 'sites' list.")
    }

    # Convert network string into separated, trimmed character vectors
    site_order <- map(site_order_raw, function(sites){
      stringr::str_split(sites, ",")[[1]] %>%
        stringr::str_trim()
    })
    return(site_order)

  }
  if(file_ext == "csv"){
    site_order_raw <- tryCatch({
      readr::read_csv(file_path, show_col_types = FALSE)
    },
    error = function(e) {
      stop(paste0("Error reading CSV file: ", e$message))
    })

    # Validate structure of loaded CSV
    if(!all(c("network_name", "network") %in% colnames(site_order_raw))){
      stop("CSV file must contain 'network_name' and 'network' columns.")
    }

    # Convert dataframe to named list
    site_order <- site_order_raw %>%
      # Convert network string into separated, trimmed character vectors
      dplyr::mutate(network = stringr::str_split(network, ",")) %>%
      dplyr::mutate(network = purrr::map(network, ~stringr::str_trim(.x))) %>%
      tibble::deframe()
    return(site_order)

  }
}
