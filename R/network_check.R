#' @title Reduce overflagging by comparing across monitoring network
#' @export
#'
#' @description
#' Identifies and corrects overflagging by comparing data patterns across upstream 
#' and downstream monitoring sites. When quality flags at a site coincide with similar 
#' patterns at neighboring sites, these flags are often indicating real water quality 
#' events rather than sensor malfunctions and can be removed.
#'
#' Different site configurations are handled based on the monitoring network specified.
#' For chlorophyll measurements in the FCW/CSU network, no changes are made as these
#' measurements are site-specific.
#'
#' @param df A site-parameter dataframe that has undergone initial flagging. Must include:
#' - `site`: Standardized site name
#' - `parameter`: The measurement type 
#' - `DT_round`: Timestamp for measurements
#' - `flag`: Existing quality flags
#' 
#' @param intrasensor_flags_arg A list of data frames that have gone through the 
#' intra-sensor flagging functions which are indexed by their corresponding 
#' site-parameter combination.
#' @param site_order_arg A list defining the order of sites in the network. This requires 
#' a user create a yaml or csv file with formatting consistent 
#' with the example in `load_network_order()` and to create a site_order_list object using `load_network_order()`
#'
#' @return A dataframe with the same structure as the input, plus an `auto_flag` column
#' that contains cleaned flags where network-wide events have been accounted for.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [load_network_order()]

network_check <- function(df, intrasensor_flags_arg = intrasensor_flags, site_order_arg = site_order_list){
  
  # Extract site and parameter name from dataframe
  site_name <- unique(na.omit(df$site))
  parameter_name <- unique(na.omit(df$parameter))
  
  #Check if site_order_arg exists and is a list
  if(missing(site_order_arg) || !is.list(site_order_arg)){
    stop("site_order_arg is missing or not a list. Please provide a valid site order list.")
  }

  #grab the lists that have the site name in them
  sites_order <- site_order_arg %>%
    keep(~ any(.x == site_name))
  #if no site order is found for the site, return original dataframe with message
  if(length(sites_order) == 0){
    message(paste0("Skipping network check (No site order found in site_order_arg) for: ", site_name, "\nPlease check the site name and site order list."))
    return(df %>% mutate(auto_flag = flag))
  }
  #if a list that matches the site name, use that one otherwise use the first one
  if (site_name %in% names(sites_order)) {
    sites_order <- sites_order[[site_name]]
  } else {
    sites_order <- sites_order[[1]]
  }
  
  # If no site order is found for the site (ie only one site in network list), return original dataframe with message
  if(length(sites_order) ==  1){
    message(paste0("Skipping network check (no upstream/downstream relationship) for: ", site_name))
    return(df %>% mutate(auto_flag = flag))
  }
  
  # Find the index of current site in ordered list
  site_index <- which(sites_order == sites_order[grep(site_name, sites_order, ignore.case = TRUE)])
  
  # Create site-parameter identifier
  site_param <- paste0(site_name, "-", parameter_name)
  
  # Initialize empty dataframes for upstream/downstream sites
  upstr_site_df <- tibble::tibble(DT_round = NA) # Upstream sites
  dnstr_site_df <- tibble::tibble(DT_round = NA) # Downstream sites
  
  # Try to get upstream site data
  tryCatch({
    # Skip trying to find upstream sites for first site (ie top of monitoring network).
    if (site_index != 1){
      previous_site <- paste0(sites_order[site_index-1],"-",parameter_name)
      upstr_site_df <- intrasensor_flags_arg[[previous_site]] %>%
        dplyr::select(DT_round, site_up = site, flag_up = flag) %>%
        data.table::data.table()
    }
  },
  error = function(err) {
    message(paste0("No UPSTREAM data found for ", site_param, ". Expected at site '", previous_site, "'."))
  }) 
  
  # Try to get downstream site data
  tryCatch({
    # Skip trying to find downstream sites for last site (ie bottom of monitoring network).
    if (site_index != length(sites_order)){
      next_site <- paste0(sites_order[site_index+1],"-",parameter_name)
      dnstr_site_df <- intrasensor_flags_arg[[next_site]] %>%
        dplyr::select(DT_round, site_down = site, flag_down = flag) %>%
        data.table::data.table()
    }
  },
  error = function(err) {
    message(paste0("No DOWNSTREAM data found for ", site_param, ". Expected at site '", next_site, "'."))
  })
  
  # Join current site data with upstream and downstream data
  up_down_join <- df %>%
    dplyr::left_join(upstr_site_df, by = "DT_round") %>%
    dplyr::left_join(dnstr_site_df, by = "DT_round")
  
  # <<< Establish helper functions >>> ----
  
  # Function to check if any flags exist in a time window
  check_2_hour_window_fail <- function(x) {
    sum(x) >= 1
  }
  
  # <<< Establish helper objects >>> ----
  
  # String object that is used to ignore flags that we do not want to remove.
  ignore_flags <- "drift|DO interference|repeat|sonde not employed|frozen|
  unsubmerged|missing data|site visit|sv window|sensor malfunction|burial|
  sensor biofouling|improper level cal|sonde moved"
  
  # Numeric object that determines the width for the rolling window check (2 hours)
  width_fun = 17 
  
  # <<< Process flags based on upstream/downstream patterns >>> ----
  final_df <- up_down_join %>%
    # Add placeholder columns if joinging didn't provide them
    add_column_if_not_exists("flag_down") %>% 
    add_column_if_not_exists("flag_up") %>% 
    add_column_if_not_exists("site_down") %>% 
    add_column_if_not_exists("site_up") %>% 
    # Create binary indicator for upstream/downstream flags
    ## 0 = no relevant flags upstream/downstream
    ## 1 = at least one site has relevant flags
    dplyr::mutate(flag_binary = dplyr::if_else(
      (is.na(flag_up) | grepl(ignore_flags, flag_up)) & 
        (is.na(flag_down) | grepl(ignore_flags, flag_down)), 0, 1
    )) %>%
    # Check for flags in 4-hour window (+/-2 hours around each point, 17 observations at 15-min intervals)
    dplyr::mutate(overlapping_flag = zoo::rollapply(flag_binary, width = width_fun, FUN = check_2_hour_window_fail, fill = NA, align = "center")) %>%
    add_column_if_not_exists(column_name = "auto_flag") %>%
    # If flag exists but is also present up/downstream, it likely represents a real event
    # In that case, remove the flag (set auto_flag to NA)
    dplyr::mutate(auto_flag = ifelse(!is.na(flag) & !grepl(ignore_flags, flag) & 
                                       (overlapping_flag == TRUE & !is.na(overlapping_flag)), NA, flag)) %>%
    dplyr::select(-c(flag_up, flag_down, site_up, site_down, flag_binary, overlapping_flag))
  
  return(final_df)
}
