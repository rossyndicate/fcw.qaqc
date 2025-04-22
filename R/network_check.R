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
#' @param network Character string specifying the monitoring network to use for site 
#' relationships. Options are "all" (default), "CSU", or "FCW" (case-insensitive).
#'
#' @return A dataframe with the same structure as the input, plus an `auto_flag` column
#' that contains cleaned flags where network-wide events have been accounted for.
#'
#' @examples
#' # Examples are temporarily disabled

network_check <- function(df, network = "all") {
  # Extract site and parameter name from dataframe
  site_name <- unique(na.omit(df$site))
  parameter_name <- unique(na.omit(df$parameter))
  
  # Skip processing for chlorophyll in CSU/FCW network
  if(network  %in% c("csu", "CSU", "fcw", "FCW") & parameter_name == "Chl-a Fluorescence"){
    no_change <- df %>%
      dplyr::mutate(auto_flag = flag)
    return(no_change)
  }
  
  # Define site order based on spatial arrangement along river
  if(network  %in% c("csu", "CSU", "fcw", "FCW")){
    sites_order <- c("bellvue", # rist
                     "salyer",
                     "udall",
                     "riverbend",
                     "cottonwood",
                     "elc", # elc
                     "archery",
                     "riverbluffs")
    width_fun = 17 #2 hours before/after
  } else if(network %in% c("all", "All")){
    # More extensive site order for full network
    sites_order <-  c("joei",
                      "cbri",
                      "chd",
                      "pfal",
                      "pbd",
                      "bellvue", # rist
                      "salyer",
                      "udall",
                      "riverbend",
                      "cottonwood",
                      "elc", # elc
                      "archery",
                      "riverbluffs")
    width_fun = 17 # 2 hours before/after
    
    # Special cases for certain site groups
    if(site_name %in% c("penn", "sfm", "lbea")){
      sites_order <- c("penn",
                       "sfm",
                       "lbea")
    }
    if(site_name == "springcreek"){
      sites_order <- c("timberline virridy",
                       "springcreek",
                       "prospect virridy")
    }
    if(site_name == "boxcreek"){
      sites_order <- c("boxelder virridy",
                       "boxcreek",
                       "archery virridy")
    }
  }
  
  # Find the index of current site in ordered list
  site_index <- which(sites_order == sites_order[grep(gsub(" virridy", "", site_name), sites_order, ignore.case = TRUE)])
  
  # Create site-parameter identifier
  site_param <- paste0(site_name, "-", parameter_name)
  
  # Initialize empty dataframes for upstream/downstream sites
  prev_site_df <- tibble::tibble(DT_round = NA)
  next_site_df <- tibble::tibble(DT_round = NA)
  
  # Try to get upstream site data
  tryCatch({
    previous_site <- paste0(sites_order[site_index-1],"-",parameter_name)
    prev_site_df <- intersensor_flags[[previous_site]] %>%
      dplyr::select(DT_round, site_up = site, flag_up = flag) %>%
      data.table::data.table()},
    error = function(err) {
      cat(paste0(site_name," has no upstream site with ", parameter_name, ".\n"))}) # differentiate between when it should have one, and when it shouldn't have one
  
  # Try to get downstream site data
  tryCatch({
    next_site <- paste0(sites_order[site_index+1],"-",parameter_name)
    next_site_df <- intersensor_flags[[next_site]] %>%
      dplyr::select(DT_round, site_down = site, flag_down = flag) %>%
      data.table::data.table()},
    error = function(err) {
      cat(paste0(site_name, " has no downstream site with ", parameter_name, ".\n"))})
  
  # Join current site data with upstream and downstream data
  join <- df %>%
    dplyr::left_join(., prev_site_df, by = "DT_round") %>%
    dplyr::left_join(., next_site_df, by = "DT_round")
  
  # Add placeholder columns if joining didn't provide them
  if(!("flag_down" %in% colnames(join))) {join$flag_down <- NA}
  if(!("flag_up" %in% colnames(join))) {join$flag_up <- NA}
  if(!("site_down" %in% colnames(join))) {join$site_down <- NA}
  if(!("site_up" %in% colnames(join))) {join$site_up <- NA}
  
  # Function to check if any flags exist in a time window
  check_2_hour_window_fail <- function(x) {
    sum(x) >= 1
  }
  
  # Function to add column if it doesn't exist
  add_column_if_not_exists <- function(df, column_name, default_value = NA) {
    if (!column_name %in% colnames(df)) {
      df <- df %>% dplyr::mutate(!!sym(column_name) := default_value)
    }
    return(df)
  }
  
  ignore_flags <- "drift|DO interference|repeat|sonde not employed|frozen|
  unsubmerged|missing data|site visit|sv window|sensor malfunction|burial|
  sensor biofouling|improper level cal|sonde moved"
  
  # Process flags based on upstream/downstream patterns
  df <- join %>%
    # Create binary indicator for upstream/downstream flags
    # 0 = no relevant flags upstream/downstream, 1 = at least one site has relevant flags
    dplyr::mutate(flag_binary = ifelse(
      (is.na(flag_up) | grepl(ignore_flags, flag_up)) &
        (is.na(flag_down) | grepl(ignore_flags, flag_down)), 0, 1)) %>%
    # Check for flags in 2-hour window (17 observations at 15-min intervals)
    dplyr::mutate(overlapping_flag = zoo::rollapply(flag_binary, width = width_fun, FUN = check_2_hour_window_fail, fill = NA, align = "center")) %>%
    add_column_if_not_exists(column_name = "auto_flag") %>%
    # If flag exists but is also present up/downstream, it likely represents a real event
    # In that case, remove the flag (set auto_flag to NA)
    dplyr::mutate(auto_flag = ifelse(!is.na(flag) & !grepl(ignore_flags, flag) & 
                                       (overlapping_flag == TRUE & !is.na(overlapping_flag)), NA, flag)) %>%
    dplyr::select(-c(flag_up, flag_down, site_up, site_down, flag_binary, overlapping_flag))
  
  return(df)
}
