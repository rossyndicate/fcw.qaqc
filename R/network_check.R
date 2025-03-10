#' @title Network Check
#'
#' @description
#' This function performs a network check on a given data frame, flagging potential
#' issues in the data based on upstream and downstream sites.
#'
#' @param df A site-parameter data frame that has gone through the initial flagging
#' process.
#' @param network Whether the network check is happening across all sites or CSU sites.
#' @return A modified data frame flags that have been altered based on the network check.
#'
#' @examples
#' network_check(df = all_data_flagged$`archery-Actual Conductivity`)

network_check <- function(df, network = "all") {

  # get the site name from the site column in the df of interest
  site_name <- unique(na.omit(df$site))
  # get the parameter from the parameter column in the df of interest
  parameter_name <- unique(na.omit(df$parameter))
  
  if(network  %in% c("csu", "CSU", "fcw", "FCW") & parameter_name == "Chl-a Fluorescence"){
    
    no_change <- df %>%
      dplyr::mutate(auto_flag = flag)
    
    return(no_change)
    
  }

  # vector of sites in the order that they are in spatially
  # some sites have some funkiness going on
if(network  %in% c("csu", "CSU", "fcw", "FCW")){
  sites_order <- c("tamasag", # rist
                   "legacy",
                   "lincoln",
                   "timberline",
                   "prospect",
                   "boxelder", # elc
                   "archery",
                   "riverbluffs")

  width_fun = ifelse(site_name == "tamasag", 17, # 2 hours before/after
              ifelse(site_name == "legacy", 17,
              ifelse(site_name == "lincoln", 17,
              ifelse(site_name == "timberline", 17,
              ifelse(site_name == "prospect", 17,
              ifelse(site_name == "boxelder", 17,
              ifelse(site_name == "archery", 17,
              ifelse(site_name == "riverbluffs", 17, NA))))))))

} else if(network %in% c("all", "All")){

    sites_order <-  c("joei",
                      "cbri",
                      "chd",
                      "pfal",
                      "pbd",
                      "tamasag",
                      "legacy",
                      "lincoln",
                      "timberline",
                      #"springcreek",
                      "prospect",
                      "boxelder",
                      #boxcreek,"
                      "archery",
                      "riverbluffs")

    width_fun = ifelse(site_name == "joei", 17, # 2 hours before/after
                ifelse(site_name == "cbri", 17,
                ifelse(site_name == "chd", 17,
                ifelse(site_name == "pfal", 17,
                ifelse(site_name == "pbd", 17,
                ifelse(site_name == "sfm", 17,
                ifelse(site_name == "lbea", 17,
                ifelse(site_name == "penn", 17,
                ifelse(site_name == "tamasag", 17,
                ifelse(site_name == "legacy", 17,
                ifelse(site_name == "lincoln", 17,
                ifelse(site_name == "timberline", 17,
                ifelse(site_name == "timberline virridy", 17,
                ifelse(site_name == "springcreek", 17,
                ifelse(site_name == "prospect", 17,
                ifelse(site_name == "prospect virridy", 17,
                ifelse(site_name == "boxelder", 17,
                ifelse(site_name == "boxcreek", 17,
                ifelse(site_name == "archery", 17,
                ifelse(site_name == "archery virridy", 17,
                ifelse(site_name == "riverbluffs", 17, NA)))))))))))))))))))))

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

  # determining the index for the site of interest.
  site_index <- which(sites_order == sites_order[grep(gsub(" virridy", "", site_name), sites_order, ignore.case = TRUE)])

  # Generating df name to pull from df_list list
  site_param <- paste0(site_name, "-", parameter_name)

  prev_site_df <- tibble::tibble(DT_round = NA)
  next_site_df <- tibble::tibble(DT_round = NA)

  tryCatch({
    previous_site <- paste0(sites_order[site_index-1],"-",parameter_name)
    prev_site_df <- intersensor_flags[[previous_site]] %>%
      dplyr::select(DT_round, site_up = site, flag_up = flag) %>%
      data.table::data.table()},
    error = function(err) {
      cat(paste0(site_name," has no upstream site with ", parameter_name, ".\n"))})

  tryCatch({
    next_site <- paste0(sites_order[site_index+1],"-",parameter_name)
    next_site_df <- intersensor_flags[[next_site]] %>%
      dplyr::select(DT_round, site_down = site, flag_down = flag) %>%
      data.table::data.table()},
    error = function(err) {
      cat(paste0(site_name, " has no downstream site with ", parameter_name, ".\n"))})


  join <- df %>%
    dplyr::left_join(., prev_site_df, by = "DT_round") %>%
    dplyr::left_join(., next_site_df, by = "DT_round")

  if(!("flag_down" %in% colnames(join))) {join$flag_down <- NA}
  if(!("flag_up" %in% colnames(join))) {join$flag_up <- NA}
  if(!("site_down" %in% colnames(join))) {join$site_down <- NA}
  if(!("site_up" %in% colnames(join))) {join$site_up <- NA}

  # Define a function to check if a given 2-hour window has any instances of the same word
  check_2_hour_window_fail <- function(x) {
    sum(x) >= 1
  }

  # Function to add a column if it doesn't already exist
  add_column_if_not_exists <- function(df, column_name, default_value = NA) {
    if (!column_name %in% colnames(df)) {
      df <- df %>% dplyr::mutate(!!sym(column_name) := default_value)
    }
    return(df)
  }
  
  df_test <- join %>%
    # No upstream/downstream flag = 0
    dplyr::mutate(flag_binary = ifelse(
      (is.na(flag_up) | grepl("drift|DO interference|repeat|sonde not employed|frozen|unsubmerged|missing data|site visit|sv window|sensor malfunction|burial|sensor biofouling|improper level cal|sonde moved", flag_up)) &
        (is.na(flag_down) | grepl("drift|DO interference|repeat|sonde not employed|frozen|unsubmerged|missing data|site visit|sv window|sensor malfunction|burial|sensor biofouling|improper level cal|sonde moved", flag_down)), 0, 1)) %>%
    dplyr::mutate(overlapping_flag = zoo::rollapply(flag_binary, width = width_fun, FUN = check_2_hour_window_fail, fill = NA, align = "center")) %>%
    add_column_if_not_exists(column_name = "auto_flag") %>%
    # If there is a flag (flags associated with spikes in concentration or funkiness like that), and there is also a flag up/downstream at the same time (2 hour window) it is likely a real
    # WQ event and should therefore not be considered "poor" data:
    dplyr::mutate(auto_flag = ifelse(!is.na(flag) & !grepl("drift|DO interference|repeat|sonde not employed|frozen|unsubmerged|missing data|site visit|sv window|sensor malfunction|burial|sensor biofouling|improper level cal|sonde moved", flag) & 
                                       (overlapping_flag == TRUE & !is.na(overlapping_flag)), NA, flag)) %>%
    dplyr::select(-c(flag_up, flag_down, site_up, site_down, flag_binary, overlapping_flag))

    return(df_test)

}
