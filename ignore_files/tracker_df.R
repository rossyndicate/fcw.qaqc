# if site is still using the default start date then its no good?
library(tidyverse)

# 1. Setup - Initial configuration files and tracking objects

# Manual configuration - allows manual override for known issues
operational_site_parameters <- tribble(
  ~site,              ~Temperature, ~DO,   ~pH,   ~ORP,  ~`Specific Conductivity`, ~Turbidity, ~Depth, ~`Chl-a Fluorescence`,
  "bellvue",          TRUE,         TRUE,  TRUE,  TRUE,  TRUE,                    TRUE,       TRUE,   TRUE,
  "salyer",           TRUE,         TRUE,  TRUE,  TRUE,  TRUE,                    TRUE,       TRUE,   TRUE,
  "udall",            TRUE,         TRUE,  TRUE,  TRUE,  TRUE,                    TRUE,       TRUE,   TRUE,
  "riverbend",        TRUE,         TRUE,  TRUE,  TRUE,  TRUE,                    TRUE,       TRUE,   TRUE,
  "cottonwood",       TRUE,         TRUE,  TRUE,  TRUE,  TRUE,                    TRUE,       TRUE,   TRUE,
  "elc",              TRUE,         TRUE,  TRUE,  TRUE,  TRUE,                    TRUE,       TRUE,   TRUE,
  "archery",          TRUE,         TRUE,  TRUE,  TRUE,  TRUE,                    TRUE,       TRUE,   TRUE,
  "riverbluffs",      TRUE,         TRUE,  TRUE,  TRUE,  TRUE,                    TRUE,       TRUE,   TRUE
)

# Create failure tracking dataframe - Each site-parameter combination with a counter
sites <- c("bellvue", "salyer", "udall", "riverbend", "cottonwood", "elc", "archery", "riverbluffs")
parameters <- c(
  "Temperature", "DO", "pH", "ORP", "Specific Conductivity", "Turbidity", "Depth", "Chl-a Fluorescence"
)
hv_api_pull_failure_tracker <- crossing(site = sites, parameter = parameters) %>% 
  mutate(
    failure_count = 0,             # Count of consecutive failures
    last_success = as.POSIXct("1970-01-01 00:00:00", tz = "UTC"),     # Date of last successful data pull
    auto_disabled = FALSE          # Whether this combo was auto-disabled
  )

# Save these initial configurations
# saveRDS(operational_site_parameters, "qaqc_files/site_parameters_config.rds")
# saveRDS(hv_api_pull_failure_tracker, "qaqc_files/failure_tracker.rds")

# 2. Modified get_start_dates function that respects site configuration

get_start_dates_modified <- function(incoming_historically_flagged_data_list, 
                                     site_config = readRDS("qaqc_files/site_parameters_config.rds"),
                                     failure_tracking = readRDS("qaqc_files/failure_tracker.rds")) {
  
  # Create the start date directly in Denver time
  default_denver_date <- as.POSIXct(paste0(lubridate::year(Sys.time()), "-03-01"), tz = "America/Denver")
  
  # Convert the start date to UTC for API use
  default_converted_start_DT <- lubridate::with_tz(default_denver_date, "UTC")
  
  # Get only active sites from the config
  active_sites <- site_config %>%
    # Convert from wide to long format to make filtering easier
    pivot_longer(cols = -site, names_to = "parameter", values_to = "active") %>%
    filter(active == TRUE) %>%
    # Create a unique identifier for each site-parameter combination
    mutate(site_param = paste0(site, "-", parameter))
  
  # Generate default start dates for active sites only
  active_site_names <- unique(active_sites$site)
  default_start_dates <- tibble(
    site = active_site_names,
    start_DT = default_converted_start_DT, 
    end_DT = as.POSIXct(Sys.time(), tz = "UTC") 
  )
  
  # If no historical data is provided, return default dates for active sites only
  if (length(incoming_historically_flagged_data_list) == 0) {
    return(default_start_dates)
  }
  
  # Extract only Temperature parameter dataframes from active sites
  active_temp_params <- active_sites %>% 
    filter(parameter == "Temperature") %>%
    pull(site_param)
  
  temperature_subset <- intersect(
    names(incoming_historically_flagged_data_list),
    active_temp_params
  )
  
  # Extract each active site's most recent timestamp based on temperature data
  temp_start_dates_df <- incoming_historically_flagged_data_list[temperature_subset] %>% 
    dplyr::bind_rows() %>% 
    dplyr::mutate(DT_round = lubridate::with_tz(DT_round, "UTC")) %>% 
    dplyr::group_by(site) %>% 
    dplyr::slice_max(DT_round) %>% 
    dplyr::select(start_DT = DT_round, site) %>% 
    dplyr::ungroup()
  
  # Extract each site's most recent timestamp across all active parameters
  active_params <- active_sites$site_param
  all_params_dates_df <- incoming_historically_flagged_data_list[active_params] %>% 
    dplyr::bind_rows() %>% 
    dplyr::mutate(DT_round = lubridate::with_tz(DT_round, "UTC")) %>% 
    dplyr::group_by(site) %>% 
    dplyr::slice_max(DT_round) %>% 
    dplyr::select(start_DT = DT_round, site) %>% 
    dplyr::ungroup()
  
  # Check if temperature dates equal all parameter dates for active sites
  test_dates <- dplyr::full_join(temp_start_dates_df, all_params_dates_df, by = "site") %>% 
    dplyr::mutate(dates_equal = start_DT.x == start_DT.y)
  
  all_dates_equal <- all(test_dates$dates_equal, na.rm = TRUE) # maybe NA's tell us something here?
  temp_is_earlier <- min(temp_start_dates_df$start_DT) <= min(all_params_dates_df$start_DT)
  
  # Choose which dates to use based on equality test and timestamps
  selected_dates_df <- if (!all_dates_equal && !temp_is_earlier) {
    # If dates DON'T match AND parameter dates are earlier than temperature dates
    warning("Using parameter dates because they're earlier than temperature dates.")
    all_params_dates_df
  } else {
    # Either ALL dates match OR temperature dates are earlier
    cat("Using start dates from temperature data (preferred when possile).")
    temp_start_dates_df
  }
  
  # Find global minimum to use as common start date for all sites
  global_min_startDT <- min(selected_dates_df$start_DT)
  
  # Update default dataframe with historical data where available, but only for active sites
  final_start_dates_df <- default_start_dates %>% 
    dplyr::left_join(selected_dates_df, by = "site") %>% 
    dplyr::mutate(
      start_DT = global_min_startDT,
      end_DT = as.POSIXct(Sys.time(), tz = "UTC")
    ) %>% 
    dplyr::select(-c(start_DT.y, start_DT.x)) %>% 
    dplyr::relocate(site, start_DT, end_DT)
  
  return(final_start_dates_df)
}

# 3. Function to update failure tracker after API pulls

update_hv_failure_tracker <- function(post_munged_data_list, 
                                   failure_tracking = readRDS("qaqc_files/failure_tracker.rds"),
                                   threshold = 56) { # 56 3-hour chunks in a week
  
  data_tracker_df <- imap_dfr(post_munged_data_list, function(df, idx){
    result <- df %>% 
      slice_max(DT_round) %>% 
      select(site, parameter, DT_round)
  }) 
  # Assuming pull_results is a dataframe with columns:
  # site, parameter, success (logical), data_count (numeric)
  
  # Update the failure tracker
  updated_tracker <- failure_tracking %>%
    left_join(pull_results, by = c("site", "parameter")) %>%
    mutate(
      # If success is TRUE, reset counter, otherwise increment
      failure_count = if_else(success == TRUE, 0, failure_count + 1),
      # Update last success date if successful
      last_success = if_else(success == TRUE, Sys.Date(), last_success),
      # Mark as auto-disabled if failure count exceeds threshold
      auto_disabled = failure_count >= threshold,
      # Clean up after the join
      success = NULL,
      data_count = NULL
    )
  
  # Save the updated tracker
  # saveRDS(updated_tracker, "qaqc_files/failure_tracker.rds")
  
  # Also update the configuration file based on auto-disabled status
  site_config <- readRDS("qaqc_files/site_parameters_config.rds")
  
  # Get entries that need to be auto-disabled
  new_disabled <- updated_tracker %>%
    filter(auto_disabled == TRUE & failure_count == threshold) %>%
    select(site, parameter)
  
  if (nrow(new_disabled) > 0) {
    # Convert to wide format to match site_config structure
    disabled_wide <- new_disabled %>%
      mutate(active = FALSE) %>%
      pivot_wider(
        id_cols = site,
        names_from = parameter,
        values_from = active
      )
    
    # Update the site configuration
    for (i in 1:nrow(disabled_wide)) {
      site_row <- which(site_config$site == disabled_wide$site[i])
      if (length(site_row) > 0) {
        # Update each disabled parameter
        for (param in names(disabled_wide)[-1]) {
          if (!is.na(disabled_wide[[param]][i])) {
            site_config[[param]][site_row] <- FALSE
            message(paste0("Auto-disabled ", disabled_wide$site[i], "-", param, 
                           " after ", threshold, " consecutive failures."))
          }
        }
      }
    }
    
    # Save the updated configuration
    # saveRDS(site_config, "qaqc_files/site_parameters_config.rds")
  }
  
  return(updated_tracker)
}

# 4. Function to process API pull results and generate success/failure data

process_api_results <- function(api_data_list) {
  # This function would analyze the results from api_puller to determine
  # which site-parameter combinations successfully received data
  
  # Example implementation (adjust based on your actual data structure):
  results <- map_dfr(names(api_data_list), function(name) {
    # Parse site and parameter from the name (assuming format "site-parameter")
    parts <- str_split(name, "-", simplify = TRUE)
    site <- parts[1]
    parameter <- parts[2]
    
    # Check if data was received and how much
    data <- api_data_list[[name]]
    success <- !is.null(data) && nrow(data) > 0
    data_count <- if(success) nrow(data) else 0
    
    tibble(
      site = site,
      parameter = parameter,
      success = success,
      data_count = data_count
    )
  })
  
  return(results)
}

# 5. Complete workflow with modified api_puller that respects site configuration

modified_api_workflow <- function() {
  # Load configurations
  site_config <- readRDS("qaqc_files/site_parameters_config.rds")
  failure_tracking <- readRDS("qaqc_files/failure_tracker.rds")
  
  # Load historical data
  if (file.exists("test_data/pwqn_data.RDS")) {
    historical_data <- readRDS("test_data/pwqn_data.RDS") %>%
      mutate(auto_flag = as.character(auto_flag)) %>%
      split(f = list(.$site, .$parameter), sep = "-") %>%
      purrr::keep(~!is.null(.) && nrow(.) > 0)
  } else {
    historical_data <- list()
  }
  
  # Get start dates respecting site configuration
  api_start_dates <- get_start_dates_modified(
    incoming_historically_flagged_data_list = historical_data,
    site_config = site_config,
    failure_tracking = failure_tracking
  )
  
  # Authenticate with HydroVu
  hv_creds <- yaml::read_yaml("creds/HydroVuCreds.yml")
  hv_token <- hv_auth(
    client_id = as.character(hv_creds["client"]),
    client_secret = as.character(hv_creds["secret"])
  )
  
  # Pull data only for active sites
  all_sites_up_to_date <- TRUE
  purrr::pwalk(api_start_dates, function(site, start_DT, end_DT) {
    # Get active parameters for this site
    active_params <- site_config %>%
      filter(site == !!site) %>%
      pivot_longer(cols = -site, names_to = "parameter", values_to = "active") %>%
      filter(active == TRUE) %>%
      pull(parameter)
    
    # Only pull data if there are active parameters
    if (length(active_params) > 0) {
      api_puller(
        site = site,
        start_dt = start_DT,
        end_dt = end_DT,
        api_token = hv_token,
        dump_dir = "test_data/api",
        network = "FCW",
        synapse_env = FALSE,
        fs = NULL
      )
    } else {
      message(paste0("Skipping site ", site, " as it has no active parameters."))
    }
  })
  
  # Munge and process the API data
  api_data <- munge_api_data(
    api_path = "test_data/api",
    network = "FCW"
  )
  
  # Split data by site-parameter
  new_data <- api_data %>%
    split(f = list(.$site, .$parameter), sep = "-") %>%
    purrr::keep(~ nrow(.) > 0)
  
  # Process results to determine success/failure
  pull_results <- process_api_results(new_data)
  
  # Update failure tracker
  updated_tracker <- update_failure_tracker(
    pull_results = pull_results,
    failure_tracking = failure_tracking,
    threshold = 3  # Auto-disable after 3 consecutive failures
  )
  
  # Continue with the rest of your workflow...
  # ...
  
  return(list(
    updated_tracker = updated_tracker,
    new_data = new_data
  ))
}