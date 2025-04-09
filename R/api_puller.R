#' @title Download water quality data from HydroVu API
#'
#' @description
#' Downloads raw water quality monitoring data from the HydroVu platform for 
#' specified sites and time periods. This function handles the connection to 
#' HydroVu, processes the API responses, and saves the raw data as CSV files 
#' in a specified directory.
#' 
#' The function can retrieve data from different networks (FCW, CSU, or Virridy), 
#' with appropriate filtering applied depending on the network. It also handles 
#' special cases such as sites with multiple sondes from different networks.
#'
#' @param site Character string or vector specifying the site name(s) to download 
#' data for. Site names are matched case-insensitively against the HydroVu 
#' location names.
#'
#' @param network Character string specifying which network of sensors to query. 
#' Options include "FCW", "CSU", "virridy", or "all". Different networks may have 
#' different data processing requirements.
#'
#' @param start_dt POSIXct timestamp indicating the starting point for data 
#' retrieval. Usually derived from the most recent timestamp in existing 
#' historical data.
#'
#' @param end_dt POSIXct timestamp indicating the endpoint for data retrieval. 
#' Default is the current system time (Sys.time()).
#'
#' @param api_token OAuth client object obtained from hv_auth() function, used 
#' for authentication with the HydroVu API.
#'
#' @param dump_dir Character string specifying the directory path where 
#' downloaded CSV files should be saved.
#'
#' @return No direct return value. The function writes CSV files to the specified 
#' dump_dir, with filenames formatted as "sitename_timestamp.csv".
#'
#' @examples
#' # Get start dates from historical data
#' start_dates <- get_start_dates(incoming_historically_flagged_data_list = historical_data)
#'
#' # Authenticate with HydroVu
#' hv_token <- hv_auth(client_id = as.character(hv_creds["client"]),
#'                     client_secret = as.character(hv_creds["secret"]))
#'
#' # Download data for a specific site
#' api_puller(site = "riverbluffs",
#'            network = "FCW", 
#'            start_dt = start_dates$DT_round[start_dates$site == "riverbluffs"],
#'            api_token = hv_token,
#'            dump_dir = "data/api")
#'
#' @seealso [get_start_dates()]
#' @seealso [hv_auth()]
#' @seealso [munge_api_data()]

api_puller <- function(site, network, start_dt, end_dt = Sys.time(), api_token, dump_dir) {

  # Retrieve appropriate sensor locations based on the requested network
  # The "all" or "virridy" options will include all available locations
  # while "CSU" or "FCW" will filter out Virridy-specific locations
  if(network %in% c("all", "All", "virridy", "Virridy")){
    locs <- hv_locations_all(hv_token)
  } else if(network %in% c("csu", "fcw", "CSU", "FCW")){
    locs <- hv_locations_all(hv_token) %>%
      dplyr::filter(!grepl("virridy", name, ignore.case = TRUE))
  }

  # Suppress scientific notation to ensure consistent formatting
  options(scipen = 999)
  
  # Loop through each site to retrieve and save data
  for(i in 1:length(site)){

    # Special handling for "River Bluffs" site which has inconsistent naming in HydroVu
    # (sometimes with space, sometimes without)
    if(tolower(site[i]) == "riverbluffs" | tolower(site[i]) == "river bluffs"){
      site_loc <- locs %>%
        dplyr::mutate(name = tolower(name)) %>%
        dplyr::filter(grepl("River Bluffs|RiverBluffs", name, ignore.case = TRUE))
    } else {
      # For other sites, filter locations that contain the site name
      site_loc <- locs %>%
        dplyr::mutate(name = tolower(name)) %>%
        dplyr::filter(grepl(site[i], name, ignore.case = TRUE))
    }

    # Extract the HydroVu location IDs for API requests
    site_loc_list <- site_loc$id

    # Convert timestamps to UTC format for API compatibility
    # The +0 hours ensures proper timezone handling without adjusting the time
    utc_start_date <- format(as.POSIXct(start_dt, tz = "UTC") + lubridate::hours(0), format = "%Y-%m-%d %H:%M:%S")
    utc_end_date <- format(as.POSIXct(end_dt, tz = "UTC") + lubridate::hours(0), format = "%Y-%m-%d %H:%M:%S")
    timezone <- "UTC"

    # Request data for each location ID within the specified time period
    # Note: 404 errors are expected for inactive locations and handled below
    alldata <- site_loc_list %>% purrr::map(~hv_data_id(.,
                                                      start_time = utc_start_date,
                                                      end_time = utc_end_date,
                                                      token = api_token,
                                                      tz = timezone))

    # Filter out error responses (404s) and keep only valid data frames
    filtered <- purrr::keep(alldata, is.data.frame)

    # If no data was found for this site during the time period, report and continue
    if(length(filtered) == 0){
      print(paste0("No data at ", site[i], " during this time frame"))
    } else {
      # Combine all dataframes, standardize column names, and join with location metadata
      one_df <- dplyr::bind_rows(filtered) %>%
        data.table::data.table() %>%
        dplyr::rename(id = Location,
                      parameter = Parameter,
                      units = Units) %>%
        dplyr::left_join(., site_loc, by = "id") %>%
        dplyr::mutate(site = tolower(site[i])) %>%
        dplyr::select(site, id, name, timestamp, parameter, value, units)

      # Handle network-specific data saving procedures
      if(network %in% c("csu", "CSU", "fcw", "FCW")){
        # For FCW/CSU networks, exclude Virridy sensors and FDOM parameter
        readr::write_csv(one_df %>% dplyr::filter(!grepl("virridy", name, ignore.case = TRUE),
                                                parameter != "FDOM Fluorescence"),
                       here(dump_dir, paste0(site[i], "_", stringr::str_replace(stringr::str_replace(substr(end_dt, 1, 16), "[:\\s]", "_"), ":", ""), ".csv")))

      } else if(network %in% c("all","All","Virridy","virridy")){
        # Special handling for sites that may have both CSU and Virridy sondes
        if(site[i] %in% c("Timberline", "Prospect", "Archery")){
          # Save Virridy sonde data separately
          try(virridy_df <- one_df %>%
                dplyr::filter(grepl("virridy", name, ignore.case = TRUE)) %>%
                dplyr::mutate(site = paste0(site[i], " virridy")))

            try(readr::write_csv(virridy_df,
                     here(dump_dir, paste0(site[i], " virridy_", stringr::str_replace(stringr::str_replace(substr(end_dt, 1, 16), "[:\\s]", "_"), ":", ""), ".csv"))))

          # Save CSU sonde data separately
          csu_df <- one_df %>%
            dplyr::filter(!grepl("virridy", name, ignore.case = TRUE))

            readr::write_csv(csu_df,
                   here(dump_dir, paste0(site[i], "_", stringr::str_replace(stringr::str_replace(substr(end_dt, 1, 16), "[:\\s]", "_"), ":", ""), ".csv")))

        } else {
          # For sites with only one type of sonde, save all data together
            readr::write_csv(one_df,
                   here(dump_dir, paste0(site[i], "_", stringr::str_replace(stringr::str_replace(substr(end_dt, 1, 16), "[:\\s]", "_"), ":", ""), ".csv")))
        }
      }
    }
  }
}
