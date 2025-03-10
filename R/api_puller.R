#' @title Download API data from HydroVu
#'
#' @description A function designed to download raw API data from HydroVu and store in a user-selected file location.
#'
#' @param site List of sites to download data for.
#' @param start_dt Start DT for downloading data.
#' @param end_dt End DT for downloading data. Default is the current time.
#' @param api_token API token for accessing HydroVu. Pulled in via `hv_auth()`.
#' @param dump_dir Directory where raw API data will be saved to.

api_puller <- function(site, network, start_dt, end_dt = Sys.time(), api_token, dump_dir) {

  if(network %in% c("all", "All", "virridy", "Virridy")){

    locs <- hv_locations_all(hv_token)

  } else if(network %in% c("csu", "fcw", "CSU", "FCW")){

    locs <- hv_locations_all(hv_token) %>%
      dplyr::filter(!grepl("virridy", name, ignore.case = TRUE))

  }

  # make a list of site names

  options(scipen = 999)
  for(i in 1:length(site)){

    # River Bluffs had no spaces between the words at some point on HydroVu so
    # dealing with that here:
    if(tolower(site[i]) == "riverbluffs" | tolower(site[i]) == "river bluffs"){
      site_loc <- locs %>%
        dplyr::mutate(name = tolower(name)) %>%
        dplyr::filter(grepl("River Bluffs|RiverBluffs", name, ignore.case = TRUE))
  } else {
    site_loc <- locs %>%
      dplyr::mutate(name = tolower(name)) %>%
      dplyr::filter(grepl(site[i], name, ignore.case = TRUE))
  }

    site_loc_list <- site_loc$id

    # Get data for each site location. Note this maps over the entire list of locations,
    # many of which are unlikely to be active during the time you specify. Don't freak out if
    # you see a bunch of '404 Not Found' errors, you're just seeing the list of locations
    # that are not active. The data frame 'alldata' should contain your data from all applicable
    # sites during the time frame indicated. Note that this will take some time (one month of
    # data for 5 sites takes ~10 mins. Be patient!

    # Add date range you are interested in; data are stored in HydroVu in UTC
    # Here, a way to find the most recent download of the data. Use this as the start date to
    # reduce overlapping data

    # doing this fixes the mismatch in date times during the combined_data step - jd
    utc_start_date <- format(as.POSIXct(start_dt, tz = "UTC") + lubridate::hours(0), format = "%Y-%m-%d %H:%M:%S")

    utc_end_date <- format(as.POSIXct(end_dt, tz = "UTC") + lubridate::hours(0), format = "%Y-%m-%d %H:%M:%S")

    timezone <- "UTC"

    # Map over the location ids
    alldata <- site_loc_list %>% purrr::map(~hv_data_id(.,
                                                        start_time = utc_start_date,
                                                        end_time = utc_end_date,
                                                        token = api_token,
                                                        tz = timezone))

    # grab only locations with data (stored as a data frame) / drop 404 errors
    filtered <- purrr::keep(alldata, is.data.frame)

    if(length(filtered) == 0){

      print(paste0("No data at ", site[i], " during this time frame"))

    } else {

      # bind lists together (now that all are dataframes, we can just collate quickly)
      one_df <- dplyr::bind_rows(filtered) %>%
        data.table::data.table() %>%
        dplyr::rename(id = Location,
                      parameter = Parameter,
                      units = Units) %>%
        dplyr::left_join(., site_loc, by = "id") %>%
        dplyr::mutate(site = tolower(site[i])) %>%
        dplyr::select(site, id, name, timestamp, parameter, value, units)

      if(network %in% c("csu", "CSU", "fcw", "FCW")){

        # Only download FC sonde data:
        readr::write_csv(one_df %>% dplyr::filter(!grepl("virridy", name, ignore.case = TRUE),
                                                  # No FCW sensors should have FDOM data:
                                                  parameter != "FDOM Fluorescence"),
                         paste0(dump_dir, "/", site[i], "_", stringr::str_replace(stringr::str_replace(substr(end_dt, 1, 16), "[:\\s]", "_"), ":", ""), ".csv"))

        # if site contains both a CSU and a Virridy sonde, split them up to store them
        # separately:
      } else if(network %in% c("all","All","Virridy","virridy")){

        if(site[i] %in% c("Timberline", "Prospect", "Archery")){

          try(virridy_df <- one_df %>%
                dplyr::filter(grepl("virridy", name, ignore.case = TRUE)) %>%
                dplyr::mutate(site = paste0(site[i], " virridy")))

          try(readr::write_csv(virridy_df,
                               paste0(dump_dir, "/", site[i], " virridy_", stringr::str_replace(stringr::str_replace(substr(end_dt, 1, 16), "[:\\s]", "_"), ":", ""), ".csv")))

          csu_df <- one_df %>%
            dplyr::filter(!grepl("virridy", name, ignore.case = TRUE))

          readr::write_csv(csu_df,
                           paste0(dump_dir, "/", site[i], "_", stringr::str_replace(stringr::str_replace(substr(end_dt, 1, 16), "[:\\s]", "_"), ":", ""), ".csv"))

        } else {

          # otherwise, save the full data set

          readr::write_csv(one_df,
                           paste0(dump_dir, "/", site[i], "_", stringr::str_replace(stringr::str_replace(substr(end_dt, 1, 16), "[:\\s]", "_"), ":", ""), ".csv"))
        }
      }
    }
}

  }
