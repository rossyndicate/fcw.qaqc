#' @title Munge API data for QAQC workflow
#'
#' @description
#' A function designed to munge the raw API data for the QAQC workflow.
#'
#' @param api_path Path where the raw API data lives.
#' @param network Options include "csu", "all"
#' @param summarize_interval At what time interval the user would like the data set to be aggregated and rounded to. Default is 15 minutes.
#'
#' @return A dataframe with the munged API data.

munge_api_data <- function(api_path, network, summarize_interval = "15 minutes") {

  api_data <- list.files(path = api_path, full.names = TRUE, recursive = TRUE, pattern = "*.csv") %>%
    purrr::map_dfr(~data.table::fread(.) %>%
                     dplyr::select(-id)) %>%
    dplyr::mutate(units = as.character(units)) %>%
    # remove overlapping API-pull data
    dplyr::distinct()

  if(network %in% c("csu", "CSU",  "FCW", "fcw")){

  api_data <- api_data %>%
    # remove VuLink data
    dplyr::filter(!grepl("vulink", name, ignore.case = TRUE)) %>%
    # remove Virridy sondes
    dplyr::filter(!grepl("virridy", name, ignore.case = TRUE)) %>%
    dplyr::select(-name) %>%
    # Convert UTC (as it is sent from HydroVU API) to MST:
    dplyr::mutate(DT = lubridate::as_datetime(timestamp, tz = "UTC")) %>%
    dplyr::mutate(DT = lubridate::with_tz(DT, tzone = "MST"),
           DT_round = lubridate::round_date(DT, summarize_interval),
           DT_join = as.character(DT_round),
           site = tolower(site)) %>%
    # These sites will be considered the same site for this workflow
    dplyr::mutate(site = ifelse(site == "rist", "tamasag",
                  ifelse(site == "elc", "boxelder", site))) %>%
    # Lastly, we swapped Boxelder's sonde out for Rist's late in 2022:
    dplyr::mutate(site = ifelse(site == "tamasag" & DT > lubridate::ymd("2022-09-20", tz = "MST") & DT < lubridate::ymd("2023-01-01", tz = "MST"), "boxelder", site)) %>%
    dplyr::mutate(site = ifelse(grepl("river bluffs", site, ignore.case = TRUE), "riverbluffs", site)) %>%
    dplyr::distinct(.keep_all = TRUE)
  }

  if(network %in% c("all", "All")){

    api_data <- api_data %>%
      # remove VuLink data
      dplyr::filter(!grepl("vulink", name, ignore.case = TRUE)) %>%
      dplyr::select(-name) %>%
      # Convert UTC (as it is sent from HydroVU API) to MST:
      dplyr::mutate(DT = lubridate::as_datetime(timestamp, tz = "UTC")) %>%
      dplyr::mutate(DT = lubridate::with_tz(DT, tzone = "MST"),
                    DT_round = lubridate::round_date(DT, summarize_interval),
                    DT_join = as.character(DT_round),
                    site = tolower(site)) %>%
      # These sites will be considered the same site for this workflow
      dplyr::mutate(site = ifelse(site == "rist", "tamasag",
                                  ifelse(site == "elc", "boxelder", site))) %>%
      # Lastly, we swapped Boxelder's sonde out for Rist's late in 2022:
      dplyr::mutate(site = ifelse(site == "tamasag" & DT > lubridate::ymd("2022-09-20", tz = "MST") & DT < lubridate::ymd("2023-01-01", tz = "MST"), "boxelder", site)) %>%
      dplyr::distinct(.keep_all = TRUE)
    }

  return(api_data)

}
