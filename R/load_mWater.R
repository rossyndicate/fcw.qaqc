#' @title Load and tidy mWater field notes
#'
#' @description A function that downloads and cleasn field notes from mWater. This
#' funciton handles time zone conversion, standardizes text fields, and prepares
#' the data for integration with sonde readings. 
#'
#' @param creds A .yml file with necessary credentials for accessing the field 
#' notes. Must contain a 'url' field. 
#' 
#' @param summarize_interval Character string specifying the time interval to round timestamps to.
#' Default is "15 minutes". Accepts any interval format compatible with 
#' lubridate::floor_date() like "1 hour", "30 mins", etc.
#'
#' @return A dataframe containing processed field notes with standardized columns:
#' - site: Standardized site name (lowercase, no spaces)
#' - DT_round: Rounded timestamp for joining with sensor data
#' - start_DT/end_dt: Start and end times of field visits (MST timezone)
#' - visit_type: Type of field visit (standardized)
#' - sensor_pulled/sensor_deployed: Serial numbers of equipment
#' - And various other field observation columns
#' 
#' @examples
#' # Load field notes with default 15-minute interval
#' field_notes <- load_mWater(creds = yaml::read_yaml("creds/mWaterCreds.yml"))
#'
#' # Load field notes with hourly interval
#' hourly_notes <- load_mWater(creds = yaml::read_yaml("creds/mWaterCreds.yml"),
#'                           summarize_interval = "1 hour")
#' 
#' @seealso [grab_mWater_sensor_notes()]
#' @seealso [grab_mWater_malfunction_notes()]

load_mWater <- function(creds = yaml::read_yaml("creds/mWaterCreds.yml"), summarize_interval = "15 minutes"){

  # Retrieve the API URL from the credentials file
  api_url <- as.character(creds["url"])
  # TODO: how do we want to handle access when this package is public? (in relation to 
  # creds file)

  # Download field notes from mWater API and perform data cleaning operations
  all_notes_cleaned <- readr::read_csv(url(api_url), show_col_types = FALSE) %>%
    dplyr::mutate(
      # Convert timestamps from UTC to Mountain Standard Time (MST)
      # Handle multiple possible date-time formats using lubridate
      start_DT = lubridate::with_tz(lubridate::parse_date_time(start_dt, orders = c("%Y%m%d %H:%M:%S", "%m%d%y %H:%M", "%m%d%Y %H:%M", "%b%d%y %H:%M" )), tz = "MST"),
      end_dt = lubridate::with_tz(lubridate::parse_date_time(end_dt, orders = c("%Y%m%d %H:%M:%S", "%m%d%y %H:%M", "%m%d%Y %H:%M", "%b%d%y %H:%M" )), tz = "MST"),
      malfunction_end_dt = lubridate::with_tz(lubridate::parse_date_time(malfunction_end_dt, orders = c("%Y%m%d %H:%M:%S", "%m%d%y %H:%M", "%m%d%Y %H:%M", "%b%d%y %H:%M" )), tz = "MST"),
      
      # Extract date and time components for convenience
      date = as.Date(start_DT, tz = "MST"),
      start_time_mst = format(start_DT, "%H:%M"),

      # Ensure sensor serial numbers are character type
      sensor_pulled = as.character(sn_removed),
      sensor_deployed = as.character(sn_deployed),

      # Handle "Other" site names by using the free text response
      # Also standardize by removing spaces and converting to lowercase
      site = ifelse(site == "Other (please specify)", tolower(stringr::str_replace_all(site_other, " ", "")), site),
      
      # Fix a survey design issue where "???" appears in visit_type
      visit_type = dplyr::case_when(stringr::str_detect(visit_type, "\\?\\?\\?") ~ stringr::str_replace(string = visit_type,
                                                                               pattern =  "\\?\\?\\?",
                                                                               replacement = "Sensor Calibration or Check"),
                             TRUE ~ visit_type),
      
      # Replace "Other" in visit_type with the free text response when applicable
      visit_type = dplyr::case_when(stringr::str_detect(visit_type, "Other") ~ stringr::str_replace(string = visit_type,
                                                                           pattern =  "Other \\(please specify\\)",
                                                                           replacement = visit_type_other),
                             TRUE ~ visit_type),
      
      # Replace "Other" in sensor_malfunction with the free text response
      which_sensor_malfunction = dplyr::case_when(stringr::str_detect(which_sensor_malfunction, "Other") ~ stringr::str_replace(string = which_sensor_malfunction,
                                                                                                       pattern =  "Other \\(please specify\\)",
                                                                                                       replacement = as.character(other_which_sensor_malfunction)),
                                           TRUE ~ which_sensor_malfunction),

      # Replace "Other" in photos_downloaded with the free text response
      photos_downloaded = ifelse(photos_downloaded == "Other (please specify)", photos_downloaded_other, photos_downloaded),
      
      # Create a rounded timestamp for joining with sensor data at consistent intervals
      DT_round = lubridate::floor_date(start_DT, unit = summarize_interval)) %>%
    # arrange by timestamp (most recent first)
    dplyr::arrange(DT_round)%>%
    # Remove redundant "other" columns that have been merged into primary columns
    dplyr::select(-c(photos_downloaded_other,visit_type_other, site_other, other_which_sensor_malfunction ))

  return(all_notes_cleaned)

}