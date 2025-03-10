#' @title Extract sensor maintenance field notes from mWater data
#'
#' @description
#' Processes mWater field data to extract and format records specifically related to 
#' sensor maintenance, calibration, and deployment activities. This function filters 
#' for relevant visit types, formats equipment change information, and standardizes 
#' the data structure to match downstream QAQC processing requirements.
#'
#' This function focuses on routine sensor operations (cleaning, calibration, deployment)
#' and explicitly excludes sensor malfunction records, which are handled separately by
#' `grab_mWater_malfunction_notes()`.
#'
#' @param mWater_api_data A dataframe containing field notes from mWater, typically
#' the output from `load_mWater()`.
#'
#' @return A dataframe containing only sensor maintenance records with standardized columns:
#' - site: Location identifier
#' - DT_round/DT_join: Timestamps for joining with sensor data
#' - sonde_employed: Binary indicator of deployment status
#' - sensor_swapped_notes: Formatted notes about equipment changes
#' - Various maintenance fields (sensors_cleaned, cals_performed, etc.)
#'
#' @examples
#' # Load mWater data and extract sensor notes
#' mWater_data <- load_mWater(creds = yaml::read_yaml("creds/mWaterCreds.yml"))
#' sensor_notes <- grab_mWater_sensor_notes(mWater_data)
#'
#' @seealso [load_mWater()]
#' @seealso [add_field_notes()]

grab_mWater_sensor_notes <- function(mWater_api_data){

  # Extract and format sensor maintenance notes from mWater data
  # These notes record routine sensore operations like cleaning, calibration, 
  # and deployment

  mWater_field_notes <- mWater_api_data %>%
    # Filter for sensor-related activities but exclude malfunction reports
    # (malfunction reports are handled separately in `grab_mWater_malfunction_notes()`)
    dplyr::filter(grepl("Sensor", visit_type, ignore.case = TRUE) & 
                  !grepl("Sensor malfunction", visit_type, ignore.case = TRUE)) %>%

    # Create derived fields to track equipment status and changes
    dplyr::mutate(
      # Track sonde deployment status based on field technician actions:
      # 0 = deployed, 1 = pulled (removed), NA = other actions or unknown
      sonde_employed = dplyr::case_when(
        is.na(sensor_change)  ~ NA,
        sensor_change == "Swapped" ~ NA,
        sensor_change == "Pulled" ~ 1,
        sensor_change == "Deployed" ~ 0),

      # Create formatted notes about equipment changes with serial numbers
      sensor_swapped_notes = dplyr::case_when(is.na(sensor_change)  ~ NA,
                                              sensor_change == "Pulled" & !is.na(sensor_pulled) ~ paste0("SN Removed: ", sensor_pulled),
                                              sensor_change == "Swapped" ~ paste0("SN Removed: ", sensor_pulled, " SN Deployed: ", sensor_deployed),
                                              sensor_change == "Deployed" ~ sensor_deployed),
      
      # Create standardized date/time fields for joining with sensor data
      DT_join = as.character(DT_round),
      field_season = lubridate::year(DT_round),
      last_site_visit = DT_round,
      date = as.character(date)) %>%
    
    # Sort by timestamp (most recent first)
    dplyr::arrange(desc(DT_round))%>%
    
    # Select and reorder columns to match expected format for QAQC workflow
    dplyr::select(
      site, crew, DT_round, sonde_employed, sensors_cleaned, wiper_working, 
      rdo_cap_condition, rdo_cap_replaced, ph_junction_replaced, cals_performed, 
      cal_report_collected, sonde_moved, sensor_malfunction, sensor_pulled, 
      sensor_deployed, sensor_swapped_notes, visit_type, start_time_mst, DT_join, 
      start_DT, end_dt, date, visit_comments, photos_downloaded, field_season, 
      last_site_visit
    )
  
  return(mWater_field_notes)

}