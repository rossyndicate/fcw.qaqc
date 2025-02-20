grab_mWater_malfunction_notes <- function(mWater_api_data){
  
  # Grab notes about sensor malfunction
  malfunction_records <- mWater_api_data %>%
    dplyr::filter(grepl("Sensor malfunction", visit_type, ignore.case = TRUE)) %>%
    dplyr::select(start_DT, site, crew, which_sensor_malfunction, malfunction_end_dt, notes = visit_comments)
  
  #write to csv
  # readr::write_csv(malfunction_records, "data/mWater_malfunction_records.csv")
  
  parameters <- c("Battery Level",
                  "Baro",
                  "Chl-a Fluorescence",
                  "Depth",
                  "DO",
                  "External Voltage",
                  "ORP",
                  "pH",
                  "Specific Conductivity",
                  "Temperature",
                  "Turbidity")
  
  malfunction_records <- malfunction_records %>%
    # keep records relevant to {target} analysis
    dplyr::select(start_DT, end_DT = malfunction_end_dt, site, parameter = which_sensor_malfunction, notes) %>%
    # match the text in the sensor column to the text in the target analysis
    tidyr::separate_rows(parameter, sep = ", ") %>%
    dplyr::mutate(
      parameter = dplyr::case_when(
        parameter == "Chlorophyll a" ~ "Chl-a Fluorescence",
        parameter == "RDO" ~ "DO",
        parameter == "Conductivity" ~ "Specific Conductivity",
        .default = parameter
      ),
      site = dplyr::case_when(
        site == "river bluffs" ~ "riverbluffs",
        .default = site
      )
    ) %>%
    dplyr::filter((is.na(parameter)) | (parameter %in% parameters))
  
  return(malfunction_records)
  
}
