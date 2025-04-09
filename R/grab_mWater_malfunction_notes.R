#' @title Extract sensor malfunction records from mWater field data
#'
#' @description
#' Processes mWater field data to extract and format records specifically related to
#' sensor malfunctions. This function filters for visits tagged as "Sensor malfunction",
#' standardizes parameter names to match the system's naming conventions, and
#' reformats the data for integration with the QAQC workflow. When field technicians
#' identify sensor issues, these records can be used to flag periods of known
#' malfunctions in the dataset.
#'
#' @param mWater_api_data A dataframe containing field notes from mWater, typically
#' the output from load_mWater().
#'
#' @return A dataframe containing only sensor malfunction records with standardized columns:
#' - start_DT: When the malfunction was first observed (timestamp)
#' - end_DT: When the malfunction was resolved or expected to be resolved (timestamp)
#' - site: Location identifier (standardized format)
#' - parameter: Affected sensor parameter (standardized name)
#' - notes: Technician observations about the malfunction
#'
#' @examples
#' # Load mWater data and extract malfunction records
#' mWater_data <- load_mWater(creds = yaml::read_yaml("creds/mWaterCreds.yml"))
#' malfunction_data <- grab_mWater_malfunction_notes(mWater_data)
#'
#' @seealso [load_mWater()]
#' @seealso [add_malfunction_flag()]

grab_mWater_malfunction_notes <- function(mWater_api_data){

  # Filter mWater data to extract only records tagged as sensor malfunction visits
  # These represent instances where field technicians identified sensor problems
  malfunction_records <- mWater_api_data %>%
    dplyr::filter(grepl("Sensor malfunction", visit_type, ignore.case = TRUE)) %>%
    dplyr::select(start_DT, site, crew, which_sensor_malfunction, malfunction_end_dt, notes = visit_comments)
  
  # Define the standard parameter names used in the QAQC system
  # This ensures consistent naming when matching malfunction records to sensor data
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
  
  # Process and standardize the malfunction records
  malfunction_records <- malfunction_records %>%
    
    # Select and rename columns to match expected format for QAQC workflow
    dplyr::select(start_DT, end_DT = malfunction_end_dt, site, parameter = which_sensor_malfunction, notes) %>%
    
    # Handle cases where multiple parameters are affected by splitting comma-separated values
    # This creates separate rows for each affected parameter
    tidyr::separate_rows(parameter, sep = ", ") %>%
    
    # Standardize parameter names to match the system's naming conventions
    # Field technicians may use different terminology for the same sensors
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
    
    # Filter to include only parameters relevant to the QAQC analysis
    dplyr::filter((is.na(parameter)) | (parameter %in% parameters))
  
  return(malfunction_records)
}
