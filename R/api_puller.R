#' @title Download water quality data from HydroVu API
#' @export
#'
#' @description
#' Downloads raw water quality monitoring data from the HydroVu platform for 
#' specified sites and time periods. This function handles the connection to 
#' HydroVu, processes the API responses, and saves the raw data as parquet files 
#' in a specified directory.
#'
#' @param site Character string or vector specifying the site name(s) to download 
#' data for. Site names are matched case-insensitively against the HydroVu 
#' location names.
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
#' @param hv_sites_arg A dataframe which contains the identifying information
#' for the data stored in the HydroVu API with columns `c(id, name, description`. 
#' Usually generated by calling `hv_locations_call()`.
#'
#' @param dump_dir Character string specifying the directory path where 
#' downloaded CSV files should be saved.
#' 
#' @param fs Logical, whether to use the file system functions
#'
#' @return No direct return value. The function writes parquet files to the specified 
#' dump_dir, with filenames formatted as "sitename_timestamp.parquet".
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [get_start_dates()]
#' @seealso [hv_auth()]
#' @seealso [munge_api_data()]
#' @seealso [hv_locations_all()]

api_puller <- function(site, 
                       start_dt, end_dt = Sys.time(), 
                       api_token,
                       hv_sites_arg = hv_sites,
                       dump_dir, 
                       synapse_env = FALSE, fs = NULL) {
  
  # Synapse runs in parallel, so stagger API calls to prevent overloading server
  Sys.sleep(runif(1, 2, 5))
  
  # For other sites, filter locations that contain the site name
  site_loc <- hv_sites_arg %>%
    dplyr::mutate(name = tolower(name)) %>%
    dplyr::filter(grepl(site, name, ignore.case = TRUE))
  
  # Request data for each location ID within the specified time period
  all_data_filtered <- purrr::map(site_loc$id, # Extract the HydroVu location IDs for API requests
                                  function(id){
                                    hv_data_id(loc_id = id,
                                               start_time = start_dt,
                                               end_time = end_dt,
                                               token = api_token,
                                               tz = "UTC")}) %>% 
    # Filter out error responses (404s) and keep only valid data frames
    purrr::keep(., is.data.frame)
  
  # If no data was found for this site during the time period, report, end current iteration, and continue
  if(length(all_data_filtered) == 0){
    message(paste0("No data at ", site, " during this time frame"))
    return() # This will end the current iteration and move to the next one
  } 
  
  # Combine all dataframes, standardize column names, and join with location metadata
  site_df <- dplyr::bind_rows(all_data_filtered) %>%
    data.table::data.table() %>%
    dplyr::rename(id = Location,
                  parameter = Parameter,
                  units = Units) %>%
    dplyr::left_join(., site_loc, by = "id") %>%
    dplyr::mutate(site = tolower(site)) %>%
    dplyr::select(site, id, name, timestamp, parameter, value, units) %>% 
    # For FCW/CSU networks exclude FDOM parameter
    dplyr::filter(parameter != "FDOM Fluorescence")
  
  # Format the timestamp string for filenames
  timestamp_str <- format(end_dt, "%Y%m%d-T%H%M%SZ", tz = "UTC")
  # Create clean path (remove any double slashes) for file upload
  file_name <- paste0(site, timestamp_str, ".parquet")
  file_path <- file.path(dump_dir, file_name)
  
  if (synapse_env){
    # Upload to ADLS via ADLS-compatible write procedure
    tryCatch({
      # First write to a temporary file
      temp_file <- tempfile(fileext = ".parquet")
      arrow::write_parquet(site_df, temp_file)
      
      # Upload file to ADLS 
      AzureStor::upload_adls_file(
        filesystem = fs, 
        src = temp_file,
        dest = file_path)
      message("...Upload complete for: ", site, "\n")
    }, error = function(e) {
      message("...Error in upload process: ", e$message, "\n")
    })
  } else {
    arrow::write_parquet(site_df, file_path)
    message(paste("...Upload into", dump_dir, "complete for:", site, "\n"))
  }
}
