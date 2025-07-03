#' @title Process raw API data for water quality monitoring workflow
#' @export
#'
#' @description
#' Process raw HydroVu API data files into a standardized format for analysis.
#' This function reads parquet files from either Azure Data Lake Storage or
#' local storage and transforms them into a consistent format for downstream
#' processing.
#'
#' @param api_dir Character string specifying the directory path containing the
#' raw parquet files downloaded from the HydroVu API.
#'
#' @param summarize_interval Character string specifying the time interval to
#' round timestamps to. Default is "15 minutes". Accepts any interval format
#' compatible with lubridate::round_date().
#' 
#' @param fs Logical, whether to use the file system functions
#'
#' @return A dataframe containing processed water quality monitoring data with
#' standardized columns:
#' - site: Standardized site name (lowercase, no spaces)
#' - DT: Original timestamp (UTC timezone)
#' - DT_round: Rounded timestamp for consistent time intervals
#' - DT_join: Character representation of rounded timestamp for joining
#' - parameter: Measurement type (e.g., "Temperature", "DO")
#' - value: Measured value
#' - units: Measurement units (e.g., "°C", "mg/L")
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [api_puller()]
#' @seealso [tidy_api_data()]

munge_api_data <- function(api_dir, summarize_interval = "15 minutes", 
                           synapse_env = FALSE, fs = NULL) {
  
  # Handle cloud-based storage scenario when running in Synapse environment
  if (synapse_env) {
    
    # Get list of all files in the Azure Data Lake Storage (ADLS) directory 
    file_list <- AzureStor::list_adls_files(fs, api_dir, info = "name")
    
    # Read in each file in the ADLS raw/.../incoming folder and combine results
    # into a dataframe.
    api_data <- purrr::map_dfr(file_list, function(adls_path){
      
      # Create temporary file in the local file system (fs)
      temp_file <- tempfile(fileext = '.parquet')
      
      # Download the file from ADLS into the temporary file
      AzureStor::download_adls_file(fs, adls_path, temp_file)
      
      # Read the temporary parquet file as a dataframe
      site_df <- arrow::read_parquet(temp_file, as_data_frame = TRUE) 
      
      return(site_df)
    }) 
    
  } else {
    # Handle local storage scenario when working outside of Synapse
    
    # Process each local parquet file and combine results into a single dataframe.
    api_data <- map_dfr(list.files(api_dir, full.names = TRUE), 
                        function(file_path) {
                          site_df <- arrow::read_parquet(file_path, as_data_frame = TRUE) 
                          return(site_df)
                        })
    
  }
  
  # Standardize and clean the combined data
  final_api_data <- api_data %>%
    # Remove ID column
    dplyr::select(-id) %>%
    # Ensure units are stored as character strings for consistency
    dplyr::mutate(units = as.character(units)) %>%
    # Filter out VuLink data (not used in CSU/FCW networks)
    dplyr::filter(!grepl("vulink", name, ignore.case = TRUE)) %>%
    # Filter out Virridy data (not used in CSU/FCW networks)
    dplyr::filter(!grepl("virridy", name, ignore.case = TRUE)) %>%
    # Remove the equipment name column
    dplyr::select(-name) %>%
    dplyr::mutate(
      # Round timestamps to specified interval for consistent time series
      DT = timestamp, 
      DT_round = lubridate::round_date(DT, summarize_interval),
      # Create string version of timestamp for joining operations
      DT_join = as.character(DT_round),
      # Ensure site names are lowercase for consistency
      site = tolower(site)
    ) %>%
    # Ensure no duplicates after all transformations
    dplyr::distinct(.keep_all = TRUE)
  
  return(final_api_data)
}

