#' @title Archive API data files after processing
#' @export
#'
#' @description
#' Moves raw data files from the working API directory to an archive directory 
#' after they have been processed through the water quality monitoring workflow.
#' This function performs several steps:
#' 
#' 1. Identifies files in the API directory that aren't already in the archive
#' 2. Copies those files to the archive directory
#' 3. Verifies the copy operation
#' 4. Removes all files from the original API directory
#'
#' This function is typically called at the end of the data processing workflow
#' to maintain a clean working environment while preserving raw data files.
#'
#' @param api_dir File path to the working API directory containing processed data files
#' @param archive_dir File path to the archive directory where files will be stored
#'
#' @return No return value, called for side effects (file operations)
#'
#' @examples
#' # Examples are temporarily disabled

move_api_data <- function(api_dir, archive_dir,
                          synapse_env = FALSE, fs = NULL) {
  
  if (synapse_env) {
    # List files in the working and archive directories
    incoming_files <- basename(AzureStor::list_adls_files(fs, api_dir, info = "name"))
    archive_files <- basename(AzureStor::list_adls_files(fs, archive_dir, info = "name"))
    
    # Identify files that aren't already in the archive
    files_to_copy <- setdiff(incoming_files, archive_files)
    
    if (length(files_to_copy) > 0) {
      # Copy new files to the archive
      local_file_list <- map(files_to_copy, function(file_name) {
        # create a temp file 
        temp_file <- tempfile(fileext = ".parquet")
        
        # create a file path to the file name
        local_path <- file.path(api_dir, file_name)
        
        # download the data into the temporary file
        AzureStor::download_adls_file(fs, local_path, temp_file)
        
        # return the temporary file
        return(temp_file)
      }) %>% 
        unlist()
      
      # Create a list of new destination paths
      dest_paths <- file.path(archive_dir, files_to_copy)
      
      # Use multiupload the files
      AzureStor::multiupload_adls_file(
        filesystem = fs,
        src = local_file_list,
        dest = dest_paths
      )
      
      for (i in files_to_copy) {
        print(paste0("File ", i, " has been copied to ", archive_dir, " from ", api_dir, "."))
      }
    } else {
      print(paste0("All files from ", api_dir, " are already present in ", archive_dir, ". Nothing to copy."))
    }
    
    # Brief pause to ensure file operations complete
    Sys.sleep(30)
    
    # Refresh list of archive files after copying
    archive_files <- basename(AzureStor::list_adls_files(fs, archive_dir, info = "name"))
    
    # Verify copy operation was successful
    if (all(incoming_files %in% archive_files)) {
      print(paste0("All files from ", api_dir, " have been successfully copied to ", archive_dir, "."))
    } else {
      warning(paste0("Not all files from ", api_dir, " have been successfully copied to ", archive_dir, "."))
      # Note: Pipeline continues despite verification issues
    }
    
    # Remove the copied files from the working directory
    if (length(files_to_copy) > 0) {
      walk(files_to_copy, function(file_name){
        AzureStor::delete_adls_file(filesystem = fs,
                                    file = file.path(api_dir, file_name))
        print(paste0("File ", file_name, " has been deleted from ", api_dir, "."))
      })
      print("Copied files have been removed from the incoming directory.")
    }
    
    # Wait for deletion to complete
    Sys.sleep(30)
    
    # Final cleanup: remove any remaining files from the working directory
    incoming_files <- basename(AzureStor::list_adls_files(fs, api_dir, info = "name"))
    
    if (length(incoming_files) > 0) {
      files_to_delete <- AzureStor::list_adls_files(fs, api_dir, info = "name")
      walk(files_to_delete, function(file_path){
        AzureStor::delete_adls_file(filesystem = fs,
                                    file = file_path)
        print(paste0("File ", file_path, " has been deleted from ", api_dir, "."))
      })
    }
    
    print(paste0("All files removed from ", api_dir, "."))
    
  } else {
    # List files in the working and archive directories
    incoming_files <- list.files(api_dir, full.names = FALSE)
    archive_files <- list.files(archive_dir, full.names = FALSE)
    
    # Identify files that aren't already in the archive
    files_to_copy <- setdiff(incoming_files, archive_files)
    
    # Copy new files to the archive
    if (length(files_to_copy) > 0) {
      for (file in files_to_copy) {
        full_file_name <- file.path(api_dir, file)
        file.copy(full_file_name, archive_dir)
        print(paste0(file, " has been moved to archive API data folder."))
      }
      print("Files have been copied from the incoming directory to the archive directory.")
    } else {
      print("All files are already present in the archive directory. Nothing to copy.")
    }
    
    # Brief pause to ensure file operations complete
    Sys.sleep(5)
    
    # Refresh list of archive files after copying
    archive_files <- list.files(archive_dir, full.names = FALSE)
    
    # Verify copy operation was successful
    if (all(incoming_files %in% archive_files)) {
      print("All files in the incoming directory have been successfully copied to the archive directory.")
    } else {
      print("Not all files from the incoming directory have been successfully copied to the archive directory.")
      # Note: Pipeline continues despite verification issues
    }
    
    # Remove the copied files from the working directory
    if (length(files_to_copy) > 0) {
      for (file in files_to_copy) {
        full_file_name <- file.path(api_dir, file)
        file.remove(full_file_name)
      }
      print("Copied files have been removed from the incoming directory.")
    }
    
    # Final cleanup: remove any remaining files from the working directory
    for (file in list.files(api_dir, full.names = TRUE)) {
      file.remove(file)
    }
    print("All files removed from incoming directory.")
  }
  
  
}
