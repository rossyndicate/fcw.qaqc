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
#' @param src_dir File path to the working API directory containing processed data files
#' @param dest_dir File path to the archive directory where files will be stored
#'
#' @return No return value, called for side effects (file operations)
#'
#' @examples
#' # Examples are temporarily disabled

move_api_data <- function(src_dir, dest_dir,
                          synapse_env = FALSE, fs = NULL) {
  
  if (synapse_env) {
    # List files in the working and archive directories
    incoming_files <- basename(AzureStor::list_adls_files(fs, src_dir, info = "name"))
    incoming_files <- incoming_files[grepl("\\.parquet$", incoming_files)]
    
    archive_files <- basename(AzureStor::list_adls_files(fs, dest_dir, info = "name"))
    archive_files <- archive_files[grepl("\\.parquet$", archive_files)]
    
    # Identify files that aren't already in the archive
    files_to_copy <- setdiff(incoming_files, archive_files)
    
    if (length(files_to_copy) > 0) {
      # Copy new files to the archive
      local_file_list <- map(files_to_copy, function(file_name) {
        # create a temp file 
        temp_file <- tempfile(fileext = ".parquet")
        
        # create a file path to the file name
        local_path <- file.path(src_dir, file_name)
        
        # download the data into the temporary file
        AzureStor::download_adls_file(fs, local_path, temp_file)
        
        # return the temporary file
        return(temp_file)
      }) %>% 
        unlist()
      
      # Create a list of new destination paths
      dest_paths <- file.path(dest_dir, files_to_copy)
      
      # Use multiupload the files
      AzureStor::multiupload_adls_file(
        filesystem = fs,
        src = local_file_list,
        dest = dest_paths
      )
      
      for (i in files_to_copy) {
        message(paste0("File ", i, " has been copied to ", dest_dir, " from ", src_dir, "."))
      }
    } else {
      message(paste0("All files from ", src_dir, " are already present in ", dest_dir, ". Nothing to copy."))
    }
    
    # Brief pause to ensure file operations complete
    Sys.sleep(30)
    
    # Refresh list of archive files after copying
    archive_files <- basename(AzureStor::list_adls_files(fs, dest_dir, info = "name"))
    
    # Verify copy operation was successful
    if (all(incoming_files %in% archive_files)) {
      message(paste0("All files from ", src_dir, " have been successfully copied to ", dest_dir, "."))
    } else {
      warning(paste0("Not all files from ", src_dir, " have been successfully copied to ", dest_dir, "."))
      # Note: Pipeline continues despite verification issues
    }
    
    # Remove the copied files from the working directory
    if (length(files_to_copy) > 0) {
      walk(files_to_copy, function(file_name){
        AzureStor::delete_adls_file(filesystem = fs,
                                    file = file.path(src_dir, file_name))
        message(paste0("File ", file_name, " has been deleted from ", src_dir, "."))
      })
      message("Copied files have been removed from the ", src_dir, " directory.")
    }
    
    # Wait for deletion to complete
    Sys.sleep(30)
    
    # Final cleanup: remove any remaining files from the working directory
    incoming_files <- basename(AzureStor::list_adls_files(fs, src_dir, info = "name"))
    incoming_files <- incoming_files[grepl("\\.parquet$", incoming_files)]
    
    if (length(incoming_files) > 0) {
      files_to_delete <- AzureStor::list_adls_files(fs, src_dir, info = "name")
      walk(files_to_delete, function(file_path){
        AzureStor::delete_adls_file(filesystem = fs,
                                    file = file_path)
        message(paste0("File ", file_path, " has been deleted from ", src_dir, "."))
      })
    }
    
    message(paste0("All files removed from ", src_dir, "."))
    
  } else {
    # List files in the working and archive directories
    incoming_files <- list.files(src_dir, full.names = FALSE)
    incoming_files <- incoming_files[grepl("\\.parquet$", incoming_files)]
    
    archive_files <- list.files(dest_dir, full.names = FALSE)
    archive_files <- archive_files[grepl("\\.parquet$", archive_files)]
    
    # Identify files that aren't already in the archive
    files_to_copy <- setdiff(incoming_files, archive_files)
    
    # Copy new files to the archive
    if (length(files_to_copy) > 0) {
      for (file in files_to_copy) {
        full_file_name <- file.path(src_dir, file)
        file.copy(full_file_name, dest_dir)
        message(paste0(file, " has been moved to archive API data folder."))
      }
      message("Files have been copied from the ", src_dir, " directory to the ", dest_dir," directory.")
    } else {
      message("All files are already present in the ", dest_dir, " directory. Nothing to copy.")
    }
    
    # Brief pause to ensure file operations complete
    Sys.sleep(5)
    
    # Refresh list of archive files after copying
    archive_files <- list.files(dest_dir, full.names = FALSE)
    
    # Verify copy operation was successful
    if (all(incoming_files %in% archive_files)) {
      message("All files in the ", src_dir, " directory have been successfully copied to the ", dest_dir, " directory.")
    } else {
      message("Not all files from the ", src_dir, " directory have been successfully copied to the ", dest_dir, " directory.")
      # Note: Pipeline continues despite verification issues
    }
    
    # Remove the copied files from the working directory
    if (length(files_to_copy) > 0) {
      for (file in files_to_copy) {
        full_file_name <- file.path(src_dir, file)
        file.remove(full_file_name)
      }
      message("Copied files have been removed from the ", src_dir, " directory.")
    }
    
    # Final cleanup: remove any remaining files from the working directory
    src_dir_files <- list.files(src_dir, full.names = TRUE)
    src_dir_files <- src_dir_files[grepl("\\.parquet$", src_dir_files)]
    for (file in src_dir_files) {
      file.remove(file)
    }
    message("All files removed from ", src_dir, " directory.")
  }
  
  
}
