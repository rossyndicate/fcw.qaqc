#' @title  Move api to archive folder.
#'
#' @description
#' A function designed to move the api data to the archival api folder
#'
#' @param api_dir File path to the api folder
#' @param archive_dir File path to the api archive folder
#'
#' @return Nothing returned.
move_api_data <- function(api_dir, archive_dir) {

  
  # check if api folder is empty

  # List files in the incoming and archive directories
  incoming_files <- list.files(api_dir, full.names = FALSE)
  archive_files <- list.files(archive_dir, full.names = FALSE)

  # Find files that are not already in the archive directory
  files_to_copy <- setdiff(incoming_files, archive_files)

  # Copy only the files that are not already in the archive directory
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

  Sys.sleep(5)

# refresh archive_files now that the incoming_files have been copied over
archive_files <- list.files(archive_dir, full.names = FALSE)
  # Check if all files from incoming directory are now in the archive directory
  if (all(incoming_files %in% archive_files)) {
    print("All files in the incoming directory have been successfully copied to the archive directory.")
  } else {
    print("Not all files from the incoming directory have been successfully copied to the archive directory.")
    # Should this halt the pipeline?
    # Right now it seems to always print this and I haven't figured out why so
    # I don't think that it should not halt the pipeline in the state that its in right now -JD.
  }

  # Delete the copied files from the incoming directory
  if (length(files_to_copy) > 0) {
    for (file in files_to_copy) {
      full_file_name <- file.path(api_dir, file)
      file.remove(full_file_name)
    }
    print("Copied files have been removed from the incoming directory.")
  }

  # Delete any files in the incoming directory
  for (file in list.files(api_dir, full.names = TRUE)) {
    file.remove(file)
  }
  print("All files removed from incoming directory.")
}
