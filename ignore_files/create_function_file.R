combine_r_files <- function(source_dir = "R", output_file = "ignore_files/all_functions.R") {
  # Get all .R files recursively
  r_files <- list.files(
    path = source_dir,
    pattern = "\\.R$",
    full.names = TRUE,
    recursive = TRUE
  )
  
  # Check if any files were found
  if (length(r_files) == 0) {
    stop("No R files found in the specified directory")
  }
  
  # Create or open the output file
  con <- file(output_file, "w")
  
  # Write a header
  cat("# Combined R Functions\n", file = con)
  cat("# Auto-generated on ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n", sep = "", file = con)
  
  # Process each file
  for (file in r_files) {
    # Add file header comment
    cat("\n\n# ===================================\n", file = con)
    cat("# File: ", file, "\n", sep = "", file = con)
    cat("# ===================================\n\n", file = con)
    
    # Read and write content
    file_content <- readLines(file)
    cat(file_content, sep = "\n", file = con)
  }
  
  # Close connection
  close(con)
  
  # Provide summary
  cat("Combined", length(r_files), "R files into:", output_file, "\n")
  cat("Total lines:", sum(sapply(r_files, function(f) length(readLines(f)))), "\n")
  
  # Return the path to the combined file
  return(output_file)
}

# Usage:
combine_r_files(source_dir = "R", output_file = "ignore_files/all_functions_4.R")
