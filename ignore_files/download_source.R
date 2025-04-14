package_loader <- function(x) {
  if (x %in% installed.packages()) {
    suppressMessages({
      library(x, character.only = TRUE)
    })
  } else {
    suppressMessages({
      install.packages(x)
      library(x, character.only = TRUE)
    })
  }
}

invisible(
  lapply(c("data.table", "httr2", "tidyverse", 
           "rvest", "readxl", "lubridate", 
           "zoo", "padr", "stats",
           "plotly", "feather", "RcppRoll", 
           "yaml", "ggpubr", "profvis", 
           "janitor", "here"), package_loader)
)

# ----
# Function to download package source and ensure it's in tar.gz format
# Function to download package source and dependencies in tar.gz format
download_package_source <- function(pkg_name, dest_dir = "ignore_files/package_source", include_dependencies = TRUE) {
  # Create destination directory if it doesn't exist
  if (!dir.exists(dest_dir)) {
    dir.create(dest_dir, recursive = TRUE)
  }
  
  # Set temporary directory for initial download
  temp_dir <- tempdir()
  
  # Function to process a single package
  process_package <- function(pkg) {
    tryCatch({
      # Download package source
      pkg_info <- download.packages(pkg, destdir = temp_dir, type = "source")
      
      # Get the downloaded file path
      src_file <- pkg_info[1, 2]
      
      # Check if it's already a tar.gz file
      if (grepl("\\.tar\\.gz$", src_file)) {
        # Copy to destination
        file.copy(src_file, file.path(dest_dir, basename(src_file)), overwrite = TRUE)
        cat(paste0("Downloaded ", pkg, " as tar.gz\n"))
      } else if (grepl("\\.tar$", src_file)) {
        # If it's just a .tar file, compress it
        targz_file <- file.path(dest_dir, paste0(basename(src_file), ".gz"))
        
        # Use R.utils to compress
        R.utils::gzip(src_file, destname = targz_file, overwrite = TRUE)
        cat(paste0("Downloaded and compressed ", pkg, " to tar.gz\n"))
      } else {
        # Handle other formats if needed
        cat(paste0("Warning: Downloaded ", pkg, " in unexpected format: ", src_file, "\n"))
      }
      
      return(TRUE)
    }, error = function(e) {
      cat(paste0("Error downloading ", pkg, ": ", e$message, "\n"))
      return(FALSE)
    })
  }
  
  # Get dependencies if requested
  if (include_dependencies) {
    # Get all dependencies
    pkg_deps <- tools::package_dependencies(pkg_name, recursive = TRUE)
    if (!is.null(pkg_deps[[pkg_name]])) {
      all_packages <- unique(c(pkg_name, pkg_deps[[pkg_name]]))
    } else {
      all_packages <- pkg_name
    }
    
    cat(paste0("Processing ", pkg_name, " and ", length(all_packages) - 1, " dependencies\n"))
  } else {
    all_packages <- pkg_name
  }
  
  # Process all packages
  results <- sapply(all_packages, process_package)
  
  # Return results
  return(results)
}

# Make sure required packages are available
if (!requireNamespace("R.utils", quietly = TRUE)) {
  install.packages("R.utils")
}

# List of packages to download
packages <- c('data.table',
              'tidyverse',
              'rvest',
              'lubridate',
              'zoo',
              'padr',
              'stats',
              'plotly',
              'RcppRoll',
              'yaml',
              'profvis',
              'janitor',
              'Rcpp',
              'magrittr',
              'dplyr',
              'purrr',
              'rlang',
              'httr2')

packages <- c("janitor")

# Process all packages with dependencies
results <- list()
for (pkg in packages) {
  results[[pkg]] <- download_package_source(pkg, include_dependencies = TRUE)
}

# Summary
cat("\nDownload summary:\n")
total_success <- sum(sapply(results, function(x) sum(x)))
total_failed <- sum(sapply(results, function(x) sum(!x)))
cat(paste0("Successfully processed: ", total_success, " packages\n"))
cat(paste0("Failed: ", total_failed, " packages\n"))

# List the downloaded files
cat("\nDownloaded files:\n")
list.files("ingore_files/package_source", pattern = "\\.tar\\.gz$")
