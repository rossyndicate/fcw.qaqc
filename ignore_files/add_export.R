# Script to add @export tags to all functions with roxygen documentation

# Get all R files in the package
r_files <- list.files("R", pattern = "\\.R$", full.names = TRUE, recursive = TRUE)

# Process each file
for (file in r_files) {
  # Read the file content
  lines <- readLines(file, warn = FALSE)
  file_modified <- FALSE
  
  # Find where function definitions start
  function_starts <- grep("\\s*[A-Za-z0-9_.]+\\s*<-\\s*function\\s*\\(", lines)
  
  for (fn_start in function_starts) {
    # Extract function name for logging
    fn_name <- sub("\\s*([A-Za-z0-9_.]+)\\s*<-\\s*function.*", "\\1", lines[fn_start])
    fn_name <- trimws(fn_name)
    
    # Look backward to find roxygen block that precedes this function
    i <- fn_start - 1
    while (i > 0 && (grepl("^\\s*$", lines[i]) || grepl("^\\s*#'", lines[i]))) {
      i <- i - 1
    }
    
    # If we found a roxygen block
    if (i < fn_start - 1) {
      roxygen_end <- i + 1
      
      # Find the first roxygen line (moving backward)
      i <- roxygen_end
      while (i > 0 && grepl("^\\s*#'", lines[i])) {
        i <- i - 1
      }
      roxygen_start <- i + 1
      
      # Extract the roxygen block
      roxygen_block <- lines[roxygen_start:roxygen_end]
      
      # Check if @export already exists
      if (!any(grepl("^\\s*#'\\s*@export\\b", roxygen_block))) {
        # Find the last roxygen comment
        last_roxygen_line <- roxygen_end
        
        # Add @export tag after the last roxygen comment
        lines <- c(
          lines[1:last_roxygen_line],
          "#' @export",
          lines[(last_roxygen_line+1):length(lines)]
        )
        
        file_modified <- TRUE
        cat("Added @export tag to function", fn_name, "in", file, "\n")
        
        # Adjust indices for subsequent functions
        function_starts <- function_starts + 1
      }
    }
  }
  
  # Write modified file back
  if (file_modified) {
    writeLines(lines, file)
  }
}

cat("\nFinished adding @export tags to all functions with roxygen documentation.\n")
cat("Run devtools::document() next to update your NAMESPACE file.\n")

# Script to find incorrect @export tags

# Get all R files
r_files <- list.files("R", pattern = "\\.R$", full.names = TRUE, recursive = TRUE)

# Search for problematic export tags
for (file in r_files) {
  lines <- readLines(file, warn = FALSE)
  
  # Find lines with potentially problematic exports
  problematic_lines <- grep("#'\\s*@export\\s+data|#'\\s*@export\\s+flagged", lines)
  
  if (length(problematic_lines) > 0) {
    cat("Found problematic @export in file:", file, "\n")
    cat("Line numbers:", problematic_lines, "\n")
    cat("Content:", lines[problematic_lines], "\n\n")
  }
}

# Script to detect multiple @export tags in R files

# Get all R files in the package
r_files <- list.files("R", pattern = "\\.R$", full.names = TRUE, recursive = TRUE)

# Process each file
for (file in r_files) {
  # Read the file content
  lines <- readLines(file, warn = FALSE)
  
  # Count @export tags
  export_lines <- grep("#'\\s*@export\\b", lines)
  export_count <- length(export_lines)
  
  # If more than one export tag, report it
  if (export_count > 1) {
    cat("File:", file, "contains", export_count, "@export tags at lines:\n")
    
    # Show line numbers and context
    for (line_num in export_lines) {
      # Find the function this export belongs to
      function_line <- min(grep("\\s*[A-Za-z0-9_.]+\\s*<-\\s*function\\s*\\(", lines[line_num:length(lines)], perl = TRUE) + line_num - 1)
      function_name <- sub("\\s*([A-Za-z0-9_.]+)\\s*<-\\s*function.*", "\\1", lines[function_line])
      function_name <- trimws(function_name)
      
      cat("  Line", line_num, ":", lines[line_num], "\n")
      cat("    Related to function:", function_name, "at line", function_line, "\n\n")
    }
    
    # Determine if there are any "orphan" exports not associated with functions
    cat("  Analysis:\n")
    if (any(diff(export_lines) == 1)) {
      cat("  ⚠️ WARNING: Some @export tags appear on consecutive lines, which may indicate duplicates\n")
    }
    
    cat("\n")
  }
}

# Also check for exports in data documentation
for (file in r_files) {
  lines <- readLines(file, warn = FALSE)
  
  # Look for @export tags not followed by function definitions
  export_lines <- grep("#'\\s*@export\\b", lines)
  
  if (length(export_lines) > 0) {
    orphan_exports <- c()
    
    for (line_num in export_lines) {
      # Find the next non-comment, non-empty line
      next_code_line <- min(grep("^[^#'].*\\S", lines[line_num:length(lines)], perl = TRUE) + line_num - 1)
      
      # Check if it's a function definition
      if (!grepl("\\s*[A-Za-z0-9_.]+\\s*<-\\s*function\\s*\\(", lines[next_code_line], perl = TRUE)) {
        orphan_exports <- c(orphan_exports, line_num)
      }
    }
    
    if (length(orphan_exports) > 0) {
      cat("File:", file, "contains", length(orphan_exports), "possible orphaned @export tags:\n")
      for (line_num in orphan_exports) {
        cat("  Line", line_num, ":", lines[line_num], "\n")
        cat("  Next code line:", lines[next_code_line], "\n\n")
      }
      cat("\n")
    }
  }
}

cat("Scan complete.\n")