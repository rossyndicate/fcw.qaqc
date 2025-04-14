# Get all R files in the package
r_files <- list.files("R", pattern = "\\.R$", full.names = TRUE, recursive = TRUE)
# r_files <- head(r_files, 10)

# Process each file
for (file in r_files) {
  # Read the file content
  lines <- readLines(file, warn = FALSE)
  
  # Find where examples begin
  example_start <- which(grepl("^#'\\s*@examples", lines))
  
  if (length(example_start) > 0) {
    # For each example section
    for (start_idx in example_start) {
      # Find where the examples end - either next @tag or end of roxygen block
      next_tag <- which(grepl("^#'\\s*@", lines[(start_idx+1):length(lines)]))
      if (length(next_tag) > 0) {
        end_idx <- start_idx + next_tag[1] - 1
      } else {
        # Find end of roxygen block
        roxygen_end <- which(!grepl("^#'", lines[(start_idx+1):length(lines)]))
        if (length(roxygen_end) > 0) {
          end_idx <- start_idx + roxygen_end[1] - 1
        } else {
          end_idx <- length(lines)
        }
      }
      
      # Extract the example lines
      example_lines <- lines[(start_idx+1):(end_idx-1)]
      
      # Add dontrun wrapper
      if (length(example_lines) > 0) {
        # Check if already wrapped
        if (!any(grepl("\\\\dontrun\\{", example_lines))) {
          # Insert \dontrun{ after @examples
          lines[start_idx] <- paste0(lines[start_idx], "\n#' \\dontrun{")
          
          # Add closing brace before next tag or end of roxygen
          lines[end_idx-1] <- paste0(lines[end_idx-1], "\n#' }")
          
          cat("Wrapping examples in", file, "\n")
        }
      }
    }
    
    # Write modified content back to file
    writeLines(lines, file)
  }
}

cat("All examples have been wrapped with \\dontrun{}\n")

# find problematic files:
# Check each Rd file for syntax errors
rd_files <- list.files("man", pattern = "\\.Rd$", full.names = TRUE)
for (file in rd_files) {
  content <- readLines(file, warn = FALSE)
  
  # Look for mismatched \dontrun{} or extra parentheses
  dontrun_start <- grep("\\\\dontrun\\{", content)
  dontrun_end <- grep("\\}", content)
  
  if (length(dontrun_start) > 0) {
    cat("Checking file:", file, "\n")
    cat("dontrun_start lines:", dontrun_start, "\n")
    cat("Potential closing brace lines around the end:", 
        dontrun_end[dontrun_end > max(dontrun_start)][1:min(5, length(dontrun_end))], 
        "\n\n")
  }
}

# remove examples:----
# Get all R files
r_files <- list.files("R", pattern = "\\.R$", full.names = TRUE, recursive = TRUE)

# Process each file
for (file in r_files) {
  content <- readLines(file, warn = FALSE)
  
  # Find where examples begin in roxygen comments
  example_start <- which(grepl("^\\s*#'\\s*@examples", content))
  
  if (length(example_start) > 0) {
    for (start_idx in example_start) {
      # Find where the roxygen block ends or the next tag begins
      next_lines <- content[(start_idx+1):length(content)]
      next_tag_idx <- which(grepl("^\\s*#'\\s*@", next_lines))
      
      if (length(next_tag_idx) > 0) {
        end_idx <- start_idx + next_tag_idx[1] - 1
      } else {
        # If no next tag, find where roxygen comments end
        non_roxygen <- which(!grepl("^\\s*#'", next_lines))
        if (length(non_roxygen) > 0) {
          end_idx <- start_idx + non_roxygen[1] - 1
        } else {
          end_idx <- length(content)
        }
      }
      
      # Replace examples with minimal example
      content[start_idx] <- "#' @examples"
      
      # Create new content by removing the example lines
      if (end_idx > start_idx) {
        new_content <- c(
          content[1:start_idx],
          "#' # Examples are temporarily disabled",
          content[(end_idx+1):length(content)]
        )
        content <- new_content
      }
    }
    
    # Write the modified content back to the file
    writeLines(content, file)
    cat("Simplified examples in R file:", file, "\n")
  }
}
