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
  lapply(c("data.table", 
           "httr2", 
           "tidyverse", 
           "lubridate", 
           "zoo", 
           "padr", 
           "stats", 
           "RcppRoll", 
           "yaml", 
           "janitor", 
           "here"), 
         package_loader)
)

# Loading the testthat data ----

