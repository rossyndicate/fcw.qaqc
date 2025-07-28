#' Fix and standardize site names in a dataframe
#'
#' This function standardizes site names by converting them to lowercase and 
#' replacing specific site names with their standardized equivalents. It handles
#' case-insensitive matching and performs the following replacements:
#' - "tamasag" -> "bellvue"
#' - "legacy" -> "salyer" 
#' - "lincoln" -> "udall"
#' - "timberline" -> "riverbend"
#' - "prospect" -> "cottonwood"
#' - "boxelder" -> "elc"
#' - "archery" -> "archery" (no change)
#' - "river bluffs" -> "riverbluffs" (removes space)
#'
#' @param df A dataframe containing a column with site names to be standardized
#' @param site_col The name of the column containing site names (default: "site")
#'
#' @return A dataframe with the same structure as the input, but with standardized site names
#'
#' @examples
#' \dontrun{
#' # Example usage with default column name
#' sample_df <- data.frame(site = c("Tamasag", "LEGACY", "River Bluffs"))
#' fixed_df <- fix_site_names(sample_df)
#' 
#' # Example usage with custom column name
#' sample_df2 <- data.frame(location = c("Tamasag", "LEGACY", "River Bluffs"))
#' fixed_df2 <- fix_site_names(sample_df2, site_col = "location")
#' }
#'
#' @export
fix_site_names <- function(df, site_col = "site") {
  fixed_df <- df %>%
    dplyr::mutate(!!sym(site_col) := tolower(!!sym(site_col))) %>%
    # renaming all the sites, just in case
    dplyr::mutate(!!sym(site_col) := dplyr::case_when(
      grepl("tamasag", !!sym(site_col), ignore.case = TRUE) ~ stringr::str_replace(!!sym(site_col), "tamasag", "bellvue"),
      grepl("legacy", !!sym(site_col), ignore.case = TRUE) ~ stringr::str_replace(!!sym(site_col), "legacy", "salyer"),
      grepl("lincoln", !!sym(site_col), ignore.case = TRUE) ~ stringr::str_replace(!!sym(site_col), "lincoln", "udall"),
      grepl("timberline", !!sym(site_col), ignore.case = TRUE) ~ stringr::str_replace(!!sym(site_col), "timberline", "riverbend"),
      grepl("prospect", !!sym(site_col), ignore.case = TRUE) ~ stringr::str_replace(!!sym(site_col), "prospect", "cottonwood"),
      grepl("boxelder", !!sym(site_col), ignore.case = TRUE) ~ stringr::str_replace(!!sym(site_col), "boxelder", "elc"),
      grepl("archery", !!sym(site_col), ignore.case = TRUE) ~ stringr::str_replace(!!sym(site_col), "archery", "archery"),
      grepl("river bluffs", !!sym(site_col), ignore.case = TRUE) ~ stringr::str_replace(!!sym(site_col), "river bluffs", "riverbluffs"),
      grepl("mountaincampus", !!sym(site_col), ignore.case = TRUE) ~ stringr::str_replace(!!sym(site_col), "mountaincampus", "mtncampus"),
      TRUE ~ !!sym(site_col))
    )
  return(fixed_df)
}
