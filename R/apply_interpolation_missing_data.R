#' @title Interpolate missing data within a maximum gap
#' @export
#'
#' @description
#' Fills NA values in a numeric column using linear or spline interpolation,
#' limited to gaps of at most `max_gap` consecutive missing observations. Rows
#' belonging to runs of NAs longer than `max_gap` are left as NA.
#'
#' The function validates two preconditions before interpolating:
#' - The datetime column must be of class `POSIXct`/`POSIXt`; an error is
#'   thrown otherwise.
#' - Within each site/parameter group the timestamps must be equally spaced; a
#'   warning is issued when they are not, because unequal spacing can cause
#'   `zoo::na.approx` and `zoo::na.spline` to produce misleading estimates.
#'   Use `padr::pad()` to fill gaps before calling this function.
#'
#' Interpolation is performed independently for every combination of
#' `site_col` and `parameter_col`, so a single call can process a data frame
#' that contains multiple sites and parameters simultaneously.
#'
#' @param df A data frame containing water quality measurements.
#' @param site_col Name of the column identifying the monitoring site. Defaults
#'   to `"site"`.
#' @param parameter_col Name of the column identifying the measured parameter.
#'   Defaults to `"parameter"`.
#' @param dt_col Name of the `POSIXct` datetime column used for temporal
#'   ordering and equal-spacing validation. Defaults to `"DT_round"`.
#' @param value_col Name of the numeric column containing the values to
#'   interpolate (may contain NAs). Defaults to `"smoothed_mean"`.
#' @param max_gap Maximum number of consecutive NA observations that will be
#'   filled. Gaps longer than this are left as NA. Defaults to `4`.
#' @param new_value_col Name of the output column that will hold the
#'   interpolated values. Defaults to `"mean_filled"`.
#' @param method Interpolation method to use. Must be one of:
#' - `"linear"`: straight-line interpolation via `zoo::na.approx`.
#' - `"spline"`: cubic spline interpolation via `zoo::na.spline`.
#'
#' Defaults to `"linear"`.
#'
#' @return A data frame with the same structure as the input plus a new column
#' named `new_value_col` containing the interpolated values. Rows within gaps
#' exceeding `max_gap` retain `NA`. The data are sorted by `site_col`,
#' `parameter_col`, and `dt_col`.
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [low_pass_filter()]

apply_interpolation_missing_data <- function(df, site_col = "site", parameter_col = "parameter",
                                             dt_col = "DT_round", value_col = "smoothed_mean",
                                             max_gap = 4, new_value_col = "mean_filled",
                                             method = "linear") {
  
  dt_vals <- df[[dt_col]]
  if (!lubridate::is.POSIXt(dt_vals)) {
    stop("Error: The datetime column must be POSIXct/POSIXt.")
  }
  
  pad_check <- df %>%
    dplyr::group_by(dplyr::across(all_of(c(site_col, parameter_col)))) %>%
    dplyr::group_walk(~ {
      dt_vals      <- sort(unique(.x[[dt_col]]))
      dt_diffs     <- diff(dt_vals)
      unique_diffs <- unique(dt_diffs)
      if (length(unique_diffs) > 1) {
        warning(
          paste0("Error: Datetime column is not equally spaced for site = ",
                 unique(.y[[site_col]]), ", parameter = ", unique(.y[[parameter_col]]),
                 ". Please pad with padr::pad() first.")
        )
      }
    })
  
  `%nin%` = Negate(`%in%`)
  
  if(method %nin% c("spline", "linear")){
    stop("method must be either 'spline' or 'linear'")
  }
  
  if(method == "linear") {
    return(
      df %>%
        arrange(!!sym(site_col), !!sym(parameter_col), !!sym(dt_col)) %>%
        group_by(!!sym(site_col), !!sym(parameter_col)) %>%
        mutate(
          !!sym(new_value_col) := zoo::na.approx(
            x      = as.numeric(!!sym(dt_col)),
            object = !!sym(value_col),
            maxgap = max_gap,
            na.rm  = FALSE
          )
        ) %>%
        ungroup()
    )
  }
  
  if(method == "spline"){
    return(
      df %>%
        arrange(!!sym(site_col), !!sym(parameter_col), !!sym(dt_col)) %>%
        group_by(!!sym(site_col), !!sym(parameter_col)) %>%
        mutate(
          !!sym(new_value_col) := zoo::na.spline(
            x      = as.numeric(!!sym(dt_col)),
            object = !!sym(value_col),
            maxgap = max_gap,
            na.rm  = FALSE
          )
        ) %>%
        ungroup()
    )
  }
}