add_drift_flag <- function(df){

  # Only test for biofilm growth on turbidity sensors
  if(unique(df$parameter) %in% c("FDOM Fluorescence", "Turbidity", "Chl-a Fluorescence")){

    # subset data to turbidity and conductivity only
    # sub <- df %>%
    #   dplyr::filter(parameter %in% c("Turbidity", "Specific Conductivity")) %>%
    #   dplyr::select(DT_round, DT_join, parameter, mean) %>%
    #   tidyr::pivot_wider(names_from = parameter, values_from = mean)
    # names(sub) <- make.names(names(sub))
    #
    # Check if a given window's worth of sensor data has a linear
    # relationship with time (indicates a "steady" drift)
    progressive_drift <- function(x) {
      # Only assess time series with less than 90% missing data in
      # the rolling window:
      if(length(x[!is.na(x)]) > (length(x) - (length(x)*0.1))){

        # Fit linear model
        model <- lm(x ~ c(1:length(x)), na.action = na.omit)

        # Extract R-squared value
        r_squared <- summary(model)$r.squared

        # Return R-squared value
        return(r_squared)

      } else {

        # If not enough data, set slope to 0
        no_slope <- 0

        return(no_slope)

      }
    }

    # Function to check if a selected time window's mean R-squared value is
    # at least 60% (i.e., fairly high linear relationship with time indicates a "steady" drift)
    check_too_steady <- function(x) {
      mean(x) >= 0.60
    }

    # Use all the functions above to see if a given time window's R-squared with time is strong. If the one-day OR the three-day slope
    # for a selected parameter is greater than 60%, we determine it has "failed" (i.e., drift seems to exist)


      df <- df %>%
        data.table::data.table() %>%
        dplyr::mutate(r2_s_right = data.table::frollapply(mean, n = 96, FUN = progressive_drift, align = "right", fill = NA),
                      r2_s_center = data.table::frollapply(mean, n = 96, FUN = progressive_drift, align = "left", fill = NA),
                      r2_l_right = data.table::frollapply(mean, n = 288, FUN = progressive_drift, align = "right", fill = NA),
                      r2_l_center = data.table::frollapply(mean, n = 288, FUN = progressive_drift, align = "left", fill = NA),
                      tightest_r = pmax(r2_s_center, r2_s_right, r2_l_center, r2_l_right, na.rm = TRUE),
                      failed = data.table::frollapply(tightest_r, n = 96, FUN = check_too_steady, align = "right", fill = NA)) %>%
        add_flag(., failed == 1 & !grepl("drift", flag), "drift")

      return(df)

  } else {

    return(df)
  }

  }


