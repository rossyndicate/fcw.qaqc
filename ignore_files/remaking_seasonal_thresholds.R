library(tidyverse)
library(here)
library(data.table)
library(lubridate)
library(plotly)
library(ggplot2)
library(arrow)

options(arrow.unsafe_metadata = TRUE)

# Setting configuration
site_levels <- c(
  "bellvue",
  "salyer",
  "udall",
  "riverbend",
  "cottonwood",
  "elc",
  "archery",
  "riverbluffs"
)

parameter_levels <- c(
  "Depth",
  "DO",
  "ORP",
  "pH",
  "Specific Conductivity",
  "Temperature",
  "Turbidity",
  "Chl-a Fluorescence"
) # Chla at the end with turb because both are hard to analyze visually

# Read in data ====

## Read in AutoQAQC'd 2024 data
auto_verified_2024 <- arrow::read_parquet(here("ignore_files", "data", "syn_output_AutoQAQCPWQN20250505-T165159Z.parquet"),
  as_data_frame = TRUE
) %>%
  select(DT_round, DT_join, site, parameter, mean, auto_flag)

## Read in verified 2023 data
pwqn_dir_path <- here("..", "poudre_sonde_network")

verified_data_files <- list.files(here(pwqn_dir_path, "data", "virridy_verification", "verified_directory"),
  full.names = TRUE
)

manual_verified_2023 <- verified_data_files %>%
  map_dfr(function(file_path) {
    verified_df <- read_rds(file_path) %>%
      mutate(verification_status = as.character(verification_status))
  }) %>%
  data.table() %>%
  filter(!grepl("virridy", site, ignore.case = TRUE)) %>% 
  mutate(
    site = case_when(
      site == "tamasag" ~ "bellvue",
      site == "legacy" ~ "salyer",
      site == "lincoln" ~ "udall",
      site == "timberline" ~ "riverbend",
      site == "prospect" ~ "cottonwood",
      site == "boxelder" ~ "elc",
      site == "archery" ~ "archery",
      site == "river bluffs" ~ "riverbluffs",
      TRUE ~ site # Convert any sites that don't match to NA's
    ),
    clean_mean = case_when(
      is.na(flag) & verification_status == "PASS" ~ mean,
      is.na(flag) & verification_status == "FAIL" ~ NA,
      !is.na(flag) & verification_status == "PASS" ~ NA,
      !is.na(flag) & verification_status == "FAIL" ~ mean
    ),
    clean_flag = case_when(
      is.na(flag) & verification_status == "PASS" ~ NA,
      is.na(flag) & verification_status == "FAIL" ~ "MANUAL FLAG",
      !is.na(flag) & verification_status == "PASS" ~ flag,
      !is.na(flag) & verification_status == "FAIL" ~ NA
    )
  ) %>%
  filter(
    !is.na(site),
    site %in% site_levels
  ) %>%
  select(DT_round, DT_join, site, parameter, mean = clean_mean, auto_flag = clean_flag, verification_status)

## Read in various seasonal thresholds and filter/select the appropriate data
### Seasonal Thresholds via make_threshold_table()
auto_seasonal_thresholds <- read_csv(here("test_data", "qaqc_files", "seasonal_thresholds.csv"),
  show_col_types = FALSE
) %>%
  filter(season == "winter_baseflow") %>%
  select(season, site, parameter, t_mean01, t_mean99)

### Seasonal Thresholds based on field experience
manual_seasonal_thresholds <- read_csv(here("..", "poudre_sonde_network", "data", "qaqc", "realistic_thresholds.csv"), show_col_types = FALSE) %>%
  filter(parameter %in% parameter_levels)

### Sensor Specification Thresholds
sensor_specification_thresholds <- yaml::read_yaml(here("test_data", "qaqc_files", "sensor_spec_thresholds.yml"))

sensor_specification_thresholds <- tibble(
  parameter = names(sensor_specification_thresholds),
  sensor_min = unlist(map(sensor_specification_thresholds, 1)),
  sensor_max = unlist(map(sensor_specification_thresholds, 2))
) %>%
  filter(parameter %in% parameter_levels)

# Setting functions
threshold_retriever <- function(df = seasonal_thresholds, site_arg, param_arg) {
  filtered_df <- df %>%
    filter(
      season == "winter_baseflow",
      site %in% site_arg,
      parameter %in% param_arg
    )
  return(c(filtered_df$t_mean01, filtered_df$t_mean99))
}


# Subset the two data sets by parameter
## 2024 subset by parameter
auto_subset_2024 <- setNames(
  map(
    parameter_levels,
    function(parameter_arg) {
      df <- auto_verified_2024 %>%
        filter(parameter == parameter_arg)
      # Get the sites that actually exist in this parameter subset
      existing_sites <- unique(df$sites)
      # Maintain the original ordering from site_levels for the existing sites
      ordered_existing_sites <- site_levels[site_levels %in% existing_sites]
      df %>% 
        mutate(site = factor(site, levels = site_levels)) %>%
        arrange(site)
      return(df)
    }
  ),
  parameter_levels
)

## 2023 subset by parameter
manual_subset_2023 <- setNames(
  map(
    parameter_levels,
    function(parameter_arg) {
      df <- manual_verified_2023 %>%
        filter(parameter == parameter_arg)
      # Get the sites that actually exist in this parameter subset
      existing_sites <- unique(df$sites)
      # Maintain the original ordering from site_levels for the existing sites
      ordered_existing_sites <- site_levels[site_levels %in% existing_sites]
      df %>% 
        mutate(site = factor(site, levels = site_levels)) %>%
        arrange(site)
      return(df)
    }
  ),
  parameter_levels
)

# Making the initial plots
plot_list_2024 <- imap(
  auto_subset_2024,
  function(param_subset_df, idx) {
    # Generating the seasonal backgrounds
    season_backgrounds <- param_subset_df %>%
      mutate(
        month = lubridate::month(DT_round),
        year = lubridate::year(DT_round),
        season = case_when(
          month %in% c(12,1,2,3,4) ~ "winter_baseflow",
          month %in% c(5,6) ~ "snowmelt",
          month %in% c(7,8,9) ~ "monsoon",
          month %in% c(10,11) ~ "fall_baseflow",
          TRUE ~ NA_character_
          ),
        year_month_season = paste0(as.character(year), as.character(month),"-",season)
        ) %>% 
      group_by(year_month_season) %>%
      summarize(
        start = min(DT_round, na.rm = TRUE),
        end = max(DT_round, na.rm = TRUE)
      ) %>%
      mutate(
        season = case_when(
          grepl("winter_baseflow", year_month_season, ignore.case = TRUE) ~ "winter_baseflow",
          grepl("snowmelt", year_month_season, ignore.case = TRUE)~ "snowmelt",
          grepl("monsoon", year_month_season, ignore.case = TRUE) ~ "monsoon",
          grepl("fall_baseflow", year_month_season, ignore.case = TRUE) ~ "fall_baseflow",
          TRUE ~ "no_season"
        )
      ) %>%
    ungroup()  %>%
    arrange(start)
    # Factorizing the sites
    param_subset_df <- param_subset_df %>%
      mutate(site = factor(site, levels = site_levels))
    # Create a dataframe of thresholds for each site
    threshold_data <- map_df(
      unique(param_subset_df$site),
      function(site_arg) {
        thresholds <- threshold_retriever(df = auto_seasonal_thresholds, site_arg = site_arg, param_arg = idx)
        if (length(thresholds) == 2) {
          # Making an autothresholds table
          auto_thresholds <- tibble(
            site = site_arg,
            parameter = idx,
            auto_min = thresholds[1],
            auto_max = thresholds[2]
          )
          # Making a manual thresholds table
          manual_thresholds <- manual_seasonal_thresholds %>%
            filter(parameter == idx) %>%
            rename(manual_min = min, manual_max = max)
          # Combining the two tables
          combined_thresholds <- left_join(auto_thresholds, manual_thresholds, by = "parameter")
          return(combined_thresholds)
        } else {
          # Making an autothresholds table
          auto_thresholds <- tibble(
            site = site_arg,
            parameter = idx,
            auto_min = NA,
            auto_max = NA
          )
          # Making a manual thresholds table
          manual_thresholds <- manual_seasonal_thresholds %>%
            filter(parameter == idx) %>%
            rename(manual_min = min, manual_max = max)
          # Combining the two tables
          combined_thresholds <- left_join(auto_thresholds, manual_thresholds, by = "parameter")
          return(combined_thresholds)
        }
      }
    ) %>%
      mutate(site = factor(site, levels = site_levels))
    # Generating the plots
    p <- ggplot(data = param_subset_df, aes(x = DT_round, y = mean)) +
      geom_rect(
        data = season_backgrounds,
        aes(NULL, NULL,
            xmin = start, xmax = end,
            fill = season),
        ymin = -1000, ymax = 1000,
        inherit.aes = FALSE, 
        alpha = 0.4
      ) +
      scale_fill_manual(
        values = c(
          "winter_baseflow" = "blue",
          "snowmelt" = "green",
          "monsoon" = "yellow",
          "fall_baseflow" = "orange",
          "white" = "white"
        ),
        name = "Season"
      ) +
      geom_point(aes(color = auto_flag)) +
      facet_wrap(vars(site),
        nrow = 2,
        ncol = 4,
        scales = "free_y",
        drop = FALSE
      ) +
      ggtitle(paste0(idx, " 2024"))
    # Add thresholds if we have any
    if (nrow(threshold_data) > 0) {
      p <- p +
        geom_hline(data = threshold_data, aes(yintercept = auto_min, linetype = "lower auto"), color = "blue") +
        geom_hline(data = threshold_data, aes(yintercept = auto_max, linetype = "upper auto"), color = "blue") +
        geom_hline(data = threshold_data, aes(yintercept = manual_min, linetype = "lower manual"), color = "green") +
        geom_hline(data = threshold_data, aes(yintercept = manual_max, linetype = "upper manual"), color = "green") +
        # You can customize the linetype scale to make lower/upper more distinct
        scale_linetype_manual(values = c(
          "lower auto" = "dashed", "upper auto" = "solid",
          "upper manual" = "solid", "lower manual" = "dashed"
        )) +
        # Position the legend at the bottom for easier reading
        theme(legend.position = "bottom")
    }
    # Convert to plotly
    p_plotly <- plotly::ggplotly(p, tooltip = c("x", "y", "color")) %>%
      plotly::layout(
        legend = list(
          orientation = "v",
          y = 0.5,
          font = list(size = 10),
          itemsizing = "constant"
        ),
        margin = list(r = 150) # Add margin for legend
      )
    return(p_plotly)
  }
)

plot_list_2023 <- imap(
  manual_subset_2023,
  function(param_subset_df, idx) {
    # Generating the seasonal backgrounds
    season_backgrounds <- param_subset_df %>%
      mutate(
        month = lubridate::month(DT_round),
        year = lubridate::year(DT_round),
        season = case_when(
          month %in% c(12,1,2,3,4) ~ "winter_baseflow",
          month %in% c(5,6) ~ "snowmelt",
          month %in% c(7,8,9) ~ "monsoon",
          month %in% c(10,11) ~ "fall_baseflow",
          TRUE ~ NA_character_
          ),
        year_month_season = paste0(as.character(year), as.character(month),"-",season)
        ) %>% 
      group_by(year_month_season) %>%
      summarize(
        start = min(DT_round, na.rm = TRUE),
        end = max(DT_round, na.rm = TRUE)
      ) %>%
      mutate(
        season = case_when(
          grepl("winter_baseflow", year_month_season, ignore.case = TRUE) ~ "winter_baseflow",
          grepl("snowmelt", year_month_season, ignore.case = TRUE)~ "snowmelt",
          grepl("monsoon", year_month_season, ignore.case = TRUE) ~ "monsoon",
          grepl("fall_baseflow", year_month_season, ignore.case = TRUE) ~ "fall_baseflow",
          TRUE ~ "no_season"
        )
      ) %>%
    ungroup()  %>%
    arrange(start)
    # Factorizing the sites
    param_subset_df <- param_subset_df %>%
      mutate(site = factor(site, levels = site_levels))
    # Create a dataframe of thresholds for each site
    threshold_data <- map_df(
      unique(param_subset_df$site),
      function(site_arg) {
        thresholds <- threshold_retriever(df = auto_seasonal_thresholds, site_arg = site_arg, param_arg = idx)
        if (length(thresholds) == 2) {
          # Making an autothresholds table
          auto_thresholds <- tibble(
            site = site_arg,
            parameter = idx,
            auto_min = thresholds[1],
            auto_max = thresholds[2]
          )
          # Making a manual thresholds table
          manual_thresholds <- manual_seasonal_thresholds %>%
            filter(parameter == idx) %>%
            rename(manual_min = min, manual_max = max)
          # Combining the two tables
          combined_thresholds <- left_join(auto_thresholds, manual_thresholds, by = "parameter")
          return(combined_thresholds)
        } else {
          # Making an autothresholds table
          auto_thresholds <- tibble(
            site = site_arg,
            parameter = idx,
            auto_min = NA,
            auto_max = NA
          )
          # Making a manual thresholds table
          manual_thresholds <- manual_seasonal_thresholds %>%
            filter(parameter == idx) %>%
            rename(manual_min = min, manual_max = max)
          # Combining the two tables
          combined_thresholds <- left_join(auto_thresholds, manual_thresholds, by = "parameter")
          return(combined_thresholds)
        }
      }
    ) %>%
      mutate(site = factor(site, levels = site_levels))
    # Generating the plots
    p <- ggplot(data = param_subset_df, aes(x = DT_round, y = mean)) +
      geom_rect(
        data = season_backgrounds,
        aes(NULL, NULL,
            xmin = start, xmax = end,
            fill = season),
        ymin = -1000, ymax = 1000,
        inherit.aes = FALSE, 
        alpha = 0.4
      ) +
      scale_fill_manual(
        values = c(
          "winter_baseflow" = "blue",
          "snowmelt" = "green",
          "monsoon" = "yellow",
          "fall_baseflow" = "orange",
          "no_season" = "white"
        ),
        name = "Season"
      ) +
      geom_point(aes(color = auto_flag)) +
      facet_wrap(vars(site),
        nrow = 2,
        ncol = 4,
        scales = "free_y",
        drop = FALSE
      ) +
      ggtitle(paste0(idx, " 2023"))
    # Add thresholds if we have any
    if (nrow(threshold_data) > 0) {
      p <- p +
        geom_hline(data = threshold_data, aes(yintercept = auto_min, linetype = "lower auto"), color = "blue") +
        geom_hline(data = threshold_data, aes(yintercept = auto_max, linetype = "upper auto"), color = "blue") +
        geom_hline(data = threshold_data, aes(yintercept = manual_min, linetype = "lower manual"), color = "green") +
        geom_hline(data = threshold_data, aes(yintercept = manual_max, linetype = "upper manual"), color = "green") +
        # You can customize the linetype scale to make lower/upper more distinct
        scale_linetype_manual(values = c(
          "lower auto" = "dashed", "upper auto" = "solid",
          "upper manual" = "solid", "lower manual" = "dashed"
        )) +
        # Position the legend at the bottom for easier reading
        theme(legend.position = "bottom")
    }
    # Convert to plotly
    p_plotly <- plotly::ggplotly(p, tooltip = c("x", "y", "color")) %>%
      plotly::layout(
        legend = list(
          orientation = "v",
          y = 0.5,
          font = list(size = 10),
          itemsizing = "constant"
        ),
        margin = list(r = 150) # Add margin for legend
      )
    return(p_plotly)
  }
)
# All that to say: the thresholds should be reworked
# What am I trying to do here... thresholds are based on 
# Remaking the thresholds

## Plotting all the thresholds against each other 
auto_seasonal_thresholds
manual_seasonal_thresholds
sensor_specification_thresholds

