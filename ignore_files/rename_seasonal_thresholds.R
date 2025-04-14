library(tidyverse)
library(here)

seasonal_thresholds <- read_csv(
  here("data", "qaqc_files", "outdated_seasonal_thresholds.csv"), 
  show_col_types = FALSE
) %>% 
  filter(!grepl("virridy", site)) %>% 
  mutate(site = gsub(" ", "", site))

site_names_lookup <- tibble(
  site = c("pbd", "tamasag", "legacy", "lincoln", "timberline", "prospect", 
           "boxelder", "archery", "riverbluffs"), 
  natural_name = c("pbd", "bellvue", "salyer", "udall", "Riverbend", 
                   "Cottonwood", "ELC", "Archery", "Riverbluffs")) %>% 
  mutate(natural_name = stringr::str_to_lower(natural_name))

altered_seasonal_thresholds <- seasonal_thresholds %>% 
  left_join(site_names_lookup, by = "site") %>% 
  mutate(site = coalesce(natural_name, site)) %>% 
  filter(site %in% site_names_lookup$natural_name)
  select(-natural_name)

write_csv(altered_seasonal_thresholds, here("data", "qaqc_files", "seasonal_thresholds.csv"))
