library(tidyverse)
library(here)

seasonal_thresholds <- read_csv(
  here("data", "qaqc_files", "outdated_seasonal_thresholds.csv"), 
  show_col_types = FALSE
) %>% 
  filter(!grepl("virridy", site)) %>% 
  mutate(site = gsub(" ", "", site))

site_names_lookup <- tibble(
  site = c("tamasag", "legacy", "lincoln", "timberline", "prospect", 
           "boxelder", "archery", "riverbluffs"), 
  natural_name = c("bellvue", "salyer", "udall", "Riverbend", 
                   "Cottonwood", "ELC", "Archery", "Riverbluffs")) %>% 
  mutate(natural_name = stringr::str_to_lower(natural_name))

site_names_lookup  <- tribble(
  ~site,         ~natural_name,
  "tamasag",     "bellvue",
  "legacy",      "salyer",
  "lincoln",     "udall",
  "timberline",  "riverbend",
  "prospect",    "cottonwood",
  "boxelder",    "elc",
  "archery",     "archery",
  "riverbluffs", "riverbluffs")

altered_seasonal_thresholds <- seasonal_thresholds %>% 
  left_join(site_names_lookup, by = "site") %>% 
  mutate(site = coalesce(natural_name, site)) %>% 
  filter(site %in% site_names_lookup$natural_name)
  select(-natural_name)

write_csv(altered_seasonal_thresholds, here("data", "qaqc_files", "seasonal_thresholds.csv"))
