#' @title Correct data with underlying bad calibrations using In-Situ calibration reports
#'
#' @description This function uses calibration reports collected by field crews to "correct"
#' instances of poor calibration. These are infrequent, and this function is not yet complete.
#'
#' @param df Site-parameter data frame
#' @param cal_errors A data frame containing a list of known instances of poor calibration
#'
#' @return A data frame with modified `mean` values corrected for improper calibration. These observations are also identified in the `cal_fix` column.

fix_calibration <- function(df, cal_errors){

  # Filter records for relevant site-param information
  df_site <- unique(df$site)
  df_parameter <- unique(df$parameter)

  # Depth calibration requires its own fix. It's hacky and I don't like it, but
  # the way that In-Situ's calibration report reports pressure calibrations makes it
  # impossible to actually back calibrate. Therefore, I'm just assuming that level
  # doesn't actually change after bad calibrations, and hard code it so that the
  # first "bad" depth is forced to equal the last "good" depth and I offset all
  # subsequent "bad" depth values by the difference between them.

    if(!"Depth" %in% df$parameter){

      nope <- df %>% dplyr::mutate(cal_fix = NA)

      return(nope)

    }

  if(df_parameter == "Depth"){


    df <- df %>%
      dplyr::mutate(raw = mean)

    if("Depth" %in% df$parameter & "archery" %in% df$site){

      #df <- all_data_flagged[["archery-Depth"]] # for testing

      site_depth <- df %>%
        dplyr::mutate(relative_depth = ifelse(DT_round >= lubridate::as_datetime('2022-05-21 15:45:00', "MST") & DT_round <= lubridate::as_datetime('2022-05-24 15:45:00', "MST"),
                                              mean +
                                                abs(
                                                  dplyr::filter(df, as.character(DT_round) == ('2022-05-24 15:15:00'))$mean -
                                                    dplyr::filter(df, as.character(DT_round) == ('2022-05-24 15:45:00'))$mean
                                                ),
                                              mean))

    } else if("Depth" %in% df$parameter & "timberline" %in% df$site){

      # df <- all_data_flagged[["timberline-Depth"]]

      site_depth <- df %>%
        dplyr::mutate(relative_depth = ifelse(year == "2022" & DT_round <= lubridate::as_datetime('2022-04-07 17:00:00', "MST"),
                                              as.numeric(mean) +
                                                abs(
                                                  dplyr::filter(df, as.character(DT_round) == ('2022-04-07 16:15:00'))$mean -
                                                    dplyr::filter(df, as.character(DT_round) == ('2022-04-07 17:15:00'))$mean
                                                ),
                                              as.numeric(mean)))

    } else if ("Depth" %in% df$parameter & "legacy" %in% df$site) {

      # df <- all_data_flagged[["legacy-Depth"]]

      site_depth <- df %>%
        dplyr::mutate(relative_depth = ifelse(DT_round >= lubridate::as_datetime('2022-04-06 06:00:00', "MST") & DT_round <= lubridate::as_datetime('2022-04-12 09:15:00', "MST"),
                                              as.numeric(mean) +
                                                abs(
                                                  dplyr::filter(df, as.character(DT_round) == ('2022-04-12 09:15:00'))$mean -
                                                    dplyr::filter(df, as.character(DT_round) == ('2022-04-12 09:30:00'))$mean
                                                ),
                                              mean))

      site_depth <-  site_depth %>%
        dplyr::mutate(relative_depth = ifelse(DT_round >= lubridate::as_datetime('2022-07-08 17:00:00', "MST") & DT_round <= lubridate::as_datetime('2022-07-12 09:00:00', "MST"),
                                              relative_depth +
                                                abs(
                                                  dplyr::filter(site_depth, as.character(DT_round) == ('2022-07-08 14:15:00'))$relative_depth -
                                                    dplyr::filter(site_depth, as.character(DT_round) == ('2022-07-08 17:00:00'))$relative_depth
                                                ),
                                              relative_depth))

      site_depth <-  site_depth %>%
        dplyr::mutate(relative_depth = ifelse(DT_round >= lubridate::as_datetime('2022-07-22 11:30:00', "MST") & DT_round <= lubridate::as_datetime('2022-07-25 14:15:00', "MST"),
                                              relative_depth +
                                                abs(
                                                  dplyr::filter(site_depth, as.character(DT_round) == ('2022-07-22 09:45:00'))$relative_depth -
                                                    dplyr::filter(site_depth, as.character(DT_round) == ('2022-07-22 11:30:00'))$relative_depth
                                                ),
                                              relative_depth))

    } else if ("Depth" %in% df$parameter & "tamasag" %in% df$site) {

      # df <- all_data_flagged[["tamasag-Depth"]]

      site_depth <- df %>%
        dplyr::mutate(relative_depth = ifelse(DT_round <= "2022-04-24 07:15:00" & year == "2022",
                                              as.numeric(mean) +
                                                abs(
                                                  dplyr::filter(df, as.character(DT_round) == ('2022-04-24 07:15:00'))$mean -
                                                    dplyr::filter(df, as.character(DT_round) == ('2022-04-24 07:30:00'))$mean
                                                ),
                                              mean))

      # return(tamasag_depth)

    } else {

      site_depth <- df %>%
        dplyr::mutate(relative_depth = mean,
                      cal_fix = NA)
    }

    depth_flagged <- site_depth %>%
      dplyr::mutate(mean = relative_depth,
                    cal_fix = ifelse(raw != mean, "calibration fix", NA)) %>%
      dplyr::select(-relative_depth)

    return(depth_flagged)

  }

  # PLACEHOLDER UNTIL OTHER CALIBRATIONS ARE FULLY DEVELOPED:
  return(df %>%
           dplyr::mutate(cal_fix = NA))

}

# # For non-depth parameters, we can refer to the calibration report:
# bad_cal <- cal_errors %>%
#   dplyr::mutate(start_DT = as.character(lubridate::as_datetime(start_DT)),
#                 end_DT = as.character(lubridate::as_datetime(end_DT)),
#                 #report_to_correct = as.character(lubridate::as_datetime(report_to_correct))
#   )
#
# bad_cal_records_filtered <- bad_cal %>%
#    dplyr::filter(site == df_site) %>%
#    dplyr::filter(grepl(df_parameter, parameter, ignore.case = TRUE)) %>%
#    dplyr::mutate(end_DT = ifelse(is.na(end_DT), ymd_hms("9999-12-31 23:59:59", tz = "MST"), end_DT)) %>%
#    dplyr::mutate(end_DT = as.character(as.POSIXct(end_DT, tz = "MST"))) %>%
#   tibble::rowid_to_column()
#
# # If there are no bad calibrations listed for that site-param, return original
# # dataframe, filling our updated "mean" column with the old unmodified values:
# if(nrow(bad_cal_records_filtered == 0)){
#
#   df <- df %>%
#     mutate(raw = mean,
#            mean = mean)
#
#   return(df)
#
# } else {
#
#   cal_tabler <- function(cal_files){
#
#     #cal_files <- list.files("data/calibration_reports")[3] # for testing
#
#     cal <- read_html(paste0(getwd(), "/data/calibration_reports/", cal_files)) %>%
#       html_nodes("div") %>%
#       html_text() %>%
#       as_tibble()
#
#     rdo <- cal %>% filter(grepl("RDO", value)) %>% pull() %>% str_replace_all(., " ", "") %>% tolower()
#
#     ph_orp <- cal %>% filter(grepl("pH/ORP", value)) %>% pull() %>% str_replace_all(., " ", "") %>% tolower()
#
#     conductivity <- cal %>% filter(grepl("Conductivity",value)) %>% pull() %>% str_replace_all(., " ", "") %>% tolower()
#
#     if(length(cal %>% filter(grepl("Turbidity",value)) %>% pull() %>% str_replace_all(., " ", "") %>% tolower()) != 0){
#
#       turbidity <- cal %>% filter(grepl("Turbidity",value)) %>% pull() %>% str_replace_all(., " ", "") %>% tolower()
#
#     } else {
#
#       turbidity <- "No Turbidity Sensor"
#
#     }
#
#     # Always the last sensor when depth is available:
#     depth <- ifelse(str_detect(cal %>% .[nrow(.),] %>% pull() %>% str_replace_all(., " ", "") %>% tolower(), "pressure"),#"psireferencedepth"),
#                     cal %>% .[nrow(.),] %>% pull() %>% str_replace_all(., " ", "") %>% tolower(),
#                     "No Depth Sensor")
#
#     time_mst <- paste0(str_sub(str_match(cal_files, "(\\d+)_mst")[, 2:1][1], 1, 2), ":",
#                        str_sub(str_match(cal_files, "(\\d+)_mst")[, 2:1][1], 3, 4))
#     #str_sub(cal_files, -13, -12),":", str_sub(cal_files, -11, -10))
#
#     date <- str_match(cal_files, "^[^_]+_([0-9]{8})_")[, 2]
#
#     #str_sub(cal_files, -22, -19),"-", str_sub(cal_files, -18, -17),"-", str_sub(cal_files, -16, -15))
#
#     cal_table <- tibble(site = sub("\\_.*", "", cal_files) %>% tolower(),
#
#                         DT = ymd_hm(paste(date, time_mst, tz = "MST")),
#
#                         # Dissolved Oxygen
#                         rdo_cal_date = as.character(mdy(str_match(rdo, "lastcalibrated\\s*(.*?)\\s*calibrationdetails")[,2])),
#                         rdo_slope = str_match(rdo, "slope\\s*(.*?)\\s*offset")[,2],
#                         rdo_offset = str_match(rdo, "offset\\s*(.*?)\\s*mg/l")[,2],
#                         rdo_100 = str_match(rdo, "premeasurement\\s*(.*?)\\s*%satpost")[,2],
#                         rdo_conc = str_match(rdo, "concentration\\s*(.*?)\\s*mg/lpremeasurement")[,2],
#                         rdo_temp = str_match(rdo, "temperature\\s*(.*?)\\s*°c")[,2],
#                         rdo_pressure = str_match(rdo, "pressure\\s*(.*?)\\s*mbar")[,2],
#
#                         # pH
#                         ph_cal_date = as.character(mdy(str_match(ph_orp, "lastcalibrated\\s*(.*?)\\s*calibrationdetails")[,2])),
#                         ph_slope_pre = str_match(ph_orp, "offset1slope\\s*(.*?)\\s*mv/ph")[,2],
#                         ph_offset_pre = str_match(ph_orp, "mv/phoffset\\s*(.*?)\\s*mvslopeandoffset2")[,2],
#                         ph_slope_post = str_match(ph_orp, "offset2slope\\s*(.*?)\\s*mv/ph")[,2],
#                         ph_offset_post = str_match(ph_orp, paste0(ph_slope_post,"mv/phoffset\\s*(.*?)\\s*mvorporp"))[,2],
#                         # Sometimes, the post value can actually be in the high 6 pH... therefore the post measurement regex matching text is conditional
#                         ph_7_nice = str_sub(str_match(ph_orp, "postmeasurementph7\\s*(.*?)\\s*mvcal")[,2], 10, nchar(str_match(ph_orp, "postmeasurementph7\\s*(.*?)\\s*mvcal")[,2])),
#                         ph_7_high = str_sub(str_match(ph_orp, "postmeasurementph8\\s*(.*?)\\s*mvcal")[,2], 10, nchar(str_match(ph_orp, "postmeasurementph8\\s*(.*?)\\s*mvcal")[,2])),
#                         ph_7_low = str_sub(str_match(ph_orp, "postmeasurementph6\\s*(.*?)\\s*mvcal")[,2], 10, nchar(str_match(ph_orp, "postmeasurementph6\\s*(.*?)\\s*mvcal")[,2])),
#                         ph_7 = ifelse(!is.na(ph_7_nice), ph_7_nice,
#                                       ifelse(!is.na(ph_7_high), ph_7_high, ph_7_low)),
#
#                         # ORP
#                         #Newly encountered thing: sometimes the calibration report calls the ORP standard Zobell's, sometimes it's just called "ORP Standard":
#                         orp_offset = ifelse(is.na(str_match(ph_orp, "zobell'soffset\\s*(.*?)\\s*mvtemperature")[,2]) & is.na(str_match(ph_orp, "quickcal\\s*(.*?)\\s*mvtemperature")[,2]),
#                                             str_match(ph_orp, "orpstandardoffset\\s*(.*?)\\s*mvtemperature")[,2],
#                                             ifelse(is.na(str_match(ph_orp, "zobell'soffset\\s*(.*?)\\s*mvtemperature")[,2]) & is.na(str_match(ph_orp, "orpstandardoffset\\s*(.*?)\\s*mvtemperature")[,2]),
#                                                    str_match(ph_orp, "quickcal\\s*(.*?)\\s*mvtemperature")[,2],
#                                                    str_match(ph_orp, "zobell'soffset\\s*(.*?)\\s*mvtemperature")[,2])),
#
#                         # Conductivity
#                         cond_cal_date = as.character(mdy(str_match(conductivity, "lastcalibrated\\s*(.*?)\\s*calibrationdetails")[,2])),
#                         tds_conversion_ppm = str_sub(str_match(conductivity, "tdsconversionfactor\\s*(.*?)\\s*cellconstant")[,2], 6, nchar(str_match(conductivity, "tdsconversionfactor\\s*(.*?)\\s*cellconstant")[,2])),
#
#                         # calibration report formatting has changed in 2024 for this variable. Therefore a post-2024 correction must occur
#                         cond_cell_constant = ifelse(year(DT) < 2024, str_match(conductivity, "cellconstant\\s*(.*?)\\s*referencetemperature")[,2],
#                                                     str_match(conductivity, "cellconstant\\s*(.*?)\\s*offset")[,2]),
#
#                         cond_offset = ifelse(year(DT) < 2024, NA,
#                                              str_match(conductivity, "offset\\s*(.*?)\\s*µs/cm")[,2]),
#
#                         cond_pre = str_match(conductivity,paste0(str_match(conductivity,
#                                                                            "premeasurementactual\\s*(.*?)\\s*specificconductivity")[,2],"specificconductivity\\s*(.*?)\\s*µs/cmpost"))[,2],
#                         cond_post = str_match(conductivity,paste0(str_match(conductivity,
#                                                                             "postmeasurementactual\\s*(.*?)\\s*specificconductivity")[,2],"specificconductivity\\s*(.*?)\\s*µs/cm"))[,2]) %>%
#                         # if(turbidity == "No Turbidity Sensor"){
#                         # # Turbidity
#                         # turb_cal_date = "None",
#                         # ntu_slope = "None",
#                         # ntu_offset = "None",
#                         # ntu_10 = "None",
#                         # ntu_100 = "None") %>%
#
#     select(-c(ph_7_nice, ph_7_high, ph_7_low))
#
#     # Not all sondes have depth.
#     if(!is.na(str_match(depth, "lastcalibrated"))){#\\s*(.*?)\\s*calibrationdetails")[,2])){
#       cal_table <- cal_table %>%
#         mutate(
#           # Depth
#           depth_cal_date = as.character(mdy(str_match(depth, "lastcalibrated\\s*(.*?)\\s*calibrationdetails")[,2])),
#           depth_offset = str_match(depth, "zerooffset\\s*(.*?)\\s*psireferencedepth")[,2],
#           depth_ref_depth = str_match(depth, "psireferencedepth\\s*(.*?)\\s*ftreferenceoffset")[,2],
#           depth_ref_offset = str_match(depth, "ftreferenceoffset\\s*(.*?)\\s*psipremeasurement")[,2],
#           depth_pre_psi = str_match(depth, "psipremeasurement\\s*(.*?)\\s*psipostmeasurement")[,2],
#           depth_post_psi = str_match(depth, "psipostmeasurement\\s*(.*?)\\s*psi")[,2])
#     }
#
#     if(depth == "No Depth Sensor"){
#
#       cal_table <- cal_table %>%
#         mutate(# Depth
#       depth_cal_date = "No Depth Sensor",
#       depth_offset = "No Depth Sensor",
#       depth_ref_depth = "No Depth Sensor",
#       depth_ref_offset = "No Depth Sensor",
#       depth_pre_psi = "No Depth Sensor",
#       depth_post_psi = "No Depth Sensor")
#     }
#
#
#     if(!is.na(str_match(turbidity, "lastcalibrated"))){#calibrationpoint1premeasurement\\s*(.*?)\\s*ntupost")[,2])){
#       # Not all sondes have turbidity.
#       cal_table <- cal_table %>%
#         mutate(
#           # Turbidity
#           turb_cal_date = as.character(mdy(str_match(turbidity, "lastcalibrated\\s*(.*?)\\s*calibrationdetails")[,2])),
#           ntu_slope = str_match(turbidity, "slope\\s*(.*?)\\s*offset")[,2],
#           ntu_offset = str_match(turbidity, "offset\\s*(.*?)\\s*ntu")[,2],
#           ntu_10 = str_match(turbidity, "calibrationpoint1premeasurement\\s*(.*?)\\s*ntupost")[,2],
#           ntu_100 = str_match(turbidity, "calibrationpoint2premeasurement\\s*(.*?)\\s*ntupost")[,2])
#     }
#
#     if(turbidity == "No Turbidity Sensor"){
#       cal_table <- cal_table %>%
#         mutate(
#           # Turbidity
#           turb_cal_date = "No Turbidity Sensor",
#           ntu_slope = "No Turbidity Sensor",
#           ntu_offset = "No Turbidity Sensor",
#           ntu_10 = "No Turbidity Sensor",
#           ntu_100 = "No Turbidity Sensor")
#
#
#
#
#     }
#
#
#     cal_table <- cal_table %>%
#       mutate(
#         #Factory Defaults
#         factory_defaults = paste0(ifelse(grepl("factorydefault", turbidity), "Turbidity ", ""),
#                                   ifelse(grepl("factorydefault", rdo), "RDO ", ""),
#                                   ifelse(is.na(ph_slope_post), "pH ", ""),
#                                   ifelse(is.na(orp_offset), "ORP ", ""),
#                                   ifelse(grepl("factorydefault", conductivity), "Conductivity ", ""),
#                                   ifelse(grepl("factorydefaults", depth), "Depth ", ""))) %>%
#       # convert all columns to character values to preserve info
#       mutate(across(.cols = everything(), .fns = as.character)) %>%
#       # remove "," from big numbers
#       mutate(across(everything(), ~str_replace_all(., ",", "")))
#
#   }
#
#   bad_cal_interval_list <- map2(
#     .x = bad_cal_records_filtered$start_DT,
#     .y = bad_cal_records_filtered$end_DT,
#     .f = ~interval(.x, .y, tz = "MST"))
#
#   if(df_parameter == "DO"){
#
#     cal_table <- list.files("data/calibration_reports", pattern=".html") %>%
#       .[grepl(df_site, ., ignore.case = TRUE)] %>%
#       map_dfr(., cal_tabler) %>%
#       distinct(.keep_all = TRUE) %>%
#       mutate(DT = as.character(round_date(ymd_hms(DT, tz = "MST"), "15 minutes"))) %>%
#       # mutate(across(-matches("date|site|DT|factory"), as.numeric)) %>%
#       dplyr::select(DT, site, rdo_slope, rdo_offset)
#
#     df_mod <- df %>%
#       left_join(., cal_table, by = c("DT_join" = "DT", "site")) %>%
#       fill(names(cal_table)[!grepl("\\b(site|DT)\\b", names(cal_table))], .direction = "down") %>%
#       mutate(rdo_slope_pre = as.numeric(rdo_slope), rdo_offset_pre = as.numeric(rdo_offset)) %>%
#       select(names(df), contains(c("pre"))) %>%
#       left_join(., cal_table, by = c("DT_join" = "DT", "site")) %>%
#       fill(names(cal_table)[!grepl("\\b(site|DT)\\b", names(cal_table))], .direction = "up") %>%
#       mutate(rdo_slope_post = as.numeric(rdo_slope), rdo_offset_post = as.numeric(rdo_offset)) %>%
#       select(names(df), contains(c("pre", "post"))) %>%
#       #mutate(raw = (mean -rdo_offset_pre)/rdo_offset_pre)
#       mutate(raw = case_when(DT_round %within% bad_cal_interval_list & is.na(rdo_slope_pre) ~ mean,
#                              DT_round %within% bad_cal_interval_list & !is.na(rdo_slope_pre) ~ ((mean - rdo_offset_pre)/rdo_slope_pre),
#                              .default = mean),
#              cal_fix = case_when(DT_round %within% bad_cal_interval_list ~ (raw*rdo_slope_post) + rdo_offset_post,
#                                  .default = mean)) %>%
#       add_flag(mean != cal_fix, "calibration fix") %>%
#       mutate(raw = mean,
#              mean = cal_fixed)
#
#
#   }
#
#   if(df_parameter == "pH"){
#
#     cal_table <- list.files("data/calibration_reports", pattern=".html") %>%
#       .[grepl(df_site, ., ignore.case = TRUE)] %>%
#       map_dfr(., cal_tabler) %>%
#       distinct(.keep_all = TRUE) %>%
#       mutate(DT = as.character(round_date(ymd_hms(DT, tz = "MST"), "15 minutes"))) %>%
#       # mutate(across(-matches("date|site|DT|factory"), as.numeric)) %>%
#       dplyr::select(DT, site, ph_slope_pre, ph_offset_pre, ph_slope_post, ph_offset_post, factory_defaults)
#
#     df_mod <- df %>%
#       left_join(., cal_table, by = c("DT_join" = "DT", "site")) %>%
#       fill(names(cal_table)[!grepl("\\b(site|DT)\\b", names(cal_table))], .direction = "down") %>%
#       mutate(ph_slope_pre = as.numeric(ph_slope_pre), ph_offset_pre = as.numeric(ph_offset_pre),
#              ph_slope_post = as.numeric(ph_slope_post), ph_offset_post = as.numeric(ph_offset_post)) %>%
#       mutate(raw = case_when(DT_round %within% bad_cal_interval_list & is.na(ph_slope_pre) & grepl("pH", factory_defaults, ignore.case = TRUE) ~ mean,
#                              DT_round %within% bad_cal_interval_list & !is.na(ph_slope_pre) & !grepl("pH", factory_defaults, ignore.case = TRUE) ~ ((mean - ph_offset_pre)/ph_slope_pre),
#                              .default = mean),
#              cal_fix = case_when(DT_round %within% bad_cal_interval_list ~ (raw*ph_slope_post) + ph_offset_post,
#                                  .default = mean)) %>%
#       add_flag(mean != cal_fix, "calibration fix")
#
#   }
#
#   if(df_parameter == "")
#
# }
# }
