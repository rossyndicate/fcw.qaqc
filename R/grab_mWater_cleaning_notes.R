grab_mWater_cleaning_notes <- function(mWater_api_data = mWater_data){
  
  cleaning_data <- mWater_api_data %>%
    select(start_DT, DT_round, date, site, sensors_cleaned,
           contains("post_clea"), contains("pre_clea"))
  
  
  return(cleaning_data)
  
}
