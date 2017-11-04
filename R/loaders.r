# Loading/Creating tibble frames from JSON files
library(jsonlite)
library(tibble)
library(lubridate)

# Private function cleanup the Service DataSet model
process_df <- function(df) {
  fdf <- mutate(
    df,
    created_at = ymd_hms(createdAt),
    updated_at = ymd_hms(updatedAt),
    dataset_metatype = factor(datasetType),
    version = factor(version)
  )
  return(fdf)
}

# Load DataSet MetaData from JSON files
#' @description Load the common metadata from a ServiceJob DataSet
#' @export
#' @param path Path to the dataset JSON
load_dataset_metadata_from_json_path <- function(path) {
  #FIXME(mpkocher)(7-7-2017) This should use a proper logging lib
  print(paste0("Loading JSON file ", path))
  xdf <- as.tibble(jsonlite::fromJSON(path, simplifyDataFrame = TRUE))
  return(process_df(xdf))
}

#' @description Load DataSets into a single tibble. This can be mixed dataset types, which will contain a union of the dataset specific fields
#' @export
#' @param paths List of DataSet JSON Paths
load_dataset_metadata_from_json_paths <- function(ds_paths) {
  # This will create a DF will a union of all DS metatype specific data
  tdf <- purrr::map_df(ds_paths, jsonlite::fromJSON, simplifyDataFrame = TRUE)
  tb <- as.tibble(tdf)
  return(process_df(tb))

}

#' @export
load_status_from_json_path <- function(path) {
  return(as.tibble(jsonlite::fromJSON(path, simplifyDataFrame = TRUE)))
}

# This must have the form created_at,http_method,url,num_events
#' @description Load CSV Log file summary
#' @export
load_csv_log_file <- function(path) {
  xdf <- readr::read_csv(path, progress = FALSE)
  # this should also convert from UTC to PST by subtracting 7 hours
  df <- xdf %>% transmute(created_at = ymd_hms(created_at) - dhours(7), http_method = factor(http_method), url, num_events)
  return(df)
}
