# Use a Hadly centric lib only approach
library(dplyr)
library(scales)
library(ggplot2)
library(lubridate)
library(jsonlite)
library(tibble)
library(purrr)
library(tidyverse)
library(stringr)
library(readr)
library(forcats)

# Enum for Job Types
#' @description SMRT Link Job Type Enum
#' @export
ALL_JOB_TYPES <- list(
  EXPORT="export-datasets",
  TS_STATUS="tech-support-status",
  TS_FAILED="tech-support-job",
  CONVERT_RS_MOVIE="convert-rs-movie",
  IMPORT="import-dataset",
  MERGE="merge-datasets",
  ANALYSIS="pbsmrtpipe",
  FASTA_BARCODE="convert-fasta-barcodes",
  FASTA_REF="convert-fasta-reference",
  DELETE_JOB="delete-job",
  DELETE_DATASETS="delete-datasets",
  DB_BACKUP="db-backup"
  )


JOB_STATES <- list(
  SUCCESSFUL="SUCCESSFUL",
  FAILED="FAILED",
  RUNNING="RUNNING",
  CREATED="CREATED"
)

#' @export
SL_SYSTEM_NAMES <- list(BETA="smrtlink-beta",
                       ALPHA="smrtlink-alpha",
                       ALPHA_NIGHTLY="smrtlink-alpha-nightly",
                       SIV="smrtlink-siv",
                       SIV_RELEASE="smrtlink-release",
                       BIHOURLY="smrtlink-bihourly"
                       )

getExampleData <- function() {
  jobId <- c(1, 2, 3, 4, 5)
  name <- c("one", "two", "three", "four", "five")
  df <- data.frame(jobId, name)
  return(df)
}

getExampleJobs <- function() {
  jobId <- c(1, 2, 3, 4, 5)
  name <- c("one", "two", "three", "four", "five")
  createdAt <-
    c(
      ymd_hms("2011-06-04 12:01:00", tz = "Pacific/Auckland"),
      ymd_hms("2011-06-04 12:01:10", tz = "Pacific/Auckland"),
      ymd_hms("2011-06-04 12:02:00", tz = "Pacific/Auckland"),
      ymd_hms("2011-06-04 12:01:20", tz = "Pacific/Auckland"),
      ymd_hms("2011-06-04 12:02:10", tz = "Pacific/Auckland")
    )
  # Don't really understand how to accumulate correctly
  countId <- c(1, 1, 1, 1, 1)
  jdf <- tibble(jobId, name, createdAt, countId)
  return(jdf)

}

# Utils for extracting the polymorphic job settings into
# specific values (e.g., pipeline_id, number of merge datasets, dataset metatype)
get_pipeline_from_json <- function(sx) {
  jx <- jsonlite::fromJSON(sx)
  pipeline_id <- jx$pipelineId
  # this returns a list chr<1> for some reason
  #short_pid <- str_replace(pipeline_id, "pbsmrtpipe.pipelines.", "")
  return(pipeline_id)
}

get_num_datasets_from_json <- function(sx) {
  jx <- jsonlite::fromJSON(sx)
  dataset_paths <- jx$paths
  return(length(dataset_paths))
}

get_dataset_metatype_from_json <- function(sx) {
  jx <- jsonlite::fromJSON(sx)
  dataset_metatype <- jx$datasetType
  return(dataset_metatype)
}

get_dataset_import_path_from_json <- function(sx) {
  jx <- jsonlite::fromJSON(sx)
  return(jx$path)
}

get_pipeline_from_df <- function(sx) {
  return(sapply(sx$jsonSettings, get_pipeline_from_json))
}

# Loaders
# Simple func to load and do some sanitizing of the data and create a
# generic job record
#' @export
load_jobs_from_json <- function(path, hostname) {
  xdf <- as.tibble(jsonlite::fromJSON(path, simplifyDataFrame = TRUE))
  # set states to Factor
  xdf$state <- as.factor(xdf$state)
  xdf$jobTypeId <- as.factor(xdf$jobTypeId)
  xdf$smrtlinkVersion <- as.factor(xdf$smrtlinkVersion)

  xdf$host <- hostname
  # sanitize with proper types for datetimes
  idf <- transmute(
    xdf,
    job_id = id,
    uuid,
    jobTypeId,
    state,
    host,
    smrtlinkVersion,
    created_at = ymd_hms(createdAt),
    updated_at = ymd_hms(updatedAt),
    run_time_sec = updated_at - created_at,
    run_time_min = convertAllToMinutesNumeric(run_time_sec),
    createdBy,
    isActive
  )
  return(idf)
}

# Load from a list of files
#' @export
load_jobs_from_json_paths <- function(paths, hostname) {
  # This will create a DF will a union of all DS metatype specific data
  loader <- function(path) {return(load_jobs_from_json(path, hostname))}
  tdf <- purrr::map_df(paths, loader)
  tb <- as.tibble(tdf)
  return(tb)
}

# Custom loaders to leverage specific Job Type metadata
# Get Analysis/pbsmrtpipe job specific DataFrame
#' @export
load_job_analysis_from_json <- function(path, hostname) {
  xdf <- as.tibble(jsonlite::fromJSON(path, simplifyDataFrame = TRUE))
  # set states to Factor
  xdf$state <- as.factor(xdf$state)
  xdf$jobTypeId <- as.factor(xdf$jobTypeId)

  # is there a better way to do this within transmute?
  # this returns a list instead of char? Which breaks the factor
  xdf$pipeline_id <-
    map_chr(xdf$jsonSettings, get_pipeline_from_json)
  # I don't really understand why this sometimes has issues.
  # doing this here
  xdf$pipeline_id <- as.factor(xdf$pipeline_id)

  # I don't understand how to compute a value from a row, or
  # what the function signature is. Can't set host
  xdf$host <- hostname

  xdf$smrtlinkVersion <- as.factor(xdf$smrtlinkVersion)

  idf <- transmute(
    xdf,
    job_id = id,
    uuid,
    jobTypeId,
    state,
    pipeline_id,
    host,
    smrtlinkVersion,
    created_at = ymd_hms(createdAt),
    updated_at = ymd_hms(updatedAt),
    run_time_sec = updated_at - created_at,
    run_time_min = convertAllToMinutesNumeric(run_time_sec),
    createdBy,
    isActive
  )
  return(idf)
}

# Get Merge DataSet Specific DataFrame
#' @export
load_merge_dataset_from_json <- function(path, hostname) {
  xdf <- as.tibble(jsonlite::fromJSON(path, simplifyDataFrame = TRUE))
  # set states to Factor
  xdf$state <- as.factor(xdf$state)
  xdf$jobTypeId <- as.factor(xdf$jobTypeId)

  xdf$dataset_num <- map_int(xdf$jsonSettings, get_num_datasets_from_json)
  xdf$dataset_metatype <-
    map_chr(xdf$jsonSettings, get_dataset_metatype_from_json)
  xdf$dataset_metatype <- as.factor(xdf$dataset_metatype)
  xdf$host <- hostname

  xdf$smrtlinkVersion <- as.factor(xdf$smrtlinkVersion)

  idf <- transmute(
    xdf,
    job_id = id,
    uuid,
    jobTypeId,
    state,
    dataset_num,
    dataset_metatype,
    host,
    smrtlinkVersion,
    created_at = ymd_hms(createdAt),
    updated_at = ymd_hms(updatedAt),
    run_time_sec = updated_at - created_at,
    run_time_min = convertAllToMinutesNumeric(run_time_sec),
    createdBy,
    isActive
  )
  return(idf)
}

# Get Import DataSet Specific DataFrame
#' @export
load_import_dataset_from_json <- function(path, hostname) {
  xdf <- as.tibble(jsonlite::fromJSON(path, simplifyDataFrame = TRUE))
  # set states to Factor
  xdf$state <- as.factor(xdf$state)
  xdf$jobTypeId <- as.factor(xdf$jobTypeId)

  xdf$dataset_metatype <-
    map_chr(xdf$jsonSettings, get_dataset_metatype_from_json)
  xdf$dataset_metatype <- as.factor(xdf$dataset_metatype)
  xdf$host <- hostname

  xdf$path <- map_chr(xdf$jsonSettings, get_dataset_import_path_from_json)

  xdf$smrtlinkVersion <- as.factor(xdf$smrtlinkVersion)

  idf <- transmute(
    xdf,
    job_id = id,
    uuid,
    jobTypeId,
    state,
    dataset_metatype,
    host,
    smrtlinkVersion,
    created_at = ymd_hms(createdAt),
    updated_at = ymd_hms(updatedAt),
    run_time_sec = updated_at - created_at,
    run_time_min = convertAllToMinutesNumeric(run_time_sec),
    createdBy,
    isActive,
    path
  )
  return(idf)
}

# Plot the number of jobs submitted
plot_job_summary <-
  function(idf,
           min_created_at,
           max_created_at,
           binwidth,
           custom_title,
           output_png) {
    df17 <- filter(
      idf,
      created_at >= min_created_at,
      created_at < max_created_at,
      run_time_sec > dseconds(0)
    )

    # Frequency of Jobs
    p1 <-
      ggplot(df17, aes(created_at)) + geom_freqpoly(binwidth = binwidth) + ggtitle(custom_title)
    ggsave(output_png, plot = p1)
    return(p1)
  }

plot_job_runtimes <- function(df, output_png) {
  p1 <- ggplot(df, mapping = aes(run_time_sec)) + geom_histogram()
  ggsave(output_png, plot = p1)
  return(p1)
}

plot_job_created_at <- function(df, output_png) {
  # This 10 sec is hardcoded somewhere?
  p1 <-
    ggplot(df, mapping = aes(created_at)) + geom_histogram() + ggtitle("Number of Jobs by Created At binned at 10 sec")
  ggsave(output_png, plot = p1)
  return(p1)
}

#' @export
to_job_json_path <- function(root_dir, smrtlink_system, job_type) {
  return(
    paste0(
      root_dir,
      "/",
      smrtlink_system,
      "/",
      "job-",
      job_type,
      ".json"
    )
  )
}

#' @export
to_ds_json_path <- function(root_dir, smrtlink_system, ds_shortname) {
  return(
    paste0(
      root_dir,
      "/",
      smrtlink_system,
      "/",
      "dataset-",
      ds_shortname,
      ".json"
    )
  )
}

#' @export
to_status_json_path <- function(root_dir, smrtlink_system) {
  return(
    paste0(
      root_dir,
      "/",
      smrtlink_system,
      "/status.json"
    )
  )
}

# These need to be double checked to make sure the computed value is correct
convertToMinutesNumeric <- function(value) {return(as.numeric(value, "minutes"))}
convertAllToMinutesNumeric <- function(values) {return(purrr::map_dbl(values, convertToMinutesNumeric))}


#' @description Generates a Histogram of the Job Run times in seconds. All run times with 0 sec (due to an old bug) are removed.
#' @export
#' @param df A tibble with run_time_sec (time diff) defined
#' @param title Title of the Plot generated
#' @param bins Number of bins for the histogram
plot_job_run_times <- function(df, title="Job Run Times seconds", bins=100) {
 tdf <- df %>% filter(run_time_sec > dseconds(0)) %>% transmute(run_time_sec, run_time_min = convertAllToMinutesNumeric(run_time_sec))

 #tdf %>% summarise(mean_run_time_min = mean(run_time_min))

 tdf %>% ggplot(aes(run_time_min)) + geom_histogram(bins = bins) + ggtitle(title) + xlab("Run Time (seconds)") + ylab("Number of Jobs")
}

# Generates a Generic Summary for a given Host and job type
# Job specific Summaries need to be added
generate_recent_summary <- function(hostname, job_type) {
  jobJsonPath <- get_host_job_type_data_path(hostname, job_type)
  xdf <- load_jobs_from_json(jobJsonPath, hostname)

  # Every hour
  # every min
  bw <- 10

  plot_name <-
    paste0(results_dir, "/", hostname, "_number_of_", job_type, "created_at.png")

  plot_title <-
    paste0("Recent ", job_type, " Jobs on ", hostname, " bin = 1hr")

  plot_job_summary(xdf,
                   ymd("2017-5-16"),
                   ymd("2017-7-1"),
                   bw,
                   plot_title,
                   plot_name)

  # this is not really useful with seconds. It should be minutes?
  plot_job_runtimes(xdf, to_output_result_path(paste0(job_type, "_job_run_times.png")))
  plot_job_created_at(xdf,  to_output_result_path(paste0(job_type, "_job_created_at.png")))
}


# DataSet Utils
#' @description Generate a Generic Summary of DataSet. This will work across all dataset types
#' @export
# There's bugs and or default values being sent to -1.
to_summary_dataset <- function(tdf) {
  s <- filter(tdf, numRecords >= 0, totalLength >= 0) %>%
    summarize(
      total = n(),
      num_records_max = max(numRecords),
      num_records_min = min(numRecords),
      num_records_mean = mean(numRecords),
      total_length_max = max(totalLength),
      total_length_min = min(totalLength),
      total_length_mean = mean(totalLength)
    )
  return(s)
}

# Generate a general DataSet Summary
# This doesn't really work as expected. The R notebook can't display the plots, only the print() calls
#' @export
to_full_summary_dataset <- function(tdf, dataset_short_name) {
  # General Summary
  cat("###", "DataSet Metrics Summary", "\n")
  to_summary_dataset(tdf) %>% print()

  s_title <- paste0(dataset_short_name, " Jobs by Job State")
  tdf %>% ggplot(aes(x = factor(1), fill = factor(state) )) + geom_bar(width = 1) + coord_polar(theta = "y") + ggtitle(s_title)

  # DataSet Version
  cat("###",  dataset_short_name, " DataSet by DataSet Version Summary", "\n")
  tdf %>% group_by(version) %>% summarize(total=n()) %>% print()

  # DataSet inActive/Active Summary. This sucks. You can't really do this because it's not a field on the dataset. You would have to
  # store the showAll=true and compare to showAll=false
  #cat("###", dataset_short_name, " DataSet inActive/Active Summary")
  #tdf %>% group_by(inActive) %>% summarize(total=n()) %>% print()

  # Largest DataSets
  cat("###", "Largest", dataset_short_name, " DataSet Version Summary", "\n")
  select(tdf, numRecords, totalLength, version, created_at, name, id, uuid, path) %>% arrange(-totalLength, -numRecords) %>% slice(1:10) %>% print(n, width=Inf)

  # There's bugs, or default values (e.g., -1)
  adf <- tdf %>% filter(numRecords >= 0, totalLength >= 0)

  # Dist of Total Length
  total_length_title <- paste0(dataset_short_name, " Total Length Distribution")
  p1 <- adf %>% ggplot(aes(totalLength)) + geom_histogram(bins = 100) + ggtitle(total_length_title) + xlab("Total Length") + scale_x_continuous(labels = comma)
  print(p1)

  # Dist of Num Records
  num_records_title <- paste0(dataset_short_name, " Number of Records Distribution")
  p2 <- adf %>% ggplot(aes(numRecords)) + geom_histogram(bins=60) + ggtitle(num_records_title) + xlab("Number of Records") + scale_x_continuous(labels = comma)
  print(p2)
  #return(adf)

  # Scatter Plot of total length vs num Records
  xy_title = paste0(dataset_short_name, " Total Length vs Num Records")
  p3 <- adf %>% ggplot(aes(totalLength, numRecords)) + geom_point(alpha = 0.4) + xlab("Total Length") + ylab("Number of Records") + ggtitle(xy_title) + scale_x_continuous(labels = comma) + scale_y_continuous(labels = comma)
  print(p3)
}

#' @description Generic Job Summary. Intended to be used from RMD
#' @export
to_summary_job <- function(xdf, smrtlink_system, job_type) {
  #xdf %>% print(n = 25, width = Inf)

  xdf %>% filter(state == "SUCCESSFUL") %>% summarise(
    total = n(),
    mean_time_min=mean(run_time_min),
    max_run_time_max=max(run_time_min),
    min_run_time_min=min(run_time_min)
  ) %>% print()

  xdf %>% group_by(state) %>% summarize(n()) %>% print()

  t_title <- paste0(smrtlink_system, " ",  job_type, " Job By State")
  xdf %>% ggplot(aes(state)) + geom_bar() + ggtitle(t_title)

  x_title <- paste0(smrtlink_system, " ",  job_type, " Successful Job Walltime (seconds)")
  plot_job_run_times(filter(xdf, state=="SUCCESSFUL"), title = x_title)

}

#' @description Load Job Summary from JSON Path
#' @export
to_summary_job_from_path <- function(path, smrtlink_system, job_type) {
  return(to_summary_job(load_jobs_from_json(path, smrtlink_system), smrtlink_system, job_type))
}

#' @description Create a summary of SL Log from the CSV
#' @export
#' @param num_seconds providing 60 will yield a binwith of 1 minute
to_log_csv_plot_summary <- function(xdf, num_seconds, subtitle = NULL) {
  binwidth <- 60 * num_seconds
  g_title <- paste0("Recent Requests per time binwidth=", binwidth, " sec")
  p <- xdf %>% ggplot(aes(created_at)) + geom_freqpoly(binwidth = binwidth) + ggtitle(g_title, subtitle = subtitle) + facet_grid(. ~ http_method) + theme(axis.text.x = element_text(angle = 90, hjust = 1)) + xlab("Created At") + ylab("Number of Requests")
  return(p)
}
