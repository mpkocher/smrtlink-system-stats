#!/usr/bin/env Rscript
library(rmarkdown)
library(knitr)
library(purrr)

# Root HTML Output dir
html_root_output <- normalizePath("output")

# All output will be written a subdir with the smrtlink host name as the directory name
generate_output <- function(smrtlink_system) {

  rmd_ds <- "notebooks/SmrtLinkDataSetSummary.rmd"
  rmd_job <- "notebooks/SmrtLinkSystemSummary.rmd"

  custom_params <- list(smrtlink_system = smrtlink_system)

  print(paste0("Converting system ", smrtlink_system, " with ", rmd_ds))
  out1 <- rmarkdown::render(rmd_ds, params = custom_params)

  print(paste0("Converting system ", smrtlink_system, " with ", rmd_job))
  out2 <- rmarkdown::render(rmd_job, params = custom_params)

  html1 <- "SmrtLinkDataSetSummary.nb.html"
  html2 <- "SmrtLinkSystemSummary.nb.html"

  out_html1 <- file.path(html_root_output, smrtlink_system, "datasets.html")
  out_html2 <- file.path(html_root_output, smrtlink_system, "jobs.html")

  # TODO create dir if doesn't exist
  file.copy(html1, out_html1)
  file.copy(html2, out_html2)
  return(smrtlink_system)
}

SL_SYSTEM_NAMES <- list(
                       ALPHA="smrtlink-alpha",
                       ALPHA_NIGHTLY="smrtlink-alpha-nightly",
                       SIV="smrtlink-siv",
                       SIV_ALPHA="smrtlink-siv-alpha"
                       )

purrr::map(unlist(SL_SYSTEM_NAMES), generate_output)

