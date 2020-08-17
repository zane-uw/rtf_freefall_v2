rm(list = ls())
gc()

# create new quarter's data for making preds

setwd(rstudioapi::getActiveProject())

library(tidyverse)
library(aws.s3)

Sys.setenv(AWS_DEFAULT_REGION = 'us-west-2')

# Import new quarter's data -----------------------------------------------
# ! This will take a few minutes ! #
fetch_new_data <- function(bucket_path){
  assgn <- save_object(paste0(bucket_path, 'assignments.txt'), file = 'data-raw/new-preds/assignments.txt')
  pv <- save_object(paste0(bucket_path, 'page-views.txt'), file = 'data-raw/new-preds/page-views.txt')
  urls <- save_object(paste0(bucket_path, 'partic-url.txt'), file = 'data-raw/new-preds/partic-url.txt')
}

# fetch_new_data('s3://canvas-api-pulls/data-raw/2020-spring/')


# import SDB data ---------------------------------------------------------
source('src-r/001_fetch-extract-data.R')

