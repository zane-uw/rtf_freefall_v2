rm(list = ls())
gc()

# create new quarter's data for making preds

setwd(rstudioapi::getActiveProject())

library(tidyverse)
library(aws.s3)

Sys.setenv(AWS_DEFAULT_REGION = 'us-west-2')

# Import new quarter's data -----------------------------------------------
# ! This will take a few minutes ! #
# load up s3 object, convert to a CSV with headers
list_new_data_objs <- function(bucket_path = 's3://canvas-api-pulls/'){
  objs <- get_bucket(bucket_path)
  objs <- objs[grepl("data-new-preds.*csv", objs)]

  # return(objs)
  paths <- lapply(y, function(x) paste0(bucket_path, x[[1]]))
  return(unlist(paths, use.names = F))

  # save_object(paste0(bucket_path, 'assignments.txt'), file = 'data-raw/new-preds/page-views.txt')
  # save_object(paste0(bucket_path, 'page-views.txt'), file = 'data-raw/new-preds/page-views.txt')
  # save_object(paste0(bucket_path, 'partic-url.txt'), file = 'data-raw/new-preds/partic-url.txt')
}

y <- list_new_data_objs()





# import SDB data ---------------------------------------------------------
source('src-r/001_fetch-extract-data.R')


