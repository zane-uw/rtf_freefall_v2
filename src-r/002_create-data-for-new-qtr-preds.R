rm(list = ls())
gc()

# create new quarter's data for making preds

setwd(rstudioapi::getActiveProject())

library(tidyverse)
library(aws.s3)

Sys.setenv(AWS_DEFAULT_REGION = 'us-west-2')

# Import new quarter data from S3 -----------------------------------------------
list_new_data_objs <- function(bucket_path = 's3://canvas-api-pulls/'){
  objs <- get_bucket(bucket_path)
  objs <- objs[grepl("data-new-preds.*csv", objs)]
  # print(objs)

  # return(objs)
  paths <- lapply(objs, function(x) paste0(bucket_path, x[[1]]))
  return(unlist(paths, use.names = F))

  # save_object(paste0(bucket_path, 'assignments.txt'), file = 'data-raw/new-preds/assignments.csv')
  # save_object(paste0(bucket_path, 'page-views.txt'), file = 'data-raw/new-preds/page-views.txt')
  # save_object(paste0(bucket_path, 'partic-url.txt'), file = 'data-raw/new-preds/partic-url.txt')
}

# y <- list_new_data_objs()

find_name_from_path <- function(p){
  data_names <- c('assignments', 'page-views', 'partic-urls')
  vname <- data_names[str_detect(p, data_names)]
  return(sub('-', '_', vname))
}

import_new_data_objs <- function(){
  s3paths <- list_new_data_objs()

  lapply(s3paths, function(x){
    tmp <- tempfile()
    on.exit(unlink(tmp))
    save_object(x, file = tmp)
    obj_name <- find_name_from_path(x)
    assign(obj_name, read_csv(tmp), envir = .GlobalEnv)
  })
}

# √
# import_new_data_objs()

# expand weeks w/in data frame of canvas activity
# ie fill missing week + values for each user and course
expand_weeks <- function(df, week_seq = 1:12){
  d <- df %>% distinct(yrq, course_id, user_id)
  week <- sort(unique(week_seq))
  return( expand(uniq_keys, nesting(yrq, course_id, user_id), week) )
}

# Create a crossover table for canvas data
can_keys <- list(user_id = union(union(assignments$user_id, page_views$user_id), partic_urls$user_id),
                 course_id = union(union(assignments$course_id, page_views$course_id), partic_urls$course_id),
                 yrq = unique(page_views$yrq),
                 user_course_df = bind_rows(select(assignments, yrq, user_id, course_id),
                                            select(page_views, yrq, user_id, course_id),
                                            select(partic_urls, yrq, user_id, course_id)) %>%
                   distinct() %>% arrange(yrq, user_id, course_id))


# import SDB data ---------------------------------------------------------
# source('src-r/001_fetch-extract-data.R')


