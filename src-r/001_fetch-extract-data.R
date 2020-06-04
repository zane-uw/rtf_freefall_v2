rm(list = ls())
gc()

setwd(rstudioapi::getActiveProject())

library(tidyverse)
library(odbc)
library(dbplyr)

# connection info ---------------------------------------------------------

# DSN setup
# VPN
# kinit
con <- dbConnect(odbc(), 'sqlserver01')

# read local data ---------------------------------------------------------

pv <- read_csv('data-raw/weekly_page_views_wide_2020-05-07.csv')
assgn <- read_csv('data-raw/weekly_assignments_wide_2020-05-13.csv')
urls <- read_csv('data-raw/weekly_url_count_wide.csv')


# globals for filtering ---------------------------------------------------

yrq.min <- min(pv$yrq)
yrq.max <- max(pv$yrq)


# skeys <- unique(c(pv$user_id, urls$user_id, assgn$user_id))


# map between db and lms
# prov_users <- read_csv('../../Retention-Analytics-Dashboard/data-raw/provisioning_csv_30_Mar_2020_15884/users.csv')
prov_courses <- read_csv('../../Retention-Analytics-Dashboard/data-raw/provisioning_csv_30_Mar_2020_15884/courses.csv')

link_students <- function(){
  # danger: a person may have more than 1 canvas ID under the same uw_netid and system_key
  # lose about 5k with the last distinct

  skeys <- data.frame('canvas_user_id' = union(union(pv$user_id, assgn$user_id), urls$user_id))
  prov_users <- read_csv('../../Retention-Analytics-Dashboard/data-raw/provisioning_csv_30_Mar_2020_15884/users.csv')

  skeys <- skeys %>% inner_join(prov_users, by = c('canvas_user_id' = 'canvas_user_id')) %>%
    filter(status == 'active', created_by_sis == T) %>%
    mutate(sortable_name = str_remove_all(sortable_name, " "))

  db <- tbl(con, in_schema('sec', 'student_1')) %>%
    filter(last_yr_enrolled >= 2006) %>%
    # caveat emptor: name_lowc is actually proper noun case, no lower
    select(system_key, uw_netid, student_name_lowc) %>%
    collect() %>%
    # correction(s) necessary for compatability
    mutate_if(is.character, str_remove_all, pattern = " ")

  # try multiple ways of merging since regid is not accessible w/o dev token to SWS
  by_netid <- db %>% inner_join(skeys, by = c('uw_netid' = 'login_id')) %>%
    select(system_key, canvas_user_id, uw_netid)

  # name_lowc to sortable_name looks most promising
  by_name <- db %>%
    mutate(student_name_lowc = str_remove_all(student_name_lowc, " ")) %>%
    inner_join(skeys, by = c('student_name_lowc' = 'sortable_name')) %>%
    select(system_key, canvas_user_id, uw_netid)

  res <- bind_rows(by_netid, by_name) %>% distinct(system_key, uw_netid, .keep_all = F)

  return(res)
}

link_courses <- function(){

  crskeys <- data.frame('canvas_course_id' = union(union(pv$course_id, assgn$course_id), urls$course_id))
  # prov_courses <- read_csv('../../../../../canvas-data/courses_all.csv') %>%
  #   filter(created_by_sis == T)

  cal <- tbl(con, in_schema('EDWPresentation.sec', 'dimDate')) %>%
    filter(AcademicYrQtrCode >= yrq.min,
           AcademicYrQtrCode <= yrq.max) %>%
    select(AcademicYrQtrCode,
           AcademicQtrName) %>%
    distinct() %>%
    collect() %>%
    drop_na() %>%
    # create canvas-style term_id
    mutate(term_id = paste(str_sub(AcademicYrQtrCode, end = 4),
                           tolower(AcademicQtrName), sep = '-'),
           yrq = as.numeric(AcademicYrQtrCode)) %>%
    select(yrq, term_id)

  courses <- read_csv('../../../../../canvas-data/courses_all.csv') %>%
    filter(created_by_sis == T) %>%
    left_join(cal, by = c('term_id' = 'term_id')) %>%
    semi_join(crskeys)

  xmat <- data.frame(str_split(courses$course_id, "-", simplify = T))
  courses$dept_abbrev <- xmat[,3]
  courses$course_no <- xmat[,4]
  # courses$section_short <- xmat[,5]     # this isn't a good match for the time schedule b/c of this truncated section #
  rm(xmat)

  res <- courses %>%
    select(yrq, canvas_course_id, dept_abbrev, course_no)

  # tsdb <- tbl(con, in_schema('sec', 'time_schedule')) %>%
  #   mutate(yrq = ts_year*10 + ts_quarter) %>%
  #   filter(yrq >= yrq.min,
  #          yrq <= yrq.max) %>%
  #   select(yrq,
  #          course_branch,
  #          dept_abbrev,
  #          course_no,
  #          section_id,
  #          ) %>%
  #   collect()



  res <- inner_join(db, skeys, by = c('uw_netid' = 'login_id')) %>%
    select(system_key, canvas_user_id, uw_netid)

  return(res)
}


# fetch raw SDB data ------------------------------------------------------
get_cal <- function(con = con){
  res <- tbl(con, in_schema('EDWPresentation.sec', 'dimDate')) %>%
    filter(AcademicYrQtrCode >= yrq.min,
           AcademicYrQtrCode <= yrq.max) %>%
    select(AcademicYrQtrCode,
           AcademicYrQtrDesc,
           AcademicFiscalYr,
           AcademicQtr,
           AcademicQtrWeekNum) %>%
    distinct() %>%
    collect()
  return(res)
}
# x <- get_cal(con)

get_stu1 <- function(con = con){
  sr_mm <- tbl(con, in_schema('sec', 'sr_mini_master')) %>%
    filter()

}
