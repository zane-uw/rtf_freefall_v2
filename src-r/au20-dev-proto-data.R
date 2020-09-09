# Try building model based on spring 20 data gathered for RAD

library(tidyverse)
library(odbc)
library(dbplyr)

setwd(rstudioapi::getActiveProject())

data_path <- '../../Retention-Analytics-Dashboard/data-raw/spr20/'
(dirlist <- dir(data_path, pattern = '^week-', all.files = T, full.names = T))
# spring20 is empty for week03 b/c data collection started in week02
dirlist <- dirlist[-grep('03', dirlist)]
wks <- seq_along(dirlist)

# fetch calendar weeks ----------------------------------------------------
# DSN setup
# VPN
# kinit
fetch_cal_wks <- function(){
  con <- dbConnect(odbc(), 'sqlserver01')
  res <- tbl(con, in_schema('EDWPresentation.sec', 'dimDate')) %>%
    filter(AcademicYrQtrCode == 20202) %>%
    select(AcademicYrQtrCode,
           AcademicQtrWeekNum,
           CalendarDate,
           AcademicQtrDayNum) %>%
    distinct() %>%
    collect() %>%
    drop_na(AcademicYrQtrCode)
  return(res)
}

cal_wks <- fetch_cal_wks()

# build data from raw -----------------------------------------------------

# tasks:
# list directories:
#   for each directory:
       # combine the grades and assignments; adding the week #

# TODO: Will need expansion of assignment data to create 'full' dataset (only do this once, not weekly, or accomplish it through merging with partic)

mrg_assgn <- function(dir, wk){
  anames <- c('canvas_course_id', 'canvas_user_id', 'assgn_id', 'due_at', 'pts_possible', 'assgn_status', 'score')
  atypes <- c('nnnTncn')
  navals = c('NA', 'None', 'none', 'NULL')
  afiles <- list.files(dir, pattern = '^assgn-', full.names = T)

  A <- lapply(afiles, read_delim, delim = '|', col_types = atypes, col_names = anames, na = navals)
  A <- bind_rows(A)
  A <- A %>% distinct(canvas_course_id, canvas_user_id, assgn_id, .keep_all = T) %>%
    mutate(week = wk) %>%
    replace_na(list(pts_possible = 0, score = 0))

  return(A)
}

mrg_partic <- function(dir, wk) {
  pnames <- c('canvas_course_id', 'canvas_user_id', 'page_views', 'page_views_level',
              'partic', 'partic_level', 'tot_assgns', 'tot_assgns_on_time',
              'tot_assgn_late', 'tot_assgn_missing', 'tot_assgn_floating')
  ptypes <- c('nnnnnnnnnnn')
  navals = c('NA', 'None', 'none', 'NULL')
  pfiles <- list.files(dir, pattern = '^partic-', full.names = T)

  P <- lapply(pfiles, read_delim, delim = '|', col_types = ptypes, col_names = pnames, na = navals)
  P <- bind_rows(P)
  P <- P %>% distinct(canvas_course_id, canvas_user_id, .keep_all = T) %>%
    drop_na(canvas_user_id) %>%
    mutate(week = wk)

  return(P)
}

# Use the calendar to calculate week + course vars
# Pts possible this week; number of assignments with a due date in this week
mrg_crs_vars <- function(dir, wk, cal_wks = cal_wks){
  anames <- c('canvas_course_id', 'canvas_user_id', 'assgn_id', 'due_at', 'pts_possible', 'assgn_status', 'score')
  atypes <- c('nnnTncn')
  navals = c('NA', 'None', 'none', 'NULL')
  afiles <- list.files(dir, pattern = '^assgn-', full.names = T)

  A <- lapply(afiles, read_delim, delim = '|', col_types = atypes, col_names = anames, na = navals)
  A <- bind_rows(A)
  A <- A %>% distinct(canvas_course_id, canvas_user_id, assgn_id, .keep_all = T) %>%
    mutate(week = wk,
           due_date = lubridate::floor_date(due_at, unit = "day")) %>%
    replace_na(list(pts_possible = 0, score = 0)) %>%
    inner_join(cal_wks, by = c('due_date' = 'CalendarDate',
                               'week' = 'AcademicQtrWeekNum')) # %>%
    # filter(AcademicQtrWeekNum == week)

  res <- A %>%
    select(week, canvas_course_id, assgn_id, pts_possible) %>%
    distinct() %>%
    group_by(canvas_course_id, week) %>%
    summarize(n_assign = n_distinct(assgn_id),
              wk_pts_poss = sum(pts_possible)) %>%
    ungroup()

  return(res)
}


# TODO: Loops for all partic, assignments








# fetch quarterly results from SDB ----------------------------------------

# return a named list of transcripts and transcript-courses-taken
fetch_trans <- function(){
  con <- dbConnect(odbc(), 'sqlserver01')
  # empty list for results
  res <- list()

  mjr <- tbl(con, in_schema('sec', 'transcript_tran_col_major')) %>%
    filter(tran_yr >= 2020,
           index1 == 1) %>%
    select(-index1,
           -tran_evening)

  tran <- tbl(con, in_schema('sec', 'transcript')) %>%
    mutate(yrq = tran_yr * 10 + tran_qtr) %>%
    filter(yrq >= 20202) %>%
    inner_join(mjr) %>%
    select(system_key,
           yrq,
           resident,
           class,
           special_program,
           honors_program,
           tenth_day_credits,
           num_courses,
           add_to_cum,
           qtr_grade_points,
           qtr_graded_attmp,
           qtr_nongrd_earned,
           qtr_deductible,
           tran_branch,
           tran_college,
           tran_pathway,
           tran_deg_level,
           tran_deg_type,
           tran_major_abbr) %>%
    collect()

  tran_crs <- tbl(con, in_schema('sec', 'transcript_courses_taken')) %>%
    mutate(yrq = tran_yr * 10 + tran_qtr) %>%
    filter(yrq >= 20202) %>%
    select(system_key,
           yrq,
           index1,
           dept_abbrev,
           course_number,
           section_id,
           course_credits,
           course_branch,
           grade_system,    # 0 = standard; 5 = C/NC; 4 = S/NS; 9 = audit
           grade,
           honor_course,
           incomplete,
           repeat_course,
           writing,
           major_disallowed) %>%
    distinct() %>%
    collect()

  res$tran <- tran
  res$tran_crs <- tran_crs

  return(res)
}

tran_list <- fetch_trans()




# DEPR --------------------------------------------------------------------

#
# x <- mrg_assgn(dirlist[1], wks[1], cal_wks)
# y <- mrg_assgn(dirlist[2], wks[2], cal_wks)
# z <- mrg_assgn(dirlist[3], wks[3], cal_wks)
# q <- bind_rows(x, y, z) %>% arrange(canvas_course_id, canvas_user_id, week)
