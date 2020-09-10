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

mrg_assgn <- function(dir, wk, cal_wks = cal_wks){
  anames <- c('canvas_course_id', 'canvas_user_id', 'assgn_id', 'due_at', 'pts_possible', 'assgn_status', 'score')
  atypes <- c('nnnTncn')
  navals = c('NA', 'None', 'none', 'NULL')
  afiles <- list.files(dir, pattern = '^assgn-', full.names = T)

  A <- lapply(afiles, read_delim, delim = '|', col_types = atypes, col_names = anames, na = navals)
  A <- bind_rows(A)
  A <- A %>% distinct(canvas_course_id, canvas_user_id, assgn_id, .keep_all = T) %>%
    mutate(week = wk,
           due_date = lubridate::floor_date(due_at, unit = "day")) %>%
    replace_na(list(pts_possible = 0, score = 0))

  # >> diverge here to make two diff streams for student and course before re-combining them <<
  ## STUDENT
  stu <- A %>%
    group_by(canvas_user_id, canvas_course_id, week) %>%
    summarize(score = sum(score)) %>%
    ungroup()

  ## COURSE
  crs <- A %>%
    inner_join(cal_wks, by = c('due_date' = 'CalendarDate',
                               'week' = 'AcademicQtrWeekNum')) %>%
    select(week, canvas_course_id, assgn_id, pts_possible) %>%
    distinct() %>%
    group_by(canvas_course_id, week) %>%
    summarize(n_assign = n_distinct(assgn_id),
              wk_pts_poss = sum(pts_possible)) %>%
    ungroup()

  res <- full_join(stu, crs) %>%
    mutate(score = if_else(score < 0, 0, score)) %>%
    replace_na(list(n_assign = 0,
                    wk_pts_poss = 0))
  return(res)
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

# 'loop' assignments the r-ish way, in a surprising reversal of lapply syntax
# lapply 'applies' to the `seq_along` ie make the argument to lapply the index itself, as in a for-loop,
# and pass the value to the merge fun from above.
# Then we can merge, cleanup, aggregate, etc.
a_all <- lapply(seq_along(dirlist), function(i) mrg_assgn(dirlist[[i]], wks[[i]], cal_wks))
# we could probably avoid the dirlist entirely with map() or mapply()
p_all <- lapply(seq_along(dirlist), function(i) mrg_partic(dirlist[i], wks[i]))



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

