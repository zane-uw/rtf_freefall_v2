# Try building model based on spring 20 data gathered for RAD

library(tidyverse)
library(odbc)
library(dbplyr)


# fetch calendar weeks ----------------------------------------------------
# DSN setup
# VPN
# kinit
fetch_cal_wks <- function(){
  con <- dbConnect(odbc(), 'sqlserver01')
  res <- tbl(con, in_schema('EDWPresentation.sec', 'dimDate')) %>%
    filter(AcademicFiscalYr > 2010) %>%
    select(AcademicYrQtrCode,
           AcademicQtrWeekNum,
           CalendarDate,
           AcademicQtrDayNum) %>%
    distinct() %>%
    collect() # %>%
    # mutate(CalendarDate = strftime(CalendarDate, format = "%Y-%m-%d", tz = "UTC", usetz = F))
  return(res)
}

cal_wks <- fetch_cal_wks ()

# build data from raw -----------------------------------------------------
data_path <- '../../Retention-Analytics-Dashboard/data-raw/spr20/'
ahead <- c('')
phead <-

(dirlist <- dir(data_path, pattern = '^week-', all.files = T, full.names = T))
# spring20 is empty for week03 b/c data collection started in week02
dirlist <- dirlist[-grep('03', dirlist)]
wks <- seq_along(dirlist)
# lapply(seq_along(x), function(y, n, i) { paste(n[[i]], y[[i]]) }, y=x, n=names(x))
# (partlist <- list.files(dirlist, pattern = 'partic-'))
# (assglist <- lapply(dirlist, list.files, pattern = 'assgn-', full.names = T))
# lapply(assglist, function(x) lapply(x, function(y) read_delim(y, delim = '|')))

# tasks:
# list directories:
#   for each directory:
       # combine the grades and assignments; adding the week #

mrg_fun <- function(dir, wk, cal_wks = cal_wks){
  anames <- c('canvas_course_id', 'canvas_user_id', 'assgn_id', 'due_at', 'pts_possible', 'assgn_status', 'score')
  atypes <- c('nnnTncn')
  pnames <- c('canvas_course_id', 'canvas_user_id', 'page_views', 'page_views_level',
              'partic', 'partic_level', 'tot_assgns', 'tot_assgns_on_time',
              'tot_assgn_late', 'tot_assgn_missing', 'tot_assgn_floating')
  ptypes <- c('nnnnnnnnnnn')
  navals = c('NA', 'None', 'none', 'NULL')

  alist <- list.files(dir, pattern = '^assgn-', full.names = T)
  plist <- list.files(dir, pattern = '^partic-', full.names = T)

  A <- lapply(alist, read_delim, delim = '|', col_types = atypes, col_names = anames, na = navals)
  A <- bind_rows(A)
  A <- A %>% distinct(canvas_course_id, canvas_user_id, assgn_id, .keep_all = T) %>%
    mutate(week = wk,
           due_date = lubridate::round_date(due_at, unit = "day")) %>%
    replace_na(list(pts_possible = 0, score = 0))
  # A$week <- wk

  # 3 calcs here for course points/scores:
  #  course's total points possible
  #  student's total points in the course so far;
  #  course actual points in a week

          # crs_pts <- A %>%
          #   replace_na(list(pts_possible = 0)) %>%
          #   # inner_join(cal_wks, by = c('due_at' = 'CalendarDate', 'week' = 'AcademicQtrWeekNum')) %>%
          #   distinct(canvas_course_id, assgn_id, pts_possible, week) %>%
          #   group_by(canvas_course_id, week) %>%
          #   summarize(pts_possible = sum(pts_possible, na.rm = T))
          #
          # stu_pts <- A %>%
          #   replace_na(list(score = 0)) %>%
          #   group_by(canvas_user_id, canvas_course_id, week) %>%
          #   summarize(score = sum(score))
# √ #
  # pts <- A %>%
  #   # replace_na(list(pts_possible = 0, score = 0)) %>%
  #   group_by(canvas_user_id, canvas_course_id, week) %>%
  #   summarize(tot_pts_poss = sum(pts_possible),
  #             score = sum(score)) %>%
  #   ungroup()
####
  wkpts <- A %>%
    select(canvas_course_id, assgn_id, due_date, pts_possible) %>%
    distinct() %>%
    left_join(cal_wks, by = c('due_date' = 'CalendarDate'))



return(wkpts)

# √ #
    # P <- lapply(plist, read_delim, delim = '|', col_types = ptypes, col_names = pnames, na = navals)
    # P <- bind_rows(P)
    # P <- P %>% distinct(canvas_course_id, canvas_user_id, .keep_all = T)
    # P$week <- wk
####

}

x <- mrg_fun(dirlist[1], wks[1], cal_wks)

merge.partic <- function(){
  (files <- list.files(in.folder, pattern = 'partic-batch', full.names = T))
  cnames <- c('canvas_course_id', 'canvas_user_id', 'page_views', 'page_views_level',
              'partic', 'partic_level', 'tot_assgns', 'tot_assgns_on_time',
              'tot_assgn_late', 'tot_assgn_missing', 'tot_assgn_floating')
  ctypes <- c('nnnnnnnnnnn')

  X <- lapply(files, read_delim, delim = '|', col_types = ctypes, col_names = cnames, na = c('NA', 'None', 'none', 'NULL'))
  X <- bind_rows(X)
  X <- X %>% distinct(canvas_course_id, canvas_user_id, .keep_all = T)
  return(X)
}
