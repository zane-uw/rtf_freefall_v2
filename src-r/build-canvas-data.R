# Create canvas data for training/testing

rm(list = ls())

library(tidyverse)
library(odbc)
library(dbplyr)

setwd(rstudioapi::getActiveProject())

# Set/update term:
TERM <- 'sp20'

# Canvas helper functions -------------------------------------------------
# req'd to connect id keys
get_edw_keys <- function(){
  con <- dbConnect(odbc(), 'sqlserver01')
  skeys <- tbl(con, in_schema('sec', 'student_1')) %>%
    select(system_key, uw_netid) %>%
    collect() %>%
    mutate_if(is.character, trimws) %>%
    filter(uw_netid != "")

  return(skeys)
}

# fetch calendar weeks
fetch_cal_wks <- function(){
  con <- dbConnect(odbc(), 'sqlserver01')
  res <- tbl(con, in_schema('EDWPresentation.sec', 'dimDate')) %>%
    filter(AcademicYrQtrCode >= 20202) %>%
    select(AcademicYrQtrCode,
           AcademicQtrWeekNum,
           CalendarDate,
           AcademicQtrDayNum) %>%
    distinct() %>%
    collect() %>%
    drop_na(AcademicYrQtrCode)
  return(res)
}

# merge funs for assignments and participation
mrg_assgn <- function(dir, wk){

  print(paste0('dir: ', dir))
  afiles <- list.files(dir, pattern = '^assgn-', full.names = T)
  if(length(afiles) == 0) {print('no files')} else {

    cal_wks <- fetch_cal_wks()

    anames <- c('canvas_course_id', 'canvas_user_id', 'assgn_id', 'due_at', 'pts_possible', 'assgn_status', 'score')
    atypes <- c('nnnTncn')
    navals <- c('NA', 'None', 'none', 'NULL')

    A <- lapply(afiles, read_delim, delim = '|', col_types = atypes, col_names = anames, na = navals)
    A <- bind_rows(A)
    A <- A %>% distinct(canvas_course_id, canvas_user_id, assgn_id, .keep_all = T) %>%
      mutate(week = wk,
             due_date = lubridate::floor_date(due_at, unit = "day")) %>%
      replace_na(list(pts_possible = 0, score = 0))
    print('merging folder ok...')

    # >> diverge here to make two diff streams for student and course before re-combining them <<
    ## STUDENT
    stu <- A %>%
      group_by(canvas_user_id, canvas_course_id, week) %>%
      summarize(score = sum(score)) %>%
      ungroup()
    print('stu summarize ok...')

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
    print('course summarize ok...')

    rm(A)
    print('removed giant A file...')

    res <- full_join(stu, crs) %>%
      mutate(score = if_else(score < 0, 0, score)) %>%
      replace_na(list(n_assign = 0,
                      wk_pts_poss = 0))
    return(res)
  }
}

mrg_partic <- function(dir, wk) {
  print(paste0('dir: ', dir))
  pfiles <- list.files(dir, pattern = '^partic-', full.names = T)
  if(length(pfiles) == 0) {print('no files')} else {

    pnames <- c('canvas_course_id', 'canvas_user_id', 'page_views', 'page_views_level',
                'partic', 'partic_level', 'tot_assgns', 'tot_assgns_on_time',
                'tot_assgn_late', 'tot_assgn_missing', 'tot_assgn_floating')
    ptypes <- c('nnnnnnnnnnn')
    navals <- c('NA', 'None', 'none', 'NULL')

    P <- lapply(pfiles, read_delim, delim = '|', col_types = ptypes, col_names = pnames, na = navals)
    P <- bind_rows(P)
    P <- P %>% distinct(canvas_course_id, canvas_user_id, .keep_all = T) %>%
      drop_na(canvas_user_id) %>%
      mutate(week = wk)

    return(P)
  }
}

# Canvas create function
create_data_file_from_canvas <- function(term = TERM){
  data_path <- paste0('../../Retention-Analytics-Dashboard/data-raw/', term)
  (dirlist <- dir(data_path, pattern = '^week-', all.files = T, full.names = T))
  dirlist <- dirlist[-grepl('-00', dirlist) == F]
  # check n_files
  sapply(dirlist, function(x) length(list.files(x, pattern = "assgn|partic")))
  (dirlist <- dirlist[sapply(dirlist, function(x) length(list.files(x, pattern = "assgn|partic"))) >= 2])

  WKS <- seq_along(dirlist)

  PROV_USR_PATH <- '~/Google Drive File Stream/My Drive/canvas-data/users.csv'
  PROV_CRS_PATH <- paste0('~/Google Drive File Stream/My Drive/canvas-data/courses_', TERM, '.csv')

  # read provisioning; these currently need periodic updates
  prov_usrs <- read_csv(PROV_USR_PATH, col_types = 'nc--c------cc') %>% filter(status == 'active', created_by_sis == 'true') %>% select(-status, -created_by_sis)
  prov_crss <- read_csv(PROV_CRS_PATH, col_types = 'dc-cc--ncc----l') %>% filter(created_by_sis == T, status == 'active') %>% select(-status, -created_by_sis)

  # 'loop' assignments the r-ish way, in a surprising reversal of lapply syntax
  # lapply 'applies' to the `seq_along` ie make the argument to lapply the index itself, as in a for-loop,
  # and pass the value to the merge fun from above.
  # Then we can merge, cleanup, aggregate, etc.
  a_all <- lapply(seq_along(dirlist), function(i) mrg_assgn(dirlist[[i]], WKS[[i]]))   # I have no idea why this behaves differently and tends to crash my desktop but runs in seconds on laptop...
  # we could probably avoid the dirlist entirely with map() or mapply()
  p_all <- lapply(seq_along(dirlist), function(i) mrg_partic(dirlist[i], WKS[i]))
  a_all <- bind_rows(a_all)
  p_all <- bind_rows(p_all)

  # filtering down the data from API to current courses, users
  dat <- p_all %>% inner_join(a_all)
  dat <- dat %>% inner_join(prov_crss) %>% inner_join(prov_usrs)

  # link student records:
  # 1) edw <-> merged data on netid = login
  dat <- dat %>% inner_join(get_edw_keys(), by = c('login_id' = 'uw_netid'))

  # link courses:
  # 1) turn dat/prov into dept + ### with yrq
  xmat <- data.frame(str_split(dat$course_id, '-', simplify = T))
  xmat$qtr <- match(xmat$X2, table = c('winter', 'spring', 'summer', 'autumn'))
  # ymat <- data.frame(str_split(x$course_id, '-', simplify = T))
  dat$dept_abbrev <- xmat$X3
  dat$course <- paste(xmat$X3, xmat$X4, sep = "_")
  dat$yrq <- as.numeric(paste0(xmat$X1, xmat$qtr))
  dat$section <- xmat$X5

  return(dat)

}
candat <- create_data_file_from_canvas(TERM)
candat %>% write_csv(., paste0('data-intermediate/canvas-', TERM, '.csv'), append = F)
