# Source this for functions to gen raw data

# rm(list = ls())
# gc()

#setwd(rstudioapi::getActiveProject())

library(tidyverse)
library(odbc)
library(dbplyr)

# connection info ---------------------------------------------------------

# DSN setup
# VPN
# kinit
con <- dbConnect(odbc(), 'sqlserver01')

# globals for filtering ---------------------------------------------------

# read from local files, create a list of the sub-elements we need

read_local_canvas <- function(){
  res <- list()

  col_def <- cols_only('course_id' = col_double(),
                       'user_id' = col_double(),
                       'yrq' = col_double())

  pv <- read_csv('data-raw/weekly_page_views_wide_2020-05-07.csv',
                 col_types = col_def)
  assgn <- read_csv('data-raw/weekly_assignments_wide_2020-06-12.csv',
                    col_types = col_def)
  urls <- read_csv('data-raw/weekly_url_count_wide.csv',
                   col_types = col_def)

  res$uid <- data.frame('canvas_user_id' = union(union(pv$user_id, assgn$user_id), urls$user_id))
  res$cid <- data.frame('canvas_course_id' = union(union(pv$course_id, assgn$course_id), urls$course_id))
  res$yrq.min <- min(pv$yrq, na.rm = T)
  res$yrq.max <- max(pv$yrq, na.rm = T)

  return(res)
}

canvas_globs <- read_local_canvas()


# mappings between db and lms ---------------------------------------------

link_students <- function(){
  # danger: a person may have more than 1 canvas ID under the same uw_netid and system_key
  # lose about 5k with the last distinct

  skeys <- canvas_globs$uid

  prov_users <- read_csv('../../Retention-Analytics-Dashboard/data-raw/provisioning_csv_30_Mar_2020_15884/users.csv')
  prov_users <- prov_users[prov_users$status == 'active' & prov_users$created_by_sis == T,]

  skeys <- skeys %>% inner_join(prov_users, by = c('canvas_user_id' = 'canvas_user_id')) %>%
    mutate(sortable_name = str_remove_all(sortable_name, " "))

  db <- tbl(con, in_schema('sec', 'student_1')) %>%
    filter(last_yr_enrolled >= 2006) %>%
    # caveat emptor: name_lowc is actually proper noun case, no lower
    select(system_key, uw_netid, student_name_lowc) %>%
    collect() %>%
    # correction(s) necessary for compatability
    mutate_if(is.character, str_replace_all, pattern = " ", replacement = "")

  # try multiple ways of merging since regid is not accessible w/o dev token to SWS
  by_netid <- db %>% inner_join(skeys, by = c('uw_netid' = 'login_id')) %>%
    select(system_key, canvas_user_id, uw_netid)

  # name_lowc to sortable_name looks most promising
  by_name <- db %>%
    mutate(student_name_lowc = str_remove_all(student_name_lowc, " ")) %>%
    inner_join(skeys, by = c('student_name_lowc' = 'sortable_name')) %>%
    select(system_key, canvas_user_id, uw_netid)

  res <- bind_rows(by_netid, by_name) %>% distinct(system_key, uw_netid, .keep_all = T)

  return(res)
}

# mapping sections and students, these are kind of a mess b/c the db rules for generating keys are inconsistent over time
# this includes course codes
link_enrollments <- function(){
  # crskeys <- data.frame('canvas_course_id' = union(union(pv$course_id, assgn$course_id), urls$course_id))

  # crskeys <- read_canvas_course_ids()
  crskeys <- canvas_globs$cid

  prov_sect <- read_csv('../../../../../canvas-data/enrollments_all.csv',
                        col_types = cols_only(canvas_course_id = col_guess(),
                                              course_id = col_guess(),
                                              canvas_user_id = col_guess(),
                                              canvas_section_id = col_guess(),
                                              section_id = col_guess())) %>%
    # select(canvas_course_id, course_id, canvas_user_id, canvas_section_id, section_id) %>%
    drop_na() %>%
    semi_join(crskeys)
  # prov_sect <- prov_sect[prov_sect$role == 'student',]

  # also filter out courses that start with 'course_' - these aren't useful
  prov_sect <- prov_sect[grepl("^course", prov_sect$section_id) == F,]

  # split out elements from section, replace older versions that use different chars
  strep <- function(x) (str_replace_all(x, "(\\,|\\.|\\/)", "-"))
  x <- prov_sect %>% mutate_at(c('section_id', 'course_id'), strep)

  xmat <- data.frame(str_split(x$section_id, '-', simplify = T))
  xmat$qtr <- match(xmat$X2, table = c('winter', 'spring', 'summer', 'autumn'))
  ymat <- data.frame(str_split(x$course_id, '-', simplify = T))

  res <- data.frame('canvas_course_id' = x$canvas_course_id,
                    'canvas_user_id' = x$canvas_user_id,
                    'yrq' = as.numeric(xmat$X1)*10 + xmat$qtr,
                    'term_id' = paste(xmat$X1, xmat$X2, sep = '-'),   # may not be needed
                    'course' = paste(xmat$X3, xmat$X4, sep = '_'),
                    'dept_abbrev' = xmat$X3,
                    'course_no' = xmat$X4,
                    'section_1' = ymat$X5,
                    'section2' = xmat$X5) %>%
    filter(yrq >= canvas_globs$yrq.min,
           yrq <= canvas_globs$yrq.max)

  return(res)
}

# funs fetch raw SDB data ------------------------------------------------------
# in principle, don't return collected data by default, think of these are pre-defined CTEs
# may or may not be needed for different analyses; track branching analyses w/ MLFlow?

# push temp filtering tables and save a local reference so we don't need the name of the temp table
canvas_filter <- link_students() %>% copy_to(con, ., overwrite = T)

# fetch calendar weeks
get_cal <- function(){
  res <- tbl(con, in_schema('EDWPresentation.sec', 'dimDate')) %>%
    filter(AcademicYrQtrCode >= canvas_globs$yrq.min,
           AcademicYrQtrCode <= canvas_globs$yrq.max) %>%
    select(AcademicYrQtrCode,
           AcademicYrQtrDesc,
           AcademicFiscalYr,
           AcademicQtr,
           AcademicQtrWeekNum) %>%
    distinct() %>%
    collect()
  return(res)
}

# fetch frozen rep of students from census day (default) or first day (=1). 3 is also a valid option but unlikely
# to be used for current case
get_mm_census <- function(indicator = 2){
  ymin <- canvas_globs$yrq.min %/% 10
  sr_mm <- tbl(con, in_schema('sec', 'sr_mini_master')) %>%
    semi_join(canvas_filter, by = c('mm_system_key' = 'system_key')) %>%
    filter(mm_year >= ymin,
           mm_proc_ind == indicator) %>%
    select(mm_year,
           mm_qtr,
           mm_student_no,
           mm_system_key,
           mm_honors_program,
           mm_resident,
           mm_class,
           mm_curr_credits,
           mm_spcl_program)

  sr_mm_deg <- tbl(con, in_schema('sec', 'sr_mini_master_deg_program')) %>%
    filter(index1 <= 1,
           mm_proc_ind == indicator) %>%
    select(mm_year,
           mm_qtr,
           mm_student_no,
           mm_branch,
           mm_college,
           mm_major_abbr,
           mm_pathway,
           mm_deg_level,
           mm_deg_type,
           mm_maj_cip_code)

  cip_stem <- tbl(con, in_schema('EDWPresentation.sec', 'CIPAcademicYear')) %>%
    group_by(CIPCode) %>%
    mutate(r = row_number(CIPCode)) %>%
    filter(r == max(r)) %>%
    ungroup() %>%
    select(CIPCode,
           WAStateOFMSTEMInd)

  unmet_reqs <- tbl(con, in_schema('sec', 'sr_unmet_request')) %>%
    group_by(unmet_system_key,
             unmet_yr,
             unmet_qtr) %>%
    summarize(n_unmet = n()) %>%
    ungroup()

  res <- sr_mm %>%
    left_join(sr_mm_deg) %>%
    left_join(cip_stem, by = c('mm_maj_cip_code' = 'CIPCode')) %>%
    left_join(unmet_reqs, by = c('mm_system_key' = 'unmet_system_key',
                                 'mm_year' = 'unmet_yr',
                                 'mm_qtr' = 'unmet_qtr'))
  return(res)
}


get_stu_1 <- function(){
  ymin <- canvas_globs$yrq.min %/% 10
  st1 <- tbl(con, in_schema('sec', 'student_1')) %>%
    semi_join(canvas_filter, by = c('system_key' = 'system_key')) %>%
    filter(last_yr_enrolled >= ymin) %>%
    select(system_key,
           student_no,
           uw_netid,
           s1_high_satv,
           s1_high_satm,
           s1_high_satrw,
           s1_high_satmth,
           s1_high_act,
           s1_high_acte,
           s1_high_actm,
           s1_high_actr,
           s1_high_actsr)

  st2 <- tbl(con, in_schema('sec', 'student_2')) %>%
    select(system_key,
           first_yr_regis,
           first_qtr_regis,
           conditional,
           provisional,
           hs_for_lang_yrs,
           hs_math_level)

  res <- st1 %>%
    inner_join(st2)

  return(res)
}

# may not be required since mm_ will get current if past census day
# there are other circumstances where this could be a useful query
get_current_registration <- function(){
  reg <- tbl(con, in_schema('sec', 'registration')) %>%
    semi_join(tbl(con, in_schema('sec', 'sdbdb01')),
               by = c('regis_yr' = 'current_yr',
                     'regis_qtr' = 'current_qtr')) %>%
    semi_join(canvas_filter) %>%
    select(system_key,
           regis_yr,
           regis_qtr,
           regis_class,
           tenth_day_credits,
           current_credits)

  return(reg)
}

get_current_reg_courses <- function(){
  res <- tbl(con, in_schema('sec', 'registration_courses')) %>%
    semi_join(tbl(con, in_schema('sec', 'sdbdb01')),
              by = c('regis_yr' = 'current_yr',
                     'regis_qtr' = 'current_qtr')) %>%
    semi_join(canvas_filter) %>%
    select(system_key,
           regis_yr,
           regis_qtr,
           index1,
           sln,
           credits,
           grading_system,
           course_branch,
           honor_course,
           dup_enroll,
           `repeat`,
           writing_ind,
           crs_curric_abbr,
           crs_number,
           crs_section_id,
           request_status,
           crs_fee_amt)

  return(res)
}

# get transcripts
get_transcript <- function(){
  # past: transcript
  # current reg is today's (current quarter) information
  tr <- tbl(con, in_schema('sec', 'transcript')) %>%
    semi_join(canvas_filter) %>%
    select(system_key,
           tran_yr,
           tran_qtr,
           resident,
           class,
           special_program,
           honors_program,
           tenth_day_credits,
           num_courses,
           enroll_status,
           add_to_cum,
           qtr_grade_points,
           qtr_graded_attmp,
           qtr_nongrd_earned,
           qtr_deductible,
           over_qtr_grade_pt,
           over_qtr_grade_at,
           over_qtr_nongrd,
           over_qtr_deduct)

  mjr1 <- tbl(con, in_schema('sec', 'transcript_tran_col_major')) %>%
    semi_join(canvas_filter) %>%
    filter(index1 == 1) %>%
    select(system_key,
           tran_yr,
           tran_qtr,
           tran_major_abbr)

  mjr2 <- tbl(con, in_schema('sec', 'transcript_tran_col_major')) %>%
    semi_join(canvas_filter) %>%
    filter(index1 == 2) %>%
    select(system_key,
           tran_yr,
           tran_qtr,
           tran_major_abbr2 = tran_major_abbr)

  nmjrs <- tbl(con, in_schema('sec', 'transcript_tran_col_major')) %>%
    semi_join(canvas_filter) %>%
    group_by(system_key,
             tran_yr,
             tran_qtr) %>%
    # summarize(n_majors = max(index1, na.rm = T)) %>%
    filter(index1 == max(index1)) %>%
    select(system_key,
           tran_yr,
           tran_qtr,
           n_majors = index1) %>%
    ungroup()

  res <- tr %>%
    left_join(mjr1) %>%
    left_join(mjr2) %>%
    left_join(nmjrs)

  return(res)
}

get_tran_courses <- function(){
  crs <- tbl(con, in_schema('sec', 'transcript_courses_taken')) %>%
    semi_join(canvas_filter) %>%
    select(system_key,
           tran_yr,
           tran_qtr,
           index1,
           dept_abbrev,
           course_number,
           section_id,
           course_credits,
           course_branch,
           grade_system,
           grade,
           duplicate_indic,
           deductible,
           honor_course,
           incomplete,
           repeat_course,
           writing)

  return(crs)

}

# fetch facts from EDW (may want to use these calculated vals)
get_stu_prog_enr <- function(){

  stu_id <- tbl(con, in_schema('EDWPresentation.sec', 'dimStudent')) %>%
    select(StudentKeyId,
           SDBSrcSystemKey) %>%
    semi_join(canvas_filter, by = c('SDBSrcSystemKey' = 'system_key'))

  cal_id <- tbl(con, in_schema('EDWPresentation.sec', 'dimDate')) %>%
    # filter(AcademicQtrCensusDayInd == 'Y') %>%
    select(CalendarDateKeyId,
           AcademicQtrKeyId,
           AcademicFiscalYr,
           AcademicQtr,
           AcademicQtrCensusDayInd,
           AcademicQtrBeginInd,
           AcademicQtrLastInstructionDayInd)

  # This is a mess this doesn't really support a student-centered view like transcripts b/c
  # anyone not enrolled on one of the 3 specific dates won't show up
  res <- tbl(con, in_schema('EDWPresentation.sec', 'factStudentProgramEnrollment')) %>%
    inner_join(stu_id) %>%
    inner_join(cal_id) %>%
    select(AcademicQtrKeyId,
           AcademicFiscalYr,
           AcademicQtr,
           AcademicQtrCensusDayInd,
           AcademicQtrBeginInd,
           AcademicQtrLastInstructionDayInd,
           StudentKeyId,
           SDBSrcSystemKey,
           MajorKeyId,
           CumAddtnlAppliedCredits,
           CumAddtnlCredits,
           CumDeductibleCredits,
           CumGPA,
           CumGPACredits,
           CumGPAGradePoints,
           CumEnrolledQtrs,
           CumNonGradedEarnedCredits,
           CumTotalEarnedCredits,
           CumTotalTransferCredits,
           CumUWCredits,
           CurrQtrClassLevelFTE,
           CurrQtrCredits,
           FullTimeInd,
           MultiMajorInd,
           StudentEnrolledCnt,
           StudentRegisteredCnt)

  return(res)
}



# Utility functions for raw s3 txt files ----------------------------------


#
# # make_wide_weekly_assignments <- function(files){
#
#
#
#   wkly <- data.frame()
#   for(i in 1:length(files)){
#     dat <- read_delim(files[i],
#                       delim = '|',
#                       col_names = c('yrq', 'course_id', 'user_id', 'assignment_id', 'due_at', 'pts_possible', 'status', 'score'),
#                       na = c('None', 'NA'))
#     dat <- dat[!is.na(dat$status),]
#     # create dummies for status
#     dat <- bind_cols(dat, data.frame(model.matrix(~ 0 + status, data = dat))) %>% select(-status)
#
#     # Use the due date to stand-in for week
#     dat$CalendarDateKeyId <- str_remove_all(str_sub(dat$due_at, end = 10), '-')
#     dat$CalendarDateKeyId <- as.numeric(dat$CalendarDateKeyId)
#
#     # merge with calendar -- some data loss
#     mg <- dat %>%
#       inner_join(cal) %>%
#       select(-due_at,
#              -CalendarDateKeyId)
#
#     agg.n.assgn <- mg %>%
#       group_by(yrq, user_id, course_id, week) %>%
#       summarize(n_assgn = n_distinct(assignment_id)) %>%
#       ungroup()
#     agg.wk <- mg %>%
#       group_by(yrq, user_id, course_id, week) %>%
#       select(-assignment_id) %>%
#       summarize_all(sum) %>%
#       inner_join(agg.n.assgn) %>%
#       mutate(score = replace_na(0))
#
#     d <- dat %>% distinct(yrq, course_id, user_id)
#     week <- sort(unique(mg$week))
#     exp.d <- expand(d, nesting(yrq, course_id, user_id), week)
#
#     # now combine:
#     exp.wk <- exp.d %>%
#       left_join(agg.wk) %>%
#       mutate_at(c('pts_possible', 'score', 'statusfloating', 'statuslate', 'statusmissing', 'statuson_time', 'n_assgn'),
#                 replace_na, 0) %>%
#       ungroup()
#
#     assgn.wide <- exp.wk %>%
#       pivot_wider(id_cols = c('yrq', 'user_id', 'course_id'),
#                   names_from = 'week',
#                   names_prefix = 'week_',
#                   values_from = c('pts_possible',
#                                   'score',
#                                   'statusfloating',
#                                   'statuslate',
#                                   'statusmissing',
#                                   'statuson_time',
#                                   'n_assgn'))
#
#     wkly <- bind_rows(wkly, assgn.wide)
#   }
#
#   return(wkly)
# }

make_long_weekly_assignments <- function(f = 'data-raw/new-preds/assignments.txt',
                                         hd = 'data-raw/assignments-head.txt'){

  con <- dbConnect(odbc(), 'sqlserver01')
  cal <- tbl(con, in_schema('EDWPresentation.sec', 'dimDate')) %>%
    filter(AcademicContigYrQtrCode >= 20181) %>%
    mutate(yrq = as.numeric(AcademicYrQtrCode)) %>%
    select(CalendarDateKeyId,
           week = AcademicQtrWeekNum,
           yrq) %>%
    collect()

  dat <- read_delim(f,
                    delim = '|',
                    col_names = scan(hd, 'character', sep = '|'),
                    na = c('None', 'NA'))

  dat <- dat[!is.na(dat$status),]
  dat <- dat[!is.na(dat$due_at),]

  # create dummies for status
  dat <- bind_cols(dat, data.frame(model.matrix(~ 0 + status, data = dat))) %>% select(-status)

  # Use the due date to stand-in for week
  # Many due-dates are missing, e.g. if an assignment is floating it has no date
  dat$CalendarDateKeyId <- str_remove_all(str_sub(dat$due_at, end = 10), '-')
  dat$CalendarDateKeyId <- as.numeric(dat$CalendarDateKeyId)

  # merge with calendar -- some data loss
  mg <- dat %>%
    inner_join(cal) %>%
    select(-due_at,
           -CalendarDateKeyId)

  agg.n.assgn <- mg %>%
    group_by(yrq, user_id, course_id, week) %>%
    summarize(n_assgn = n_distinct(assignment_id)) %>%
    ungroup()
  agg.wk <- mg %>%
    group_by(yrq, user_id, course_id, week) %>%
    select(-assignment_id) %>%
    summarize_all(sum) %>%
    inner_join(agg.n.assgn)

  d <- dat %>% distinct(yrq, course_id, user_id)
  week <- sort(unique(mg$week))
  exp.d <- expand(d, nesting(yrq, course_id, user_id), week)

  # now combine:
  exp.wk <- exp.d %>%
    left_join(agg.wk) %>%
    replace_na(list(pts_possible = 0,
                    score = 0,
                    statusfloating = 0,
                    statuslate = 0,
                    statusmissing = 0,
                    statuson_time = 0,
                    n_assgn = 0)) %>%
    mutate(score = if_else(score < 0, 0, score)) %>%
    ungroup()

  dbDisconnect(con)
  return(exp.wk)
}

make_long_weekly_urls <- function(f = 'data-raw/new-preds/partic-url.txt',
                                  hd = 'data-raw/partic-url-head.txt'){

  con <- dbConnect(odbc(), 'sqlserver01')
  cal <- tbl(con, in_schema('EDWPresentation.sec', 'dimDate')) %>%
    filter(AcademicContigYrQtrCode >= 20181) %>%
    mutate(yrq = as.numeric(AcademicYrQtrCode)) %>%
    select(CalendarDateKeyId,
           week = AcademicQtrWeekNum,
           yrq) %>%
    collect()

  dat <- read_delim(f,
                    col_names = scan(hd, 'character', sep = '|'),
                    delim = '|',
                    na = c('None', 'NA'))
  dat <- drop_na(dat, date)
  dat$CalendarDateKeyId <- as.numeric(as.numeric(str_remove_all(str_sub(dat$date, end = 10), '-')))

  dat <- dat %>%
    inner_join(cal) %>%
    select(-date, -CalendarDateKeyId) %>%
    group_by(yrq, course_id, user_id, week) %>%
    summarize(nd_urls = n_distinct(url),
              n_urls = n()) %>%
    ungroup()

  # for full week expansion
  d <- dat %>% distinct(yrq, course_id, user_id)
  week <- sort(unique(cal$week))
  exp.d <- expand(d, nesting(yrq, course_id, user_id), week)

  dat <- dat %>% right_join(exp.d) %>% mutate_at(c('nd_urls', 'n_urls'), replace_na, 0)

  # text_mat <- str_split(dat$url, '\\/')
  # dat$partic_item <- sapply(text_mat, function(x) {ifelse(x[6] == 'courses', x[8], x[6])}, simplify = T)
  # dat$partic_item <- if_else(grepl('discussion_topics', dat$partic_item) == T, 'discussion', dat$partic_item)
  # rm(text_mat)
  #
  # dat <- dat %>% inner_join(cal) %>% select(-date, -url, -CalendarDateKeyId)
  #
  # # for full week expansion
  # d <- dat %>% distinct(yrq, course_id, user_id)
  # week <- sort(unique(cal$week))
  # exp.d <- expand(d, nesting(yrq, course_id, user_id), week)
  # rm(d, week)
  #
  # # files are huge, reduce to weeks
  # # then merge with the expanded yrq-user-course-week data
  # dat <- dat %>%
  #   group_by(yrq, user_id, course_id, week, partic_item) %>%
  #   summarize(n = n()) %>%
  #   ungroup() %>%
  #   right_join(exp.d)
  # mutate_at(c('partic_item'), replace_na, 0) %>%
  # pivot_wider(id_cols = c('yrq', 'user_id', 'course_id', 'week'),
  #             names_from = 'partic_item',
  #             values_from = n)

  dbDisconnect(con)
  return(dat)
}
