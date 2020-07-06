# Source this for functions to gen raw data

# rm(list = ls())
# gc()

setwd(rstudioapi::getActiveProject())

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

  # skeys <- data.frame('canvas_user_id' = union(union(pv$user_id, assgn$user_id), urls$user_id))
  skeys <- canvas_globs$uid

  prov_users <- read_csv('../../Retention-Analytics-Dashboard/data-raw/provisioning_csv_30_Mar_2020_15884/users.csv') %>%
    filter(status == 'active', created_by_sis == T)

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

  crskeys <- read_canvas_course_ids()

  prov_sect <- read_csv('../../../../../canvas-data/enrollments_all.csv') %>%
    filter(role == 'student')%>%
    select(canvas_course_id, course_id, canvas_user_id, canvas_section_id, section_id) %>%
    drop_na() %>%
    semi_join(crskeys)

  # also filter out courses that start with 'course_' - these aren't useful
  prov_sect <- prov_sect[grepl("^course_", prov_sect$course_id) == F,]

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
