# Source this to gen raw data

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
# explore how to merge these later

# globals for filtering ---------------------------------------------------

yrq.min <- min(pv$yrq)
yrq.max <- max(pv$yrq)


# mappings between db and lms ---------------------------------------------

link_students <- function(){
  # danger: a person may have more than 1 canvas ID under the same uw_netid and system_key
  # lose about 5k with the last distinct

  skeys <- data.frame('canvas_user_id' = union(union(pv$user_id, assgn$user_id), urls$user_id))
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
    mutate_if(is.character, str_remove_all, pattern = " ")

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

## actually can just use the enrollments ##
# connect canvas courses by id by extracting dept and course num
# keep the top-level section
  # link_courses <- function(){
  #
  #   crskeys <- data.frame('canvas_course_id' = union(union(pv$course_id, assgn$course_id), urls$course_id))
  #   # prov_courses <- read_csv('../../../../../canvas-data/courses_all.csv') %>%
  #   #   filter(created_by_sis == T)
  #
  #   cal <- tbl(con, in_schema('EDWPresentation.sec', 'dimDate')) %>%
  #     filter(AcademicYrQtrCode >= yrq.min,
  #            AcademicYrQtrCode <= yrq.max) %>%
  #     select(AcademicYrQtrCode,
  #            AcademicQtrName) %>%
  #     distinct() %>%
  #     collect() %>%
  #     drop_na() %>%
  #     # create canvas-style term_id
  #     mutate(term_id = paste(str_sub(AcademicYrQtrCode, end = 4),
  #                            tolower(AcademicQtrName), sep = '-'),
  #            yrq = as.numeric(AcademicYrQtrCode)) %>%
  #     select(yrq, term_id)
  #
  #   courses <- read_csv('../../../../../canvas-data/courses_all.csv') %>%
  #     filter(created_by_sis == T) %>%
  #     left_join(cal, by = c('term_id' = 'term_id')) %>%
  #     semi_join(crskeys)
  #
  #   xmat <- data.frame(str_split(courses$course_id, "-", simplify = T))
  #   courses$dept_abbrev <- xmat[,3]
  #   courses$course_no <- xmat[,4]
  #   courses$section_short <- xmat[,5]     # this isn't a good match for the time schedule b/c of this truncated section #
  #   rm(xmat)
  #
  #   res <- courses %>%
  #     select(yrq, canvas_course_id, dept_abbrev, course_no)
  #
  #   return(res)
  # }

# mapping sections and students, these are kind of a mess b/c the db rules for generating keys are inconsistent over time
# this includes course codes
link_enrollments <- function(){
  crskeys <- data.frame('canvas_course_id' = union(union(pv$course_id, assgn$course_id), urls$course_id))
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
    filter(yrq >= yrq.min,
           yrq <= yrq.max)

  return(res)
}

# fetch raw SDB data ------------------------------------------------------

# push temp filtering tables
canvas_filter <- link_students() %>% copy_to(con, ., overwrite = T)

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

get_mm_stu <- function(){
  ymin <- yrq.min %/% 10
  sr_mm <- tbl(con, in_schema('sec', 'sr_mini_master')) %>%
    filter(mm_year >= ymin,
           mm_proc_ind == 2) %>%
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
           mm_proc_ind == 2) %>%
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
                                 'mm_qtr' = 'unmet_qtr')) %>%
    collect()

  return(res)
}

get_stu_1 <- function(con = con){
  # this will also get the current quarter's census day registration
  ymin <- yrq.min %/% 10
  st1 <- tbl(con, in_schema('sec', 'student_1')) %>%
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

}

# may not be required since mm_ will get current if past census day
# there are other circumstances where this could be a useful query
get_current_registration <- function(){
  # current: registration
  # this_qtr <- tbl(con, in_schema('sec', 'sdbdb01'))

  reg <- tbl(con, in_schema('sec', 'registration')) %>%
    semi_join(tbl(con, in_schema('sec', 'sdbdb01')),
               by = c('regis_yr' = 'current_yr',
                     'regis_qtr' = 'current_qtr')) %>%
    select(system_key,
           regis_yr,
           regis_qtr,
           regis_class,
           tenth_day_credits,
           current_credits)
    collect()

  return(reg)
}

# for some students, get transcripts and current-qtr registration
get_transcript <- function(){
  # past: transcript
  tr <- tbl(con, in_schema('sec', 'transcript')) %>% filter(tran_yr == 2020, tran_qtr == 1) %>% collect()


}
