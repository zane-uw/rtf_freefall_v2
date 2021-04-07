rm(list = ls())

library(tidyverse)
library(dbplyr)
library(odbc)

setwd(rstudioapi::getActiveProject())

# floor for historical data
YRQ_0 <- 20154
# convenience var
EOP_CODES <- c(1, 2, 13, 14, 16, 17, 31, 32, 33)

# **SDB DATA** ----------------------------------------------------------------

# !kinit
con <- dbConnect(odbc(), 'sdb', UID = config::get('sdb')$uid, PWD = config::get('sdb')$pwd)

# Utility table expressions -------------------------------------------------------------------
# calendar
# current year, quarter
# filtering query for EOP students
currentq <- tbl(con, in_schema("sec", "sdbdb01")) %>%
  select(current_yr, current_qtr, gl_first_day, gl_regis_year, gl_regis_qtr) %>%
  collect() %>%
  mutate(current_yrq = current_yr*10 + current_qtr,
         regis_yrq = gl_regis_year*10 + gl_regis_qtr)

cat('current quarter result:', names(currentq), fill = T)
cat("------", unlist(currentq), fill = T)

undb.eop <- tbl(con, in_schema("sec", "transcript")) %>%
  filter(special_program %in% EOP_CODES) %>%
  select(system_key) %>%
  full_join( tbl(con, in_schema('sec', 'registration')) %>%
               filter(special_program %in% EOP_CODES) %>%
               select(system_key)
  ) %>%
  full_join( tbl(con, in_schema('sec', 'student_1')) %>%
               filter(spcl_program %in% EOP_CODES) %>%
               select(system_key)
  ) %>%
  # ADD ISS students
  full_join( tbl(con, in_schema('sec', 'student_1_college_major')) %>%
               filter(major_abbr == "ISS O") %>%
               select(system_key)
  ) %>%
  distinct() %>%
  compute(name = "##eop_temp_qry")


# Transcript -------------------------------------------------------------
get_transcript <- function(){
  transcript <- tbl(con, in_schema("sec", "transcript")) %>%
    semi_join(tbl(con, "##eop_temp_qry")) %>%
    mutate(yrq = tran_yr*10 + tran_qtr) %>%
    filter(yrq >= YRQ_0,
           yrq < local(currentq$current_yrq),
           class <= 4,
           (tenth_day_credits > 0 & num_courses > 0)) %>%       # tran_qtr != 3, add_to_cum == 1
    select(system_key,
           yrq,
           class,
           honors_program,
           tenth_day_credits,
           scholarship_type,
           num_courses) %>%
    collect() %>%

  return(transcript)
}


# GPA ---------------------------------------------------------------------

calc_qgpa <- function(){
  qgpa <- tbl(con, in_schema("sec", "transcript")) %>%
    semi_join(tbl(con, "##eop_temp_qry")) %>%
    mutate(yrq = tran_yr*10 + tran_qtr) %>%
    filter(yrq >= YRQ_0,
           yrq < local(currentq$current_yrq),
           class <= 4,
           (tenth_day_credits > 0 & num_courses > 0)) %>%       # tran_qtr != 3, add_to_cum == 1
    select(system_key,
           yrq,
           qtr_grade_points,
           qtr_graded_attmp,
           over_qtr_grade_pt,
           over_qtr_grade_at,
           qtr_nongrd_earned,
           over_qtr_nongrd,
           qtr_deductible,
           over_qtr_deduct) %>%
    collect() %>%
    mutate(pts = if_else(over_qtr_grade_pt > 0, over_qtr_grade_pt, qtr_grade_points), # pmax(qtr_grade_points, over_qtr_grade_pt, na.rm = T),
           attmp = if_else(over_qtr_grade_at > 0, over_qtr_grade_at, qtr_graded_attmp),  # pmax(qtr_graded_attmp, over_qtr_grade_at, na.rm = T),
           nongrd = if_else(over_qtr_nongrd > 0, over_qtr_nongrd, qtr_nongrd_earned), # pmax(qtr_nongrd_earned, over_qtr_nongrd, na.rm = T),
           deduct = if_else(over_qtr_deduct > 0, over_qtr_deduct, qtr_deductible), # pmax(qtr_deductible, over_qtr_deduct, na.rm = T),
           qgpa = pts / attmp,
           tot_creds = attmp + nongrd,
           # correction for div 0 error w/ pts / attmp
           ) %>%
    select(-starts_with('over_qtr'),
           -qtr_grade_points,
           -qtr_graded_attmp,
           -qtr_nongrd_earned,
           -qtr_deductible)

  return(qgpa)
}

# Courses taken -----------------------------------------------------------

get_courses_taken <- function(){
  # multiple entries per quarter
  # join with time sched to get attributes of courses too
  tr_courses_taken <- tbl(con, in_schema("sec", "transcript_courses_taken")) %>%
    mutate(yrq = tran_yr*10 + tran_qtr) %>%
    filter(yrq >= YRQ_0,
           yrq < local(currentq$current_yrq)) %>%
    semi_join(tbl(con, "##eop_temp_qry")) %>%
    select(system_key,
           yrq,
           course_index = index1,
           dept_abbrev,
           course_number,
           course_credits,
           course_branch,
           summer_term,
           grade,
           duplicate_indic,
           repeat_course,
           honor_course,
           section_id) %>%
    # add fees, etc. from time schedule
    left_join(
      tbl(con, in_schema("sec", "time_schedule")) %>%
        mutate(yrq = ts_year*10 + ts_quarter) %>%
        filter(yrq >= YRQ_0,
               yrq < local(currentq$current_yrq)) %>%
        select(yrq,
               course_branch,
               dept_abbrev,
               course_no,
               section_id,
               # sln,
               current_enroll,
               # parent_sln,
               # writing_crs,
               # diversity_crs,
               # english_comp,
               # qsr,
               # vis_lit_perf_arts,
               # indiv_society,
               # natural_world,
               # gen_elective,
               fee_amount),
      by = c('dept_abbrev' = 'dept_abbrev',
             'course_number' = 'course_no',
             'yrq' = 'yrq',
             'section_id' = 'section_id',
             'course_branch' = 'course_branch')
    ) %>%
    collect()

  return(tr_courses_taken)
}



# Majors, dual majors ------------------------------------------------------------------

get_major_data <- function(){
  mjr <- tbl(con, in_schema("sec", "transcript_tran_col_major")) %>%
    semi_join(tbl(con, "##eop_temp_qry")) %>%
    mutate(yrq = tran_yr*10 + tran_qtr) %>%
    filter(yrq >= YRQ_0,
           yrq < local(currentq$current_yrq)) %>%
    select(system_key,
           yrq,
           index1,
           tran_major_abbr) %>%
    distinct()

  # calc double+ majors
  dual_majors <- mjr %>%
    mutate(dual_major = if_else(index1 == 2, 1, 0)) %>%
    select(system_key, yrq, dual_major) %>%
    group_by(system_key, yrq) %>%
    filter(dual_major == max(dual_major)) %>%
    ungroup()

  majors <- mjr %>%
    filter(index1 == 1) %>%
    select(system_key,
           yrq,
           tran_major_abbr) %>%
    inner_join(dual_majors) %>%
    collect() %>%
    group_by(system_key) %>%
    arrange(system_key, yrq) %>%
    mutate(major_change = if_else(tran_major_abbr == lag(tran_major_abbr, order_by = yrq), 0, 1, missing = 0),
           major_change_count = cumsum(major_change)) %>%
    ungroup()

  majors$n_majors <- unlist(tapply(majors$tran_major_abbr,
                                   majors$system_key,
                                   function(x) { rep(seq_along(rle(x)$lengths), times = rle(x)$lengths) }))

  # add EDW stem to major codes
  stem_codes <- tbl(con, in_schema(sql('EDWPresentation.sec'), 'dimCIPCurrent')) %>%
    select(CIPCode,
           FederalSTEMInd) %>%
    filter(FederalSTEMInd == "Y") %>%
    inner_join( select(.data = tbl(con, in_schema('sec', 'sr_major_code')), major_abbr, major_cip_code),
                by = c('CIPCode' = 'major_cip_code'),
                copy = T) %>%
    distinct() %>%
    collect()

  majors <- majors %>%
    mutate(stem_major = if_else(tran_major_abbr %in% stem_codes$major_abbr, 1, 0))

  return(majors)
}

# Unmet_reqs ---------------------------------------------------

get_unmet_reqs <- function(){
  unmet_reqs <- tbl(con, in_schema('sec', 'sr_unmet_request')) %>%
    semi_join(tbl(con, "##eop_temp_qry"),
              by = c('unmet_system_key' = 'system_key')) %>%
    mutate(yrq = unmet_yr*10 + unmet_qtr) %>%
    filter(yrq >= YRQ_0,
           yrq < local(currentq$current_yrq)) %>%
    group_by(unmet_system_key, yrq) %>%
    summarize(n_unmet = n()) %>%
    ungroup() %>%
    select(system_key = unmet_system_key, yrq, n_unmet) %>%
    collect()

  return(unmet_reqs)
}

# Late registration ----------------------------------------------------

# including Day 0 as 'late'
# [TODO] rescaling the boundaries b/c of very unusual range of values
get_late_registrations <- function(){

  syscal <- tbl(con, in_schema("sec", "sys_tbl_39_calendar")) %>%
    filter(first_day >= "2020-01-01") %>%                           # arbitrary, some kind of limit is helpful
    select(table_key, first_day, tenth_day, last_day_add) %>%
    collect() %>%
    mutate(yrq = as.numeric(table_key),
           regis_yr = round(yrq %/% 10, 0),        # r floor div operator works incorrectly in sql tbl
           regis_qtr = yrq %% 10)

  # "fixes" still don't eliminate the extremely off dates, so I'll stick with the min and correct the ones I see right now
  regc <- tbl(con, in_schema("sec", "registration_courses")) %>%
    semi_join(tbl(con, "##eop_temp_qry")) %>%
    mutate(yrq = regis_yr*10 + regis_qtr) %>%
    filter(yrq >= 20201) %>%
    select(system_key, yrq, add_dt_tuit) %>%
    group_by(system_key, yrq) %>%
    filter(add_dt_tuit == min(add_dt_tuit)) %>%
    distinct() %>%
    collect() %>%
    left_join(syscal) %>%
    mutate(reg_late_days = as.numeric(difftime(add_dt_tuit, first_day, units = "days"))) %>%
    ungroup()

  regc$reg_late_days[regc$reg_late_days <= -365] <- median(regc$reg_late_days[regc$reg_late_days < 0], na.rm = T)
  regc$reg_late_binary <- if_else(regc$reg_late_days >= 0, 1, 0)

  result <- regc %>%
    select(system_key, yrq, reg_late_binary, reg_late_days)

  return(result)

}


# Quarterly holds -------------------------------------------------------------------

get_holds <- function(){

  # academic calendar
  cal <- tbl(con, in_schema(sql('EDWPresentation.sec'), "dimDate")) %>%
    select(yrq = AcademicQtrKeyId, dt = CalendarDate)

  holds <- tbl(con, in_schema("sec", "student_1_hold_information")) %>%
    semi_join(tbl(con, "##eop_temp_qry")) %>%
    inner_join(cal, by = c('hold_dt' = 'dt')) %>%
    collect() %>%
    group_by(system_key, yrq) %>%
    summarize(n_holds = n()) %>%
    ungroup() %>%
    mutate(yrq = as.numeric(yrq))

  return(holds)
}


# Application data --------------------------------------------------------

# get_appl_data <- function(){
#   # test scores no longer req'd
#
#   # create an in-db filtering file for the applications
#   app_filter <- tbl(con, in_schema("sec", "student_1")) %>%
#     inner_join(db.eop) %>%
#     select(system_key,
#            appl_yr = current_appl_yr,
#            appl_qtr = current_appl_qtr,
#            appl_no = current_appl_no) %>%
#     distinct()
#
#   gpa <- tbl(con, in_schema('sec', 'sr_adm_appl')) %>%
#     semi_join(tbl(con, "##eop_temp_qry")) %>%
#     group_by(system_key) %>%
#     summarize(high_sch_gpa = max(high_sch_gpa, na.rm = T),
#               trans_gpa = max(trans_gpa, na.rm = T)) %>%
#     ungroup()
#
#   sr_appl <- tbl(con, in_schema("sec", "sr_adm_appl")) %>%
#     semi_join(app_filter) %>%
#     select(system_key,
#            conditional,
#            provisional,
#            with_distinction,
#            res_in_question,
#            low_family_income,
#            starts_with("hs_"),
#            last_school_type,
#            -hs_esl_engl)
#
#   appl_guardian <- tbl(con, in_schema("sec", "sr_adm_appl_guardian_data")) %>%
#     semi_join(app_filter) %>%
#     group_by(system_key) %>%
#     summarize(guardian_ed_max = max(guardian_ed_level, na.rm = T),
#               guardian_ed_min = min(guardian_ed_level, na.rm = T)) %>%
#     ungroup()
#
#   # combine applications:
#   result <- sr_appl %>%
#     inner_join(gpa) %>%
#     left_join(appl_guardian) %>%
#     collect() %>%
#     mutate_if(is.character, trimws) %>%
#     mutate(trans_gpa = na_if(trans_gpa, 0),
#            high_sch_gpa = na_if(high_sch_gpa, 0))
#
#   return(result)
#
# }


# Stu_1, age ---------------------------------------------------------
get_stu_1 <- function(){
  # server version of sql doesn't support %/%
  ENR_DIV <- YRQ_0 %/% 10
  stu1 <- tbl(con, in_schema("sec", "student_1")) %>%
    select(system_key,
           s1_gender,
           uw_netid,
           resident,
           last_yr_enrolled) %>%
    filter(last_yr_enrolled >= ENR_DIV) %>%
    semi_join(tbl(con, "##eop_temp_qry"),
              by = c('system_key' = 'system_key')) %>%
    select(-last_yr_enrolled)

  return(stu1)
}

calc_adjusted_age <- function(){

  bd <- tbl(con, in_schema('sec', 'student_1')) %>%
    select(system_key, birth_dt) %>%
    semi_join(tbl(con, "##eop_temp_qry")) %>%
    inner_join( tbl(con, in_schema('sec', 'registration')) %>%
                  select(system_key, regis_yr, regis_qtr) ) %>%
    mutate(table_key = paste0('0', as.character(regis_yr), as.character(regis_qtr), ' '))

  result <- tbl(con, in_schema('sec', 'sys_tbl_39_calendar')) %>%
    filter(first_day >= '2000-01-01') %>%
    select(table_key, tenth_day) %>%
    inner_join(bd) %>%
    collect() %>%
    mutate(age = as.numeric(difftime(tenth_day, birth_dt, units = "days")) / 364.25,
           yrq = regis_yr*10 + regis_qtr) %>%
    select(system_key, yrq, age) %>%
    filter(yrq < local(currentq$current_yrq))

  return(result)
}


# RUN GET --------------------------------------------------------------------

# [TODO] moving mutate funs from transcript/reg functions
# mutate(cum.pts = cumsum(pts),
#     cum.attmp = cumsum(attmp),
#    cum.gpa = cum.pts / cum.attmp) %>%

transcript <- get_transcript()
qgpa <- calc_qgpa()
courses_taken <- get_courses_taken()
majors <- get_major_data()
stu_age <- calc_adjusted_age()
# appl <- get_appl_data()
unmet_reqs <- get_unmet_reqs()
late_reg <- get_late_registrations()
holds <- get_holds()

stu1 <- get_stu_1() %>%
  mutate(s1_gender = ifelse(s1_gender == 'F', 1, 0)) %>%
  collect()

# AGGREGATE courses taken ---------------------------------------------------------------
# Convert `courses_taken` many-records-per-qtr to one-per

courses_taken <- courses_taken %>%
  mutate_if(is.character, trimws) %>%         # doesn't work remotely
  # but don't want to build the course code w/ the extra white space
  mutate(course = paste(dept_abbrev, course_number, sep = "_"),
         duplicate_indic = as.numeric(duplicate_indic),
         numeric_grade = recode(grade,
                                "A"  = "40",
                                "A-" = "38",
                                "B+" = "34",
                                "B"  = "31",
                                "B-" = "28",
                                "C+" = "24",
                                "C"  = "21",
                                "C-" = "18",
                                "D+" = "14",
                                "D"  = "11",
                                "D-" = "08",
                                "E"  = "00",
                                "F"  = "00"),
         numeric_grade = as.numeric(numeric_grade) / 10,
         course_withdraw = if_else(grepl("W", grade), 1, 0),
         course_nogpa = if_else(grade %in% c("", "CR", "H", "HP", "HW", "I", "N", "NC", "NS", "P", "S", "W", "W3", "W4", "W5", "W6", "W7"), 1, 0),
         course_alt_grading = if_else(grade %in% c('CR', 'S', 'NS', 'P', 'HP', 'NC'), 1, 0))

# repeats, alternate grading, fees, etc.
aggr_courses_taken <- courses_taken %>%
  arrange(system_key, yrq) %>%
  group_by(system_key, yrq) %>%
  summarize(rep_courses = sum(repeat_course),
            n_w = sum(course_withdraw),
            n_alt_grading = sum(course_alt_grading),
            # grade_lo = min(numeric_grade, na.rm = T),
            # grade_hi = max(numeric_grade, na.rm = T),
            grade_var = var(numeric_grade, na.rm = T),
            sum_fees = sum(fee_amount, na.rm = T),
            avg_course_size = mean(current_enroll, na.rm = T)) %>%
  group_by(system_key, .add = F) %>%
  mutate(csum_rep_courses = cumsum(rep_courses),
         csum_w = cumsum(n_w),
         csum_alt_grading = cumsum(n_alt_grading)) %>%
  ungroup()
# aggr_courses_taken$grade_var[is.infinite(aggr_courses_taken$grade_var)] <- NA

  # # dept-wise courses, grades, etc (v. wide)
  # # agg/reduce to 1 row per student + yrq + dept_abbr
  # # then pivot wide
  # create.stu.dept.wide <- function(percent = 0.9){
  #   stu.deptwise.data <- courses.taken %>%
  #     group_by(system_key, yrq, dept_abbrev) %>%
  #     summarize(sgrade = sum(numeric.grade, na.rm = T),
  #               n = n(),
  #               creds.dept = sum(course_credits, na.rm = T)) %>%
  #     ungroup() %>%
  #     arrange(system_key, dept_abbrev, yrq) %>%
  #     group_by(system_key, dept_abbrev) %>%
  #     mutate(csum.grade = cumsum(sgrade),
  #            nclass.dept = cumsum(n),
  #            cumavg.dept = csum.grade / nclass.dept,
  #            csum.dept.creds = cumsum(creds.dept)) %>%
  #     ungroup() %>%
  #     select(system_key, dept_abbrev, yrq, cumavg.dept, nclass.dept, csum.dept.creds) %>%
  #     arrange(system_key, yrq, dept_abbrev)

# STEM courses/grades -----------------------------------------------------
# join w/ courses taken data
stem_data <- tbl(con, in_schema(sql("EDWPresentation.sec"), "dmSCH_dimCurriculumCourse")) %>%
  filter(FederalSTEMInd == "Y") %>%
  select(dept_abbrev = CurriculumCode,
         course_number = CourseNbr,
         course_level = CourseLevelNbr,
         cip = CIPCode) %>%
  distinct() %>%
  collect() %>%
  mutate_if(is.character, trimws) %>%
  mutate(course = paste(dept_abbrev, course_number, sep = "_"),
         is_stem = 1) %>%
  right_join( select(.data = courses_taken, system_key, yrq, course, numeric_grade, course_credits) ) %>%
  arrange(system_key, yrq) %>%
  group_by(system_key, yrq) %>%
  summarize(stem_courses = sum(is_stem, na.rm = T),
            stem_credits = sum(course_credits * is_stem, na.rm = T),
            avg_stem_grade = mean(numeric_grade * is_stem, na.rm = T)) %>%
  group_by(system_key, add = F) %>%
  mutate(csum_stem_courses = cumsum(stem_courses),
         csum_stem_credits = cumsum(stem_credits),
         avg_stem_grade = ifelse(is.nan(avg_stem_grade), NA, avg_stem_grade),
         avg_stem_grade_lag = lag(avg_stem_grade)) %>%
  ungroup() %>%
    select(-avg_stem_grade)



# TRANSFORM, LAGS, etc --------------------------------------------------------
# variables that need to be lagged include anything that wouldn't be visible before the end of term

# 1) mutate transcripts
# to create quarter/time sequence; technically this may not be correct since the baseline measures from 20154, not
# necessarily the very first transcripted year
transcript <- transcript %>%
  group_by(system_key) %>%
  arrange(system_key, yrq) %>%
  mutate(n_quarters = row_number(),
         csum_courses = cumsum(num_courses),
         scholarship_type_lag = lag(scholarship_type)) %>%
  ungroup() %>%
  select(-scholarship_type)



# 2) Majors - derived fields
# premajors, ext_premajor
majors$ext_premajor <- if_else(majors$tran_major_abbr == 'EPRMJ', 1, 0)
majors$premajor <- if_else(grepl('PRE', majors$tran_major_abbr), 1, 0)
majors$nonmatr <- if_else(majors$tran_major_abbr == 'N MATR', 1, 0)


# 3) lags
lag_aggr_courses_taken <- aggr_courses_taken %>%
  group_by(system_key) %>%
  arrange(system_key, yrq) %>%
  mutate_at(.vars = vars(n_w, grade_var, csum_w, csum_alt_grading), list(lag = lag)) %>%
  ungroup() %>%
  select(system_key,
         yrq,
         rep_courses,
         n_alt_grading,
         sum_fees,
         avg_course_size,
         csum_rep_courses,
         ends_with("_lag"))

lag_qgpa <- qgpa %>%
  group_by(system_key) %>%
  arrange(system_key, yrq) %>%
  mutate_at(.vars = vars(pts, nongrd, deduct, qgpa, tot_creds), list(lag = lag)) %>%
  ungroup() %>%
  select(system_key,
         yrq,
         attmp,
         ends_with('_lag'))


# JOIN --------------------------------------------------------------------

dat <- transcript %>%
  left_join(majors) %>%
  left_join(stem_data) %>%
  left_join(stu1) %>%
  left_join(lag_qgpa) %>%
  left_join(lag_aggr_courses_taken) %>%
  left_join(holds) %>%
  left_join(late_reg) %>%
  left_join(stu_age) %>%
  left_join(unmet_reqs)

write_csv(dat, 'data-intermediate/sdb_eop_isso_sp21.csv')
