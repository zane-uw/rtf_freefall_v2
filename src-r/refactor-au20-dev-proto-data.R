## This is a rough translation of freefall V1 data pull to
## create an intermediate dataset that is passed on to the `merge-intermediate-data.R` file.

## Incorporating ISS w/ addtl query

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
con <- dbConnect(odbc(), 'sqlserver01')

# Utility table expressions -------------------------------------------------------------------
# calendar
# current year, quarter
# filtering query for EOP students



currentq <- tbl(con, in_schema("sec", "sdbdb01")) %>%
  select(current_yr, current_qtr, gl_first_day, gl_regis_year, gl_regis_qtr) %>%
  # collect() %>%
  mutate(current_yrq = current_yr*10 + current_qtr,
         regis_yrq = gl_regis_year*10 + gl_regis_qtr)

db.eop <- tbl(con, in_schema("sec", "transcript")) %>%
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
  distinct()


# TRANSCRIPTS -------------------------------------------------------------

get.transcripts <- function(from_yrq = YRQ_0){
  transcript <- tbl(con, in_schema("sec", "transcript")) %>%
    semi_join(db.eop) %>%
    mutate(yrq = tran_yr*10 + tran_qtr) %>%
    filter(yrq >= from_yrq) %>%       # tran_qtr != 3, add_to_cum == 1
    select(system_key,
           yrq,
           class,
           honors_program,
           tenth_day_credits,
           scholarship_type,
           # yearly_honor_type,
           num_ind_study,
           num_courses,
           qtr_grade_points,
           qtr_graded_attmp,
           over_qtr_grade_pt,
           over_qtr_grade_at,
           qtr_nongrd_earned,
           over_qtr_nongrd,
           qtr_deductible,
           over_qtr_deduct) %>%
    collect() %>%
    mutate(pts = pmax(qtr_grade_points, over_qtr_grade_pt, na.rm = T),           # NB mssql doesn't support pmax
           attmp = pmax(qtr_graded_attmp, over_qtr_grade_at, na.rm = T),
           nongrd = pmax(qtr_nongrd_earned, over_qtr_nongrd, na.rm = T),
           deduct = pmax(qtr_deductible, over_qtr_deduct, na.rm = T),
           qgpa = pts / attmp,
           tot_creds = attmp + nongrd - deduct) %>%
    # removing these for simplifying this querying in favor of transformations/feat-eng later
    # qgpa15 = if_else(qgpa <= 1.5, 1, 0),
    # qgpa20 = if_else(qgpa <= 2, 1, 0)) %>%
    # Removed `probe` in favor of using set of dummy or factor encoding for scholarship_type late
    select(-starts_with('over_qtr'),
           -qtr_grade_points,
           -qtr_graded_attmp,
           -qtr_nongrd_earned,
           -qtr_deductible)

  return(transcript)
}

get.registration <- function(){
  # combine with current quarter from registration
  # need to calculate attempted from the regis_courses current
  reg.courses <- tbl(con, in_schema('sec', 'registration_courses')) %>%
    semi_join(db.eop) %>%
    semi_join(currentq, by = c('regis_yr' = 'gl_regis_year', 'regis_qtr' = 'gl_regis_qtr')) %>%
    # E = course canceled
    # L = withdrawal before quarter
    # filter(!(request_status %in% c('E', 'L')))
    filter(request_status %in% c('A', 'C', 'R'))

  # [TODO] need to include everyone - repeats ok, duplicate not?
  #  ...   and that means figuring out how to include students who w/drew after 10th day?

  # NOTES
  # grading_system != 9,
  # system_key == 777087,  # testing
  #request_status %in% c('A', 'C', 'R')
  # `repeat` == '0' | is.na(`repeat`) | `repeat` == '',)
  # Can we use the first day to selectively preserve records where student dropped?
  # d1 <- currentq %>% select(gl_first_day) %>% collect()

  # reg.courses <- reg.courses %>%
  #   left_join( select(currentq, current_yr, current_qtr, gl_first_day),
  #              by = c('regis_yr' = 'current_yr', 'regis_qtr' = 'current_qtr')) %>%
  #   mutate(to.drop = if_else(request_status == 'D' & request_dt < gl_first_day, 1, 0)) %>%
  #   filter(to.drop == 0) %>%
  #   select(-to.drop,
  #          -gl_first_day)

  calc.attmp <- reg.courses %>%
    group_by(system_key) %>%
    summarize(attmp = sum(credits, na.rm = T)) %>%
    ungroup()
  calc.num.courses <- reg.courses %>%
    group_by(system_key, crs_curric_abbr) %>%
    summarize(num_courses = n_distinct(crs_number)) %>%
    group_by(system_key, add = F) %>%
    summarize(num_courses = sum(num_courses, na.rm = T)) %>%
    ungroup()

  # get.current.quarter.reg <- function(){
  curr.reg <- tbl(con, in_schema('sec', 'registration')) %>%
    semi_join(currentq, by = c('regis_yr' = 'gl_regis_year', 'regis_qtr' = 'gl_regis_qtr')) %>%
    semi_join(db.eop) %>%
    inner_join(calc.attmp) %>%
    inner_join(calc.num.courses) %>%
    mutate(yrq = regis_yr*10 + regis_qtr) %>%
    select(system_key,
           yrq,
           class = regis_class,
           honors_program = regis_hnrs_prg,
           tenth_day_credits,
           attmp,
           num_courses) %>%
    collect()

  return(curr.reg)
}

get.transcript.courses.taken <- function(){

  courses.taken <- tbl(con, in_schema("sec", "transcript_courses_taken")) %>%
    semi_join(db.eop) %>%
    mutate(yrq = tran_yr*10 + tran_qtr) %>%
    filter(yrq >= YRQ_0) %>%
    select(system_key,
           yrq,
           course.index = index1,
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
    collect() %>%
    mutate_if(is.character, trimws) %>%         # doesn't work correctly remotely, unsafe to do other string mutations w/ output before trimming
    # but don't want to build the course code w/ the extra whitespace :\
    # rename(course.index = index1) %>%
    mutate(course = paste(dept_abbrev, course_number, sep = "_"),
           duplicate_indic = as.numeric(duplicate_indic),
           numeric.grade = recode(grade,
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
           numeric.grade = as.numeric(numeric.grade) / 10,
           course.withdraw = if_else(grepl("W", grade), 1, 0),
           course.nogpa = if_else(grade %in% c("", "CR", "H", "HP", "HW", "I", "N", "NC", "NS", "P", "S", "W", "W3", "W4", "W5", "W6", "W7"), 1, 0),
           course.alt.grading = if_else(grade %in% c('CR', 'S', 'NS', 'P', 'HP', 'NC'), 1, 0))

  # combine with current quarter course reg
  curr.qtr <- tbl(con, in_schema('sec', 'registration_courses')) %>%
    semi_join(db.eop) %>%
    semi_join(currentq, by = c('regis_yr' = 'current_yr', 'regis_qtr' = 'current_qtr')) %>%
    filter(request_status %in% c('A', 'C', 'R')) %>%
    collect() %>%
    mutate_if(is.character, trimws) %>%
    mutate(yrq = regis_yr*10 + regis_qtr,
           course.alt.grading = if_else(grading_system > 0, 1, 0),
           duplicate_indic = as.numeric(dup_enroll),
           # replace_na(list('duplicate_indic' = 0)),
           repeat_course = if_else(`repeat` %in% c('1', '2', '3'), T, F),
           course = paste(crs_curric_abbr, crs_number, sep = '_')) %>%
    select(system_key,
           yrq,
           course.index = index1,
           dept_abbrev = crs_curric_abbr,
           course_number = crs_number,
           course_credits = credits,
           course_branch,
           summer_term,
           # grade
           duplicate_indic,
           repeat_course,
           honor_course,
           section_id = crs_section_id,
           course,
           course.alt.grading) %>%
    group_by(system_key, course) %>%
    arrange(system_key, course, section_id) %>%
    filter(row_number() == 1) %>%
    ungroup()


  result <- bind_rows(courses.taken, curr.qtr) %>% distinct() %>% replace_na(list('duplicate_indic' = 0))

  return(result)
}

# DERIVED COURSES-TAKEN FIELDS ----------------------------------------------

create.derived.courses.taken.tscs.data <- function(){
  # combining the following calculations that have `courses.taken` as a dependency and
  # return ~ equally long combined table

  courses.taken <- get.courses.taken()

  # total of fees, courses taken in different gen ed categories
  # vlpa, qsr, etc.
  create.fees.gened.reqs <- function(){
    sched <- tbl(con, in_schema("sec", "time_schedule")) %>%
      mutate(yrq = ts_year*10 + ts_quarter) %>%
      filter(yrq >= YRQ_0) %>%
      select(yrq,
             course_branch,
             dept_abbrev,
             course_no,
             section_id,
             sln,
             current_enroll,
             parent_sln,
             current_enroll,
             writing_crs,
             diversity_crs,
             english_comp,
             qsr,
             vis_lit_perf_arts,
             indiv_society,
             natural_world,
             gen_elective,
             fee_amount) %>%
      collect() %>%
      mutate_if(is.character, trimws) %>%
      mutate(course = paste(dept_abbrev, course_no, sep = "_"))

    result <- courses.taken %>%
      select(system_key, yrq, course, section_id, numeric.grade) %>%
      left_join(sched, by = c('course' = 'course', 'yrq' = 'yrq', 'section_id' = 'section_id')) %>%
      group_by(system_key, yrq) %>%
      summarize(avg.class.size = mean(current_enroll, na.rm = T),
                n.writing = sum(writing_crs, na.rm = T),
                n.diversity = sum(diversity_crs, na.rm = T),
                n.engl_comp = sum(english_comp, na.rm = T),
                n.qsr = sum(qsr, na.rm = T),
                n.vlpa = sum(vis_lit_perf_arts, na.rm = T),
                n.indiv_soc = sum(indiv_society, na.rm = T),
                n.nat_world = sum(natural_world, na.rm = T),
                n.gen_elective = sum(gen_elective, na.rm = T),
                sum.fees = sum(fee_amount, na.rm = T)) %>%
      ungroup()

    # rm(sched)
    return(result)

  }

  # repeats, alternate grading
  create.repeats.w.alt.grading <- function(){
    result <- courses.taken %>%
      arrange(system_key, yrq) %>%
      group_by(system_key, yrq) %>%
      summarize(rep.courses = sum(repeat_course),
                n.w = sum(course.withdraw),
                n.alt.grading = sum(course.alt.grading)) %>%
      group_by(system_key, add = F) %>%
      mutate(csum.rep.courses = cumsum(rep.courses),
             csum.w = cumsum(n.w),
             csum.alt.grading = cumsum(n.alt.grading)) %>%
      ungroup()

    return(result)
  }

  # dept-wise courses, grades, etc (v. wide)
  # agg/reduce to 1 row per student + yrq + dept_abbr
  # then pivot wide
  create.stu.dept.wide <- function(percent = 0.9){
    stu.deptwise.data <- courses.taken %>%
      group_by(system_key, yrq, dept_abbrev) %>%
      summarize(sgrade = sum(numeric.grade, na.rm = T),
                n = n(),
                creds.dept = sum(course_credits, na.rm = T)) %>%
      ungroup() %>%
      arrange(system_key, dept_abbrev, yrq) %>%
      group_by(system_key, dept_abbrev) %>%
      mutate(csum.grade = cumsum(sgrade),
             nclass.dept = cumsum(n),
             cumavg.dept = csum.grade / nclass.dept,
             csum.dept.creds = cumsum(creds.dept)) %>%
      ungroup() %>%
      select(system_key, dept_abbrev, yrq, cumavg.dept, nclass.dept, csum.dept.creds) %>%
      arrange(system_key, yrq, dept_abbrev)

    # Too many columns w/o reducing the number of depts
    top.n.depts <- stu.deptwise.data %>%            # this might break things later if the top_n shift around, safer to use names() later? Hmmmm, have to think about approach to that
      group_by(dept_abbrev) %>%                     # it doesn't matter now but next time it might be nice to drop these column-wise rather than row-wise
      summarize(nd = n_distinct(system_key)) %>%
      ungroup() %>%
      filter(cume_dist(nd) >= percent) %>%
      select(-nd)

    result <- stu.deptwise.data %>%
      inner_join(top.n.depts) %>%
      pivot_wider(.,
                  id_cols = c(system_key, yrq),
                  names_from = dept_abbrev,
                  values_from = c(cumavg.dept, nclass.dept, csum.dept.creds),
                  values_fill = list(cumavg.dept = NA,
                                     nclass.dept = 0,
                                     csum.dept.creds = 0))

    names(result) <- str_replace_all(names(result), " ", "")

    return(result)
  }

  # "stem.courses"      "stem.credits"      "avg.stem.grade"    "csum.stem.courses" "csum.stem.credits"
  create.stem.data <- function(){
    result <- tbl(con, in_schema("EDWPresentation.sec", "dmSCH_dimCurriculumCourse")) %>%
      filter(FederalSTEMInd == "Y") %>%
      select(dept_abbrev = CurriculumCode,
             course_number = CourseNbr,
             course_level = CourseLevelNbr,
             cip = CIPCode) %>%
      distinct() %>%
      collect() %>%
      mutate_if(is.character, trimws) %>%
      mutate(course = paste(dept_abbrev, course_number, sep = "_"),
             is.stem = 1) %>%
      right_join( select(.data = courses.taken, system_key, yrq, course, numeric.grade, course_credits) ) %>%
      arrange(system_key, yrq) %>%
      group_by(system_key, yrq) %>%
      summarize(stem.courses = sum(is.stem, na.rm = T),
                stem.credits = sum(course_credits * is.stem, na.rm = T),
                avg.stem.grade = mean(numeric.grade * is.stem, na.rm = T)) %>%
      group_by(system_key, add = F) %>%
      mutate(csum.stem.courses = cumsum(stem.courses),
             csum.stem.credits = cumsum(stem.credits),
             avg.stem.grade = ifelse(is.nan(avg.stem.grade), NA, avg.stem.grade)) %>%
      ungroup()

    return(result)
  }

  fees <- create.fees.gened.reqs()
  repeats <- create.repeats.w.alt.grading()
  stu.wide <- create.stu.dept.wide()
  stem <- create.stem.data()

  merged.data <- fees %>%
    left_join(repeats) %>%
    left_join(stem) %>%
    left_join(stu.wide)

  return(merged.data)
}


# MAJORS ------------------------------------------------------------------

create.major.data <- function(){

  # for current q
  reg.major <- tbl(con, in_schema('sec', 'registration_regis_col_major')) %>%
    semi_join(db.eop) %>%
    inner_join(currentq, by = c('regis_yr' = 'current_yr', 'regis_qtr' = 'current_qtr')) %>%
    select(system_key,
           yrq = current_yrq,
           index1,
           tran_yr = regis_yr,
           tran_qtr = regis_qtr,
           tran_major_abbr = regis_major_abbr)

  mjr <- tbl(con, in_schema("sec", "transcript_tran_col_major")) %>%
    semi_join(db.eop) %>%
    mutate(yrq = tran_yr*10 + tran_qtr) %>%
    filter(yrq >= YRQ_0) %>%
    select(system_key,
           yrq,
           index1,
           tran_yr,
           tran_qtr,
           tran_major_abbr) %>%
    full_join(reg.major) %>%
    distinct() %>%
    collect() %>%
    mutate_if(is.character, trimws)

  # calc double+ majors
  dual.majors <- mjr %>%
    mutate(dual_major = if_else(index1 == 2, 1, 0)) %>%
    select(system_key, yrq, dual_major) %>%
    group_by(system_key, yrq) %>%
    filter(dual_major == max(dual_major)) %>%
    ungroup()

  stu.major <- mjr %>%
    filter(index1 == 1) %>%
    select(system_key,
           yrq,
           tran_major_abbr) %>%
    # collect() %>%
    group_by(system_key) %>%
    arrange(system_key, yrq) %>%
    mutate(major.change = if_else(tran_major_abbr == lag(tran_major_abbr, order_by = yrq), 0, 1, missing = 0),     # once again, not everything has an MSSQL analogue
           major.change.count = cumsum(major.change)) %>%
    ungroup() %>%
    # add double majors
    inner_join(dual.majors)
  rm(dual.majors)

  stu.major$n.majors <- unlist(tapply(stu.major$tran_major_abbr,
                                      stu.major$system_key,
                                      function(x) { rep(seq_along(rle(x)$lengths), times = rle(x)$lengths) }))

  # this is one way to do it, i initially thought i might make use of the combined strings
  maj.w <- mjr %>% pivot_wider(., id_cols = c('system_key', 'yrq'), names_from = index1, names_prefix = 'maj_', values_from = tran_major_abbr)

  # find: pre-major, 'N MATR', 'T NM', 'B NM'
  maj.w$majors <- apply(maj.w[,3:5], 1, paste0, collapse = " - ")
  maj.w$premajor <- ifelse(grepl("PRE|EPRM", maj.w$majors, ignore.case = T), 1, 0)
  maj.w$nonmatric <- ifelse(grepl("N MATR | T NM | B NM", maj.w$majors), 1, 0)
  maj.w$extpremajor <- ifelse(grepl("EPRM", maj.w$majors, ignore.case = T), 1, 0)
  maj.w <- maj.w %>%
    select(system_key,
           yrq,
           premajor,
           nonmatric,
           extpremajor)

  # Then do the 'classes in major' by making a larger table from major + xf.trs.courses, then reducing it
  maj.courses.taken <- courses.taken %>%
    select(system_key, yrq, dept_abbrev) %>%
    left_join(mjr) %>%
    mutate(course.equals.major = ifelse(tran_major_abbr == dept_abbrev, 1, 0)) %>%
    group_by(system_key, yrq) %>%
    summarize(n.major.courses = sum(course.equals.major)) %>%
    group_by(system_key, .add = F) %>%
    arrange(system_key, yrq) %>%
    mutate(csum.major.courses = cumsum(n.major.courses)) %>%
    ungroup()

  # simplify the result(s) to 1 table
  result <- stu.major %>%
    inner_join(maj.w) %>%
    left_join(maj.courses.taken)

  return(result)
}


# STUDENT_1, EDW, AGE -------------------------------------------------------

create.stu1 <- function(){
  # dimstu <- tbl(con, in_schema('EDWPresentation.sec', 'dimStudent')) %>%
  #   semi_join(db.eop, by = c('SDBSrcSystemKey' = 'system_key'), copy = T) %>%
  #   group_by(SDBSrcSystemKey) %>%
  #   filter(RecordEffEndDttm == max(RecordEffEndDttm, na.rm = T)) %>%
  #   ungroup() %>%
  #   select(SDBSrcSystemKey,
  #          InternationalStudentInd)

  # There are no International Students in this group, don't need the above

  result <- tbl(con, in_schema("sec", "student_1")) %>%
    filter(last_yr_enrolled >= YRQ_0 %/% 10) %>%
    semi_join(db.eop, by = c('system_key' = 'system_key')) %>%
    select(system_key,
           s1_gender,
           uw_netid,
           last_yr_enrolled,
           last_qtr_enrolled,
           admitted_for_yr,
           admitted_for_qtr,
           current_appl_yr,
           current_appl_qtr,
           current_appl_no,
           # high_sch_ceeb_cd, # ethnic_code, hispanic_code,
           child_of_alum,
           running_start,
           # s1_high_satv,
           # s1_high_satm,
           # s1_high_act,
           resident) %>%
    collect()

  # rm(dimstu)

  return(result)
}


# Age, calc from birth_dt + calendar ------------------------------------------------------------------

calc.adjusted.age <- function(){

  bd <- tbl(con, in_schema('sec', 'student_1')) %>%
    select(system_key, birth_dt) %>%
    semi_join(db.eop) %>%
    inner_join( tbl(con, in_schema('sec', 'registration')) %>%
                  select(system_key, regis_yr, regis_qtr) ) %>%
    mutate(table_key = paste0('0', as.character(regis_yr), as.character(regis_qtr), ' '))

  result <- tbl(con, in_schema('sec', 'sys_tbl_39_calendar')) %>%
    select(table_key, tenth_day) %>%
    inner_join(bd) %>%
    collect() %>%
    mutate(age = as.numeric(difftime(tenth_day, birth_dt, units = "days")) / 364.25,
           yrq = regis_yr*10 + regis_qtr) %>%
    select(system_key, yrq, age)

  # rm(bd)
  return(result)
}

# APPLICATIONS ------------------------------------------------------------

# Create application filter  --------------------------------------------------------------

create.application.data <- function(){

  # create an in-db filtering file for the applications
  app.filter <- tbl(con, in_schema("sec", "student_1")) %>%
    inner_join(db.eop) %>%
    select(system_key,
           appl_yr = current_appl_yr,
           appl_qtr = current_appl_qtr,
           appl_no = current_appl_no) %>%
    distinct()

  # ...get (many) --------------------------------------------------------

  gpa <- tbl(con, in_schema('sec', 'sr_adm_appl')) %>%
    semi_join(db.eop) %>%
    group_by(system_key) %>%
    summarize(high_sch_gpa = max(high_sch_gpa, na.rm = T),
              trans_gpa = max(trans_gpa, na.rm = T)) %>%
    ungroup()

  sr.appl <- tbl(con, in_schema("sec", "sr_adm_appl")) %>%
    semi_join(app.filter) %>%
    # filter(appl_type %in% c(1, 2, 4, 6, "R"),
    #        appl_yr >= 2003,
    #        appl_status %in% c(11, 12, 15, 16, 26)) %>%
    select(system_key,
           # trans_gpa,
           conditional,
           provisional,
           with_distinction,
           res_in_question,
           low_family_income,
           # appl_class = class,
           starts_with("hs_"),
           last_school_type,
           -hs_esl_engl) %>%
    inner_join(gpa) %>%
    mutate(trans_gpa = na_if(trans_gpa, 0),
           high_sch_gpa = na_if(high_sch_gpa, 0))

  # best test scores
  appl.hist <- tbl(con, in_schema("sec", "APPLHistApplication")) %>%
    filter(appl_type %in% c(1, 2, 4, 6, "R"),
           appl_yr >= 2003,
           appl_status %in% c(11, 12, 15, 16, 26)) %>%
    select(system_key, appl_yr, appl_qtr, appl_no, best_satr_v, best_satr_m, best_satr_c) %>%
    semi_join(app.filter)

  appl.guardian <- tbl(con, in_schema("sec", "sr_adm_appl_guardian_data")) %>%
    semi_join(app.filter) %>%
    group_by(system_key) %>%
    summarize(guardian.ed.max = max(guardian_ed_level, na.rm = T),
              guardian.ed.min = min(guardian_ed_level, na.rm = T)) %>%
    ungroup()

  # combine applications:
  appl.data <- sr.appl %>%
    # full_join(appl.req.major) %>%
    # full_join(appl.init.major) %>%
    # full_join(appl.income) %>%
    full_join(appl.hist) %>%
    full_join(appl.guardian) %>%
    collect() %>%
    mutate_if(is.character, trimws)

  return(appl.data)

}


# UNMET ADD REQ'S ---------------------------------------------------------

create.unmet.requests <- function(){
  unmet.reqs <- tbl(con, in_schema('sec', 'sr_unmet_request')) %>%
    filter(unmet_yr >= 2019) %>%
    semi_join(db.eop, by = c("unmet_system_key" = "system_key")) %>%
    group_by(unmet_system_key, unmet_yr, unmet_qtr) %>%
    summarize(n.unmet = n()) %>%
    ungroup() %>%
    collect() %>%
    mutate(yrq = unmet_yr*10 + unmet_qtr) %>%
    select(system_key = unmet_system_key, yrq, n.unmet)
}


# LATE REGISTRATIONS ----------------------------------------------------

# including Day 0 as 'late'
create.late.registrations <- function(){

  syscal <- tbl(con, in_schema("sec", "sys_tbl_39_calendar")) %>%
    filter(first_day >= "2020-01-01") %>%                           # arbitrary, some kind of limit is helpful
    select(table_key, first_day, tenth_day, last_day_add) %>%
    collect() %>%
    mutate(yrq = as.numeric(table_key),
           regis_yr = round(yrq %/% 10, 0),        # r floor div operator works incorrectly in sql tbl
           regis_qtr = yrq %% 10)

  # "fixes" still don't eliminate the extremely off dates, so I'll stick with the min and correct the ones I see right now
  regc <- tbl(con, in_schema("sec", "registration_courses")) %>%
    semi_join(db.eop) %>%
    mutate(yrq = regis_yr*10 + regis_qtr) %>%
    filter(yrq >= 20194) %>%
    select(system_key, yrq, add_dt_tuit) %>%
    group_by(system_key, yrq) %>%
    filter(add_dt_tuit == min(add_dt_tuit)) %>% distinct() %>%
    distinct() %>%
    collect() %>%
    left_join(syscal) %>%
    mutate(reg.late.days = as.numeric(difftime(add_dt_tuit, first_day, units = "days"))) %>%
    ungroup()

  regc$reg.late.days[regc$reg.late.days <= -365] <- median(regc$reg.late.days[regc$reg.late.days < 0])
  regc$reg.late.binary <- if_else(regc$reg.late.days >= 0, 1, 0)

  result <- regc %>%
    select(system_key, yrq, reg.late.binary, reg.late.days)

  return(result)

}


# HOLDS -------------------------------------------------------------------

create.holds <- function(from_yrq = YRQ_0){

  # academic calendar
  cal <- tbl(con, in_schema("EDWPresentation.sec", "dimDate")) %>%
    select(yrq = AcademicQtrKeyId, dt = CalendarDate)

  holds <- tbl(con, in_schema("sec", "student_1_hold_information")) %>%
    semi_join(db.eop) %>%
    inner_join(cal, by = c('hold_dt' = 'dt')) %>%
    collect() %>%
    group_by(system_key, yrq) %>%
    summarize(n.holds = n()) %>%
    ungroup() %>%
    mutate(yrq = as.numeric(yrq))
}




# RUN create funcs --------------------------------------------------------------

# Yes, could do more of this server-side
# this is useful for verification

transcript <- create.transcripts()

# [TODO] moving mutate funs from transcript/reg functions
# mutate(cum.pts = cumsum(pts),
  #     cum.attmp = cumsum(attmp),
   #    cum.gpa = cum.pts / cum.attmp) %>%

regis <- get.registration()



courses.taken <- get.courses.taken()
derived.courses.taken.data <- create.derived.courses.taken.tscs.data() %>% select(-starts_with('cumavg.dept'), -starts_with('nclass.dept'), -starts_with('csum.dept'))
majors <- create.major.data()
stu.age <- calc.adjusted.age()
appl.data <- create.application.data() %>% select(-c(appl_yr, appl_qtr, appl_no), -starts_with('best_'))
unmet.reqs <- create.unmet.requests()
late.reg <- create.late.registrations()
holds <- create.holds()

stu1 <- create.stu1() %>%
  # some fixes
  mutate(running_start = ifelse(running_start == 'Y', 1, 0),
         s1_gender = ifelse(s1_gender == 'F', 1, 0)) %>%
  select(-c(last_yr_enrolled,
            last_qtr_enrolled,
            admitted_for_yr,
            admitted_for_qtr,
            current_appl_yr,
            current_appl_qtr,
            current_appl_no))

# COMBINE -----------------------------------------------------------------
mrg.dat <- transcript %>%
  inner_join(stu1) %>%
  left_join(appl.data, by = c('system_key' = 'system_key')) %>%
  left_join(holds) %>%
  left_join(late.reg) %>%
  left_join(majors) %>% rename(major_abbr = tran_major_abbr) %>%  # may want to keep this
  left_join(stu.age) %>%
  left_join(unmet.reqs) %>%
  # left_join(compass.feats) %>%
  # left_join(compass.weekly)%>%
  left_join(derived.courses.taken.data)

# derived features and corrections for 'fresh' data ----------------------------------

# (i <- sapply(mrg.dat, is.character))
# apply(mrg.dat[,i], 2, unique)

# Fix logical vars for modeling
names(mrg.dat)[sapply(mrg.dat, is.logical)]
mrg.dat <- mrg.dat %>%
  mutate_if(is.logical, as.numeric)

# Full time = 12 credits
# add qtr.sequence for 'time' (rough as that is)
# and extended premajor
mrg.dat <- mrg.dat %>%
  mutate(ft = if_else(tenth_day_credits >= 12, 1, 0),
         ft.creds.over = if_else(tenth_day_credits >= 12, tenth_day_credits - 12, 0),
         ext_premajor = if_else(major_abbr == 'EPRMJ', 1, 0)) %>%
  group_by(system_key) %>%
  arrange(system_key, yrq) %>%
  mutate(qtr.seq = row_number()) %>%
  ungroup()


# name check --------------------------------------------------------------
#
# source.names <- names(read_csv('src_r/risk_gpa25/data/data_any-adverse-qtrly-outcome-no-preproc.csv', n_max = 2))
# x <- names(mrg.dat)
#
# print('***?CHECK MODEL DATA NAMES AGAINST OUTPUT FILE?***')
# cbind(setdiff(source.names, x))

# save --------------------------------------------------------------------

# save(mrg.dat, file = paste0('OMAD_adverse_outcome_mod/data/merged-sdb-compass_', Sys.Date(), '.RData'))
# save(courses.taken, file = 'OMAD_adverse_outcome_mod/data/Y-courses-taken.RData')

write_csv(mrg.dat, 'data-intermediate/refac-au20-eop-sdb-data.csv')
