## Script to create data for generating a new quarter's predictions
## It should start by identifying the students and registration quarter


# Setup -------------------------------------------------------------------
rm(list = ls())

library(tidyverse)
library(dbplyr)
library(odbc)
library(edwHelpers)


setwd(rstudioapi::getActiveProject())

# __globals ---------------------------------------------------------------
# data quarter for predictions, e.g. year+quarter for registration
REG_YRQ <- 20211
reg_y <- get.y(REG_YRQ)
reg_q <- get.q(REG_YRQ)
# floor for historical data
YRQ_0 <- 20154
# convenience var
EOP_CODES <- c(1, 2, 13, 14, 16, 17, 31, 32, 33)


# SDB DATA ----------------------------------------------------------------
con <- dbConnect(odbc(), 'sqlserver01')


# __filtering query for student list --------------------------------------
all_elig <- tbl(con, in_schema('EDWPresentation.sec', 'ACAD_dsetEligibleToRegister')) %>%
  filter(EligibleQtrCode == REG_YRQ) %>%
  select(SDBSrcSystemKey)

isso_stus <- tbl(con, in_schema('sec', 'student_1_college_major')) %>%
  filter(major_abbr == "ISS O") %>%
  semi_join(all_elig, by = c('system_key' = 'SDBSrcSystemKey')) %>%
  mutate(isso = 1) %>%
  select(system_key, isso)

eop_stus <- all_elig %>%
  semi_join( tbl(con, in_schema('sec', 'student_1')) %>%
               filter(spcl_program %in% EOP_CODES) %>%
               select(system_key),
             by = c('SDBSrcSystemKey' = 'system_key')) %>%
  mutate(eop = 1)

# Combine
stu_cte <- isso_stus %>% full_join(eop_stus, by = c('system_key' = 'SDBSrcSystemKey'))



# __transcripts -----------------------------------------------------------
transcripts <- tbl(con, in_schema("sec", "transcript")) %>%
  semi_join(stu_cte) %>%
  mutate(yrq = tran_yr*10 + tran_qtr) %>%
  filter(yrq >= YRQ_0,
         yrq <= REG_YRQ) %>%   # there should not be transcripts for REG_YRQ generally speaking
  select(system_key,
         yrq,
         class,
         honors_program,
         tenth_day_credits,
         scholarship_type,
         # yearly_honor_type,
         num_ind_study,
         num_courses,
         qtr_grade_points,   # I haven't found cases where the overrides are non-zero and different than the normal columns
         qtr_graded_attmp,
         qtr_nongrd_earned,
         qtr_deductible)

# __registration (current) ------------------------------------------------
# join the registered courses to the time schedule to create calculated fields
reg_courses_cte <- tbl(con, in_schema('sec', 'registration_courses')) %>%
  filter(regis_yr == reg_y,
         regis_qtr == reg_q,
         request_status %in% c('A', 'C', 'R')) %>%
  semi_join(stu_cte) %>%
  inner_join( tbl(con, in_schema('sec', 'time_schedule')),
              by = c('regis_yr' = 'ts_year',
                     'regis_qtr' = 'ts_quarter',
                     'sln' = 'sln',
                     'course_branch' = 'course_branch')) %>%
  group_by(system_key) %>%
  filter(credits > 0) %>%
  summarize(num_courses = n_distinct(sln),
            avg_class_size = mean(current_enroll, na.rm = T),
            n_writing = sum(as.numeric(writing_crs), na.rm = T),
            n_diversity = sum(as.numeric(diversity_crs), na.rm = T),
            n_engl_comp = sum(as.numeric(english_comp), na.rm = T),
            n_qsr = sum(as.numeric(qsr), na.rm = T),
            n_vlpa = sum(as.numeric(vis_lit_perf_arts), na.rm = T),
            n_indiv_soc = sum(as.numeric(indiv_society), na.rm = T),
            n_nat_world = sum(as.numeric(natural_world), na.rm = T),
            n_gen_elective = sum(as.numeric(gen_elective), na.rm = T),
            sum_fees = sum(crs_fee_amt, na.rm = T))

regis <- tbl(con, in_schema('sec', 'registration')) %>%
  filter(regis_yr == reg_y,
         regis_qtr == reg_q) %>%
  semi_join(stu_cte) %>%
  select(system_key,
         resident,
         regis_hnrs_prg,
         regis_class,
         tenth_day_credits,
         current_credits) %>%
  left_join(reg_courses_cte)


# __transcript courses; derived taken ----------------------------------------------
# In the original implementation there were a number of aggregations/summations derived
# from transcript data.

# 1) transcript courses taken
courses_taken <- tbl(con, in_schema("sec", "transcript_courses_taken")) %>%
  mutate(yrq = tran_yr*10 + tran_qtr) %>%
  filter(yrq >= YRQ_0,
         yrq <= REG_YRQ) %>%     # again, there really shouldn't be any but one never knows
  semi_join(stu_cte) %>%
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
  mutate_if(is.character, trimws) %>%         # doesn't work remotely
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

# 2) repeat courses and alt grading
repeats_w_alt_grading <- courses_taken %>%
  arrange(system_key, yrq) %>%
  group_by(system_key) %>%
  summarize(csum_rep_courses = sum(repeat_course, na.rm = T),
            csum_w = sum(course.withdraw, na.rm = T),
            csum_alt_grading = sum(course.alt.grading, na.rm = T))

# 3) stem courses
stem_course_data <- tbl(con, in_schema("EDWPresentation.sec", "dmSCH_dimCurriculumCourse")) %>%
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
    right_join( select(.data = courses_taken, system_key, yrq, course, numeric.grade, course_credits) ) %>%
    arrange(system_key, yrq) %>%
    group_by(system_key) %>%
    summarize(csum_stem_courses = sum(is.stem, na.rm = T),
              csum_stem_credits = sum(course_credits * is.stem, na.rm = T),
              avg_stem_grade = mean(numeric.grade * is.stem, na.rm = T)) %>%
  ungroup()

#  avg.stem.grade = ifelse(is.nan(avg.stem.grade), NA, avg.stem.grade)) %>%



# name fixes/transformations/feat engineering ----------------------------------------
#
# mutate(pts = pmax(qtr_grade_points, over_qtr_grade_pt, na.rm = T),           # NB mssql doesn't support pmax
#        attmp = pmax(qtr_graded_attmp, over_qtr_grade_at, na.rm = T),
#        nongrd = pmax(qtr_nongrd_earned, over_qtr_nongrd, na.rm = T),
#        deduct = pmax(qtr_deductible, over_qtr_deduct, na.rm = T),
#        qgpa = pts / attmp,
#        tot_creds = attmp + nongrd - deduct) %>%

# full time
# f/t creds over

# 'attmp' credits from registration  << is this different at all from tenth day in this table?
# use 'current_credits'? not perfectly correlated but pretty close...

# honors_program = regis_hnrs_prg,
# class = regis_class,
