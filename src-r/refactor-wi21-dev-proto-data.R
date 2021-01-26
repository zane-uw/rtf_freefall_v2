
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
con <- dbConnect(odbc(), 'sqlserver01', UID = config::get('sdb')$uid, PWD = config::get('sdb')$pwd)


# Utility table expressions -------------------------------------------------------------------
# calendar
# current year, quarter
# filtering query for EOP students
currentq <- tbl(con, in_schema("sec", "sdbdb01")) %>%
  select(current_yr, current_qtr, gl_first_day, gl_regis_year, gl_regis_qtr) %>%
  collect() %>%
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


# transcript -------------------------------------------------------------
transcript <- tbl(con, in_schema("sec", "transcript")) %>%
  semi_join(db.eop) %>%
  mutate(yrq = tran_yr*10 + tran_qtr) %>%
  filter(yrq >= YRQ_0,
         yrq < local(currentq$current_yrq)) %>%       # tran_qtr != 3, add_to_cum == 1
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
         over_qtr_deduct)

# courses taken -----------------------------------------------------------
tr_courses_taken <- tbl(con, in_schema("sec", "transcript_courses_taken")) %>%
  mutate(yrq = tran_yr*10 + tran_qtr) %>%
  filter(yrq >= YRQ_0,
         yrq < local(currentq$current_yrq)) %>%
  semi_join(db.eop) %>%
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
  # add fees, gen ed reqs, etc. from time schedule
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
             fee_amount),
    by = c('dept_abbrev' = 'dept_abbrev',
           'course_number' = 'course_no',
           'yrq' = 'yrq',
           'section_id' = 'section_id',
           'course_branch' = 'course_branch')
  )

# majors, dual majors ------------------------------------------------------------------
mjr <- tbl(con, in_schema("sec", "transcript_tran_col_major")) %>%
  semi_join(db.eop) %>%
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

# Unmet course requests ---------------------------------------------------
unmet_reqs <- tbl(con, in_schema('sec', 'sr_unmet_request')) %>%
  semi_join(db.eop, by = c('unmet_system_key' = 'system_key')) %>%
  mutate(yrq = unmet_yr*10 + unmet_qtr) %>%
  filter(yrq >= YRQ_0,
         yrq < local(currentq$current_yrq)) %>%
  group_by(unmet_system_key, yrq) %>%
  summarize(n_unmet = n()) %>%
  ungroup() %>%
  select(system_key = unmet_system_key, yrq, n_unmet)

