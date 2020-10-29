# Try building model based on spring 20 data gathered for RAD

rm(list = ls())

library(tidyverse)
library(odbc)
library(dbplyr)

setwd(rstudioapi::getActiveProject())

# combine intermediate into `dat`
mrg_canvas <- function(){
  files <- list.files('data-intermediate/', pattern = "canvas-[a-z]{2}[0-9]{2}.csv", full.names = T)
  dat <- data.frame()
  for(i in 1:length(files)){
    x <- read_csv(files[i])
    x <- x %>% select(-starts_with('canvas'), -long_name, -section) %>% distinct()
    dat <- bind_rows(dat, x)
  }
  return(dat)
}
dat <- mrg_canvas()

# notes -------------------------------------------------------------------

# student record can be source for _new_ data but since it's continuously updated it shouldn't be used
# for anything involving historical models, training data

# fetch calendar weeks ----------------------------------------------------
# DSN setup
# VPN
# kinit
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

# Helper CTEs for data from SDB ----------------------------------------

# need these keys from EDW
get_edw_keys <- function(){
  con <- dbConnect(odbc(), 'sqlserver01')
  skeys <- tbl(con, in_schema('sec', 'student_1')) %>%
    select(system_key, uw_netid) %>%
    collect() %>%
    mutate_if(is.character, trimws) %>%
    filter(uw_netid != "")

  return(skeys)
}

# registration query ---------------------------------------------
# Registration data is what we need for new predictions in the future
fetch_reg_data <- function(){
  con <- dbConnect(odbc(), 'sqlserver01')

  reg <- tbl(con, in_schema('sec', 'registration')) %>%
    filter(regis_yr == 2020,
           regis_qtr == 4,  # 2 is for training, 4 is for new preds later
           regis_class <= 4) %>%
    mutate(yrq = regis_yr*10 + regis_qtr,
           # eop = if_else(special_program %in% c(1, 2, 13, 14, 16, 17, 31, 32, 33), 1, 0),
           regis_ncr) %>%
    select(system_key,
           yrq,
           # resident,
           # eop,
           special_program,
           regis_ncr,
           regis_class,
           # pc = pending_class,   # TODO - check on which to use - i think pmax of reg_class and transcript class, pending is T/F
           tenth_day_credits,
           current_credits)

  regmaj <- tbl(con, in_schema('sec', 'registration_regis_col_major')) %>%
    filter(regis_yr == 2020,
           regis_qtr == 4,
           index1 == 1) %>%
    mutate(yrq = regis_yr*10 + regis_qtr) %>%
    select(system_key,
           yrq,
           regis_major_abbr)

  # some student 1 fields for most current data since many reg fields will be empty until ...?
  # BUT keep regmaj if not blank
  # PROBLEM: student_1 isn't helpful for training data b/c that would be peeking at future
  s1_fields <- tbl(con, in_schema('sec', 'student_1')) %>%
    filter(class <= 4,
           projected_class <= 4) %>%
    # mutate(eop = if_else(spcl_program %in% c(1, 2, 13, 14, 16, 17, 31, 32, 33), 1, 0)) %>%
    select(system_key,
           uw_netid,
           projected_class,
           spcl_program,     # use this, NOT registration
           class,
           resident,
           # num_holds,
           ncr_code) %>%
           # spp_qtrs_used,
           # tot_grade_points,
           # tot_graded_attmp,
           # tot_nongrd_earn,
           # tot_deductible,
           # tot_lowd_transfer,
           # tot_upd_transfer) %>%
    left_join(
      tbl(con, in_schema('sec', 'student_1_college_major')) %>%
        filter(index1 == 1) %>%
        select(system_key,
               s1_maj = major_abbr))

  reg_data <- reg %>%
    left_join(regmaj) %>%
    distinct() %>%
    left_join(s1_fields) %>%
    collect() %>%
    mutate(eop = if_else(spcl_program %in% c(1, 2, 13, 14, 16, 17, 31, 32, 33) | special_program %in% c(1, 2, 13, 14, 16, 17, 31, 32, 33), 1, 0),
           class = pmax(regis_class, class, projected_class, na.rm = T),
           credits = pmax(tenth_day_credits, current_credits),
           # new_student = if_else(regis_ncr == 1 | ncr_code == 1, 1, 0),
           major_abbr = if_else(is.na(regis_major_abbr), s1_maj, regis_major_abbr),
           resident = if_else(resident %in% c(1, 2), 1, 0)) %>%
    mutate_if(is.character, trimws) %>%
    select(-spcl_program,
           -special_program,
           -regis_class,
           -projected_class,
           -tenth_day_credits,
           -current_credits,
           -regis_ncr,
           -ncr_code,
           -regis_major_abbr,
           -s1_maj)

  # I will count sections as an enrollment, label it to indicate that
  # (don't take the max of index1, in this instance it indicates interactions with the system, not # of courses)
  regcr <- tbl(con, in_schema('sec', 'registration_courses')) %>%
    filter(regis_yr == 2020,
           regis_qtr == 4,
           request_status %in% c('A', 'C', 'R'),
           credits > 0) %>%
    mutate(yrq = regis_yr*10 + regis_qtr,
           rep_course = if_else(`repeat` %in% c('1', '2' ,'3'), 1, 0),
           # course = paste(crs_curric_abbr, crs_number, sep = "_"),
           nonstd_grading = if_else(grading_system > 0, 1, 0)) %>%
    select(system_key,
           yrq,
           nonstd_grading,
           # honor_course,
           # major_disallowed,
           #course,
           crs_curric_abbr,
           crs_number,
           crs_section_id,
           rep_course) %>%
    # join w/ major to calc course == major_abbr
    left_join(regmaj) %>%
    left_join( tbl(con, in_schema('sec', 'student_1_college_major')) %>%
                 filter(index1 == 1) %>%
                 select(system_key,
                        major_abbr)
    ) %>%
    left_join( tbl(con, in_schema("EDWPresentation.sec", "dmSCH_dimCurriculumCourse")) %>%
                 filter(FederalSTEMInd == "Y") %>%
                 select(crs_curric_abbr = CurriculumCode,
                        crs_number = CourseNbr) %>%
                 distinct() %>%
                 mutate(stem_course = 1)
    ) %>%
    collect() %>%
    mutate_if(is.logical, as.numeric) %>%
    mutate_if(is.character, trimws) %>%
    mutate(# course = paste(crs_curric_abbr, crs_number, sep = "_"),
      same_crs_maj = if_else(crs_curric_abbr == regis_major_abbr | crs_curric_abbr == major_abbr, 1, 0)) %>%
    group_by(system_key, yrq) %>%
    summarize(num_courses = n(),
              n_nstd_grd = sum(nonstd_grading),
              # n_honors = sum(honor_course),
              # n_major_disallowed = sum(major_disallowed),
              n_repeat_crs = sum(rep_course),
              # tot_fees = sum(crs_fee_amt),
              n_mjr_crs = sum(same_crs_maj, na.rm = T),
              n_stem_crs = sum(stem_course, na.rm = T)) %>%
    ungroup()

  reg_data <- reg_data %>%
    left_join(regcr) %>%
    mutate_at(vars(resident:n_stem_crs, -major_abbr), replace_na, 0) %>%
    mutate(ft = if_else(credits >= 12, 1, 0),
           ft_creds_over = credits - 12,
           premajor = if_else(grepl("PRE|EPRMJ", major_abbr) == T, 1, 0))
  # replace_na(list(resident = 0,
  #                 num_holds = 0,
  #                 spp_qtrs_used = 0,
  #                 n_mjr_crs = 0,
  #                 n_stem_crs = 0,
  #                 tot_fees = 0,
  #                 n_courses = 0,
  #                 n_nstd_grd = 0,
  #                 n_honors = 0,
  #                 n_major_disallowed = 0,
  #                 n_repeat = 0,
  #                 ))
  return(reg_data)
}

# transcript data ---------------------------------------------------------

# transcripts are for training data and some elements for testing

# return transcripts with majors
# don't include information that we wouldn't be able to know at the _beginning_ of the time period for the training data
# Some can be solved with lag for accumulated queries (and we will want that 20202 data for 20204)
fetch_trans <- function(){

  con <- dbConnect(odbc(), 'sqlserver01')

  ##
  # As-of 09/30 copy_to/dbWriteTable no longer work passing data to SDB for some reason
  ##
  filter_table <- dat %>% select(system_key) %>% distinct()
  # filter_table <- dat %>% select(system_key) %>% distinct() %>% copy_to(con, overwrite = T)

  mjr <- tbl(con, in_schema('sec', 'transcript_tran_col_major')) %>%
    filter(tran_yr >= 2015,       # arbitrary, should yield sufficient data for students based on starting point
           index1 == 1) %>%       # this _will_ get proportionally slower with more quarters unless first filtering by
    select(-index1,               # the desired students
           -tran_evening)

  tran <- tbl(con, in_schema('sec', 'transcript')) %>%
    mutate(yrq = tran_yr * 10 + tran_qtr) %>%
    filter(yrq >= 20154,
           !(tenth_day_credits == 0 &
               qtr_grade_points == 0 &
               qtr_graded_attmp == 0 &
               qtr_nongrd_earned == 0 &
               qtr_deductible == 0)) %>%
    ##
    # semi_join(filter_table, copy = T) %>%
    ##
    left_join(mjr) %>%
    select(system_key,
           yrq,
           resident,
           class,
           special_program,
           honors_program,
           tenth_day_credits,
           num_courses,
           add_to_cum,
           qtr_grade_points,   # need to lag
           qtr_graded_attmp,   # ...
           qtr_nongrd_earned,  # ...
           qtr_deductible,     # ...
           # tran_branch,
           # tran_college,
           # tran_pathway,
           # tran_deg_level,
           # tran_deg_type,
           tran_major_abbr) %>%
    mutate(resident = if_else(resident %in% c(1, 2), 1, 0)) %>%
    collect() %>%
    ##
    semi_join(filter_table) %>%
    ##
    mutate_if(is.character, trimws) %>%
    mutate_if(is_logical, as.numeric) %>%
    mutate(resident = if_else(resident %in% c(1, 2), 1, 0),
           premajor = if_else(grepl("PRE|EPRMJ", tran_major_abbr) == T, 1, 0),
           qtr_gpa = qtr_grade_points / qtr_graded_attmp,
           ft = if_else(qtr_graded_attmp + qtr_nongrd_earned >= 12 | tenth_day_credits >= 12, 1, 0),
           ft_creds_over = pmax(tenth_day_credits, (qtr_graded_attmp + qtr_nongrd_earned)) - 12,
           eop = if_else(special_program %in% c(1, 2, 13, 14, 16, 17, 31, 32, 33), 1, 0)) %>%
    # windowed transformations
    group_by(system_key) %>%
    arrange(system_key, yrq) %>%
    mutate(n_qtrs = row_number(yrq),
           csum_premaj_qtrs = cumsum(premajor),
           csum_courses = cumsum(num_courses),
           csum_grdpts = cumsum(qtr_grade_points),
           csum_attmp = cumsum(qtr_graded_attmp),
           csum_nongrd = cumsum(qtr_nongrd_earned),
           cum_gpa = csum_grdpts / csum_attmp,
           csum_honors = cumsum(honors_program),
           mjr_change = if_else(tran_major_abbr == lag(tran_major_abbr), 0, 1),
           mjr_change = replace_na(mjr_change, 0),
           mjr_ch_count = cumsum(mjr_change)) %>%
    ungroup()




  # transcript courses query, aggr ------------------------------------------

  # add STEM CIP indicator to courses
  stem_courses <- tbl(con, in_schema("EDWPresentation.sec", "dmSCH_dimCurriculumCourse")) %>%
    filter(FederalSTEMInd == "Y") %>%
    select(dept_abbrev = CurriculumCode,
           course_number = CourseNbr) %>%
    distinct() %>%
    mutate(stem_course = 1)

  tran_crs <- tbl(con, in_schema('sec', 'transcript_courses_taken')) %>%
    mutate(yrq = tran_yr * 10 + tran_qtr) %>%
    filter(yrq >= 20154) %>%
    ##
    # semi_join(filter_table) %>%
    ##
    left_join(stem_courses) %>%
    left_join(mjr) %>%
    select(system_key,
           yrq,
           index1,
           dept_abbrev,
           course_number,
           section_id,
           course_credits,
           course_branch,
           grade_system,    # 0 = standard; 5 = C/NC; 4 = S/NS; 9 = audit
           grade,           # lag
           honor_course,
           incomplete,      # lag
           repeat_course,
           writing,         # lag - this doesn't show in timely fashion in registration data so we won't know
           major_disallowed,
           stem_course,
           tran_major_abbr) %>%
    distinct() %>%
    collect() %>%
    ##
    semi_join(filter_table) %>%
    ##
    mutate_if(is.character, trimws) %>%
    mutate_if(is_logical, as.numeric) %>%
    replace_na(list(stem_course = 0)) %>%
    mutate(course = paste(dept_abbrev, course_number, sep = "_"),
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
           nonpass_grade = if_else(grade %in% c('HW', 'I', 'NC', 'NS', 'W', 'W3', 'W4', 'W5', 'W6', 'W7'), 1, 0),
           w_grade = if_else(grepl("W", grade) == T, 1, 0),
           major_course = if_else(dept_abbrev == tran_major_abbr, 1, 0))


  # agg the courses data up to 1-row per quarter
  agg_tran_crs <- tran_crs %>%
    # calculate major+class (best we can do w/o degree audit or requirements data)
    # We know that this has different meaning for various majors, in part correlates with how deterministic the
    # degree requirements are for a program
    group_by(system_key, yrq) %>%
    # summarize by n __ in quarter, then gen cumsums
    summarize(mean_grd = mean(numeric_grade, na.rm = T),
              n_nonpass = sum(nonpass_grade, na.rm = T),
              n_w = sum(w_grade, na.rm = T),
              n_mjr_crs = sum(major_course, na.rm = T),
              n_mjr_disallowed = sum(major_disallowed, na.rm = T),
              n_writing = sum(writing, na.rm = T),
              n_repeat_crs = sum(repeat_course, na.rm = T)) %>%
    group_by(system_key) %>%
    arrange(system_key, yrq) %>%
    # Q: keep n_ or just csum?
    mutate(csum_nonpass = cumsum(n_nonpass),
           csum_w = cumsum(n_w),
           csum_mjr_crs = cumsum(n_mjr_crs),
           csum_mjr_disallowed = cumsum(n_mjr_disallowed),
           csum_writing = cumsum(n_writing),
           csum_repeat = cumsum(n_repeat_crs)) %>%
    ungroup()

  # TODO
  # STEM, PREMAJ, MAJ gpa, credit calculations
  stem <- tran_crs %>%
    filter(stem_course == 1,
           !is.na(numeric_grade)) %>%
    mutate(stem_pts = numeric_grade * course_credits) %>%
    group_by(system_key, yrq) %>%
    summarize(n_stem_crs = n(),
              stem_credits = sum(course_credits, na.rm = T),
              stem_pts = sum(stem_pts),
              stem_gpa = stem_pts / stem_credits) %>%
    group_by(system_key) %>%
    arrange(system_key, yrq) %>%
    mutate(csum_stem_crs = cumsum(n_stem_crs),
           csum_stem_creds = cumsum(stem_credits),
           cum_stem_gpa = cumsum(stem_pts) / cumsum(stem_credits))

  pmaj <- tran %>%
    filter(premajor == 1) %>%
    distinct(system_key, yrq, qtr_grade_points, qtr_graded_attmp) %>%
    group_by(system_key) %>%
    arrange(system_key, yrq) %>%
    mutate(pmaj_qtr_gpa = qtr_grade_points / qtr_graded_attmp,
           cum_pmaj_gpa = cumsum(qtr_grade_points) / cumsum(qtr_graded_attmp)) %>%
    select(-qtr_grade_points, -qtr_graded_attmp) %>%
    ungroup()

  maj <- tran_crs %>%
    filter(major_course == 1,
           !is.na(numeric_grade)) %>%
    distinct(system_key, yrq, course, course_credits, numeric_grade) %>%
    mutate(gp = numeric_grade * course_credits) %>%
    group_by(system_key, yrq) %>%
    summarize(sum_attmp = sum(course_credits),
              sum_gp = sum(gp),
              maj_qtr_gpa = sum(gp) / sum(course_credits)) %>%
    group_by(system_key) %>%
    arrange(system_key, yrq) %>%
    mutate(cum_maj_gpa = cumsum(sum_gp) / cumsum(sum_attmp)) %>%
    select(-sum_attmp, -sum_gp) %>%
    ungroup()

  ##
  # Recombine
  ##
  tran <- tran %>%
    inner_join(agg_tran_crs) %>%
    left_join(stem) %>%
    left_join(pmaj) %>%
    left_join(maj) %>%
    replace_na(list(n_stem_crs = 0)) %>%
    group_by(system_key) %>%
    arrange(system_key, yrq) %>%
    fill(stem_gpa, cum_pmaj_gpa, cum_maj_gpa, cum_stem_gpa)

  # NAN/NA aren't the same but this simplifies understanding later when trying to use the data b/c NAN values
  # create problems with funs or reading data into python which may not be obvious
  tran <- data.frame(lapply(tran, function(x) replace(x, is.nan(x), NA)))

  train_target <- tran_crs %>%
    # TODO fix hard coding here
    # filter(yrq == 20202) %>%
    mutate(target = case_when(
      grade == '' ~ 1,
      nonpass_grade == 1 ~ 1,
      numeric_grade <= 2.5 ~ 1,
      T ~ 0),
      std_grading = if_else(grade_system == 0, 1, 0)) %>%
    select(system_key,          # also keep some relevant information about a course
           yrq,
           course,
           numeric_grade,       # may want this as an optional target
           target,
           std_grading,
           stem_course,
           writing,
           repeat_course,
           major_course)

  res <- list('tran' = tran, 'train_target' = train_target)
  return(res)
}


# >> run data and transcript funs << --------------------------------------------

tran <- fetch_trans()             # reminder - this is a named list
# TODO see above re: this data
# reg_data for _new_ preds needs to have names standardized with transcript file fields
# class might be a problem b/c of pending updates that result in a lot of 0's
reg_data <- fetch_reg_data()

## TEST/CHECK
x <- names(tran$tran)
y <- names(reg_data)
setdiff(y, x)

# We need to normalize some  names for model training
tran$tran <- tran$tran %>% rename(major_abbr = tran_major_abbr, credits = tenth_day_credits)
reg_data <- reg_data %>% select(-n_nstd_grd, -uw_netid)

## TEST/CHECK
x <- names(tran$tran)
y <- names(reg_data)
setdiff(y, x)


# Merging, FE w/ transcript data --------------------------------------------

# need to exclude transcript information that wouldn't be known, either by lagging before merging or by being careful about removing
# vars from queries






# basically on day 1 we have resident, class, honors, and special program code (eop)
# so let's assume we can use transcript to calculate _all_ of the above for whoever we need
# then to predict t_n we'll use t_n-1 b/c when a new quarter begins we won't have current data
# See: registration and regis major
#  - cf. tenth_day in registration (will be whatever's current)
#  - regis courses (for qtr) > repeats, n courses, writing, major disallowed (filter on request status),
#    courses matching major, stem courses
# to sidestep that issue, lag the yrq by 1 before merging with dat (what I should do is re-write this so that it uses REG for
# those fields when building the training data)

# split on the vars we can keep that will go with the new q's data
# TODO finish this
# TODO write up the mapping so that the `tran` can combine with `reg`
# TODO will also need course info for new quarter

lagvars <- vars(qtr_gpa,
                starts_with('cum_'),
                # mjr_change,
                # mjr_ch_count,
                # mean_grd,
                starts_with('csum_'))
trans_training_data <- tran$tran %>%
  select(system_key,
         yrq,
         eop,
         resident,
         class,
         honors_program,
         credits = tenth_day_credits,     # renaming for compatibility with reg data
         num_courses,
         n_qtrs,
         premajor,
         ft,
         ft_creds_over,
         !!!lagvars) %>%     # note: need to unquote list created by `vars` w/in select here but see below
  group_by(system_key) %>%
  arrange(system_key, yrq) %>%
  mutate_at(lagvars, lag)   # however, mutate_at isn't bothered by the expression created by vars. This is all consistent if somewhat opaque and confusing

# Finalize merge + cleanup -----------------------------------------------------------------

out_dat <- dat %>%
  left_join(trans_training_data) %>%
  left_join(tran$train_target %>% distinct(system_key, yrq, course, .keep_all = T)) %>%
  # bug(?) fix
  group_by(system_key, yrq) %>%
  mutate(num_courses = n_distinct(short_name)) %>%
  ungroup()
# out_dat %>% filter(week <= 5) %>% write_csv('data-prepped/ffv2-scratch-py-data.csv')


# TODO aggregate out_dat to make a model for overall outcome as before, not class-level -- then fetch registration class data so that we can do both models
# # only keep final result (for sourcing file)
# rm(ls()[which(ls() != 'dat')])


time_invariant <- out_dat %>%
  distinct(system_key, yrq, week, eop, resident, class, credits, num_courses, premajor, ft, ft_creds_over, cum_gpa)

needs_aggregation <- out_dat %>%
  filter(week <= 4) %>%
  group_by(system_key, week) %>%
  # n_assign is n_distinct for a course in a week
  summarize(across(c(score, n_assign, wk_pts_poss, page_views, page_views_level, partic, partic_level),
                   .fns = list(tot = ~sum(.x, na.rm = T), mean = ~mean(.x, na.rm = T)),
                   .names = '{.fn}_{.col}'),
            across(starts_with('tot_'), sum),
            across(c(std_grading, stem_course, repeat_course, major_course, target), ~sum(.x, na.rm = T), .names = 'n_{.col}')) %>%
  ungroup()

aggr_training_data <- time_invariant %>%
  inner_join(needs_aggregation) %>%
  mutate(target = if_else(n_target > 0, 1, 0),
         n_nstd_grd = num_courses - n_std_grading) %>%
  select(-n_std_grading)

aggr_training_data %>% select(-n_target) %>% write_csv('data-prepped/ffv2-scratch-py-aggregated-data.csv')



# NEW prediction data -----------------------------------------------------
# TODO import new CANVAS data
# we won't have numeric_grade or target of course but also, if we're going to do it by course we need
# std_grading, stem_course, writing, repeat_course, major_course
new_pred_data <- reg_data %>%
  select(-major_abbr, -uw_netid) %>%
  left_join( tran %>% select(system_key, yrq))
  # left_join( tran$tran %>%
  #              filter(yrq == 20202) %>%
  #              select(system_key, n_qtrs, !!!lagvars),
  #            by = c('system_key' = 'system_key'))

setdiff(names(new_pred_data), names(aggr_training_data))
setdiff(names(aggr_training_data), names(new_pred_data))

au20path <- '../../Retention-Analytics-Dashboard/data-raw/au20'
(au20dirlist <- dir(au20path, pattern = '^week-', all.files = T, full.names = T))
au20wks <- seq_along(au20dirlist)
au20_assgn <- lapply(seq_along(au20dirlist), function(i) mrg_assgn(au20dirlist[[i]], au20wks[i]))
au20_part <- lapply(seq_along(au20dirlist), function(i) mrg_partic(au20dirlist[[i]], au20wks[i]))
au20_a <- bind_rows(au20_assgn[unlist(lapply(au20_assgn, is.data.frame))])
au20_p <- bind_rows(au20_part[unlist(lapply(au20_part, is.data.frame))])
au20_data <- full_join(au20_a, au20_p)
# now do that same calc/aggregation for this
au20_data <- au20_data %>%
  filter(week <= 4) %>%
  group_by(canvas_user_id, week) %>%
  # n_assign is n_distinct for a course in a week
  summarize(across(c(score, n_assign, wk_pts_poss, page_views, page_views_level, partic, partic_level),
                   .fns = list(tot = ~sum(.x, na.rm = T), mean = ~mean(.x, na.rm = T)),
                   .names = '{.fn}_{.col}'),
            across(starts_with('tot_'), ~sum(.x, na.rm = T))) %>%
  ungroup()
# TODO augment with system key so I can combine
