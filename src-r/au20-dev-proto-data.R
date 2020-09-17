# Try building model based on spring 20 data gathered for RAD

library(tidyverse)
library(odbc)
library(dbplyr)

setwd(rstudioapi::getActiveProject())

data_path <- '../../Retention-Analytics-Dashboard/data-raw/spr20/'
(dirlist <- dir(data_path, pattern = '^week-', all.files = T, full.names = T))
# spring20 is empty for week03 b/c data collection started in week02
dirlist <- dirlist[-grep('03', dirlist)]
wks <- seq_along(dirlist)

prov_usr_path <- '~/Google Drive File Stream/My Drive/canvas-data/provisioning_csv_09_Sep_2020_1716420200909-18916-180jrta.csv'
prov_crs_path <- '~/Google Drive File Stream/My Drive/canvas-data/provisioning_csv_14_Sep_2020_1719820200914-5303-al9flx.csv'

# fetch calendar weeks ----------------------------------------------------
# DSN setup
# VPN
# kinit
fetch_cal_wks <- function(){
  con <- dbConnect(odbc(), 'sqlserver01')
  res <- tbl(con, in_schema('EDWPresentation.sec', 'dimDate')) %>%
    filter(AcademicYrQtrCode == 20202) %>%
    select(AcademicYrQtrCode,
           AcademicQtrWeekNum,
           CalendarDate,
           AcademicQtrDayNum) %>%
    distinct() %>%
    collect() %>%
    drop_na(AcademicYrQtrCode)
  return(res)
}

# build data from raw -----------------------------------------------------

# tasks:
# list directories:
#   for each directory:
       # combine the grades and assignments; adding the week #

# TODO: Will need expansion of assignment data to create 'full' dataset (only do this once, not weekly, or accomplish it through merging with partic)

mrg_assgn <- function(dir, wk){
  cal_wks <- fetch_cal_wks()

  anames <- c('canvas_course_id', 'canvas_user_id', 'assgn_id', 'due_at', 'pts_possible', 'assgn_status', 'score')
  atypes <- c('nnnTncn')
  navals = c('NA', 'None', 'none', 'NULL')
  afiles <- list.files(dir, pattern = '^assgn-', full.names = T)

  A <- lapply(afiles, read_delim, delim = '|', col_types = atypes, col_names = anames, na = navals)
  A <- bind_rows(A)
  A <- A %>% distinct(canvas_course_id, canvas_user_id, assgn_id, .keep_all = T) %>%
    mutate(week = wk,
           due_date = lubridate::floor_date(due_at, unit = "day")) %>%
    replace_na(list(pts_possible = 0, score = 0))

  # >> diverge here to make two diff streams for student and course before re-combining them <<
  ## STUDENT
  stu <- A %>%
    group_by(canvas_user_id, canvas_course_id, week) %>%
    summarize(score = sum(score)) %>%
    ungroup()

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

  res <- full_join(stu, crs) %>%
    mutate(score = if_else(score < 0, 0, score)) %>%
    replace_na(list(n_assign = 0,
                    wk_pts_poss = 0))
  return(res)
}


mrg_partic <- function(dir, wk) {
  pnames <- c('canvas_course_id', 'canvas_user_id', 'page_views', 'page_views_level',
              'partic', 'partic_level', 'tot_assgns', 'tot_assgns_on_time',
              'tot_assgn_late', 'tot_assgn_missing', 'tot_assgn_floating')
  ptypes <- c('nnnnnnnnnnn')
  navals = c('NA', 'None', 'none', 'NULL')
  pfiles <- list.files(dir, pattern = '^partic-', full.names = T)

  P <- lapply(pfiles, read_delim, delim = '|', col_types = ptypes, col_names = pnames, na = navals)
  P <- bind_rows(P)
  P <- P %>% distinct(canvas_course_id, canvas_user_id, .keep_all = T) %>%
    drop_na(canvas_user_id) %>%
    mutate(week = wk)

  return(P)
}

# Fetch funs for data from SDB ----------------------------------------

# return transcripts with majors
fetch_trans <- function(){
  con <- dbConnect(odbc(), 'sqlserver01')

  mjr <- tbl(con, in_schema('sec', 'transcript_tran_col_major')) %>%
    filter(tran_yr >= 2015,       # arbitrary, should yield sufficient data for students based on starting point
           index1 == 1) %>%       # this _will_ get proportionally slower with more quarters unless first filtering by
    select(-index1,               # the desired students
           -tran_evening)

  tran <- tbl(con, in_schema('sec', 'transcript')) %>%
    mutate(yrq = tran_yr * 10 + tran_qtr) %>%
    filter(yrq >= 20154,
           class <= 4) %>%
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
           qtr_grade_points,
           qtr_graded_attmp,
           qtr_nongrd_earned,
           qtr_deductible,
           tran_branch,
           tran_college,
           tran_pathway,
           tran_deg_level,
           tran_deg_type,
           tran_major_abbr)

  tran_crs <- tbl(con, in_schema('sec', 'transcript_courses_taken')) %>%
    mutate(yrq = tran_yr * 10 + tran_qtr) %>%
    filter(yrq >= 20154) %>%
    select(system_key,
           yrq,
           index1,
           dept_abbrev,
           course_number,
           section_id,
           course_credits,
           course_branch,
           grade_system,    # 0 = standard; 5 = C/NC; 4 = S/NS; 9 = audit
           grade,
           honor_course,
           incomplete,
           repeat_course,
           writing,
           major_disallowed) %>%
    distinct()

  # TODO finish STEM indicator join

  # add STEM CIP indicator to courses
  stem.courses <- tbl(con, in_schema("EDWPresentation.sec", "dmSCH_dimCurriculumCourse")) %>%
    filter(FederalSTEMInd == "Y") %>%
    select(dept_abbrev = CurriculumCode,
           course_number = CourseNbr) %>%
    distinct() %>%
    mutate(stem_course = 1)

  res <- tran %>% left_join(tran_crs) %>% left_join(stem.courses) %>% collect()

  return(res)
}

# return transcript-courses-taken
# fetch_tran_courses <- function(){
#   con <- dbConnect(odbc(), 'sqlserver01')
#
#   # # create an initial filter w/ eop students
#   # my_filt <- tbl(con, in_schema('sec', 'transcript')) %>%
#   #   filter(special_program %in% c(1, 2, 13, 14, 16, 17, 31, 32, 33)) %>%
#   #   select(system_key)
#
#   tran_crs <- tbl(con, in_schema('sec', 'transcript_courses_taken')) %>%
#     mutate(yrq = tran_yr * 10 + tran_qtr) %>%
#     filter(yrq >= 20154) %>%
#     # semi_join(my_filt) %>%
#     select(system_key,
#            yrq,
#            index1,
#            dept_abbrev,
#            course_number,
#            section_id,
#            course_credits,
#            course_branch,
#            grade_system,    # 0 = standard; 5 = C/NC; 4 = S/NS; 9 = audit
#            grade,
#            honor_course,
#            incomplete,
#            repeat_course,
#            writing,
#            major_disallowed) %>%
#     distinct() %>%
#     collect()
#
#   return(tran_crs)
# }


# RUN above ---------------------------------------------------------------------

# 'loop' assignments the r-ish way, in a surprising reversal of lapply syntax
# lapply 'applies' to the `seq_along` ie make the argument to lapply the index itself, as in a for-loop,
# and pass the value to the merge fun from above.
# Then we can merge, cleanup, aggregate, etc.
a_all <- lapply(seq_along(dirlist), function(i) mrg_assgn(dirlist[[i]], wks[[i]]))
# we could probably avoid the dirlist entirely with map() or mapply()
p_all <- lapply(seq_along(dirlist), function(i) mrg_partic(dirlist[i], wks[i]))

a_all <- bind_rows(a_all)
p_all <- bind_rows(p_all)

# Get/clean canvas provisioning -------------------------------------------

# cur_usrs <- c(a_all$canvas_user_id, p_all$canvas_course_id) %>% unique()
# cur_crss <- c(a_all$canvas_course_id, p_all$canvas_course_id) %>% unique()

# read provisioning; these currently need periodic updates
prov_usrs <- read_csv(prov_usr_path, col_types = 'nc--c------cc') %>% filter(status == 'active', created_by_sis == 'true') %>% select(-status, -created_by_sis)
prov_crss <- read_csv(prov_crs_path, col_types = 'dc-cc--ncc----l') %>% filter(created_by_sis == T, status == 'active') %>% select(-status, -created_by_sis)

# full join assignments and participation, leave NA's
dat <- full_join(a_all, p_all)

# combine provisioning with dat
dat <- dat %>% inner_join(prov_crss) %>% inner_join(prov_usrs)

# need keys from EDW
get_edw_keys <- function(){
  con <- dbConnect(odbc(), 'sqlserver01')
  skeys <- tbl(con, in_schema('sec', 'student_1')) %>%
    select(system_key, uw_netid) %>%
    collect() %>%
    mutate_if(is.character, trimws) %>%
    filter(uw_netid != "")

  return(skeys)
}
edw_keys <- get_edw_keys()

# TODO finish matching w/ provisioning
# link student records:
# 1) edw <-> merged data on netid = login
dat <- dat %>% inner_join(edw_keys, by = c('login_id' = 'uw_netid'))

# link courses:
# 1) turn dat/prov into dept + ### with yrq
xmat <- data.frame(str_split(dat$course_id, '-', simplify = T))
xmat$qtr <- match(xmat$X2, table = c('winter', 'spring', 'summer', 'autumn'))
# ymat <- data.frame(str_split(x$course_id, '-', simplify = T))
dat$dept_abbrev <- xmat$X3
dat$course <- paste(xmat$X3, xmat$X4, sep = "_")
dat$yrq <- as.numeric(paste0(xmat$X1, xmat$qtr))
dat$section <- xmat$X5


rm(xmat, edw_keys)

# Merging and feat engineering --------------------------------------------

# transcript features (it's ok to have 20203+ so long as we're merging to the current file on yrq)
tranraw <- fetch_trans()

ndfilt <- if_else(tranraw$tenth_day_credits == 0 &
                    tranraw$qtr_grade_points == 0 &
                    tranraw$qtr_graded_attmp == 0 &
                    tranraw$qtr_nongrd_earned == 0 &
                    tranraw$qtr_deductible == 0, T, F)
tranraw <- tranraw[!ndfilt,]
rm(ndfilt)

tran <- tranraw %>%
  filter(system_key %in% dat$system_key) %>%
  select(-index1) %>%
  mutate_if(is.character, trimws) %>%
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
         major_course = if_else(dept_abbrev == tran_major_abbr, 1, 0),
         #
         # these are of the 1-record-per-qtr variety
         #
         premajor = if_else(grepl("PRE|EPRMJ", tran_major_abbr) == T, 1, 0),
         qtr_gpa = qtr_grade_points / qtr_graded_attmp,
         ft = if_else(qtr_graded_attmp + qtr_nongrd_earned >= 12 | tenth_day_credits >= 12, 1, 0),
         ft_creds_over = pmax(tenth_day_credits, (qtr_graded_attmp + qtr_nongrd_earned)) - 12,
         eop = if_else(special_program %in% c(1, 2, 13, 14, 16, 17, 31, 32, 33), 1, 0)) %>%
  mutate_if(is_logical, as.numeric)


# windowed transformations
# - going to split transcript/courses again to apply some different calculations
#   -- be careful not to over-count the transcript v. courses data (multiple rows per quarter/student here)
tr1 <- tran %>%
  select(system_key:tran_major_abbr, eop, premajor, qtr_gpa, ft, ft_creds_over) %>%
  distinct() %>%
  group_by(system_key) %>%
  arrange(system_key, yrq) %>%
  mutate(n_qtrs = row_number(yrq),
         cum_premaj_qtrs = cumsum(premajor),
         cum_courses = cumsum(num_courses),
         cum_grdpts = cumsum(qtr_grade_points),
         cum_attmp = cumsum(qtr_graded_attmp),
         cum_nongrd = cumsum(qtr_nongrd_earned),
         cum_gpa = cum_grdpts / cum_attmp,
         cum_honors = cumsum(honors_program),
         mjr_change = if_else(tran_major_abbr == lag(tran_major_abbr), 0, 1),
         mjr_change = replace_na(mjr_change, 0),
         mjr_ch_count = cumsum(mjr_change)) %>%
  ungroup()

# agg the courses data up to 1-row per quarter
trmany <- tran %>%
  select(system_key, yrq, tran_major_abbr, dept_abbrev:major_course) %>%
  # calculate major+class (best we can do w/o degree audit or requirements data)
  # We know that this has different meaning for various majors, in part correlates with how deterministic the
  # degree requirements are for a program
  # mutate(crs_equal_mjr = if_else(dept_abbrev == tran_major_abbr, 1, 0)) %>%
  # aggregate/summarize
  group_by(system_key, yrq) %>%
  summarize(mean_grd = mean(numeric_grade, na.rm = T),
            n_nonpass = sum(nonpass_grade, na.rm = T),
            n_w = sum(w_grade, na.rm = T),
            # n_stem_crs = sum(stem_course),
            n_mjr_crs = sum(major_course, na.rm = T),
            n_mjr_disallowed = sum(major_disallowed, na.rm = T),
            n_writing = sum(writing, na.rm = T),
            n_repeat = sum(repeat_course, na.rm = T)) %>%
  group_by(system_key) %>%
  arrange(system_key, yrq) %>%
  mutate(csum_nonpass = cumsum(n_nonpass),
         csum_w = cumsum(n_w),
         csum_mjr_crs = cumsum(n_mjr_crs),
         csum_mjr_disallowed = cumsum(n_mjr_disallowed),
         csum_writing = cumsum(n_writing),
         csum_repeat = cumsum(n_repeat)) %>%
  ungroup()

# STEM, PREMAJ, MAJ gpa, credit calculations
stem <- tran %>%
  filter(stem_course == 1) %>%
  group_by(system_key, yrq) %>%
  summarize(n_stem_crs = n(),
            mean_stem_grd = mean(numeric_grade, na.rm = T))  # TODO figure out what to do with W's here

pmaj <- tran %>%
  filter(premajor == 1) %>%
  distinct(system_key, yrq, qtr_grade_points, qtr_graded_attmp) %>%
  group_by(system_key) %>%
  arrange(system_key, yrq) %>%
  mutate(pmaj_qtr_gpa = qtr_grade_points / qtr_graded_attmp,
         cum_pmaj_gpa = cumsum(qtr_grade_points) / cumsum(qtr_graded_attmp)) %>%
  select(-qtr_grade_points, -qtr_graded_attmp) %>%
  ungroup()

maj <- tran %>%
  filter(major_course == 1,
         !is.na(numeric_grade)) %>%
  distinct(system_key, yrq, course, course_credits, numeric_grade) %>%
  mutate(gp = numeric_grade * course_credits) %>%
  group_by(system_key, yrq) %>%
  summarize(sum_attmp = sum(course_credits),
            sum_gp = sum(gp),
            maj_qtr_gpa = sum(gp) / sum(course_credits)) %>%
  group_by(system_key) %>%
  mutate(cum_maj_gpa = cumsum(sum_gp) / cumsum(sum_attmp)) %>%
  select(-sum_attmp, -sum_gp) %>%
  ungroup()

##
# Recombine
##
tran <- tr1 %>%
  inner_join(trmany) %>%
  left_join(stem) %>%
  left_join(pmaj) %>%
  left_join(maj) %>%
  replace_na(list(n_stem_crs = 0)) %>%
  group_by(system_key) %>%
  arrange(system_key, yrq) %>%
  mutate(csum_stem_courses = cumsum(n_stem_crs)) %>%
  fill(mean_stem_grd, cum_pmaj_gpa, cum_maj_gpa)

# NAN/NA aren't the same but this simplifies understanding later when trying to use the data b/c NAN values
# create problems with funs or reading data into python which may not be obvious
tran <- data.frame(lapply(tran, function(x) replace(x, is.nan(x), NA)))
rm(tr1, trmany, stem, pmaj, maj)

# remember, we won't know cumulative information for the data we're trying to predict
# - subset transcripts for final dataset

# TODO decide how to deal with this - we won't see most of the 'current' data once registration starts
subtr <- tran %>%
  mutate_at(cum_)
  select(system_key, yrq, eop, resident, class, tenth_day_credits, num_courses, tran_branch, premajor,
         ft, ft_creds_over, n_qtrs, cum_premaj_qtrs, cum_courses,)


# Finalize merge + cleanup -----------------------------------------------------------------


dat <- dat %>%
  left_join( select(tran,
                    system_key,
                    yrq,
                    resident,
                    class,
                    eop,
                    honors_program,
                    tenth_day_credits,
                    num_courses,
                    ) )


# only keep final result (for sourcing file)
# rm(ls()[which(ls) != "dat)]
