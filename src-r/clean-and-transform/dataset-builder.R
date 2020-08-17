# compile a data set from:
# 001_fetch...
# input canvas output merged
rm(list = ls())
gc()

library(tidyverse)

source('src-r/clean-and-transform/input-canvas-output-merged-intermediate.R') # add lms_wkly_data to env
source('src-r/001_fetch-extract-data.R')

# convenience function to write out txt's of character and numeric variables for python handling
write_vartypes <- function(df){
  n <- names(df)
  t <- sapply(df, typeof)
  # print(n[t == 'character'])
  # print(n[t != 'character'])
  write_lines(n[t == 'character'], path = 'data-prepped/cat-var-list.txt')
  write_lines(n[t != 'character'], path = 'data-prepped/num-var-list.txt')
}

# get + merge from sdb ----------------------------------------------------

db.linker <- link_enrollments() %>%
  inner_join(link_students()) %>%
  distinct(canvas_course_id,
           canvas_user_id,
           system_key,
           uw_netid,
           yrq,
           course,
           dept_abbrev,
           course_no)

transcript_grades <- get_tran_courses() %>%
  collect() %>%
  mutate(yrq = tran_yr * 10 + tran_qtr,
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
         duplicate_indic = as.numeric(duplicate_indic),
         numeric.grade = as.numeric(numeric.grade) / 10) %>%
  mutate_if(is_logical, as.numeric) %>%
  mutate_if(is.character, trimws)

# transcript_qtrly <- get_transcript() %>% collect() %>% mutate_if(is.character, trimws)


# create scratch data for python test -------------------------------------

adv.grades <- c('I', 'W', '*W', 'W3', 'W4', 'W5', 'W6', 'W7', 'HW', 'NC', 'NS', 'LP')
yvar <- transcript_grades %>%
  mutate(adv = if_else(numeric.grade <= 2.5 | grade %in% adv.grades | incomplete == 1, 1, 0),
         course = paste(dept_abbrev, course_number, sep = '_')) %>%
  select(Y = adv,
         system_key,
         course,
         yrq,
         course_credits,
         course_branch,
         honor_course,
         repeat_course,
         writing)

scr <- lms_wkly_data %>%
  select(yrq, user_id, course_id, ends_with(c('wk01', 'wk02', 'wk03'))) %>%
  mutate_all(replace_na, 0) %>%
  inner_join( select(db.linker, canvas_course_id, canvas_user_id, system_key, yrq, course),
             by = c('user_id' = 'canvas_user_id',
                    'course_id' = 'canvas_course_id',
                    'yrq' = 'yrq')) %>%
  inner_join(yvar)

# write_vartypes(scr)
# write_csv(scr, 'data-prepped/scratch-py-data.csv')
