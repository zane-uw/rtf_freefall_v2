
# Generate an aggregate Y variable for modeling ---------------------------

library(tidyverse)
library(odbc)
library(dbplyr)

make_aggr_yvar <- function(){

  con <- dbConnect(odbc(), 'sqlserver01')

  eop <- tbl(con, in_schema('sec', 'student_1')) %>%
    filter(spcl_program %in% c(1, 2, 13, 14, 16, 17, 31, 32, 33)) %>%
    select(system_key) %>%
    full_join( tbl(con, in_schema('sec', 'transcript')) %>%
                 filter(special_program %in% c(1, 2, 13, 14, 16, 17, 31, 32, 33)) %>%
                 select(system_key) %>%
                 distinct()
               ) %>%
    distinct()

  course_grades <- tbl(con, in_schema('sec', 'transcript_courses_taken')) %>%
    mutate(yrq = tran_yr*10 + tran_qtr) %>%
    filter(yrq >= 20194) %>%
    semi_join(eop) %>%
    select(system_key,
           yrq,
           index1,
           grade) %>%
    collect() %>%
    mutate_if(is.character, trimws) %>%
    mutate(numeric_grade = recode(grade,
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
           tempy = if_else(numeric_grade <= 2.5 | course_withdraw == 1 | grade %in% c('HW', "I", "NC", "NS"), 1, 0)) %>%
    replace_na(list(tempy = 0)) %>%
    group_by(system_key, yrq) %>%
    summarize(y = max(tempy))

  return(course_grades)
}

make_aggr_yvar() %>% write_csv('data-prepped/eop-aggr-yvar.csv')
