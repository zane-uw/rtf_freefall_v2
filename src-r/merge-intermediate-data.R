rm(list = ls())

library(tidyverse)

setwd(rstudioapi::getActiveProject())


# combine SDB and canvas --------------------------------------------------
can_au20 <- read_csv('data-intermediate/canvas-au20.csv')
can_sp20 <- read_csv('data-intermediate/canvas-sp20.csv')
can_su20 <- read_csv('data-intermediate/canvas-su20.csv')
can_dat <- bind_rows(can_au20, can_sp20, can_su20)
rm(can_au20, can_sp20, can_su20)

# Need to split the canvas data up based on what we can/can't know and lag a subset of variables
# First filtering by the canvas data to reduce some overhead
sdb_dat <- read_csv('data-intermediate/refac-au20-eop-sdb-data.csv') %>%
  filter(system_key %in% unique(can_dat$system_key)) %>%
  select(-num_ind_study)

# Then reduce canvas to EOP students
# This current setup requires aggregating the canvas data
can_dat <- can_dat %>%
  semi_join(sdb_dat, by = c('system_key' = 'system_key')) %>%
  group_by(system_key, user_id, yrq, week) %>%
  summarize_at(vars(page_views, page_views_level, partic, partic_level, tot_assgns, tot_assgns_on_time, tot_assgn_late,
                    tot_assgn_missing, tot_assgn_floating, score, n_assign, wk_pts_poss), mean)



# Gen lags and subset data ------------------------------------------------

# now create lag vars and subset years
lvars <- c('honors_program',
           'scholarship_type',
           'yearly_honor_type',
           'pts',
           'nongrd',
           'deduct',
           'qgpa',
           'tot_creds',
           'qgpa15',
           'qgpa20',
           'probe',
           'cum.pts',
           'cum.gpa',
           'n.w',
           'csum.w',
           'avg.stem.grade')
sdb_dat <- sdb_dat %>%
  group_by(system_key) %>%
  arrange(system_key, yrq) %>%
  mutate(across(.cols = lvars, .fns = lag, default = 0, .names = "lag_{.col}")) %>%
  ungroup() %>%
  filter(yrq %in% unique(can_dat$yrq)) %>%
  select(-lvars)
# rm(lvars)


# tidy up and add compass data ----------------------------------------------------
source('src-r/source-compass-data.R')
x <- compass.feats %>% select(AddDropclass:Other)
compass.feats$sum_visit_advising <- rowSums(x)
x <- compass.feats %>% select(starts_with("IC_"))
compass.feats$sum_visit_ic <- rowSums(x)
rm(x)
compass.feats <- compass.feats %>%
  select(system_key, yrq, visit_advising, visit_ic, sum_visit_advising, sum_visit_ic)
compass.weekly <- compass.weekly %>% filter(week > 0) %>% arrange(system_key, yrq, week)

# Combine sdb and canvas to create dataset --------------------------------
# Merge SDB+Canvas+Compass.
# Compass feats has duplicated name, let's use weekly data

dat <- can_dat %>%
  left_join(sdb_dat, by = c('system_key' = 'system_key', 'yrq' = 'yrq')) %>%
  select(system_key,
         uw_netid,
         user_id,
         major_abbr,
         yrq,
         week,
         everything()) %>%
  arrange(system_key,
          yrq,
          week) %>%
  # left_join(compass.weekly) %>%
  left_join(compass.weekly,
            by = c('system_key' = 'system_key',
                   'yrq' = 'yrq',
                   'week' = 'week')) %>%
  janitor::clean_names() %>%
  select(-visit_ic) %>%
  replace_na(list(qgpa15 = 0,
                  qgpa20 = 0,
                  probe = 0,
                  n_holds = 0,
                  n_unmet = 0,
                  n_major_courses = 0,
                  csum_major_courses = 0,
                  n_writing = 0,
                  n_diversity = 0,
                  n_engl_comp = 0,
                  n_vlpa = 0,
                  n_indiv_soc = 0,
                  n_nat_world = 0,
                  n_gen_elective = 0,
                  n_alt_grading = 0,
                  csum_rep_courses = 0,
                  csum_alt_grading = 0,
                  stem_courses = 0,
                  stem_credits = 0,
                  csum_stem_courses = 0,
                  csum_stem_credits = 0,
                  visit_advising = 0))


# add y -------------------------------------------------------------------

yv <- read_csv('data-prepped/eop-aggr-yvar.csv')
dat %>% inner_join(yv) %>% write_csv('data-prepped/eop-aggr-au20-prototyping-data.csv')
dat %>% filter(yrq == 20204) %>% write_csv('data-prepped/eop-aggr-au20-for-new-preds.csv')
