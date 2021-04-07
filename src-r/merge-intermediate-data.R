rm(list = ls())

library(tidyverse)

setwd(rstudioapi::getActiveProject())


# combine SDB and canvas --------------------------------------------------
# TODO automate reading + binding canvas-xxxx.csv files
#      as in `au20-dev-proto-data.R`
can_au20 <- read_csv('data-intermediate/canvas-au20.csv')
can_sp20 <- read_csv('data-intermediate/canvas-sp20.csv')
can_su20 <- read_csv('data-intermediate/canvas-su20.csv')
can_wi21 <- read_csv('data-intermediate/canvas-wi21.csv')
can_long_raw <- bind_rows(can_au20, can_sp20, can_su20, can_wi21)
# rm(can_au20, can_sp20, can_su20, )
rm(list = ls()[!grepl('long', ls())])


# Widen canvas ------------------------------------------------------------
# canvas_wide_courses <- can_long %>% pivot_wider(names_from = 'week',
#                                    values_from = c('page_views', 'page_views_level', 'partic', 'partic_level',
#                                                    'tot_assgns', 'tot_assgns_on_time', 'tot_assgn_late',
#                                                    'tot_assgn_missing', 'tot_assgn_floating', 'score',
#                                                    'n_assign', 'wk_pts_poss', ),
#                                    names_prefix = 'wk')


# ? Filtering out students who don't appear in the canvas data at all
sdb_dat <- read_csv('data-intermediate/sdb_eop_isso_wi21.csv') # %>%
  # filter(system_key %in% unique(can_long_raw$system_key))

# Reduce canvas to EOP+ISS students
# This current setup requires aggregating the canvas data
can_long_eop_iss <- can_long_raw %>%
  semi_join(sdb_dat, by = c('system_key' = 'system_key'))

# %>%
  group_by(system_key, user_id, yrq, week) %>%
  summarize_at(vars(page_views,
                    page_views_level,
                    partic,
                    partic_level,
                    tot_assgns,
                    tot_assgns_on_time,
                    tot_assgn_late,
                    tot_assgn_missing,
                    tot_assgn_floating,
                    score,
                    n_assign,
                    wk_pts_poss), mean)

# WIDEN aggregate canvas data
can_wide_aggr <- can_long_aggr %>%
  pivot_wider(names_from = 'week',
              values_from = c('page_views',
                              'page_views_level',
                              'partic',
                              'partic_level',
                              'tot_assgns',
                              'tot_assgns_on_time',
                              'tot_assgn_late',
                              'tot_assgn_missing',
                              'tot_assgn_floating',
                              'score',
                              'n_assign',
                              'wk_pts_poss'),
              names_prefix = 'wk')

# Gen lags and subset data ------------------------------------------------

# # now create lag vars and subset years
# lvars <- c('honors_program',
#            'scholarship_type',
#            'yearly_honor_type',
#            'pts',
#            'nongrd',
#            'deduct',
#            'qgpa',
#            'tot_creds',
#            'qgpa15',
#            'qgpa20',
#            'probe',
#            'cum.pts',
#            'cum.gpa',
#            'n.w',
#            'csum.w',
#            'avg.stem.grade')
sdb_dat <- sdb_dat %>%
  group_by(system_key) %>%
  arrange(system_key, yrq) %>%
  mutate(across(.cols = lvars, .fns = lag, default = 0, .names = "lag_{.col}")) %>%
  ungroup() %>%
  filter(yrq %in% unique(can_long$yrq)) %>%
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

compass.wide <- compass.weekly %>%
  select(-visit_ic) %>%
  filter(week > 0) %>%
  arrange(yrq, week) %>%
  pivot_wider(names_from = 'week', values_from = 'visit_advising', names_prefix = 'visit_advising_wk',
              values_fill = list(visit_advising = 0))


# Combine sdb and canvas to create dataset --------------------------------
# Merge SDB+Canvas+Compass.

dat <- can_wide_aggregate %>%
  left_join(sdb_dat, by = c('system_key' = 'system_key', 'yrq' = 'yrq')) %>%
  select(system_key,
         uw_netid,
         user_id,
         major_abbr,
         yrq,
         everything()) %>%
  arrange(system_key,
          yrq) %>%
  # left_join(compass.weekly) %>%
# <<<<<<< HEAD
  left_join(compass.wide,
            by = c('system_key' = 'system_key',
                   'yrq' = 'yrq')) %>%
# =======
#   left_join(compass.weekly,
#             by = c('system_key' = 'system_key',
#                    'yrq' = 'yrq',
#                    'week' = 'week')) %>%
# >>>>>>> c89b9e3a4f50fd53cb442a2dad8f9811a0e0b9c6
  janitor::clean_names() %>%
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
dat %>% filter(yrq < 20204) %>% inner_join(yv) %>% write_csv('data-prepped/eop-aggr-au20-prototyping-data.csv')
dat %>% filter(yrq == 20204) %>% write_csv('data-prepped/eop-aggr-au20-for-new-preds.csv')

# save some canvas + y files too for other analyses
can_wide_aggr %>% left_join(yv) %>% write_csv('data-intermediate/canvas-eop-wide-prototyping.csv')
can_long_aggr %>% left_join(yv) %>% write_csv('data-intermediate/canvas-eop-long-prototyping.csv')
