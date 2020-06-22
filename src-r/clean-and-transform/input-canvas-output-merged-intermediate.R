# assignment data - use long format for new calcs, then transform to wide

rm(list = ls())
gc()

library(tidyverse)

# wide.assgn <- read_csv('data-raw/weekly_assignments_wide_2020-06-12.csv')
long.assgn <- read_csv('data-raw/weekly_assignments_long_2020-06-12.csv',
                       col_types = 'nnnnnnnnnnn-')

# remove courses with 0 points possible in canvas
zero.pt.crss <- long.assgn %>%
  group_by(course_id) %>%
  filter(sum(pts_possible, na.rm = T) == 0) %>%
  distinct(course_id) %>%
  ungroup

# calculate cumulative totals for students in courses
accum <- long.assgn %>%
  filter(!(course_id %in% zero.pt.crss$course_id)) %>%
  arrange(yrq, user_id, course_id, week) %>%
  group_by(user_id, yrq, course_id) %>%
  mutate(csum_pts = cumsum(pts_possible),
         csum_score = cumsum(score),
         # csum_ontime = cumsum(statuson_time),
         csum_late = cumsum(statuslate),
         csum_miss = cumsum(statusmissing),
         # csum_assgn = cumsum(n_assgn),
         curr_pct_score = score / pts_possible,
         csum_pct_score = csum_score / csum_pts) %>%
  ungroup() %>%
  replace_na(list(curr_pct_score = 1,
                  pct_csum_score = 1)) %>%             # treat 0 / 0 as 100%
  group_by(user_id, yrq, week) %>%
  # calculate aggregations for each student by week of each quarter
  # e.g. average all current courses
  mutate(all_late = sum(statuslate, na.rm = T),
         all_miss = sum(statusmissing, na.rm = T),
         all_mean_curr_score = mean(curr_pct_score, na.rm = T),
         all_assignments = sum(n_assgn, na.rm = T)) %>%
  ungroup()

# calculate a 'final score' (may or may not end up using)
finpct <- accum %>%
  group_by(user_id, yrq, course_id) %>%
  summarize(tot_crs_pts = sum(pts_possible, na.rm = T),
            tot_crs_assgn = sum(n_assgn, na.rm = T),
            fin_pts = sum(score, na.rm = T),
            fin_pct = fin_pts / tot_crs_pts,
            fin_pct = if_else(fin_pct > 1, 1, fin_pct),
            fin_miss = sum(statusmissing, na.rm = T) / tot_crs_assgn,
            fin_ontime = sum(statuson_time, na.rm = T) / tot_crs_assgn,
            fin_late = sum(statuslate, na.rm = T) / tot_crs_assgn) %>%
  ungroup()
# viz distributions
qplot(x = yrq, y = fin_pct, data = finpct, group = yrq, geom = 'violin')
qplot(x = yrq, y = fin_miss, data = finpct, group = yrq, geom = 'violin')
qplot(x = yrq, y = fin_ontime, data = finpct, group = yrq, geom = 'violin')
qplot(x = yrq, y = fin_late, data = finpct, group = yrq, geom = 'violin')
cor(finpct$fin_pct, finpct$fin_miss, use = 'complete.obs')
cor(finpct$fin_pct, finpct$fin_ontime, use = 'complete.obs')
cor(finpct$fin_pct, finpct$fin_late, use = 'complete.obs')
qplot(x = log(tot_crs_pts), y = log(fin_pts), data = finpct, geom = 'point')

# convert to wide, bind final percent, save to intermediate ---------------

# not using all fields, see below
wide <- accum %>%
  # trying to maintain ordering
  mutate(week = str_pad(week, 2, 'left', pad = '0')) %>%
  replace_na(list(curr_pct_score = 1,
                  csum_pct_score = 1,
                  pct_ontime = 0,
                  pct_late = 0,
                  pct_miss = 0,
                  all_pct = 1)) %>%
  rename(floating = statusfloating,
         late = statuslate,
         missing = statusmissing,
         ontime = statuson_time) %>%
  pivot_wider(id_cols = c('yrq', 'user_id', 'course_id'),
                              names_from = 'week',
                              names_prefix = 'wk',
                              values_from = c('curr_pct_score',
                                              'csum_pct_score',
                                              'late',
                                              'missing',
                                              'csum_late',
                                              'csum_miss',
                                              'all_late',
                                              'all_miss',
                                              'all_mean_curr_score',
                                              'all_assignments'))

wide <- wide %>% left_join( select(finpct,
                                   user_id,
                                   yrq,
                                   course_id,
                                   fin_pct),
                            by = c('user_id' = 'user_id',
                                   'yrq' = 'yrq',
                                   'course_id' = 'course_id')
                            )
# tidy up
rm(accum, finpct, long.assgn, zero.pt.crss)

# transfrom page views ----------------------------------------------------

pv <- read_csv('data-raw/weekly_page_views_wide_2020-05-07.csv')
# xform to long, calculate fields, repair names, back to wide
pv <- pv %>%
  pivot_longer(-c('yrq', 'user_id', 'course_id'),
               names_to = 'week',
               names_prefix = 'views_week_',
               values_to = 'views')
pv$week <- as.numeric(pv$week)
pv$views[is.na(pv$views)] <- 0

pv <- pv %>%
  arrange(yrq, user_id, course_id, week) %>%
  group_by(user_id, course_id, yrq) %>%
  mutate(csum_pgvw = cumsum(views)) %>%
  ungroup() %>%
  group_by(course_id, yrq, week) %>%
  mutate(crs_avg_pgvw = mean(views, na.rm = T)) %>%
  ungroup() %>%
  group_by(user_id, yrq, week) %>%
  mutate(usr_avg_pgvw = mean(views, na.rm = T)) %>%
  ungroup()

# to wide:
widepv <- pv %>%
  mutate(week = str_pad(week, 2, side = 'left', pad = '0')) %>%
  pivot_wider(id_cols = c('user_id', 'yrq', 'course_id'),
              names_from = week,
              names_prefix = 'wk',
              names_sep = '_',
              values_from = c(views, csum_pgvw, crs_avg_pgvw, usr_avg_pgvw))

# # repair names
# names(pv) <- c('yrq',
#                'user_id',
#                'course_id',
#                'views_wk07',
#                'views_wk08',
#                'views_wk09',
#                'views_wk10',
#                'views_wk11',
#                'views_wk12',
#                'views_wk01',
#                'views_wk02',
#                'views_wk03',
#                'views_wk04',
#                'views_wk05',
#                'views_wk06')
# pv <- pv %>% mutate_all(replace_na, 0)





# cleanup url visits file -------------------------------------------------

urls <- read_csv('data-raw/weekly_url_count_wide.csv')
# repair names
orig <- names(urls)
w <- str_pad(1:12, 2, 'left', '0')
w <- paste0('wk', w)
(new.names <- str_remove(orig, pattern = "week_\\d+"))
# old[3:length(old)] <-
new.names[4:length(new.names)] <- paste0(new.names[4:length(new.names)], w)
names(urls) <- new.names
rm(orig, w, new.names)

