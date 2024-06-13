library(tidyverse)
library(bit64)
library(here)
library(furrr)

# Import data -------------------------------------------------------------
master_data <- read_rds(here("pre-processed data", "master_data.rds"))
active_sku <- read_rds(here("pre-processed data", "active_sku.rds"))
sell_in <- read_rds(here("pre-processed data", "sell_in.rds"))
forecast <- read_rds(here("pre-processed data", "forecast.rds"))

# Aggregate at base code level --------------------------------------------
bcode_sales <-
  sell_in |>
  left_join(
    master_data,
    by = join_by(COUNTRY, SKU),
    na_matches = "never",
    relationship = "many-to-one"
  ) |>
  group_by(PERIOD, COUNTRY, BASECODE, APO_CUST_LEVEL2_CODE) |>
  summarize(
    SELL_IN_VOL = sum(SELL_IN_VOL),
    .groups = "drop"
  ) |>
  na.omit()

# bcode_lifecycle <-
#   active_sku |>
#   distinct(COUNTRY, BASECODE, APO_CUST_LEVEL2_CODE, LIFECYCLE_BASECODE) |>
#   right_join(
#     bcode_sales,
#     by = join_by(COUNTRY, BASECODE, APO_CUST_LEVEL2_CODE),
#     na_matches = "never",
#     relationship = "one-to-many"
#   ) |>
#   na.omit()


# Feature engineering on history ------------------------------------------
last_period_observed <- max(bcode_lifecycle$PERIOD)

history_features <-
  bcode_sales |>
  group_by(COUNTRY, BASECODE, APO_CUST_LEVEL2_CODE) |>
  arrange(PERIOD, .by_group = TRUE) |>
  # Lag-1 calculations
  # May be necessary to add lag-n for seasonal vs in & out discrimination
  mutate(
    lag_1 = lag(SELL_IN_VOL, 1L),
    lag_1 = if_else(is.na(lag_1), SELL_IN_VOL, lag_1),
    drop_zero_flag = case_when(
      lag_1 > 0 & SELL_IN_VOL == 0 ~ "drop_to_zero",
      lag_1 == 0 & SELL_IN_VOL > 0 ~ "raise_from_zero",
      lag_1 == 0 & SELL_IN_VOL == 0 ~ "constant_zero",
      .default = "constant_positive"
    )
  ) |>
  select(-matches("^lag_\\d{1,2}$")) |>
  ungroup()

# Parallel backend
workers <- parallel::detectCores(logical = FALSE)
plan(multisession, workers = workers)

history_features |>
  nest(history = c(PERIOD, SELL_IN_VOL, drop_zero_flag)) |>
  mutate(
    history_36 = future_map(history, ~ slice_tail(.x, n = 36)),
    history_24 = future_map(history, ~ slice_tail(.x, n = 24)),
    history_12 = future_map(history, ~ slice_tail(.x, n = 12))
  )
  summarize(
    positive_last_12 = sum(SELL_IN_VOL > 0),
    zeros_last_12 = sum(SELL_IN_VOL == 0),
    .groups = "drop"
  )




