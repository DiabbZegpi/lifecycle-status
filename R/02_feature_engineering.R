library(tidyverse)
library(here)
library(furrr)
library(bit64, include.only = "integer64")

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
  na.omit() |>
  filter(COUNTRY != "AE")

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
# last_period_observed <- max(bcode_lifecycle$PERIOD)
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

history_features_processed <-
  history_features |>
  nest(history = c(PERIOD, SELL_IN_VOL, drop_zero_flag)) |>
  # slice(1:100) |>
  mutate(
    # Create window datasets
    history_36 = future_map(history, ~ slice_tail(.x, n = 36)),
    history_24 = future_map(history, ~ slice_tail(.x, n = 24)),
    history_12 = future_map(history, ~ slice_tail(.x, n = 12)),
    # How many positives in the last n months?
    positive_last_36 = future_map_dbl(history_36, ~ sum(.x$SELL_IN_VOL > 0)),
    positive_last_24 = future_map_dbl(history_24, ~ sum(.x$SELL_IN_VOL > 0)),
    positive_last_12 = future_map_dbl(history_12, ~ sum(.x$SELL_IN_VOL > 0)),
    # How many drop to/raise from zero in the last n months?
    drop_to_zero_36 = future_map_dbl(history_36, ~ sum(.x$drop_zero_flag == "drop_to_zero")),
    drop_to_zero_24 = future_map_dbl(history_24, ~ sum(.x$drop_zero_flag == "drop_to_zero")),
    drop_to_zero_12 = future_map_dbl(history_12, ~ sum(.x$drop_zero_flag == "drop_to_zero")),
    raise_from_zero_36 = future_map_dbl(history_36, ~ sum(.x$drop_zero_flag == "raise_from_zero")),
    raise_from_zero_24 = future_map_dbl(history_24, ~ sum(.x$drop_zero_flag == "raise_from_zero")),
    raise_from_zero_12 = future_map_dbl(history_12, ~ sum(.x$drop_zero_flag == "raise_from_zero")),
    # How long are the valleys?
    constant_zero_36 = future_map_dbl(history_36, ~ sum(.x$drop_zero_flag == "constant_zero")),
    constant_zero_24 = future_map_dbl(history_24, ~ sum(.x$drop_zero_flag == "constant_zero")),
    constant_zero_12 = future_map_dbl(history_12, ~ sum(.x$drop_zero_flag == "constant_zero")),
    constant_positive_36 = future_map_dbl(history_36, ~ sum(.x$drop_zero_flag == "constant_positive")),
    constant_positive_24 = future_map_dbl(history_24, ~ sum(.x$drop_zero_flag == "constant_positive")),
    constant_positive_12 = future_map_dbl(history_12, ~ sum(.x$drop_zero_flag == "constant_positive"))
  ) |>
  select(-matches("history"))


# Feature engineering on forecast -----------------------------------------
forecast_features_processed <-
  forecast |>
  left_join(
    master_data,
    by = join_by(COUNTRY, SKU),
    relationship = "many-to-one",
    na_matches = "never"
  ) |>
  group_by(COUNTRY, BASECODE, APO_CUST_LEVEL2_CODE, PERIOD) |>
  summarize(FORECAST = sum(FORECAST, na.rm = TRUE), .groups = "drop_last") |>
  na.omit() |>
  arrange(PERIOD, .by_group = TRUE) |>
  ungroup() |>
  nest(forecast = c(PERIOD, FORECAST)) |>
  mutate(
    # Create window datasets
    forecast_3 = future_map(forecast, ~ slice_head(.x, n = 3)),
    forecast_6 = future_map(forecast, ~ slice_head(.x, n = 6)),
    # How many positives in the first h months?
    positive_first_3 = future_map_dbl(forecast_3, ~ sum(.x$FORECAST > 0)),
    positive_first_6 = future_map_dbl(forecast_6, ~ sum(.x$FORECAST > 0))
  ) |>
  select(-matches("forecast"))

# Merge history and forecast ----------------------------------------------
processed_dataset <-
  left_join(
    history_features_processed,
    forecast_features_processed,
    by = join_by(COUNTRY, BASECODE, APO_CUST_LEVEL2_CODE),
    relationship = "one-to-one",
    na_matches = "never"
  ) |>
  mutate(across(matches("positive_first_\\d"), ~ replace_na(.x, 0))) |>
  # Get the target label `LIFECYCLE_BASECODE`
  left_join(
    distinct(active_sku, COUNTRY, BASECODE, APO_CUST_LEVEL2_CODE, LIFECYCLE_BASECODE),
    by = join_by(COUNTRY, BASECODE, APO_CUST_LEVEL2_CODE),
    relationship = "one-to-one",
    na_matches = "never"
  ) |>
  na.omit()


# Export data -------------------------------------------------------------
write_rds(processed_dataset, here("pre-processed data", "processed_dataset.rds"), compress = "gz")
