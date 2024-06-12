library(tidyverse)
library(bit64)
library(here)

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
