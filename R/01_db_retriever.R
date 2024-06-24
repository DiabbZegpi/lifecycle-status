library(tidyverse)
library(DBI)
library(here)
source(here("R", "00_functions.R"))

# Database connection -----------------------------------------------------
conn <- set_connection()

master_data_qr <- query(conn, "PRODUCT_MASTER_DATA")
active_sku_qr <- query(conn, "ACTIVE_SKU")
# customer_qr <- query(conn, "PRODUCT_CUSTOMER")
sell_in_qr <- query(conn, "SELL_IN_ACTUALS")

# Pre-processing -----------------------------------------------------------
master_data <-
  master_data_qr |>
  distinct(COUNTRY, BASECODE, SKU) |>
  collect()

active_sku <-
  active_sku_qr |>
  filter(VERSION == max(VERSION, na.rm = TRUE)) |>
  select(-c(VERSION, ACTIVE_NET_WEIGHT, ACTIVE_YIELDS)) |>
  collect()

sell_in <-
  sell_in_qr |>
  group_by(PERIOD, COUNTRY, SKU, APO_CUST_LEVEL2_CODE) |>
  summarize(
    SELL_IN_VOL = sum(SELL_IN_VOL, na.rm = TRUE),
    .groups = "drop"
  ) |>
  complete(
    PERIOD, nesting(COUNTRY, SKU, APO_CUST_LEVEL2_CODE),
    fill = list(SELL_IN_VOL = 0)
  ) |>
  collect()

dbDisconnect(conn)

# Export data -------------------------------------------------------------
write_rds(master_data, here("pre-processed data", "master_data.rds"), compress = "gz")
write_rds(active_sku, here("pre-processed data", "active_sku.rds"), compress = "gz")
write_rds(sell_in, here("pre-processed data", "sell_in.rds"), compress = "gz")
