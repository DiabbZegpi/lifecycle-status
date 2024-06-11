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

# Import IBP forecast -----------------------------------------------------
forecast_raw <-
  read_delim(
    here("input data", "APO_LISTCUBE_ZDPAMBK1_MONTHLY.TXT"),
    col_types = cols_only(
      CAL_MONTH = col_integer(),
      COUNTRY = col_character(),
      SKU_CODE = col_double(),
      CUST_LEVEL2 = col_character(),
      LOC_CODE = col_character(),
      OPER_FCST = col_double()
    ),
    col_select = c(
      PERIOD = CAL_MONTH,
      COUNTRY,
      SKU = SKU_CODE,
      APO_CUST_LEVEL2_CODE = CUST_LEVEL2,
      LOCATION = LOC_CODE,
      FORECAST = OPER_FCST
    ),
    locale = locale(decimal_mark = ",")
  )

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

# customers <-
#   customer_qr |>
#   collect()

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

forecast <-
  forecast_raw |>
  group_by(PERIOD, COUNTRY, SKU, APO_CUST_LEVEL2_CODE) |>
  summarize(
    FORECAST = sum(FORECAST, na.rm = TRUE),
    .groups = "drop"
  ) |>
  complete(
    PERIOD, nesting(COUNTRY, SKU, APO_CUST_LEVEL2_CODE),
    fill = list(FORECAST = 0)
  )

dbDisconnect(conn)

# Export data -------------------------------------------------------------
write_rds(master_data, here("pre-processed data", "master_data.rds"), compress = "gz")
write_rds(active_sku, here("pre-processed data", "active_sku.rds"), compress = "gz")
write_rds(sell_in, here("pre-processed data", "sell_in.rds"), compress = "gz")
write_rds(forecast, here("pre-processed data", "forecast.rds"), compress = "gz")
