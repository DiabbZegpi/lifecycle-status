library(tidyverse)
library(DBI)
source(here::here("R", "00_functions.R"))
# Database connection -----------------------------------------------------

conn <- set_connection()

master_data_qr <- query(conn, "PRODUCT_MASTER_DATA")
active_sku_qr <- query(conn, "ACTIVE_SKU")
customer_qr <- query(conn, "PRODUCT_CUSTOMER")
sellin_qr <- query(conn, "SELL_IN_ACTUALS")
