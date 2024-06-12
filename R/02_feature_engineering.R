library(tidyverse)
library(bit64)
library(here)

# Import data -------------------------------------------------------------
master_data <- read_rds(here("pre-processed data", "master_data.rds"))
active_sku <- read_rds(here("pre-processed data", "active_sku.rds"))
sell_in <- read_rds(here("pre-processed data", "sell_in.rds"))
forecast <- read_rds(here("pre-processed data", "forecast.rds"))

# Aggregate at base code level --------------------------------------------
sell_in
