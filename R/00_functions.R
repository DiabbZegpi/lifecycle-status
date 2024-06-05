# Query the database connection
query <- function(conn, table_name) {
  db_name <- DBI::dbGetInfo(conn)$dbname
  gen_tables <- "GenTabs"
  table_path <- paste(db_name, gen_tables, table_name, sep = ".")
  dplyr::tbl(conn, I(table_path))
}

# Report NAs for exploratory reasons
map_na <- function(data, ...) {
  map(data, ~ sum(is.na(.x)), ...)
}