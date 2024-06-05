# Set databse connection
set_connection <- function(db_name = "cat-db") {
  cat_db <- config::get(value = db_name)
  conn <-
    DBI::dbConnect(
      drv = odbc::odbc(),
      driver = cat_db$driver,
      server = cat_db$server,
      database = cat_db$database,
      uid = cat_db$uid,
      pwd = cat_db$pwd
    )
  conn
}

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