library(tidyverse)
library(tidymodels)
library(here)

# Import data -------------------------------------------------------------
processed_dataset <- readr::read_rds(here("pre-processed data", "processed_dataset.rds")) |>
  mutate(LIFECYCLE_BASECODE = as_factor(LIFECYCLE_BASECODE) |> fct_relevel("REGULAR", "NO REGULAR"))


# Data partition ----------------------------------------------------------
set.seed(2024)
splits <- initial_split(processed_dataset, strata = LIFECYCLE_BASECODE)
train_df <- training(splits)
test_df <- testing(splits)
folds <- vfold_cv(train_df, strata = LIFECYCLE_BASECODE)

# Recipe ------------------------------------------------------------------
base_rec <-
  recipe(LIFECYCLE_BASECODE ~ ., data = train_df) |>
  update_role(COUNTRY, BASECODE, APO_CUST_LEVEL2_CODE, new_role = "id")

rec_normalized <-
  base_rec |>
  step_zv(all_predictors()) |>
  step_normalize(all_numeric_predictors())

rec_interact <-
  base_rec |>
  step_interact(
    terms = ~ starts_with("positive_last_"):starts_with("positive_first"),
    keep_original_cols = TRUE
  )

preprocessors <- list(
  base = base_rec,
  normalized = rec_normalized,
  interactions = rec_interact
)

# Model definitions -------------------------------------------------------
elastic_spec <-
  logistic_reg(penalty = tune(), mixture = tune()) |>
  set_engine("glmnet") |>
  set_mode("classification")

knn_spec <-
  nearest_neighbor(neighbors = tune(), weight_func = tune()) |>
  set_engine("kknn") |>
  set_mode("classification")

gbm_spec <-
  boost_tree(mtry = tune(), trees = tune(), min_n = tune()) |>
  set_engine("xgboost") |>
  set_mode("classification")

models <- list(
  elastic_net = elastic_spec,
  knn = knn_spec,
  xgboost = gbm_spec
)

# Model workflows ---------------------------------------------------------
binary_metrics <- metric_set(brier_class, roc_auc, accuracy)

binary_models <- workflow_set(
  preproc = preprocessors,
  models = models,
  cross = TRUE
) |>
  anti_join(tibble(wflow_id = c("normalized_xgboost")), by = join_by("wflow_id"))

wlows_res <- workflow_map(
  binary_models,
  fn = "tune_grid",
  resamples = folds,
  grid = 20,
  metrics = binary_metrics,
  verbose = TRUE
)

# Try benchmark model
logistic_fit <- fit(workflow(spec = logistic_reg(), preprocessor = base_rec), data = train_df)

augment(logistic_fit, new_data = val_df, type = "prob") |>
  brier_class(truth = LIFECYCLE_BASECODE, .pred_REGULAR)


wlows_res |> filter(!str_detect(wflow_id, "xgboost")) |> autoplot(select_best = TRUE)







