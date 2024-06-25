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
    terms = ~ matches("positive_last_0"):matches("positive_last_[123]"),
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

gbm_spec <-
  boost_tree(mtry = tune(), trees = tune(), min_n = tune()) |>
  set_engine("xgboost") |>
  set_mode("classification")

models <- list(
  elastic_net = elastic_spec,
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

wflow_res <- workflow_map(
  binary_models,
  fn = "tune_grid",
  resamples = folds,
  grid = 20,
  metrics = binary_metrics,
  verbose = TRUE
)

# XGBoost doesn´t outperform elastic net by much
# (isn't even outside the interval range)
wflow_res |>
  autoplot(select_best = TRUE)

wflow_res |>
  collect_metrics(summarize = TRUE) |>
  filter(.metric == "brier_class") |>
  filter(model == "logistic_reg") |>
  slice_min(mean, n = 20, with_ties = FALSE)


# Finalize model ----------------------------------------------------------
best_model <-
  wflow_res |>
  extract_workflow_set_result("interactions_elastic_net") |>
  select_by_one_std_err(metric = "brier_class", desc(penalty))

binary_classifier <-
  wflow_res |>
  extract_workflow("interactions_elastic_net") |>
  finalize_workflow(best_model) |>
  fit(data = train_df)

# Test set metrics match validation set estimations
binary_classifier |>
  augment(new_data = test_df) |>
  # brier_class(truth = LIFECYCLE_BASECODE, .pred_REGULAR)
  # accuracy(truth = LIFECYCLE_BASECODE, .pred_class)
  roc_auc(truth = LIFECYCLE_BASECODE, .pred_REGULAR)

theme_set(theme_bw(base_size = 14, base_family = "Roboto"))

binary_classifier |>
  augment(new_data = test_df) |>
  mutate(LIFECYCLE_BASECODE = paste("True class:", LIFECYCLE_BASECODE)) |>
  ggplot(aes(x = .pred_REGULAR, fill = LIFECYCLE_BASECODE)) +
  geom_histogram(bins = 30, color = "white", show.legend = FALSE) +
  facet_wrap(~LIFECYCLE_BASECODE, ncol = 1) +
  labs(x = "P(Y = REGULAR)", title = "Predictions on the test set", y = "Frequency") +
  scale_fill_brewer(palette = "Set2")

# Export models -----------------------------------------------------------
write_rds(binary_classifier, here("models", "elastic_net_interactions.rds"), compress = "gz")
write_rds(wflow_res, here("models", "workflow_sets.rds"), compress = "gz")
