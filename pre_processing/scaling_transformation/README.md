# Scaler and Transformer for Predicting Churning Users for Publishers

## Purpose

The purpose of this class is to take a dask dataframe on initialization, scale and transform its values, save the hyperparameters to the disk and return the transformed dask dataframe.

## Usage

Make sure the following environment variables are set:

    - `MODEL_DAY_AS_STR`: the current day as a string.
    - `UNIQUE_HASH`: a unique hash that will be attributed to the model and scores files.
    - `MODELS_DIR`: the models directory.
    - `TRAINING_OR_PREDICTION`: holds the string `training` or `prediction`, used to determine if the data is processed for training or prediction.

Initialize a "ScalerTransformer" object with a dask dataframe. If the env variable `TRAINING_OR_PREDICTION` is set to `training`, binary files containing the fit data will be saved to the disk. If it is set to `prediction`, the `churned` column will be omitted and the fit values used to transform the data will be read from the disk.

The following files get saved to the disk and need to be present if TRAINING_OR_PREDICTION is set to 'prediction':

    - `{MODELS_DIR}/{MODEL_DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_bq_box_cox_bounces.pkl`.
    - `{MODELS_DIR}/{MODEL_DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_bq_box_cox_events.pkl`.
    - `{MODELS_DIR}/{MODEL_DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_bq_box_cox_page_views.pkl`.
    - `{MODELS_DIR}/{MODEL_DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_bq_box_cox_sessions.pkl`.
    - `{MODELS_DIR}/{MODEL_DAY_AS_STR}_{UNIQUE_HASH}_ga_chp_bq_pipeline.pkl`.

Call the "ScalerTransfomer" object's `get_transformed_data()` method to get the transformed dataframe.
