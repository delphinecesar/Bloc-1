# Import libraries
import pandas as pd
import requests
import json
import numpy as np
import datetime
from geopy.distance import geodesic

from sklearn.pipeline import Pipeline
from sklearn.base import BaseEstimator, TransformerMixin

import joblib

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)

# Create class for feature engineering
class FeatureEngineeringTransformer(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X['current_time'] = pd.to_datetime(X['current_time'])
        X['year'] = X['current_time'].dt.year
        X['month'] = X['current_time'].dt.month
        X['day'] = X['current_time'].dt.day
        X['dayofweek'] = X['current_time'].dt.dayofweek
        X['hour'] = X['current_time'].dt.hour

        X['dob'] = pd.to_datetime(X['dob'])
        nb_of_days = X['current_time'] - X['dob']
        diff_days = nb_of_days / pd.Timedelta(days=1)
        X['age'] = round(diff_days / 365)

        X['distance'] = X.apply(lambda row: round(geodesic((row['lat'], row['long']),
                                                           (row['merch_lat'], row['merch_long'])).km, 2), axis=1)
        return X
    
# Create class for the pipeline
class OurPipeline(FeatureEngineeringTransformer):
    @staticmethod
    def ourpipeline(data):
        pipeline = Pipeline([('feature_engineering', FeatureEngineeringTransformer())])
        # Apply the pipeline to your dataset
        transformed_dataset = pipeline.fit_transform(data)
        return transformed_dataset