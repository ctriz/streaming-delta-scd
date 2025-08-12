# predict.py
import pandas as pd
import numpy as np
import joblib
import yaml

from src.features import add_technical_indicators, select_features
from tensorflow.keras.models import load_model
from tensorflow.keras.metrics import MeanSquaredError


# Load config
with open("config.yaml") as f:
    config = yaml.safe_load(f)

# Load model and scaler
model = load_model(
    "results/lstm_model.h5",
     custom_objects={'mse': MeanSquaredError()}
     )
scaler = joblib.load("results/scaler.pkl")

# Load and preprocess latest data
file_path = "data/stock.csv"
df = pd.read_csv(file_path, parse_dates=['Date'])
df.set_index("Date", inplace=True)
df = add_technical_indicators(df)
df = select_features(df)
df.dropna(inplace=True)

# Keep only the latest window_size days
recent_data = df[config['features']].iloc[-config['window_size']:]
scaled_input = scaler.transform(recent_data.values)
input_sequence = np.expand_dims(scaled_input, axis=0)

# Predict
predicted_scaled = model.predict(input_sequence)

# Inverse transform
pad = np.zeros((1, len(config['features'])))
pad[0, config['features'].index('Close')] = predicted_scaled[0, 0]
predicted_close = scaler.inverse_transform(pad)[0, config['features'].index('Close')]

print(f"Predicted Next Day Close Price: {predicted_close:.2f}")
##
