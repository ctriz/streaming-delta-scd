import pandas as pd
import numpy as np
import joblib
import yaml
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
from tensorflow.keras.models import load_model
from src.features import add_technical_indicators, select_features
from tensorflow.keras.metrics import MeanSquaredError

# Load config and model
with open("config.yaml") as f:
    config = yaml.safe_load(f)

# Load model and scaler
model = load_model(
    "results/lstm_model.h5",
     custom_objects={'mse': MeanSquaredError()}
     )
scaler = joblib.load("results/scaler.pkl")

# Load and process data
df = pd.read_csv("data/stock.csv", parse_dates=["Date"])
df.set_index("Date", inplace=True)
df = add_technical_indicators(df)
df = select_features(df)
df.dropna(inplace=True)

scaled_data = scaler.transform(df.values)

X, y = [], []
for i in range(config['window_size'], len(scaled_data)):
    X.append(scaled_data[i - config['window_size']:i])
    y.append(scaled_data[i][3])  # Close price

X, y = np.array(X), np.array(y)
_, X_test, _, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Predict
pred = model.predict(X_test)

# Inverse transform predictions and actuals
pad = np.zeros((len(pred), len(config['features'])))
pad[:, config['features'].index('Close')] = pred.flatten()
predicted_close = scaler.inverse_transform(pad)[:, config['features'].index('Close')]

pad[:, config['features'].index('Close')] = y_test
actual_close = scaler.inverse_transform(pad)[:, config['features'].index('Close')]

# RMSE and plot
print("Test RMSE:", np.sqrt(mean_squared_error(actual_close, predicted_close)))

plt.figure(figsize=(12, 6))
plt.plot(actual_close, label='Actual Close')
plt.plot(predicted_close, label='Predicted Close')
plt.title("Actual vs Predicted Close Price")
plt.legend()
plt.grid()
plt.tight_layout()
plt.show()
##