# train.py
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
import yaml
from tensorflow.keras.callbacks import EarlyStopping
from src.features import add_technical_indicators, select_features
from src.model import build_lstm_model

# Load config
with open("config.yaml") as f:
    config = yaml.safe_load(f)

# Load data
file_path = "data/stock.csv"
df = pd.read_csv(file_path, parse_dates=['Date'])
df.set_index("Date", inplace=True)
df = add_technical_indicators(df)
df = select_features(df)
df.dropna(inplace=True)

# Scaling
scaler = MinMaxScaler()
scaled_data = scaler.fit_transform(df[config['features']])

# Sequence generation
def create_sequences(data, window, target_index):
    X, y = [], []
    for i in range(window, len(data)):
        X.append(data[i-window:i])
        y.append(data[i][target_index])
    return np.array(X), np.array(y)

X, y = create_sequences(scaled_data, config['window_size'], config['features'].index('Close'))

# Split data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Build model
model = build_lstm_model(
    input_shape=(X_train.shape[1], X_train.shape[2]),
    lstm_units=config['lstm_units'],
    dropout_rate=config['dropout_rate'],
    dense_units=config['dense_units'],
    optimizer=config['optimizer'],
    loss=config['loss']
)

# Train model
history = model.fit(
    X_train, y_train,
    epochs=config['epochs'],
    batch_size=config['batch_size'],
    validation_split=config['validation_split'],
    callbacks=[EarlyStopping(patience=5, restore_best_weights=True)]
)

# Save model and scaler
model.save("results/lstm_model.h5")
import joblib
joblib.dump(scaler, "results/scaler.pkl")

# Evaluate
loss = model.evaluate(X_test, y_test)
print(f"Test Loss: {loss}")

##