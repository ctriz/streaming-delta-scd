import numpy as np
import pandas as pd
import yaml
import joblib
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras import Input
from skopt import gp_minimize

from skopt.space import Integer, Real
from skopt.utils import use_named_args
from src.features import add_technical_indicators, select_features

# --- Load config ---
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

df = pd.read_csv("data/stock.csv", parse_dates=["Date"])
df.set_index("Date", inplace=True)
df = add_technical_indicators(df)
df = select_features(df)
df.dropna(inplace=True)

features = config['features']
target_col_idx = features.index('Close')

# --- Define narrowed hyperparameter space ---
space = [
    Integer(32, 64, name='lstm_units'),
    Real(0.1, 0.3, name='dropout_rate'),
    Integer(16, 32, name='batch_size'),
    Integer(20, 40, name='window_size')
]

@use_named_args(space)
def objective(lstm_units, dropout_rate, batch_size, window_size):
    # Ensure all hyperparameters are Python-native types
    # exclipictly cast to avoid issues with skopt
    # gp_minimize can return numpy types which may not be compatible with keras
    lstm_units = int(lstm_units)
    batch_size = int(batch_size)
    window_size = int(window_size)
    dropout_rate = float(dropout_rate)
    print(f"🔧 Testing config: LSTM units={lstm_units}, Dropout={dropout_rate:.2f}, Batch size={batch_size}, Window size={window_size}")

    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(df[features])
    X, y = [], []
    for i in range(window_size, len(scaled_data)):
        X.append(scaled_data[i-window_size:i])
        y.append(scaled_data[i][target_col_idx])
    X, y = np.array(X), np.array(y)
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

    # When using Sequential() model, ensure input shape is correct
    # explicitly set input shape to match the data
    model = Sequential()
    model.add(Input(shape=(X_train.shape[1], X_train.shape[2])))
    model.add(LSTM(lstm_units, return_sequences=True))
    model.add(Dropout(dropout_rate))
    model.add(LSTM(lstm_units))
    model.add(Dropout(dropout_rate))
    model.add(Dense(1))

    model.compile(optimizer='adam', loss='mse')
    es = EarlyStopping(monitor='val_loss', patience=2, restore_best_weights=True)

    history = model.fit(
        X_train, y_train,
        validation_data=(X_val, y_val),
        epochs=5,
        batch_size=batch_size,
        verbose=0,
        callbacks=[es]
    )

    val_loss = history.history['val_loss'][-1]
    print(f"🔍 Units: {lstm_units}, Dropout: {dropout_rate:.2f}, Batch: {batch_size}, Window: {window_size} -> Loss: {val_loss:.5f}")
    return val_loss

# --- Run Bayesian Optimization ---
result = gp_minimize(objective, space, n_calls=10, random_state=42)

# --- Save best hyperparameters ---
best_config = {
    'lstm_units': result.x[0],
    'dropout_rate': result.x[1],
    'batch_size': result.x[2],
    'window_size': result.x[3]
}

print("\n✅ Best hyperparameters found:")
print(best_config)

# --- Update config.yaml ---
with open("config.yaml", "r") as f:
    base_config = yaml.safe_load(f)
base_config.update(best_config)
with open("config.yaml", "w") as f:
    yaml.dump(base_config, f)

# --- Save result for reproducibility ---
joblib.dump(result, "results/bayes_opt_result.pkl")

##
