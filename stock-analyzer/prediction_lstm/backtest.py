import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt

# --- Load actual and predicted close prices from evaluate.py ---
actual_close = pd.read_csv("results/actual_close.csv", index_col=0)
predicted_close = pd.read_csv("results/predicted_close.csv", index_col=0)

# --- Simple backtesting logic ---
signals = predicted_close.shift(1) > actual_close.shift(1)  # Buy if model expects rise
returns = actual_close.pct_change().shift(-1)  # Next day return

# Strategy return
strategy_returns = returns * signals.astype(int)

# --- Performance metrics ---
cumulative_strategy = (1 + strategy_returns).cumprod()
cumulative_benchmark = (1 + returns).cumprod()

sharpe_ratio = strategy_returns.mean() / strategy_returns.std() * np.sqrt(252)

print(f"📊 Sharpe Ratio: {sharpe_ratio:.2f}")
print(f"📈 Strategy Final Return: {cumulative_strategy.iloc[-1]:.2f}")
print(f"📉 Benchmark Final Return: {cumulative_benchmark.iloc[-1]:.2f}")

# --- Plot ---
plt.figure(figsize=(10, 5))
plt.plot(cumulative_benchmark, label="Benchmark")
plt.plot(cumulative_strategy, label="Strategy")
plt.title("Backtest Performance")
plt.legend()
plt.grid(True)
plt.show()

##