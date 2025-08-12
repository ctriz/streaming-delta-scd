import pandas as pd
import ta

def add_technical_indicators(df):
    df['RSI'] = ta.momentum.RSIIndicator(df['Close']).rsi()
    df['MACD'] = ta.trend.MACD(df['Close']).macd_diff()
    bb = ta.volatility.BollingerBands(df['Close'])
    df['BB_High'] = bb.bollinger_hband()
    df['BB_Low'] = bb.bollinger_lband()
    return df

def select_features(df):
    df = df[['Open', 'High', 'Low', 'Close', 'Volume', 'RSI', 'MACD', 'BB_High', 'BB_Low']]
    return df
##