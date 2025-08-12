import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVR

from config import Config

class HousingPricePredictor:
    def __init__(self, data_path: str = ""):
        self.data_path = Path(data_path) if data_path else Path(Config.DATA_PATH)
        self.model: Optional[SVR] = None
        self.scaler = StandardScaler()
        self.features = ['Rooms', 'YearBuilt', 'Landsize', 'BuildingArea', 'Bathroom', 'Car']
        
    def load_data(self):
        if not self.data_path.exists():
            raise FileNotFoundError(f"Data file not found: {self.data_path}")
            
        home_data = pd.read_csv(self.data_path)
        y = home_data.Price
        X = home_data[self.features]
        
        X_clean = X.dropna()
        y_clean = y[X_clean.index]
        
        return X_clean, y_clean
    
    def prepare_data(self, X, y, test_size: float = 0.2):
        X_scaled = self.scaler.fit_transform(X)
        return train_test_split(X_scaled, y, test_size=test_size, random_state=42)
    
    def train_model(self, train_X, train_y, val_X, val_y) -> None:
        self.model = SVR(
            kernel='rbf',
            C=1.0,
            gamma='scale',
            epsilon=0.1
        )
        
        self.model.fit(train_X, train_y)
    def evaluate_model(self, val_X, val_y):
        if self.model is None:
            raise ValueError("Model not trained yet")
            
        val_predictions = self.model.predict(val_X)
        
        metrics = {
            'mae': mean_absolute_error(val_y, val_predictions),
            'mse': mean_squared_error(val_y, val_predictions),
            'rmse': np.sqrt(mean_squared_error(val_y, val_predictions)),
            'r2': r2_score(val_y, val_predictions)
        }
        
        return metrics
    
    def predict_price(self, house_features):
        if self.model is None:
            raise ValueError("Model not trained yet")
            
        sample_house = pd.DataFrame([house_features])
        sample_house_scaled = self.scaler.transform(sample_house)
        return self.model.predict(sample_house_scaled)[0]

def main():
    try:
        predictor = HousingPricePredictor()
        X, y = predictor.load_data()
        
        train_X, val_X, train_y, val_y = predictor.prepare_data(X, y)
        
        print("Training Support Vector Regression model...")
        predictor.train_model(train_X, train_y, val_X, val_y)
        
        metrics = predictor.evaluate_model(val_X, val_y)
        
        print(f"Model Performance:")
        print(f"  MAE: ${metrics['mae']:,.0f}")
        print(f"  RMSE: ${metrics['rmse']:,.0f}")
        print(f"  R² Score: {metrics['r2']:.3f}")
        
        sample_house = {
            'Rooms': 3,
            'YearBuilt': 2010,
            'Landsize': 500,
            'BuildingArea': 150,
            'Bathroom': 2,
            'Car': 1
        }
        
        predicted_price = predictor.predict_price(sample_house)
        print(f"\nPredicted house price: ${predicted_price:,.0f}")
        print("Sample house features:")
        for feature, value in sample_house.items():
            print(f"  {feature}: {value}")
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()