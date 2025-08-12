import pandas as pd
import numpy as np
from pathlib import Path
import time
import json
from datetime import datetime
import argparse
import sys

from config import Config
from housing_DTR import HousingPricePredictor as DTRPredictor
from housing_XGB import HousingPricePredictor as XGBPredictor
from housing_SVR import HousingPricePredictor as SVRPredictor
from housing_RFR import HousingPricePredictor as RFRPredictor
from housing_GBR import HousingPricePredictor as GBRPredictor
from housing_BGR_DTR import HousingPricePredictor as BGRPredictor

class ModelExecutor:
    def __init__(self, data_path=None):
        self.data_path = data_path or Config.DATA_PATH
        self.results = {}
        self.cached_data = None
        
        self.models = {
            'Decision Tree': DTRPredictor,
            'XGBoost': XGBPredictor,
            'Support Vector Regression': SVRPredictor,
            'Random Forest': RFRPredictor,
            'Gradient Boosting': GBRPredictor,
            'Bagging with Decision Tree': BGRPredictor
        }
    
    def load_data(self):
        if self.cached_data is not None:
            return self.cached_data
            
        print("Loading data...")
        start_time = time.time()
        
        try:
            data_path = Path(self.data_path)
            if not data_path.exists():
                raise FileNotFoundError(f"Data file not found: {data_path}")
            
            home_data = pd.read_csv(data_path)
            y = home_data.Price
            X = home_data[['Rooms', 'YearBuilt', 'Landsize', 'BuildingArea', 'Bathroom', 'Car']]
            
            X_clean = X.dropna()
            y_clean = y[X_clean.index]
            
            loading_time = time.time() - start_time
            print(f"Data loaded: {X_clean.shape} in {loading_time:.2f}s")
            
            self.cached_data = (X_clean, y_clean)
            return X_clean, y_clean
            
        except Exception as e:
            print(f"Error loading data: {e}")
            raise
    
    def execute_models_sequentially(self, sample_house=None):
        sample_house = sample_house or Config.SAMPLE_HOUSE
        
        X, y = self.load_data()
        
        print("Starting sequential execution...")
        start_time = time.time()
        
        results = []
        for model_name, model_class in self.models.items():
            try:
                model_start_time = time.time()
                
                predictor = model_class(self.data_path)
                train_X, val_X, train_y, val_y = predictor.prepare_data(X, y)
                
                predictor.train_model(train_X, train_y, val_X, val_y)
                
                metrics = predictor.evaluate_model(val_X, val_y)
                predicted_price = predictor.predict_price(sample_house)
                
                training_time = time.time() - model_start_time
                
                result = {
                    'model_name': model_name,
                    'metrics': metrics,
                    'predicted_price': predicted_price,
                    'training_time': training_time,
                    'status': 'success',
                    'data_shape': X.shape,
                    'features_used': predictor.features
                }
                
                results.append(result)
                print(f"{model_name}: {training_time:.2f}s")
                
            except Exception as e:
                print(f"Error in {model_name}: {str(e)}")
                results.append({
                    'model_name': model_name,
                    'error': str(e),
                    'status': 'failed'
                })
        
        total_time = time.time() - start_time
        print(f"Sequential completed: {total_time:.2f}s")
        
        return {
            'results': results,
            'total_execution_time': total_time,
            'timestamp': datetime.now().isoformat(),
            'sample_house': sample_house,
            'data_shape': X.shape
        }
    
    def save_results(self, results, filename=None):
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"model_results_{timestamp}.json"
        
        output_path = Path(filename)
        
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"Results saved: {output_path}")
        return output_path
    
    def print_summary(self, results):
        print("\n" + "="*80)
        print("MODEL EXECUTION SUMMARY")
        print("="*80)
        
        print(f"Total Time: {results['total_execution_time']:.2f}s")
        print(f"Data Shape: {results['data_shape']}")
        
        successful_models = [r for r in results['results'] if r['status'] == 'success']
        failed_models = [r for r in results['results'] if r['status'] == 'failed']
        
        if successful_models:
            print(f"\nSuccessful Models ({len(successful_models)}):")
            for result in successful_models:
                print(f"\n{result['model_name']}:")
                print(f"  Time: {result['training_time']:.2f}s")
                print(f"  R²: {result['metrics']['r2']:.3f}")
                print(f"  RMSE: ${result['metrics']['rmse']:,.0f}")
                print(f"  Price: ${result['predicted_price']:,.0f}")
        
        if failed_models:
            print(f"\nFailed Models ({len(failed_models)}):")
            for result in failed_models:
                print(f"  {result['model_name']}: {result['error']}")
        
        if successful_models:
            best_model = max(successful_models, key=lambda x: x['metrics']['r2'])
            print(f"\nBest Model: {best_model['model_name']}")
            print(f"  R² Score: {best_model['metrics']['r2']:.3f}")
            print(f"  RMSE: ${best_model['metrics']['rmse']:,.0f}")

def main():
    parser = argparse.ArgumentParser(description='Housing Price Prediction with Multiple Models')
    parser.add_argument('--data-path', type=str, help='Path to the data file')
    parser.add_argument('--no-save', action='store_true', help='Do not save results to file')
    parser.add_argument('--no-print', action='store_true', help='Do not print summary')
    
    args = parser.parse_args()
    
    if not Config.validate_data_path():
        print(f"Error: Data file not found at {Config.DATA_PATH}")
        print("Please update the DATA_PATH in config.py or provide --data-path argument")
        sys.exit(1)
    
    executor = ModelExecutor(data_path=args.data_path)
    
    try:
        print("Running Model Execution...")
        results = executor.execute_models_sequentially()
        executor.print_summary(results)
        
        if not args.no_save:
            executor.save_results(results)
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()
