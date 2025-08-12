from pathlib import Path

DATA_PATH = r'data/housing_data.csv'

DASK_CONFIG = {
    'n_workers': 4,
    'threads_per_worker': 2,
    'memory_limit': '2GB'
}

MODEL_CONFIG = {
    'test_size': 0.2,
    'random_state': 42,
    'cv_folds': 5
}

SAMPLE_HOUSE = {
    'Rooms': 3,
    'YearBuilt': 2010,
    'Landsize': 500,
    'BuildingArea': 150,
    'Bathroom': 2,
    'Car': 1
}

OUTPUT_CONFIG = {
    'save_results': True,
    'print_summary': True,
    'results_dir': Path('results')
}

class Config:
    DATA_PATH = DATA_PATH
    DASK_CONFIG = DASK_CONFIG
    MODEL_CONFIG = MODEL_CONFIG
    SAMPLE_HOUSE = SAMPLE_HOUSE
    OUTPUT_CONFIG = OUTPUT_CONFIG
    
    @staticmethod
    def validate_data_path():
        return Path(DATA_PATH).exists() 