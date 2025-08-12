# __init__.py
# This file marks this directory as a Python package.
# You can leave it empty or include common imports to simplify usage.

__version__ = '0.1.0'

# Optionally import key functions for easier access
from .features import add_technical_indicators, select_features
from .model import build_lstm_model
##