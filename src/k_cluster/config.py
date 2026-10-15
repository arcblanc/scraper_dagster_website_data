"""Configuration settings for Malaysian business registry clustering."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Database Configuration
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")

# Database Connection String
DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# Table Configuration
ROB_TABLE = "rob_registration"  # Registry of Businesses
ROC_TABLE = "roc_registration"  # Registry of Companies

# Clustering Configuration
N_CLUSTERS = 5
RANDOM_STATE = 42
MAX_ITER = 300
N_INIT = 10

# Feature Engineering Configuration
SVD_COMPONENTS = 50
TFIDF_MAX_FEATURES = 1000
TFIDF_MIN_DF = 2
TFIDF_MAX_DF = 0.95

# File Paths
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
MODELS_DIR = BASE_DIR / "models"
OUTPUTS_DIR = BASE_DIR / "outputs"

# Create directories if they don't exist
DATA_DIR.mkdir(exist_ok=True)
MODELS_DIR.mkdir(exist_ok=True)
OUTPUTS_DIR.mkdir(exist_ok=True)

# Data Files
ROB_CSV = DATA_DIR / "rob_registry.csv"
ROC_CSV = DATA_DIR / "roc_registry.csv"
COMBINED_CSV = DATA_DIR / "combined_registry.csv"
FEATURES_CSV = DATA_DIR / "features.csv"

# Model Files
CLUSTER_MODEL = MODELS_DIR / "kmeans_model.pkl"
SCALER_MODEL = MODELS_DIR / "scaler_model.pkl"
FEATURE_NAMES = MODELS_DIR / "feature_names.pkl"
CLUSTER_PROFILES = OUTPUTS_DIR / "cluster_profiles.json"


# Validation
def validate_config():
    """Validate that all required environment variables are set."""
    required_vars = ["PG_USER", "PG_PASSWORD", "PG_HOST", "PG_PORT", "PG_DATABASE"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")

    return True
