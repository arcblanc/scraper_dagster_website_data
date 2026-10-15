"""Feature engineering module for Malaysian business registry clustering."""

import logging
import pickle
import re
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).resolve().parent))
from config import (
    FEATURE_NAMES,
    FEATURES_CSV,
    SCALER_MODEL,
    SVD_COMPONENTS,
    TFIDF_MAX_DF,
    TFIDF_MAX_FEATURES,
    TFIDF_MIN_DF,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)


class RegistryFeatureEngineer:
    """Feature engineering for Malaysian business registry clustering."""

    def __init__(self):
        """Initialize the feature engineer."""
        self.scaler = StandardScaler()
        self.feature_names = None
        self.text_pipeline = None

    def parse_registration_number(self, reg_number: str) -> dict:
        """Parse Malaysian registration number to extract features."""
        try:
            # Remove any non-alphanumeric characters
            clean_reg = re.sub(r"[^A-Za-z0-9]", "", str(reg_number))

            if len(clean_reg) < 8:
                return {"year_reg": 2000, "entity_type": "00", "seq_bucket": 0}

            # Extract year (first 2 digits, assume 20xx for recent years)
            year_digits = clean_reg[:2]
            year_reg = (
                2000 + int(year_digits)
                if int(year_digits) < 50
                else 1900 + int(year_digits)
            )

            # Extract entity type (next 2 digits)
            entity_type = clean_reg[2:4] if len(clean_reg) >= 4 else "00"

            # Extract sequence number (remaining digits)
            seq_num = int(clean_reg[4:]) if len(clean_reg) > 4 else 0
            seq_bucket = seq_num // 100000  # Group into buckets of 100k

            return {
                "year_reg": year_reg,
                "entity_type": entity_type,
                "seq_bucket": seq_bucket,
            }
        except Exception:
            return {"year_reg": 2000, "entity_type": "00", "seq_bucket": 0}

    def create_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create temporal features from registration date."""
        df = df.copy()

        # Extract temporal components
        df["year_reg"] = df["date_of_registration"].dt.year
        df["month_reg"] = df["date_of_registration"].dt.month
        df["weekday_reg"] = df["date_of_registration"].dt.dayofweek

        # Calculate company age
        current_year = datetime.now().year
        df["company_age"] = current_year - df["year_reg"]

        # Cyclical encoding for month
        df["month_sin"] = np.sin(2 * np.pi * df["month_reg"] / 12)
        df["month_cos"] = np.cos(2 * np.pi * df["month_reg"] / 12)

        # One-hot encoding for weekday
        weekday_dummies = pd.get_dummies(df["weekday_reg"], prefix="weekday")
        df = pd.concat([df, weekday_dummies], axis=1)

        return df

    def create_geographic_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create geographic features from state."""
        df = df.copy()

        # Clean state names
        df["state_clean"] = df["state"].str.strip().str.upper()

        # One-hot encoding for states
        state_dummies = pd.get_dummies(df["state_clean"], prefix="state")
        df = pd.concat([df, state_dummies], axis=1)

        return df

    def create_text_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create text features using TF-IDF and SVD."""
        df = df.copy()

        # Fill missing values
        df["company_name"] = df["company_name"].fillna("")
        df["nature_of_business"] = df["nature_of_business"].fillna("")

        # Create TF-IDF features for company names
        company_tfidf = TfidfVectorizer(
            max_features=TFIDF_MAX_FEATURES,
            min_df=TFIDF_MIN_DF,
            max_df=TFIDF_MAX_DF,
            stop_words="english",
            ngram_range=(1, 2),
        )

        company_features = company_tfidf.fit_transform(df["company_name"])
        company_svd = TruncatedSVD(n_components=SVD_COMPONENTS, random_state=42)
        company_svd_features = company_svd.fit_transform(company_features)

        # Create TF-IDF features for nature of business
        business_tfidf = TfidfVectorizer(
            max_features=TFIDF_MAX_FEATURES,
            min_df=TFIDF_MIN_DF,
            max_df=TFIDF_MAX_DF,
            stop_words="english",
            ngram_range=(1, 2),
        )

        business_features = business_tfidf.fit_transform(df["nature_of_business"])
        business_svd = TruncatedSVD(n_components=SVD_COMPONENTS, random_state=42)
        business_svd_features = business_svd.fit_transform(business_features)

        # Add SVD features to dataframe
        for i in range(SVD_COMPONENTS):
            df[f"company_svd_{i}"] = company_svd_features[:, i]
            df[f"business_svd_{i}"] = business_svd_features[:, i]

        # Store text pipeline for later use
        self.text_pipeline = {
            "company_tfidf": company_tfidf,
            "company_svd": company_svd,
            "business_tfidf": business_tfidf,
            "business_svd": business_svd,
        }

        return df

    def create_registration_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create features from registration number parsing."""
        df = df.copy()

        # Parse registration numbers
        reg_features = df["registration_number"].apply(self.parse_registration_number)
        reg_df = pd.DataFrame(reg_features.tolist())

        # Add parsed features
        df["year_reg"] = reg_df["year_reg"]
        df["entity_type"] = reg_df["entity_type"]
        df["seq_bucket"] = reg_df["seq_bucket"]

        # One-hot encoding for entity types
        entity_dummies = pd.get_dummies(df["entity_type"], prefix="entity")
        df = pd.concat([df, entity_dummies], axis=1)

        return df

    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Complete feature engineering pipeline."""
        logger.info("🔧 Starting feature engineering...")

        # Create all feature types
        df = self.create_registration_features(df)
        df = self.create_temporal_features(df)
        df = self.create_geographic_features(df)
        df = self.create_text_features(df)

        # Select numerical features for clustering
        numerical_features = [
            "company_age",
            "month_sin",
            "month_cos",
            "seq_bucket",
            "weekday_0",
            "weekday_1",
            "weekday_2",
            "weekday_3",
            "weekday_4",
            "weekday_5",
            "weekday_6",
        ]

        # Add entity type features
        entity_cols = [col for col in df.columns if col.startswith("entity_")]
        numerical_features.extend(entity_cols)

        # Add state features
        state_cols = [col for col in df.columns if col.startswith("state_")]
        numerical_features.extend(state_cols)

        # Add SVD features
        svd_cols = [
            col
            for col in df.columns
            if col.startswith(("company_svd_", "business_svd_"))
        ]
        numerical_features.extend(svd_cols)

        # Create feature matrix
        feature_matrix = df[numerical_features].fillna(0)

        # Scale features
        scaled_features = self.scaler.fit_transform(feature_matrix)

        # Store feature names
        self.feature_names = numerical_features

        logger.info(
            f"✅ Feature engineering completed: {scaled_features.shape[1]} features"
        )

        return scaled_features

    def save_models(self):
        """Save the fitted models and feature names."""
        try:
            # Save scaler
            with open(SCALER_MODEL, "wb") as f:
                pickle.dump(self.scaler, f)

            # Save feature names
            with open(FEATURE_NAMES, "wb") as f:
                pickle.dump(self.feature_names, f)

            # Save text pipeline
            if self.text_pipeline:
                with open(Path(SCALER_MODEL).parent / "text_pipeline.pkl", "wb") as f:
                    pickle.dump(self.text_pipeline, f)

            logger.info("💾 Models saved successfully")
        except Exception as e:
            logger.error(f"❌ Failed to save models: {e}")
            raise

    def transform_new_data(self, df: pd.DataFrame) -> np.ndarray:
        """Transform new data using fitted models."""
        # Apply the same feature engineering steps
        df = self.create_registration_features(df)
        df = self.create_temporal_features(df)
        df = self.create_geographic_features(df)
        df = self.create_text_features(df)

        # Select features and scale
        feature_matrix = df[self.feature_names].fillna(0)
        scaled_features = self.scaler.transform(feature_matrix)

        return scaled_features


def main():
    """Main function to run feature engineering."""
    try:
        from data_extractor import RegistryDataExtractor

        # Load data
        extractor = RegistryDataExtractor()
        df = extractor.extract_all_data(limit=10000)  # Limit for testing

        # Engineer features
        engineer = RegistryFeatureEngineer()
        features = engineer.engineer_features(df)

        # Save models
        engineer.save_models()

        # Save features
        features_df = pd.DataFrame(features, columns=engineer.feature_names)
        features_df.to_csv(FEATURES_CSV, index=False)

        logger.info("✅ Feature engineering pipeline completed!")

    except Exception as e:
        logger.error(f"❌ Feature engineering failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
