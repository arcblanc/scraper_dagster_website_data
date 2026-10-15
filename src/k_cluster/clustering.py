"""K-means clustering module for Malaysian business registry records."""

import json
import logging
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import calinski_harabasz_score, silhouette_score

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).resolve().parent))
from config import (
    CLUSTER_MODEL,
    CLUSTER_PROFILES,
    MAX_ITER,
    N_CLUSTERS,
    N_INIT,
    RANDOM_STATE,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)


class BusinessClusterer:
    """K-means clustering for Malaysian business registry records."""

    def __init__(self):
        """Initialize the clusterer."""
        self.kmeans = KMeans(
            n_clusters=N_CLUSTERS,
            random_state=RANDOM_STATE,
            max_iter=MAX_ITER,
            n_init=N_INIT,
        )
        self.cluster_profiles = {}

    def fit(self, features: np.ndarray) -> None:
        """Fit the K-means clustering model."""
        logger.info(f"🎯 Fitting K-means with {N_CLUSTERS} clusters...")

        # Fit the model
        self.kmeans.fit(features)

        # Calculate cluster metrics
        labels = self.kmeans.labels_
        silhouette_avg = silhouette_score(features, labels)
        calinski_avg = calinski_harabasz_score(features, labels)

        logger.info("✅ Clustering completed!")
        logger.info(f"📊 Silhouette Score: {silhouette_avg:.3f}")
        logger.info(f"📊 Calinski-Harabasz Score: {calinski_avg:.3f}")

    def predict(self, features: np.ndarray) -> np.ndarray:
        """Predict cluster labels for new data."""
        return self.kmeans.predict(features)

    def get_cluster_centers(self) -> np.ndarray:
        """Get cluster centers."""
        return self.kmeans.cluster_centers_

    def create_cluster_profiles(self, df: pd.DataFrame, features: np.ndarray) -> dict:
        """Create detailed profiles for each cluster."""
        logger.info("📋 Creating cluster profiles...")

        # Get cluster labels
        labels = self.kmeans.labels_
        df_with_clusters = df.copy()
        df_with_clusters["cluster"] = labels

        profiles = {}

        for cluster_id in range(N_CLUSTERS):
            cluster_data = df_with_clusters[df_with_clusters["cluster"] == cluster_id]

            if len(cluster_data) == 0:
                continue

            # Basic statistics
            profile = {
                "cluster_id": cluster_id,
                "size": len(cluster_data),
                "percentage": len(cluster_data) / len(df) * 100,
                "avg_company_age": cluster_data["company_age"].mean(),
                "median_company_age": cluster_data["company_age"].median(),
                "age_std": cluster_data["company_age"].std(),
            }

            # Entity type distribution
            entity_cols = [
                col for col in cluster_data.columns if col.startswith("entity_")
            ]
            if entity_cols:
                entity_counts = (
                    cluster_data[entity_cols].sum().sort_values(ascending=False)
                )
                profile["top_entity_types"] = entity_counts.head(3).to_dict()

            # State distribution
            state_cols = [
                col for col in cluster_data.columns if col.startswith("state_")
            ]
            if state_cols:
                state_counts = (
                    cluster_data[state_cols].sum().sort_values(ascending=False)
                )
                profile["top_states"] = state_counts.head(5).to_dict()

            # Registration year distribution
            if "year_reg" in cluster_data.columns:
                profile["avg_registration_year"] = cluster_data["year_reg"].mean()
                profile["registration_year_range"] = {
                    "min": cluster_data["year_reg"].min(),
                    "max": cluster_data["year_reg"].max(),
                }

            # Sequence bucket distribution
            if "seq_bucket" in cluster_data.columns:
                profile["avg_seq_bucket"] = cluster_data["seq_bucket"].mean()
                profile["seq_bucket_range"] = {
                    "min": cluster_data["seq_bucket"].min(),
                    "max": cluster_data["seq_bucket"].max(),
                }

            # Source distribution
            if "source" in cluster_data.columns:
                source_counts = cluster_data["source"].value_counts()
                profile["source_distribution"] = source_counts.to_dict()

            # Sample company names
            if "company_name" in cluster_data.columns:
                sample_names = (
                    cluster_data["company_name"]
                    .sample(min(10, len(cluster_data)))
                    .tolist()
                )
                profile["sample_companies"] = sample_names

            # Sample business types
            if "nature_of_business" in cluster_data.columns:
                sample_businesses = (
                    cluster_data["nature_of_business"]
                    .sample(min(5, len(cluster_data)))
                    .tolist()
                )
                profile["sample_business_types"] = sample_businesses

            profiles[f"cluster_{cluster_id}"] = profile

        self.cluster_profiles = profiles
        return profiles

    def save_model(self) -> None:
        """Save the fitted clustering model."""
        try:
            with open(CLUSTER_MODEL, "wb") as f:
                pickle.dump(self.kmeans, f)
            logger.info(f"💾 Model saved to {CLUSTER_MODEL}")
        except Exception as e:
            logger.error(f"❌ Failed to save model: {e}")
            raise

    def save_profiles(self) -> None:
        """Save cluster profiles to JSON."""
        try:
            with open(CLUSTER_PROFILES, "w") as f:
                json.dump(self.cluster_profiles, f, indent=2, default=str)
            logger.info(f"💾 Profiles saved to {CLUSTER_PROFILES}")
        except Exception as e:
            logger.error(f"❌ Failed to save profiles: {e}")
            raise

    def load_model(self) -> None:
        """Load a fitted clustering model."""
        try:
            with open(CLUSTER_MODEL, "rb") as f:
                self.kmeans = pickle.load(f)
            logger.info(f"📂 Model loaded from {CLUSTER_MODEL}")
        except Exception as e:
            logger.error(f"❌ Failed to load model: {e}")
            raise

    def classify_new_record(self, record: dict) -> int:
        """Classify a new business record into a cluster."""
        # This would require the feature engineering pipeline
        # For now, return a placeholder
        logger.warning("⚠️ This method requires feature engineering pipeline")
        return 0


def main():
    """Main function to run clustering."""
    try:
        from data_extractor import RegistryDataExtractor
        from feature_engineering import RegistryFeatureEngineer

        # Load and engineer data
        logger.info("🔄 Loading data...")
        extractor = RegistryDataExtractor()
        df = extractor.extract_all_data(limit=10000)  # Limit for testing

        logger.info("🔧 Engineering features...")
        engineer = RegistryFeatureEngineer()
        features = engineer.engineer_features(df)

        # Perform clustering
        logger.info("🎯 Starting clustering...")
        clusterer = BusinessClusterer()
        clusterer.fit(features)

        # Create and save profiles
        logger.info("📋 Creating cluster profiles...")
        profiles = clusterer.create_cluster_profiles(df, features)

        # Save results
        clusterer.save_model()
        clusterer.save_profiles()

        # Print summary
        logger.info("📊 Clustering Results Summary:")
        for cluster_id, profile in profiles.items():
            logger.info(
                f"  {cluster_id}: {profile['size']} companies ({profile['percentage']:.1f}%)"
            )

        logger.info("✅ Clustering pipeline completed!")

    except Exception as e:
        logger.error(f"❌ Clustering failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
