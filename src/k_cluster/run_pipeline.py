"""Main pipeline script for Malaysian business registry clustering."""

import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).resolve().parent))

from clustering import BusinessClusterer
from config import validate_config
from data_extractor import RegistryDataExtractor
from feature_engineering import RegistryFeatureEngineer

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)


def run_full_pipeline(limit: int = None):
    """Run the complete clustering pipeline."""
    try:
        logger.info("🚀 Starting Malaysian Business Registry Clustering Pipeline")

        # Step 1: Validate configuration
        logger.info("📋 Step 1: Validating configuration...")
        validate_config()
        logger.info("✅ Configuration validated")

        # Step 2: Extract data from PostgreSQL
        logger.info("📊 Step 2: Extracting data from PostgreSQL...")
        extractor = RegistryDataExtractor()
        df = extractor.extract_all_data(limit=limit)
        logger.info(f"✅ Extracted {len(df)} records")

        # Step 3: Feature engineering
        logger.info("🔧 Step 3: Engineering features...")
        engineer = RegistryFeatureEngineer()
        features = engineer.engineer_features(df)
        engineer.save_models()
        logger.info(f"✅ Engineered {features.shape[1]} features")

        # Step 4: Clustering
        logger.info("🎯 Step 4: Performing K-means clustering...")
        clusterer = BusinessClusterer()
        clusterer.fit(features)

        # Step 5: Create cluster profiles
        logger.info("📋 Step 5: Creating cluster profiles...")
        profiles = clusterer.create_cluster_profiles(df, features)

        # Step 6: Save results
        logger.info("💾 Step 6: Saving results...")
        clusterer.save_model()
        clusterer.save_profiles()

        # Step 7: Print summary
        logger.info("📊 Step 7: Pipeline Results Summary")
        logger.info("=" * 50)
        logger.info(f"Total records processed: {len(df)}")
        logger.info(f"Features created: {features.shape[1]}")
        logger.info(f"Clusters: {len(profiles)}")

        for cluster_id, profile in profiles.items():
            logger.info(
                f"  {cluster_id}: {profile['size']} companies ({profile['percentage']:.1f}%)"
            )
            logger.info(f"    Avg age: {profile['avg_company_age']:.1f} years")
            if "sample_companies" in profile:
                logger.info(f"    Sample: {profile['sample_companies'][0]}")

        logger.info("=" * 50)
        logger.info("✅ Pipeline completed successfully!")

        return True

    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        return False


def main():
    """Main function."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Malaysian Business Registry Clustering"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of records for testing (default: all records)",
    )
    parser.add_argument(
        "--test", action="store_true", help="Run with 1000 records for testing"
    )

    args = parser.parse_args()

    # Set limit for testing
    limit = args.limit
    if args.test:
        limit = 1000
        logger.info("🧪 Running in test mode with 1000 records")

    # Run pipeline
    success = run_full_pipeline(limit=limit)

    if success:
        logger.info("🎉 Pipeline completed successfully!")
        sys.exit(0)
    else:
        logger.error("💥 Pipeline failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
