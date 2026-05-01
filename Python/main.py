from kaggle_to_sql import run_kaggle_pipeline
from pl_manager_scd import run_scd_pipeline


def main():
    try:
        print("🚀 Starting ETL pipeline...")

        run_kaggle_pipeline()
        run_scd_pipeline()

        print("✅ ETL pipeline completed successfully")

    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()