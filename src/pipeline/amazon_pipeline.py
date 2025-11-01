"""
Amazon Reviews to BigQuery Pipeline
Production-scale data processing using Apache Spark on Google Cloud Dataproc

Author: Anmol Srinivas
GitHub: https://github.com/SDM13297/amazon-reviews-bigquery
"""

import argparse
import logging
import time
import json
from datetime import datetime
from typing import Dict, Any, List

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datasets import load_dataset

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class AmazonReviewsPipeline:
    """
    Main pipeline class for processing Amazon Reviews data
    """

    def __init__(self, project_id: str, dataset_id: str, table_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.table_full_name = f"{project_id}.{dataset_id}.{table_id}"

        # Initialize Spark Session
        self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session with BigQuery connector"""

        spark = (
            SparkSession.builder.appName("Amazon-Reviews-to-BigQuery")
            .config(
                "spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
            )
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        return spark

    def create_bigquery_schema(self) -> str:
        """Define BigQuery table schema"""

        schema_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.table_full_name}` (
            main_cat STRING,
            title STRING,
            average_rating FLOAT64,
            rating_number INT64,
            features ARRAY<STRING>,
            description ARRAY<STRING>,
            price STRING,
            images ARRAY<STRING>,
            videos ARRAY<STRING>,
            store STRING,
            categories ARRAY<STRING>,
            details STRING,
            parent_asin STRING,
            bought_together ARRAY<STRING>,
            subtitle STRING,
            author STRING,
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            ingestion_date DATE DEFAULT CURRENT_DATE()
        )
        PARTITION BY ingestion_date
        CLUSTER BY main_cat, store
        OPTIONS (
            description="Amazon Beauty Products Metadata - Processed via Spark Pipeline",
            labels=[("environment", "production"), ("source", "huggingface")]
        )
        """

        logger.info(f"BigQuery schema defined for table: {self.table_full_name}")
        return schema_sql

    def process_batch_data(
        self, batch_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Process a batch of raw data for BigQuery compatibility
        """
        processed_batch = []

        for record in batch_data:
            try:
                processed_record = {}

                # Process each field with proper type handling
                for key, value in record.items():
                    if key in [
                        "features",
                        "categories",
                        "images",
                        "videos",
                        "description",
                        "bought_together",
                    ]:
                        # Keep arrays as arrays for BigQuery
                        processed_record[key] = value if isinstance(value, list) else []
                    elif isinstance(value, (dict, list)) and key not in [
                        "features",
                        "categories",
                        "images",
                        "videos",
                        "description",
                        "bought_together",
                    ]:
                        # Convert complex nested structures to JSON strings
                        processed_record[key] = json.dumps(value) if value else None
                    elif key in ["average_rating"]:
                        # Handle numeric fields
                        processed_record[key] = (
                            float(value) if value is not None else None
                        )
                    elif key in ["rating_number"]:
                        processed_record[key] = (
                            int(value) if value is not None else None
                        )
                    else:
                        # Handle string fields
                        processed_record[key] = (
                            str(value) if value is not None else None
                        )

                # Add metadata fields
                processed_record["ingestion_timestamp"] = datetime.now()
                processed_record["ingestion_date"] = datetime.now().date()

                processed_batch.append(processed_record)

            except Exception as e:
                logger.warning(f"Error processing record: {str(e)}")
                continue

        return processed_batch

    def write_to_bigquery(self, df, write_mode: str = "append"):
        """Write Spark DataFrame to BigQuery"""

        try:
            df.write.format("bigquery").option("writeMethod", "direct").option(
                "table", self.table_full_name
            ).option(
                "writeDisposition",
                "WRITE_APPEND" if write_mode == "append" else "WRITE_TRUNCATE",
            ).option(
                "createDisposition", "CREATE_IF_NEEDED"
            ).mode(
                write_mode
            ).save()

            return True

        except Exception as e:
            logger.error(f"Error writing to BigQuery: {str(e)}")
            return False

    def run_pipeline(self, batch_size: int = 10000, max_batches: int = None):
        """
        Main pipeline execution method
        """
        logger.info("Starting Amazon Reviews to BigQuery pipeline")
        logger.info(f"Target table: {self.table_full_name}")
        logger.info(f"Batch size: {batch_size}")
        logger.info(f"Max batches: {max_batches if max_batches else 'unlimited'}")

        # Load HuggingFace dataset in streaming mode
        dataset = load_dataset(
            "mcauley-lab/amazon-reviews-2023",
            "raw_meta_All_Beauty",
            split="full",
            streaming=True,
        )

        # Initialize counters
        batch_count = 0
        total_records = 0
        failed_batches = 0
        start_time = time.time()
        batch_data = []

        logger.info("Dataset loaded successfully. Starting processing...")

        try:
            for record in dataset:
                batch_data.append(record)

                # Process batch when it reaches batch_size
                if len(batch_data) >= batch_size:
                    success = self._process_single_batch(
                        batch_data, batch_count + 1, total_records
                    )

                    if success:
                        total_records += len(batch_data)
                        batch_count += 1
                    else:
                        failed_batches += 1

                    # Clear batch
                    batch_data = []

                    # Progress logging
                    if batch_count % 10 == 0:
                        elapsed_time = time.time() - start_time
                        rate = total_records / elapsed_time if elapsed_time > 0 else 0
                        logger.info(
                            f"Progress: {batch_count} batches, {total_records:,} records, "
                            f"{rate:.1f} records/sec, {failed_batches} failed batches"
                        )

                    # Stop if max_batches reached
                    if max_batches and batch_count >= max_batches:
                        logger.info(f"Reached max_batches limit: {max_batches}")
                        break

            # Process remaining records
            if batch_data:
                success = self._process_single_batch(
                    batch_data, batch_count + 1, total_records
                )
                if success:
                    total_records += len(batch_data)
                    batch_count += 1
                else:
                    failed_batches += 1

        except Exception as e:
            logger.error(f"Pipeline execution error: {str(e)}")
            raise

        # Final statistics
        total_time = time.time() - start_time
        avg_rate = total_records / total_time if total_time > 0 else 0

        logger.info("=" * 50)
        logger.info("PIPELINE EXECUTION COMPLETED")
        logger.info(f"Total batches processed: {batch_count}")
        logger.info(f"Total records processed: {total_records:,}")
        logger.info(f"Failed batches: {failed_batches}")
        logger.info(
            f"Success rate: {((batch_count-failed_batches)/batch_count*100):.1f}%"
        )
        logger.info(f"Total execution time: {total_time:.1f} seconds")
        logger.info(f"Average processing rate: {avg_rate:.1f} records/second")
        logger.info("=" * 50)

        return {
            "total_records": total_records,
            "total_batches": batch_count,
            "failed_batches": failed_batches,
            "execution_time": total_time,
            "avg_rate": avg_rate,
        }

    def _process_single_batch(
        self, batch_data: List[Dict], batch_num: int, total_so_far: int
    ) -> bool:
        """Process a single batch of data"""

        try:
            # Process batch for BigQuery compatibility
            processed_data = self.process_batch_data(batch_data)

            if not processed_data:
                logger.warning(f"Batch {batch_num}: No valid records to process")
                return False

            # Create Spark DataFrame
            df = self.spark.createDataFrame(processed_data)

            # Write to BigQuery
            success = self.write_to_bigquery(df)

            if success:
                logger.info(
                    f"Batch {batch_num}: Successfully processed {len(batch_data)} records. "
                    f"Total: {total_so_far + len(batch_data):,}"
                )
                return True
            else:
                logger.error(f"Batch {batch_num}: Failed to write to BigQuery")
                return False

        except Exception as e:
            logger.error(f"Batch {batch_num}: Processing failed - {str(e)}")

            # Save failed batch for debugging
            try:
                with open(
                    f"failed_batch_{batch_num}_{int(time.time())}.json", "w"
                ) as f:
                    json.dump(
                        batch_data[:5], f, indent=2, default=str
                    )  # Save first 5 records
                logger.info(f"Failed batch data saved for debugging")
            except:
                pass

            return False

    def cleanup(self):
        """Clean up resources"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main entry point"""

    parser = argparse.ArgumentParser(description="Amazon Reviews to BigQuery Pipeline")
    parser.add_argument("--project-id", required=True, help="GCP Project ID")
    parser.add_argument(
        "--dataset-id", default="amazon_reviews", help="BigQuery dataset ID"
    )
    parser.add_argument(
        "--table-id", default="beauty_metadata", help="BigQuery table ID"
    )
    parser.add_argument(
        "--batch-size", type=int, default=10000, help="Batch size for processing"
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        help="Maximum number of batches to process (for testing)",
    )

    args = parser.parse_args()

    # Initialize pipeline
    pipeline = AmazonReviewsPipeline(
        project_id=args.project_id, dataset_id=args.dataset_id, table_id=args.table_id
    )

    try:
        # Run pipeline
        results = pipeline.run_pipeline(
            batch_size=args.batch_size, max_batches=args.max_batches
        )

        logger.info("Pipeline completed successfully!")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

    finally:
        # Cleanup
        pipeline.cleanup()


if __name__ == "__main__":
    main()
