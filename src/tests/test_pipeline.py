"""
Unit tests for Amazon Reviews pipeline components
"""
import pytest
import json
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql.types import *
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

from src.pipeline.amazon_pipeline import AmazonReviewsPipeline

class TestAmazonReviewsPipeline:
    """Test cases for AmazonReviewsPipeline class"""
    
    def test_pipeline_initialization(self, pipeline_config):
        """Test pipeline initialization with correct parameters"""
        with patch('src.pipeline.amazon_pipeline.SparkSession'):
            pipeline = AmazonReviewsPipeline(
                project_id=pipeline_config["project_id"],
                dataset_id=pipeline_config["dataset_id"], 
                table_id=pipeline_config["table_id"]
            )
            
            assert pipeline.project_id == "test-project"
            assert pipeline.dataset_id == "test_dataset"
            assert pipeline.table_id == "test_table"
            assert pipeline.table_full_name == "test-project.test_dataset.test_table"

    def test_spark_session_creation(self, pipeline_config):
        """Test Spark session is created with correct configuration"""
        with patch('src.pipeline.amazon_pipeline.SparkSession') as mock_spark:
            mock_builder = Mock()
            mock_spark.builder = mock_builder
            mock_builder.appName.return_value = mock_builder
            mock_builder.config.return_value = mock_builder
            mock_builder.getOrCreate.return_value = Mock()
            
            pipeline = AmazonReviewsPipeline(
                project_id=pipeline_config["project_id"],
                dataset_id=pipeline_config["dataset_id"],
                table_id=pipeline_config["table_id"]
            )
            
            # Verify Spark session configuration calls
            mock_builder.appName.assert_called_with("Amazon-Reviews-to-BigQuery")
            assert mock_builder.config.call_count >= 4  # Multiple config calls

    def test_bigquery_schema_creation(self, pipeline_config):
        """Test BigQuery schema SQL generation"""
        with patch('src.pipeline.amazon_pipeline.SparkSession'):
            pipeline = AmazonReviewsPipeline(
                project_id=pipeline_config["project_id"],
                dataset_id=pipeline_config["dataset_id"],
                table_id=pipeline_config["table_id"]
            )
            
            schema_sql = pipeline.create_bigquery_schema()
            
            # Verify schema contains required elements
            assert "CREATE TABLE IF NOT EXISTS" in schema_sql
            assert pipeline.table_full_name in schema_sql
            assert "main_cat STRING" in schema_sql
            assert "average_rating FLOAT64" in schema_sql
            assert "rating_number INT64" in schema_sql
            assert "PARTITION BY ingestion_date" in schema_sql
            assert "CLUSTER BY main_cat, store" in schema_sql

    def test_process_batch_data_valid_input(self, pipeline_config, sample_batch_data):
        """Test batch data processing with valid input"""
        with patch('src.pipeline.amazon_pipeline.SparkSession'):
            pipeline = AmazonReviewsPipeline(
                project_id=pipeline_config["project_id"],
                dataset_id=pipeline_config["dataset_id"],
                table_id=pipeline_config["table_id"]
            )
            
            processed_batch = pipeline.process_batch_data(sample_batch_data)
            
            assert len(processed_batch) == 5
            
            # Verify first record processing
            first_record = processed_batch[0]
            assert first_record["main_cat"] == "All Beauty"
            assert first_record["title"] == "Product 1"
            assert first_record["average_rating"] == 3.5
            assert first_record["rating_number"] == 100
            assert isinstance(first_record["features"], list)
            assert "ingestion_timestamp" in first_record
            assert "ingestion_date" in first_record

    def test_process_batch_data_handles_nulls(self, pipeline_config):
        """Test batch data processing handles null values correctly"""
        with patch('src.pipeline.amazon_pipeline.SparkSession'):
            pipeline = AmazonReviewsPipeline(
                project_id=pipeline_config["project_id"],
                dataset_id=pipeline_config["dataset_id"],
                table_id=pipeline_config["table_id"]
            )
            
            # Test data with null values
            test_data = [{
                "main_cat": None,
                "title": "Test Product",
                "average_rating": None,
                "rating_number": None,
                "features": None,
                "description": None
            }]
            
            processed_batch = pipeline.process_batch_data(test_data)
            
            assert len(processed_batch) == 1
            record = processed_batch[0]
            assert record["main_cat"] is None
            assert record["title"] == "Test Product"
            assert record["average_rating"] is None
            assert record["rating_number"] is None
            assert record["features"] == []  # Empty list for null arrays

    def test_process_batch_data_handles_complex_types(self, pipeline_config):
        """Test processing of complex nested data structures"""
        with patch('src.pipeline.amazon_pipeline.SparkSession'):
            pipeline = AmazonReviewsPipeline(
                project_id=pipeline_config["project_id"],
                dataset_id=pipeline_config["dataset_id"],
                table_id=pipeline_config["table_id"]
            )
            
            # Test data with complex nested structures
            test_data = [{
                "title": "Complex Product",
                "details": {"brand": "TestBrand", "specs": {"weight": "1kg", "color": "blue"}},
                "metadata": ["tag1", "tag2"],
                "features": ["feature1", "feature2"]
            }]
            
            processed_batch = pipeline.process_batch_data(test_data)
            
            record = processed_batch[0]
            # Complex objects should be JSON serialized
            assert isinstance(record["details"], str)
            assert "TestBrand" in record["details"]
            # Arrays should remain as arrays
            assert isinstance(record["features"], list)
            assert record["features"] == ["feature1", "feature2"]

    def test_process_batch_data_error_handling(self, pipeline_config):
        """Test batch processing handles errors gracefully"""
        with patch('src.pipeline.amazon_pipeline.SparkSession'):
            pipeline = AmazonReviewsPipeline(
                project_id=pipeline_config["project_id"],
                dataset_id=pipeline_config["dataset_id"],
                table_id=pipeline_config["table_id"]
            )
            
            # Test data that might cause processing errors
            problematic_data = [
                {"title": "Good Product"},  # Valid record
                None,  # This should cause an error
                {"title": "Another Good Product"}  # Valid record
            ]
            
            with patch('src.pipeline.amazon_pipeline.logger') as mock_logger:
                processed_batch = pipeline.process_batch_data(problematic_data)
                
                # Should process valid records and skip problematic ones
                assert len(processed_batch) == 2
                mock_logger.warning.assert_called()

    @patch('src.pipeline.amazon_pipeline.SparkSession')
    def test_write_to_bigquery_success(self, mock_spark, pipeline_config):
        """Test successful BigQuery write operation"""
        # Setup mocks
        mock_df = Mock()
        mock_write = Mock()
        mock_df.write = mock_write
        mock_format = Mock()
        mock_write.format.return_value = mock_format
        mock_format.option.return_value = mock_format
        mock_format.mode.return_value = mock_format
        mock_format.save.return_value = None
        
        pipeline = AmazonReviewsPipeline(
            project_id=pipeline_config["project_id"],
            dataset_id=pipeline_config["dataset_id"],
            table_id=pipeline_config["table_id"]
        )
        
        result = pipeline.write_to_bigquery(mock_df)
        
        assert result is True
        mock_write.format.assert_called_with("bigquery")
        mock_format.save.assert_called()

    @patch('src.pipeline.amazon_pipeline.SparkSession')  
    def test_write_to_bigquery_failure(self, mock_spark, pipeline_config):
        """Test BigQuery write operation failure handling"""
        # Setup mocks to raise exception
        mock_df = Mock()
        mock_write = Mock()
        mock_df.write = mock_write
        mock_write.format.side_effect = Exception("BigQuery connection failed")
        
        pipeline = AmazonReviewsPipeline(
            project_id=pipeline_config["project_id"],
            dataset_id=pipeline_config["dataset_id"],
            table_id=pipeline_config["table_id"]
        )
        
        with patch('src.pipeline.amazon_pipeline.logger') as mock_logger:
            result = pipeline.write_to_bigquery(mock_df)
            
            assert result is False
            mock_logger.error.assert_called()

    def test_cleanup(self, pipeline_config):
        """Test resource cleanup"""
        with patch('src.pipeline.amazon_pipeline.SparkSession') as mock_spark_session:
            mock_spark = Mock()
            mock_spark_session.builder.appName.return_value.config.return_value.getOrCreate.return_value = mock_spark
            
            pipeline = AmazonReviewsPipeline(
                project_id=pipeline_config["project_id"],
                dataset_id=pipeline_config["dataset_id"],
                table_id=pipeline_config["table_id"]
            )
            
            pipeline.cleanup()
            mock_spark.stop.assert_called_once()

class TestDataValidation:
    """Test data validation and type checking"""
    
    def test_numeric_conversion(self):
        """Test numeric data type conversions"""
        from src.pipeline.amazon_pipeline import AmazonReviewsPipeline
        
        with patch('src.pipeline.amazon_pipeline.SparkSession'):
            pipeline = AmazonReviewsPipeline("test", "test", "test")
            
            test_records = [
                {"average_rating": "4.5", "rating_number": "150"},  # String numbers
                {"average_rating": 4.2, "rating_number": 100},      # Proper numbers
                {"average_rating": None, "rating_number": None}     # Null values
            ]
            
            processed = pipeline.process_batch_data(test_records)
            
            # Verify type conversions
            assert processed[0]["average_rating"] == 4.5
            assert processed[0]["rating_number"] == 150
            assert processed[1]["average_rating"] == 4.2
            assert processed[1]["rating_number"] == 100
            assert processed[2]["average_rating"] is None
            assert processed[2]["rating_number"] is None

    def test_timestamp_addition(self):
        """Test automatic timestamp addition"""
        from src.pipeline.amazon_pipeline import AmazonReviewsPipeline
        
        with patch('src.pipeline.amazon_pipeline.SparkSession'):
            pipeline = AmazonReviewsPipeline("test", "test", "test")
            
            test_record = [{"title": "Test Product"}]
            processed = pipeline.process_batch_data(test_record)
            
            record = processed[0] 
            assert "ingestion_timestamp" in record
            assert "ingestion_date" in record
            assert isinstance(record["ingestion_timestamp"], datetime)
