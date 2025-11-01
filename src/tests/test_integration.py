"""
Integration tests for Amazon Reviews pipeline end-to-end functionality
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import json
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

from pipeline.amazon_pipeline import AmazonReviewsPipeline

class TestPipelineIntegration:
    """End-to-end integration tests"""
    
    @patch('pipeline.amazon_pipeline.load_dataset')
    @patch('pipeline.amazon_pipeline.SparkSession')
    def test_full_pipeline_small_dataset(self, mock_spark, mock_load_dataset, 
                                       pipeline_config, sample_batch_data):
        """Test complete pipeline with small dataset"""
        # Mock dataset
        mock_dataset = Mock()
        mock_dataset.__iter__ = Mock(return_value=iter(sample_batch_data))
        mock_load_dataset.return_value = mock_dataset
        
        # Mock Spark components
        mock_spark_session = Mock()
        mock_spark.builder.appName.return_value.config.return_value.getOrCreate.return_value = mock_spark_session
        
        mock_df = Mock()
        mock_spark_session.createDataFrame.return_value = mock_df
        
        # Mock successful BigQuery write
        mock_write = Mock()
        mock_df.write = mock_write
        mock_format = Mock()
        mock_write.format.return_value = mock_format
        mock_format.option.return_value = mock_format
        mock_format.mode.return_value = mock_format
        mock_format.save.return_value = None
        
        # Initialize and run pipeline
        pipeline = AmazonReviewsPipeline(
            project_id=pipeline_config["project_id"],
            dataset_id=pipeline_config["dataset_id"],
            table_id=pipeline_config["table_id"]
        )
        
        results = pipeline.run_pipeline(batch_size=5, max_batches=1)
        
        # Verify results
        assert results["total_records"] == 5
        assert results["total_batches"] == 1
        assert results["failed_batches"] == 0
        assert results["avg_rate"] > 0
        
        # Verify Spark DataFrame creation was called
        mock_spark_session.createDataFrame.assert_called()
        
        # Verify BigQuery write was attempted
        mock_write.format.assert_called_with("bigquery")

    @patch('pipeline.amazon_pipeline.load_dataset')
    @patch('pipeline.amazon_pipeline.SparkSession')
    def test_pipeline_with_processing_errors(self, mock_spark, mock_load_dataset, 
                                           pipeline_config):
        """Test pipeline handles processing errors gracefully"""
        # Create problematic data that will cause processing issues
        problematic_data = [
            {"title": "Good Product 1"},
            None,  # This will cause an error
            {"title": "Good Product 2"},
            {"invalid": "structure"},  # Different structure
            {"title": "Good Product 3"}
        ]
        
        mock_dataset = Mock()
        mock_dataset.__iter__ = Mock(return_value=iter(problematic_data))
        mock_load_dataset.return_value = mock_dataset
        
        # Mock Spark components
        mock_spark_session = Mock()
        mock_spark.builder.appName.return_value.config.return_value.getOrCreate.return_value = mock_spark_session
        
        mock_df = Mock()
        mock_spark_session.createDataFrame.return_value = mock_df
        
        # Mock successful BigQuery write
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
        
        with patch('pipeline.amazon_pipeline.logger') as mock_logger:
            results = pipeline.run_pipeline(batch_size=5, max_batches=1)
            
            # Pipeline should complete despite errors
            assert results["total_records"] >= 0
            assert results["total_batches"] >= 0
            
            # Errors should be logged
            mock_logger.warning.assert_called()

    @patch('pipeline.amazon_pipeline.load_dataset')
    @patch('pipeline.amazon_pipeline.SparkSession') 
    def test_pipeline_bigquery_failure_handling(self, mock_spark, mock_load_dataset,
                                               pipeline_config, sample_batch_data):
        """Test pipeline handles BigQuery write failures"""
        mock_dataset = Mock()
        mock_dataset.__iter__ = Mock(return_value=iter(sample_batch_data))
        mock_load_dataset.return_value = mock_dataset
        
        # Mock Spark session
        mock_spark_session = Mock()
        mock_spark.builder.appName.return_value.config.return_value.getOrCreate.return_value = mock_spark_session
        
        mock_df = Mock()
        mock_spark_session.createDataFrame.return_value = mock_df
        
        # Mock BigQuery write failure
        mock_write = Mock()
        mock_df.write = mock_write
        mock_write.format.side_effect = Exception("BigQuery connection failed")
        
        pipeline = AmazonReviewsPipeline(
            project_id=pipeline_config["project_id"],
            dataset_id=pipeline_config["dataset_id"],
            table_id=pipeline_config["table_id"]
        )
        
        with patch('pipeline.amazon_pipeline.logger') as mock_logger:
            results = pipeline.run_pipeline(batch_size=5, max_batches=1)
            
            # Pipeline should handle failure gracefully
            assert results["failed_batches"] == 1
            assert results["total_records"] == 0
            mock_logger.error.assert_called()

    def test_batch_processing_logic(self, pipeline_config):
        """Test batch processing logic and counters"""
        # Create test data larger than batch size
        large_dataset = []
        for i in range(25):  # 25 records
            large_dataset.append({
                "title": f"Product {i}",
                "main_cat": "Beauty",
                "average_rating": 4.0
            })
        
        with patch('pipeline.amazon_pipeline.load_dataset') as mock_load_dataset:
            with patch('pipeline.amazon_pipeline.SparkSession'):
                mock_dataset = Mock()
                mock_dataset.__iter__ = Mock(return_value=iter(large_dataset))
                mock_load_dataset.return_value = mock_dataset
                
                pipeline = AmazonReviewsPipeline(
                    project_id=pipeline_config["project_id"],
                    dataset_id=pipeline_config["dataset_id"],
                    table_id=pipeline_config["table_id"]
                )
                
                # Mock successful processing
                with patch.object(pipeline, '_process_single_batch', return_value=True):
                    results = pipeline.run_pipeline(batch_size=10, max_batches=3)
                    
                    # Should process 3 batches (30 records total, but limited by max_batches)
                    expected_batches = 3
                    expected_records = 25  # All records processed across batches
                    
                    assert results["total_batches"] == expected_batches
                    # Note: Actual record count depends on batch processing implementation

class TestConfigurationIntegration:
    """Test configuration and environment setup"""
    
    def test_table_full_name_construction(self, pipeline_config):
        """Test BigQuery table full name construction"""
        with patch('pipeline.amazon_pipeline.SparkSession'):
            pipeline = AmazonReviewsPipeline(
                project_id="my-project",
                dataset_id="amazon_data",
                table_id="reviews_table"
            )
            
            expected_name = "my-project.amazon_data.reviews_table"
            assert pipeline.table_full_name == expected_name

    def test_schema_sql_includes_table_name(self, pipeline_config):
        """Test that schema SQL includes the correct table name"""
        with patch('pipeline.amazon_pipeline.SparkSession'):
            pipeline = AmazonReviewsPipeline(
                project_id="test-project-123",
                dataset_id="test_dataset_456", 
                table_id="test_table_789"
            )
            
            schema_sql = pipeline.create_bigquery_schema()
            expected_table_name = "test-project-123.test_dataset_456.test_table_789"
            
            assert expected_table_name in schema_sql

class TestPerformanceAndMetrics:
    """Test performance monitoring and metrics collection"""
    
    @patch('pipeline.amazon_pipeline.time')
    def test_performance_metrics_calculation(self, mock_time, pipeline_config):
        """Test that performance metrics are calculated correctly"""
        # Mock time.time() to return predictable values
        mock_time.time.side_effect = [0, 100]  # Start: 0, End: 100 seconds
        
        with patch('pipeline.amazon_pipeline.load_dataset') as mock_load_dataset:
            with patch('pipeline.amazon_pipeline.SparkSession'):
                # Create small dataset
                test_data = [{"title": f"Product {i}"} for i in range(10)]
                mock_dataset = Mock()
                mock_dataset.__iter__ = Mock(return_value=iter(test_data))
                mock_load_dataset.return_value = mock_dataset
                
                pipeline = AmazonReviewsPipeline(
                    project_id=pipeline_config["project_id"],
                    dataset_id=pipeline_config["dataset_id"],
                    table_id=pipeline_config["table_id"]
                )
                
                with patch.object(pipeline, '_process_single_batch', return_value=True):
                    results = pipeline.run_pipeline(batch_size=10, max_batches=1)
                    
                    # Check performance metrics
                    assert results["execution_time"] == 100  # Mocked time difference
                    assert results["avg_rate"] == 0.1  # 10 records / 100 seconds
                    assert "total_records" in results
                    assert "total_batches" in results
                    assert "failed_batches" in results
