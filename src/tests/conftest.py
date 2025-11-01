"""
Test configuration and fixtures for Amazon Reviews pipeline tests
"""

import pytest
import json
from datetime import datetime
from unittest.mock import Mock, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import tempfile
import os


@pytest.fixture(scope="session")
def spark_session():
    """Create a test Spark session"""
    return (
        SparkSession.builder.appName("test-amazon-pipeline")
        .master("local[2]")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


@pytest.fixture
def sample_amazon_record():
    """Sample Amazon review record for testing"""
    return {
        "main_cat": "All Beauty",
        "title": "Sample Beauty Product",
        "average_rating": 4.2,
        "rating_number": 150,
        "features": ["Feature 1", "Feature 2", "Feature 3"],
        "description": ["Beautiful product", "Great quality"],
        "price": "$29.99",
        "images": ["image1.jpg", "image2.jpg"],
        "videos": ["video1.mp4"],
        "store": "Beauty Store",
        "categories": ["Beauty", "Skincare", "Face"],
        "details": '{"brand": "TestBrand", "model": "TestModel"}',
        "parent_asin": "B07ABC123",
        "bought_together": ["B07DEF456", "B07GHI789"],
        "subtitle": "Premium Beauty Product",
        "author": "Beauty Brand",
    }


@pytest.fixture
def sample_batch_data(sample_amazon_record):
    """Sample batch of records for testing"""
    batch = []
    for i in range(5):
        record = sample_amazon_record.copy()
        record["title"] = f"Product {i+1}"
        record["average_rating"] = round(3.5 + (i * 0.3), 1)
        record["rating_number"] = 100 + (i * 25)
        batch.append(record)
    return batch


@pytest.fixture
def mock_bigquery_client():
    """Mock BigQuery client for testing"""
    mock_client = Mock()
    mock_client.query.return_value = Mock()
    mock_client.get_table.return_value = Mock()
    return mock_client


@pytest.fixture
def pipeline_config():
    """Test configuration for pipeline"""
    return {
        "project_id": "test-project",
        "dataset_id": "test_dataset",
        "table_id": "test_table",
        "batch_size": 10,
        "max_batches": 2,
    }


@pytest.fixture
def temp_output_dir():
    """Temporary directory for test outputs"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture(autouse=True)
def setup_test_env():
    """Set up test environment variables"""
    original_env = os.environ.copy()
    os.environ.update({"TESTING": "true", "GCP_PROJECT": "test-project"})
    yield
    os.environ.clear()
    os.environ.update(original_env)
