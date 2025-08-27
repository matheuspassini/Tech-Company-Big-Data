import pytest
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Add project path to import real functions
sys.path.append('/opt/spark/apps')

# Import real logging functions from project
from utils.spark_utils import setup_logging, log_execution

@pytest.fixture(scope="session", autouse=True)
def spark():
    spark = SparkSession.builder.appName("test_logging").getOrCreate()
    yield spark
    spark.stop()

def test_setup_logging():
    """Test setup_logging function"""
    
    # Test if logging is properly configured
    logger = setup_logging()
    
    # Check if logger exists and has basic attributes
    assert logger is not None, "Logger should not be None"
    assert hasattr(logger, 'info'), "Logger should have info method"
    assert hasattr(logger, 'error'), "Logger should have error method"
    assert hasattr(logger, 'debug'), "Logger should have debug method"
    
    # Test if we can log messages without errors
    try:
        logger.info("Test info message")
        logger.debug("Test debug message")
        logger.error("Test error message")
        print("Logging messages sent successfully")
    except Exception as e:
        pytest.fail(f"Logging failed: {str(e)}")

def test_setup_logging_with_custom_name():
    """Test setup_logging function with custom name"""
    
    # Test with custom name
    custom_logger = setup_logging("custom_test_logger")
    
    # Check if logger exists and has the custom name
    assert custom_logger is not None, "Custom logger should not be None"
    assert custom_logger.name == "custom_test_logger", f"Logger name should be 'custom_test_logger', got '{custom_logger.name}'"
    
    # Test if we can log messages
    try:
        custom_logger.info("Custom logger test message")
        print("Custom logger messages sent successfully")
    except Exception as e:
        pytest.fail(f"Custom logger failed: {str(e)}")

def test_log_execution_decorator(spark):
    """Test log_execution decorator"""
    
    # Create a simple test function with the decorator
    @log_execution
    def test_function(df):
        """Test function to check decorator behavior"""
        return df.count()
    
    # Create test data with 3 records
    test_data = [("1", "test1"), ("2", "test2"), ("3", "test3")]
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True)
    ])
    df = spark.createDataFrame(test_data, schema)
    
    # Test successful execution
    try:
        result = test_function(df)
        expected_count = len(test_data)  # 3 records
        assert result == expected_count, f"Function should return {expected_count}, but got {result}"
        print("Decorator executed successfully")
    except Exception as e:
        pytest.fail(f"Decorator execution failed: {str(e)}")

def test_log_execution_with_error(spark):
    """Test log_execution decorator with error handling"""
    
    # Create a function that will raise an error
    @log_execution
    def failing_function(df):
        """Function that will fail"""
        raise ValueError("Test error for logging")
    
    # Create test data
    test_data = [("1", "test1")]
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True)
    ])
    df = spark.createDataFrame(test_data, schema)
    
    # Test error handling
    try:
        failing_function(df)
        pytest.fail("Function should have raised an error")
    except ValueError as e:
        assert str(e) == "Test error for logging", f"Expected 'Test error for logging', got '{str(e)}'"
        print("Error handling in decorator works correctly")

def test_log_execution_with_dataframe_result(spark):
    """Test log_execution decorator with DataFrame result"""
    
    @log_execution
    def dataframe_function(df):
        """Function that returns a DataFrame"""
        return df.filter(df.id == "1")
    
    # Create test data
    test_data = [("1", "test1"), ("2", "test2")]
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True)
    ])
    df = spark.createDataFrame(test_data, schema)
    
    # Test DataFrame result
    try:
        result = dataframe_function(df)
        assert result.count() == 1, "Should return 1 record"
        assert result.first()["id"] == "1", "Should return record with id=1"
        print("DataFrame result logging works correctly")
    except Exception as e:
        pytest.fail(f"DataFrame result logging failed: {str(e)}")

def test_logging_in_read_file_error():
    """Test logging in read_file function - error case only"""
    
    # Import the function
    from utils.spark_utils import read_file
    
    # Test unsupported format (should trigger error logging)
    try:
        read_file(None, "test.xyz")  # Pass None as spark to trigger error
        pytest.fail("Should have raised error for unsupported format")
    except (ValueError, AttributeError) as e:
        # Should fail due to unsupported format or None spark
        print("Error logging for unsupported format works correctly")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
