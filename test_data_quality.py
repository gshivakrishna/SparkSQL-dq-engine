import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, count, when, isnan, isnull
from pyspark.testing.utils import assertDataFrameEqual

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing"""
    return SparkSession.builder \
        .appName("Data Quality Tests") \
        .master("local[*]") \
        .getOrCreate()

@pytest.fixture(scope="session")
def employee_schema():
    """Define the expected schema for employee data"""
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("salary", DoubleType(), True)
    ])

def test_schema_validation(spark, employee_schema):
    """Test if the data matches the expected schema"""
    df = spark.read.csv("src/main/resources/employees.csv", 
                       header=True, 
                       schema=employee_schema)
    
    # Check if schema matches
    assert df.schema == employee_schema, "Schema validation failed"

def test_null_values(spark, employee_schema):
    """Test for null values in the dataset"""
    df = spark.read.csv("src/main/resources/employees.csv", 
                       header=True, 
                       schema=employee_schema)
    
    # Count null values in each column
    null_counts = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns])
    
    # Assert no null values in any column
    for column in df.columns:
        assert null_counts.first()[column] == 0, f"Found null values in column: {column}"

def test_duplicate_records(spark, employee_schema):
    """Test for duplicate records in the dataset"""
    df = spark.read.csv("src/main/resources/employees.csv", 
                       header=True, 
                       schema=employee_schema)
    
    # Count total rows
    total_rows = df.count()
    
    # Count distinct rows
    distinct_rows = df.distinct().count()
    
    # Assert no duplicates
    assert total_rows == distinct_rows, "Found duplicate records in the dataset"

def test_salary_range(spark, employee_schema):
    """Test if salaries are within expected range"""
    df = spark.read.csv("src/main/resources/employees.csv", 
                       header=True, 
                       schema=employee_schema)
    
    # Define salary range
    min_salary = 30000
    max_salary = 200000
    
    # Count records outside salary range
    invalid_salaries = df.filter((col("salary") < min_salary) | (col("salary") > max_salary)).count()
    
    # Assert all salaries are within range
    assert invalid_salaries == 0, "Found salaries outside expected range"

def test_department_values(spark, employee_schema):
    """Test if departments contain only valid values"""
    df = spark.read.csv("src/main/resources/employees.csv", 
                       header=True, 
                       schema=employee_schema)
    
    # Define valid departments
    valid_departments = ["Engineering", "Marketing", "HR", "Finance", "Sales"]
    
    # Count records with invalid departments
    invalid_departments = df.filter(~col("department").isin(valid_departments)).count()
    
    # Assert all departments are valid
    assert invalid_departments == 0, "Found invalid department values"

def test_id_uniqueness(spark, employee_schema):
    """Test if employee IDs are unique"""
    df = spark.read.csv("src/main/resources/employees.csv", 
                       header=True, 
                       schema=employee_schema)
    
    # Count total rows
    total_rows = df.count()
    
    # Count distinct IDs
    distinct_ids = df.select("id").distinct().count()
    
    # Assert IDs are unique
    assert total_rows == distinct_ids, "Found duplicate employee IDs"
    #spark.assertDataFrameEquals(total_rows, distinct_ids)

def test_name_format(spark, employee_schema):
    """Test if names follow the expected format (no special characters)"""
    df = spark.read.csv("src/main/resources/employees.csv", 
                       header=True, 
                       schema=employee_schema)
    
    # Count records with invalid name format (containing special characters)
    invalid_names = df.filter(
        ~col("name").rlike("^[A-Za-z ]+$")
    ).count()
    
    # Assert all names follow the format
    assert invalid_names == 0, "Found names with invalid format"

def test_data_completeness(spark, employee_schema):
    """Test if all required fields are present and non-empty"""
    df = spark.read.csv("src/main/resources/employees.csv", 
                       header=True, 
                       schema=employee_schema)
    
    # Check for empty strings in string columns
    empty_strings = df.filter(
        (col("name") == "") | 
        (col("department") == "")
    ).count()
    
    # Assert no empty strings
    assert empty_strings == 0, "Found empty string values in required fields" 