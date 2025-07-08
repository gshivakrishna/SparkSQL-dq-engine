import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing"""
    return SparkSession.builder \
        .appName("Data Quality SQL Tests") \
        .master("local[*]") \
        .getOrCreate()

@pytest.fixture(scope="session")
def employee_schema():
    """Define the expected schema for employee data"""
    return StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("department", StringType(), False),
        StructField("salary", DoubleType(), False)
    ])

def test_schema_validation_sql(spark, employee_schema):
    """Test if the data matches the expected schema using SQL"""
    df = spark.read.csv("src/main/resources/employees.csv", 
                       header=True, 
                       schema=employee_schema)
    
    # Create temporary view
    df.createOrReplaceTempView("employees")
    
    # Get schema information using SQL
    schema_info = spark.sql("""
        SELECT 
            col_name as column_name,
            data_type
        FROM (
            DESCRIBE TABLE employees
        )
    """)
    
    # Convert schema to dictionary for comparison
    actual_schema = {row.column_name: row.data_type for row in schema_info.collect()}
    expected_schema = {field.name: field.dataType.simpleString() for field in employee_schema.fields}
    
    assert actual_schema == expected_schema, "Schema validation failed"

def test_null_values_sql(spark, employee_schema):
    """Test for null values in the dataset using SQL"""
    df = spark.read.csv("src/main/resources/employees.csv", 
                       header=True, 
                       schema=employee_schema)
    df.createOrReplaceTempView("employees")
    
    # Count null values in each column using SQL
    null_counts = spark.sql("""
        SELECT 
            COUNT(CASE WHEN id IS NULL THEN 1 END) as id_nulls,
            COUNT(CASE WHEN name IS NULL THEN 1 END) as name_nulls,
            COUNT(CASE WHEN department IS NULL THEN 1 END) as department_nulls,
            COUNT(CASE WHEN salary IS NULL THEN 1 END) as salary_nulls
        FROM employees
    """).first()
    
    # Assert no null values in any column
    assert null_counts.id_nulls == 0, "Found null values in id column"
    assert null_counts.name_nulls == 0, "Found null values in name column"
    assert null_counts.department_nulls == 0, "Found null values in department column"
    assert null_counts.salary_nulls == 0, "Found null values in salary column"

def test_duplicate_records_sql(spark, employee_schema):
    """Test for duplicate records in the dataset using SQL"""
    df = spark.read.csv("src/main/resources/employees.csv", 
                       header=True, 
                       schema=employee_schema)
    df.createOrReplaceTempView("employees")
    
    # Count duplicates using SQL
    duplicate_count = spark.sql("""
        SELECT COUNT(*) as duplicate_count
        FROM (
            SELECT id, name, department, salary, COUNT(*) as cnt
            FROM employees
            GROUP BY id, name, department, salary
            HAVING COUNT(*) > 1
        )
    """).first().duplicate_count
    
    assert duplicate_count == 0, "Found duplicate records in the dataset"

def test_salary_range_sql(spark, employee_schema):
    """Test if salaries are within expected range using SQL"""
    df = spark.read.csv("src/main/resources/employees.csv", 
                       header=True, 
                       schema=employee_schema)
    df.createOrReplaceTempView("employees")
    
    # Define salary range
    min_salary = 30000
    max_salary = 200000
    
    # Count records outside salary range using SQL
    invalid_salaries = spark.sql(f"""
        SELECT COUNT(*) as invalid_count
        FROM employees
        WHERE salary < {min_salary} OR salary > {max_salary}
    """).first().invalid_count
    
    assert invalid_salaries == 0, "Found salaries outside expected range"

def test_department_values_sql(spark, employee_schema):
    """Test if departments contain only valid values using SQL"""
    df = spark.read.csv("src/main/resources/employees.csv", 
                       header=True, 
                       schema=employee_schema)
    df.createOrReplaceTempView("employees")
    
    # Define valid departments
    valid_departments = ["'Engineering'", "'Marketing'", "'HR'", "'Finance'", "'Sales'"]
    valid_departments_str = ", ".join(valid_departments)
    
    # Count records with invalid departments using SQL
    invalid_departments = spark.sql(f"""
        SELECT COUNT(*) as invalid_count
        FROM employees
        WHERE department NOT IN ({valid_departments_str})
    """).first().invalid_count
    
    assert invalid_departments == 0, "Found invalid department values"

def test_id_uniqueness_sql(spark, employee_schema):
    """Test if employee IDs are unique using SQL"""
    df = spark.read.csv("src/main/resources/employees.csv", 
                       header=True, 
                       schema=employee_schema)
    df.createOrReplaceTempView("employees")
    
    # Count duplicate IDs using SQL
    duplicate_ids = spark.sql("""
        SELECT COUNT(*) as duplicate_count
        FROM (
            SELECT id, COUNT(*) as cnt
            FROM employees
            GROUP BY id
            HAVING COUNT(*) > 1
        )
    """).first().duplicate_count
    
    assert duplicate_ids == 0, "Found duplicate employee IDs"

def test_name_format_sql(spark, employee_schema):
    """Test if names follow the expected format using SQL"""
    df = spark.read.csv("src/main/resources/employees.csv", 
                       header=True, 
                       schema=employee_schema)
    df.createOrReplaceTempView("employees")
    
    # Count records with invalid name format using SQL
    invalid_names = spark.sql("""
        SELECT COUNT(*) as invalid_count
        FROM employees
        WHERE NOT REGEXP(name, '^[A-Za-z ]+$')
    """).first().invalid_count
    
    assert invalid_names == 0, "Found names with invalid format"

def test_data_completeness_sql(spark, employee_schema):
    """Test if all required fields are present and non-empty using SQL"""
    df = spark.read.csv("src/main/resources/employees.csv", 
                       header=True, 
                       schema=employee_schema)
    df.createOrReplaceTempView("employees")
    
    # Count records with empty strings using SQL
    empty_strings = spark.sql("""
        SELECT COUNT(*) as empty_count
        FROM employees
        WHERE name = '' OR department = ''
    """).first().empty_count
    
    assert empty_strings == 0, "Found empty string values in required fields" 