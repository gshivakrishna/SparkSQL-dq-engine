import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from .test_utils import (
    get_schema_dict,
    count_records,
    count_distinct_records,
    get_null_counts,
    get_invalid_values_count
)

class TestSchemaValidation:
    """Tests for schema validation"""
    
    def test_schema_validation(self, spark: SparkSession, employee_schema: StructType):
        """Test if the data matches the expected schema"""
        actual_schema = get_schema_dict(spark, "employees")
        expected_schema = {field.name: field.dataType.simpleString() 
                         for field in employee_schema.fields}
        
        assert actual_schema == expected_schema, "Schema validation failed"

class TestDataQuality:
    """Tests for data quality checks"""
    
    def test_null_values(self, spark: SparkSession):
        """Test for null values in the dataset"""
        columns = ["id", "name", "department", "salary"]
        null_counts = get_null_counts(spark, "employees", columns)
        
        for column, count in null_counts.items():
            assert count == 0, f"Found null values in column: {column}"
    
    def test_duplicate_records(self, spark: SparkSession):
        """Test for duplicate records in the dataset"""
        total_rows = count_records(spark, "employees")
        distinct_rows = count_distinct_records(spark, "employees", 
                                            ["id", "name", "department", "salary"])
        
        assert total_rows == distinct_rows, "Found duplicate records in the dataset"
    
    def test_salary_range(self, spark: SparkSession):
        """Test if salaries are within expected range"""
        min_salary = 30000
        max_salary = 200000
        
        invalid_salaries = get_invalid_values_count(
            spark, "employees", "salary",
            f"salary < {min_salary} OR salary > {max_salary}"
        )
        
        assert invalid_salaries == 0, "Found salaries outside expected range"
    
    def test_department_values(self, spark: SparkSession):
        """Test if departments contain only valid values"""
        valid_departments = ["'Engineering'", "'Marketing'", "'HR'", 
                           "'Finance'", "'Sales'"]
        valid_departments_str = ", ".join(valid_departments)
        
        invalid_departments = get_invalid_values_count(
            spark, "employees", "department",
            f"department NOT IN ({valid_departments_str})"
        )
        
        assert invalid_departments == 0, "Found invalid department values"
    
    def test_id_uniqueness(self, spark: SparkSession):
        """Test if employee IDs are unique"""
        total_rows = count_records(spark, "employees")
        distinct_ids = count_distinct_records(spark, "employees", ["id"])
        
        assert total_rows == distinct_ids, "Found duplicate employee IDs"
    
    def test_name_format(self, spark: SparkSession):
        """Test if names follow the expected format"""
        invalid_names = get_invalid_values_count(
            spark, "employees", "name",
            "NOT REGEXP(name, '^[A-Za-z ]+$')"
        )
        
        assert invalid_names == 0, "Found names with invalid format"
    
    def test_data_completeness(self, spark: SparkSession):
        """Test if all required fields are present and non-empty"""
        empty_strings = get_invalid_values_count(
            spark, "employees", "name",
            "name = '' OR department = ''"
        )
        
        assert empty_strings == 0, "Found empty string values in required fields" 