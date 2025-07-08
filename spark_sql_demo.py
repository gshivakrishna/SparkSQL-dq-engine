from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, desc

def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("PySpark SQL Demo") \
        .master("local[*]") \
        .getOrCreate()

    try:
        # Read the CSV file
        df = spark.read.csv("src/main/resources/employees.csv", 
                          header=True, 
                          inferSchema=True)

        # Register the DataFrame as a temporary view
        df.createOrReplaceTempView("employees")

        # Example 1: Basic SQL query
        print("\n1. All employees:")
        spark.sql("SELECT * FROM employees").show()

        # Example 2: Department-wise average salary
        print("\n2. Department-wise average salary:")
        spark.sql("""
            SELECT department, 
                   ROUND(AVG(salary), 2) as avg_salary 
            FROM employees 
            GROUP BY department 
            ORDER BY avg_salary DESC
        """).show()

        # Example 3: Department with highest number of employees
        print("\n3. Department with highest number of employees:")
        spark.sql("""
            SELECT department, 
                   COUNT(*) as employee_count 
            FROM employees 
            GROUP BY department 
            ORDER BY employee_count DESC
        """).show()

        # Example 4: Employees with salary above average
        print("\n4. Employees with salary above department average:")
        spark.sql("""
            WITH dept_avg AS (
                SELECT department, AVG(salary) as avg_dept_salary 
                FROM employees 
                GROUP BY department
            )
            SELECT e.name, e.department, e.salary, 
                   ROUND(d.avg_dept_salary, 2) as dept_avg_salary
            FROM employees e
            JOIN dept_avg d ON e.department = d.department
            WHERE e.salary > d.avg_dept_salary
            ORDER BY e.salary DESC
        """).show()

    finally:
        # Stop SparkSession
        spark.stop()

if __name__ == "__main__":
    main() 