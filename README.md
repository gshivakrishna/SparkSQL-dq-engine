# PySpark SQL Demo

This is a simple PySpark SQL project that demonstrates various SQL operations using PySpark.

## Prerequisites

- Python 3.8 or higher
- pip (Python package installer)

## Setup

1. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Running the Demo

To run the demo, execute:
```bash
python spark_sql_demo.py
```

## Running Data Quality Tests

The project includes comprehensive data quality tests that validate:
- Schema validation
- Null value checks
- Duplicate record detection
- Salary range validation
- Department value validation
- ID uniqueness
- Name format validation
- Data completeness

To run the tests:
```bash
pytest test_data_quality_sql.py -v
```

## What the Demo Shows

The demo performs the following operations on a sample employee dataset:

1. Display all employees
2. Calculate department-wise average salary
3. Find departments with the highest number of employees
4. Show employees with salaries above their department average

## Project Structure

```
.
├── README.md
├── requirements.txt
├── spark_sql_demo.py
├── test_data_quality.py
├── test_data_quality_sql.py
└── src/
    └── main/
        └── resources/
            └── employees.csv
``` 