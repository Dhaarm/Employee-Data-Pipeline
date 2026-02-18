# *Employee Data Pipeline (PySpark + Docker + PostgreSQL)*

# Project Overview

This project implements a batch data pipeline using PySpark to:

Generate employee data

Perform data quality validation

Clean and transform records

Separate valid and rejected records

Load results into PostgreSQL

Run fully inside Docker

The pipeline demonstrates real-world ETL best practices including validation, transformation, and database loading.

# Architecture
```
Data Generation (generate_data.py)
            ↓
Raw CSV (employees_raw.csv)
            ↓
PySpark Validation & Transformation
            ↓
├── employees_clean  → PostgreSQL
└── employees_rejected → PostgreSQL 
```
# Tech Stack

Python 3

PySpark

PostgreSQL

Docker & Docker Compose

Faker (for data generation)

# Project Structure
```
employee-data-pipeline/
│
├── docker-compose.yml
├── README.md
│
├── spark/
│   ├── Dockerfile
│   └── jobs/
│       └── employee_pipeline.py
│
├── postgres/
│   └── init.sql
│
└── data/
    ├── generate_data.py
    └── employees_raw.csv
```
# How It Works
# Data Generation

generate_data.py creates a synthetic employee dataset with:

Employee details

Random salary values

Mixed data quality cases

Different email domains

Some intentionally invalid records

# Data Validation Rules

*The pipeline applies the following validation rules:*

Rule	Description
Duplicate Check	Duplicate employee_id detection
Email Validation	Regex-based email format check
Salary Validation	Null or empty salary detection
Manager ID Validation	Null or blank manager_id
Future Hire Date	hire_date > current_date
Null Critical Fields	first_name, last_name, email etc

Rejected records are stored with a structured rejection_reason.

# Data Cleaning

Trim whitespace

Convert blank strings to NULL

Remove $ and , from salary

Cast numeric fields safely

Convert dates properly

Standardize case (upper/lower/initcap)

# Transformations Applied (Valid Records)

Full name creation

Email normalization

Salary band classification

Age calculation

Tenure calculation

Email domain extraction

Salary Bands:
```
< 50000	Junior 
50000–80000	Mid 
> 80000	Senior
```
# Database Tables

#***employees_clean***

Stores valid, transformed records.

#***employees_rejected***

Stores rejected records with:

Original data

rejection_reason

audit timestamp

# Running the Project
Step 1: Start your Docker 
Step 2: Go to the folder in cmd
Step 3: Build and Run by pasting below commands in cmd
```
docker compose down -v
docker compose up --build
```

This will:

Generate employee data

Run PySpark pipeline

Load results into PostgreSQL

# Checking Data in PostgreSQL

Enter the container:
```
docker exec -it employee_postgres psql -U admin -d employees
```

Check tables:
```
\dt
```

View clean records:
```
SELECT COUNT(*) FROM employees_clean;
```

View rejected summary:
```
SELECT rejection_reason, COUNT(*) 
FROM employees_rejected
GROUP BY rejection_reason;
```
# Sample Analytical Queries

Employees by department:
```
SELECT department, COUNT(*) 
FROM employees_clean
GROUP BY department;
```

Salary distribution:
```
SELECT salary_band, COUNT(*)
FROM employees_clean
GROUP BY salary_band;
```

Average salary per department:
```
SELECT department, AVG(salary)
FROM employees_clean
GROUP BY department;
```
# Design Decisions
Why Separate Rejected Records?

Audit trail

Data governance

Reprocessing capability

Monitoring data quality

Why Use Window Function for Duplicates?

Efficient distributed duplicate detection:

Window.partitionBy("employee_id")

Why Clean Salary Before Casting?

Currency formats like $45,000 must be normalized before numeric casting to avoid NULL conversion.

# Key Features

Fully containerized

Automated batch execution

Data quality framework

Production-style transformations

Structured error tracking

Clean separation of valid and rejected data
