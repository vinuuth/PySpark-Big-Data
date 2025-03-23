**PySpark Weather Data Processing**

Overview

**This project uses Apache Spark to store weather data in an S3-compatible MinIO bucket. The pipeline reads raw fixed-width text data, extracts relevant fields, transforms them into a structured format, and writes the output in Parquet and CSV formats back to MinIO. The script is designed to run in a distributed Spark environment.**


Features

Secure credential handling using environment variables.
Reads raw weather data from MinIO (S3-compatible storage) in fixed-width format.
Extracts relevant columns and converts them into structured data.
Transforms data types for better usability (e.g., date parsing, numeric conversions).
Writes the processed data to MinIO in Parquet and CSV formats.
Includes an option to store data in MySQL for further analysis (commented out by default).

Prerequisites

**Apache Spark (with PySpark)**

**MinIO (or an S3-compatible object storage service)**

**Python 3.x**

**MySQL (optional, for database storage**


Setup & Execution

Clone the repository:

git clone https://github.com/yourusername/weather-data-pipeline.git
cd weather-data-pipeline

Install dependencies:

pip install pyspark

Configure environment variables:

export SECRETKEY=your_s3_access_key
export ACCESSKEY=your_s3_secret_key

Run the PySpark script:

spark-submit process_weather_data.py

Output

Processed data is stored in MinIO under the following paths:

s3a://vbengaluruprabhudev/30-parquet/

s3a://vbengaluruprabhudev/30-csv/

Schema and sample records are printed for verification.

Future Enhancements

**Implement data validation and quality checks.**

**Automate deployment using Apache Airflow.**

**Add logging and monitoring features.**
