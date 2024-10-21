# Data Pipeline Project: Reddit Data Pipeline

## Table of Contents
1. [Introduction](#introduction)
2. [Tools Used](#tools-used)
3. [Project Steps](#project-steps)  
    - [Step 1: Extract Data](#step-1-extract-data)  
    - [Step 2: Transform Data](#step-2-transform-data)  
    - [Step 3: Create Excel File](#step-3-create-excel-file)  
    - [Step 4: Load Data in Database](#step-4-load-data-in-database)
4. [Contact](#contact)

## Introduction
This project is focused on building a data pipeline to extract, transform, and load data from Reddit. The data is extracted using the **PRAW** library, transformed as necessary, saved into an Excel file, and finally loaded into a PostgreSQL database for storage.

## Tools Used
- **Apache Airflow**: Orchestration of the data pipeline.
- **Python (PRAW)**: Extracting data from Reddit.
- **Pandas**: Data transformation and processing.
- **PostgreSQL**: Database for storing the transformed data.
- **Excel (openpyxl)**: Saving data to an Excel file for backup and analysis.

## Project Steps

### Step 1: Extract Data
In this step, data is extracted from Reddit using the Python Reddit API Wrapper (**PRAW**). The data includes posts from specific subreddits along with their metadata.

### Step 2: Transform Data
Once the data is extracted, it's transformed and cleaned. This involves removing unnecessary fields, converting data types, and handling missing values.

### Step 3: Create Excel File
The transformed data is saved into an Excel file using the **openpyxl** library. This file serves as a backup and can be used for further manual analysis.

### Step 4: Load Data in Database
In the final step, the cleaned and transformed data is loaded into a **PostgreSQL** database for long-term storage and further querying.

![Reddit API](https://github.com/user-attachments/assets/ea040bce-e234-46d7-9a6e-ba3686b1f662)

## Contact
For any questions or inquiries related to this project, please feel free to contact me:
- **Email**: <mohamedazrou@gmail.com>
- **LinkedIn**: [Mohamed Sabbar](www.linkedin.com/in/mohamed-sabbar-463495294)

