# Parking Transaction Pipeline

## Overview
I developed the **Parking Transaction Pipeline** to efficiently process and analyze parking transaction data, leveraging cloud-based solutions for scalability and automation. The system extracts real-time parking transaction data from **Seattle's Open Data Portal**, processes it using **Python and Pandas**, and loads the transformed data into **Google Cloud Storage (GCS) and BigQuery** for further analysis. The entire infrastructure, including storage, databases, and access control, is provisioned and managed with **Terraform**, ensuring efficient resource deployment and maintainability.

## How It Works
- The pipeline is designed to run **daily**, orchestrated by **Apache Airflow**, ensuring timely data ingestion and processing.
- The pipeline extracts **new transaction records** from the **Socrata API**.
- It transforms and cleans the data using **Pandas**, ensuring consistency and removing anomalies.
- The processed data is temporarily stored in **Google Cloud Storage (GCS)**.
- Finally, the data is loaded into **BigQuery**, where it can be queried for insights and analytics.

## Why I Built This
The goal of this project was to create an **automated, scalable, and cloud-native ETL pipeline** that integrates open parking transaction data with Google Cloud’s data ecosystem. It eliminates manual data extraction, improves data quality, and ensures the data remains up-to-date for further analysis.

## Key Technologies Used
- **Python**: Core language for data processing.
- **Apache Airflow**: Automates and schedules pipeline execution.
- **Google Cloud Storage (GCS)**: Temporary storage for processed data.
- **BigQuery**: Fast, scalable storage and analytics engine.
- **Socrata API**: Source of real-time parking transaction data.
- **Terraform**: Infrastructure-as-Code (IaC) to provision and manage cloud resources.
- **Pandas & NumPy**: Data transformation and cleaning.

## Infrastructure & Automation
- Terraform is used to create and manage **BigQuery datasets**, **Cloud Storage buckets**, and **IAM roles** for access control.
- Apache Airflow DAGs orchestrate data ingestion, transformation, and storage.
- The pipeline is modular, allowing for easy modifications or extensions.

## Future Improvements
- Enhancing **data validation** and **error handling** for more robust processing.
- Implementing **streaming ingestion** instead of batch processing for near real-time updates.
- Integrating **machine learning models** in BigQuery to analyze parking patterns and predict availability.

## Conclusion
The **Parking Transaction Pipeline** is an automated, scalable, and cloud-native solution for handling parking transaction data. By leveraging **Google Cloud services**, **Terraform**, and **Apache Airflow**, it ensures efficient data processing while maintaining a modular and reproducible infrastructure. This project showcases how **cloud infrastructure, automation, and data engineering** can work together to solve real-world data problems efficiently.
