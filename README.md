# Highway Toll Data Analysis

This repository contains the code and resources for analyzing highway toll data. The project involves extracting, transforming, and loading (ETL) data using a DAG file, and utilizing Kafka for data streaming with a producer that sends data and a consumer that writes data to a database.

## Features

- **ETL Pipeline**: Extracts, transforms, and loads highway toll data.
- **Kafka Integration**: Streams data using Kafka with a producer and a consumer.
- **Database Storage**: Writes the processed data to a database.

## Technologies Used

- **Apache Airflow**: Orchestrates the ETL pipeline using DAGs.
- **Apache Kafka**: Streams data between the producer and consumer.
- **Python**: Scripting language for data processing.
- **Database**: Stores the final processed data.

## Prerequisites

Before running the application, ensure you have the following installed:

- Python 3.8+
- Apache Airflow
- Apache Kafka
- Database (MySQL)
