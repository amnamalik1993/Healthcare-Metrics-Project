# Healthcare Metrics Data Engineering Project

## Project Overview

This project implements an end-to-end healthcare data engineering pipeline using AWS Glue, Amazon S3, Amazon Athena, and Streamlit.

The pipeline ingests healthcare data from Google Drive, processes the data through Bronze, Silver, and Gold data layers, and provides analytical dashboards using Streamlit.

## Data Layers

### Raw Layer

Stores the original files ingested from Google Drive.

### Bronze Layer

Stores the data in a structured format with basic cleaning and standardization.

### Silver Layer

Contains cleaned and transformed datasets.

### Gold Layer

Contains business-ready analytical metrics:

- Staffing Metrics
- Facility Metrics
- Quality Metrics
- Patient Volume Metrics

### Visualization

The Streamlit dashboard queries the Gold tables through Amazon Athena.

## Architecture

<img width="1774" height="887" alt="ChatGPT Image Jul 15, 2026, 07_27_44 PM" src="https://github.com/user-attachments/assets/60ce60aa-3c33-4bc9-a16e-8f51dbdef28c" />
