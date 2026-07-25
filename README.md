# Healthcare Metrics Data Engineering Project

## Project Overview

This project implements an end-to-end healthcare data engineering pipeline using AWS Glue, Amazon S3, Amazon Athena, and Streamlit.

The pipeline ingests healthcare data from Google Drive, processes the data through Bronze, Silver, and Gold data layers, and provides analytical dashboards using Streamlit.

## Architecture

Google Drive
      ↓
AWS Glue
      ↓
Amazon S3 - Raw Layer
      ↓
AWS Glue
      ↓
Amazon S3 - Bronze Layer
      ↓
AWS Glue
      ↓
Amazon S3 - Silver Layer
      ↓
AWS Glue
      ↓
Amazon S3 - Gold Layer
      ↓
Amazon Athena
      ↓
Streamlit Dashboard
