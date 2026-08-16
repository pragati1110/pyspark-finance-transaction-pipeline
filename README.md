# PySpark Finance Transaction Pipeline

End-to-end PySpark pipeline for finance transaction data cleaning, transformation, and profiling. Built and run on Databricks (Free Edition / Serverless).

## What it does

1. **Synthetic data generation** - generates realistic "messy" finance data (150 customers, 300+ accounts, 1500+ transactions) with intentional nulls, inconsistent name formatting, duplicate rows, negative balances, and orphaned foreign keys — so the pipeline runs standalone with zero external dependencies.
2. **Data profiling** - null counts, distinct counts, and duplicate detection across all 3 source tables before any cleaning.
3. **Data cleaning** - name standardization (trim/casing), null handling, referential integrity enforcement, bad-value filtering.
4. **Data quality validation** - 5 explicit checks (key nulls, duplicate IDs, referential integrity, negative balances, amount sanity) run post-clean with pass/fail reporting.
5. **Joins & aggregations** - 3-way join (transactions → accounts → customers); revenue and transaction-count aggregations by branch and by customer.
6. **Ranking** - window functions: customers ranked by spend within each city (`rank()`), branches ranked by revenue overall (`dense_rank()`).
7. **Pivoting** - transaction counts pivoted by branch × transaction type; monthly spend pivoted by merchant category.
8. **map/flatMap-equivalent transformations** - DataFrame-native `select`/`explode(split())` used in place of raw RDD `map`/`flatMap` (RDDs are disabled on Databricks Serverless compute — documented reasoning included in the notebook).
9. **Export** - curated datasets written as managed Delta tables and as literal Parquet/CSV files in a Unity Catalog Volume.

## Tech stack

`PySpark` · `Spark SQL` · `DataFrame API` · `Window functions` · `Delta Lake` · `Databricks` · `Unity Catalog`

## How to run

1. Import `finance-transaction-pipeline.py` into a Databricks workspace (Workspace → Import — the `# Databricks notebook source` header auto-splits it into notebook cells)
2. Attach to a cluster (Serverless works, except the RDD section which needs Dedicated compute)
3. Run All

## Why this project

Built to demonstrate hands-on PySpark data engineering — cleaning, transformation, profiling, and quality validation — on a finance-domain dataset, as a complement to Azure SQL/ADF/Synapse experience.# pyspark-finance-transaction-pipeline
End-to-end PySpark pipeline for finance transaction data cleaning, transformation, and profiling
