Overview
========

Welcome to Astronomer! This project was generated after you ran 'astro dev init' using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.

NammaMart ETL and ELT Outputs
=============================

This project includes two NammaMart Airflow pipelines: `nammamart_etl` and
`nammamart_elt`. In the Airflow UI, open **Browse > DAG Runs** (or the DAG's
Grid view), select a run, and check the task logs for its row counts and
warehouse report.

**Where to find passed data**

- **ETL passed-row CSVs:** `include/work/nammamart/<run_id>/`. For example,
  `include/work/nammamart/manual__2026-10-20T05_42_05.875822_00_00/customers_passed.csv`.
  The same run folder contains `orders_passed.csv`, `payments_passed.csv`,
  `products_passed.csv`, and `stores_passed.csv`; when the optional increment
  file is present, it also contains `orders_increment_passed.csv`.
- **ETL final output:** successfully validated and transformed order rows are
  in `etl_fact_orders` in `include/warehouse_nammamart.duckdb`. Reporting
  tables include `etl_daily_zone_revenue` and `etl_category_margin`.
- **ELT output:** the `nammamart_elt` DAG retains source rows in raw tables,
  including rows that need review. Use the cleaned `stg_orders` table for
  staged orders and `mart_daily_store_kpis` or `mart_category_margin` for
  reporting results in the same DuckDB warehouse.
- **Failed rows:** per-run quarantine CSVs are under
  `include/quarantine/nammamart/<run_id>/`; the ETL and ELT database tables
  are `etl_quarantine` and `elt_quarantine`, respectively.

The ETL `*_passed.csv` files are validation outputs (before final warehouse
transformations), not the reporting layer. To inspect the final ETL rows or
ELT staged rows, query the DuckDB tables. For example, from the project root:

```python
import duckdb

connection = duckdb.connect("include/warehouse_nammamart.duckdb", read_only=True)
print(connection.execute("SELECT * FROM etl_fact_orders LIMIT 10").df())
print(connection.execute("SELECT * FROM stg_orders LIMIT 10").df())
connection.close()
```

Project Contents
================

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG:
    - `example_astronauts`: This DAG shows a simple ETL pipeline example that queries the list of astronauts currently in space from the Open Notify API and prints a statement for each astronaut. The DAG uses the TaskFlow API to define tasks in Python, and dynamic task mapping to dynamically print a statement for each astronaut. For more on how this DAG works, see our [Getting started tutorial](https://www.astronomer.io/docs/learn/get-started-with-airflow).
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Deploy Your Project Locally
===========================

Start Airflow on your local machine by running 'astro dev start'.

This command will spin up five Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- DAG Processor: The Airflow component responsible for parsing DAGs
- API Server: The Airflow component responsible for serving the Airflow UI and API
- Triggerer: The Airflow component responsible for triggering deferred tasks

When all five containers are ready the command will open the browser to the Airflow UI at http://localhost:8080/. You should also be able to access your Postgres Database at 'localhost:5432/postgres' with username 'postgres' and password 'postgres'.

Note: If you already have either of the above ports allocated, you can either [stop your existing Docker containers or change the port](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver).

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://www.astronomer.io/docs/astro/deploy-code/

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support.
