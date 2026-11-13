# airflow_airplane ,a data pipeline of OpenSky Flights 
A project to demonstrate how to extract flight data from the OpenSky Network API, process it using the principles of Medallion Architecture, orchestrate the workflow with Apache Airflow, and generate analytical datasets for storage in Snowflake.

# Airflow OpenSky Medallion Data Pipeline

This project is an end-to-end Data Engineering pipeline built using Apache Airflow, Python, CSV, Docker and Snowflake.

The objective was to understand how data moves through a batch processing pipeline, from API ingestion to data cleaning, transformation, analytical aggregation and loading into a data warehouse.

The project uses flight information provided by the OpenSky Network API. Apache Airflow manages the execution of different pipeline tasks, while Snowflake stores the processed analytical data.

# Project Overview

The pipeline follows the Medallion Architecture:

- **Bronze Layer:** Extracts raw flight data from the OpenSky Network API and stores the response locally.
- **Silver Layer:** Cleans and transforms the raw data, handles missing values, validates records and derives additional flight-related attributes.
- **Gold Layer:** Generates analytical datasets containing aggregated flight information and operational insights.
- **Snowflake:** Stores the Gold datasets for further analytical queries.

Apache Airflow orchestrates the complete workflow and manages task dependencies and scheduled executions.


# Architecture

```text
        OpenSky Network API
                 |
                 ▼
          Bronze Ingestion
                 |
                 ▼
           Bronze CSV
                 |
                 ▼
          Silver Processing
                 |
       Data Cleaning & Validation
       Derived Flight Attributes
                 |
                 ▼
           Silver CSV
                 |
                 ▼
           Gold Processing
                 |
        Analytical Aggregations
                 |
                 ▼
            Gold CSV
                 |
                 ▼
          Snowflake Loading
                 |
                 ▼
       Snowflake Analytical Tables
```

Apache Airflow manages the execution and dependencies between these stages.
# Some Snapshot
<img width="519" height="390" alt="Screenshot from 2026-08-07 02-36-38" src="https://github.com/user-attachments/assets/0a018d96-12cc-4086-9f4a-66d8790a9c4f" />
<img width="1875" height="770" alt="Screenshot from 2026-08-07 02-37-13" src="https://github.com/user-attachments/assets/3feb9f9b-f055-4a8f-ad7e-07abd36eb9fe" />
<img width="1875" height="770" alt="Screenshot from 2026-08-07 02-38-07" src="https://github.com/user-attachments/assets/06cc4778-7e8f-4924-9260-60291cf32321" />
<img width="1887" height="995" alt="Screenshot from 2026-08-06 21-41-54" src="https://github.com/user-attachments/assets/58051c79-99ba-46eb-ac05-a632914aa817" />
<img width="1548" height="959" alt="Screenshot from 2026-08-06 21-36-47" src="https://github.com/user-attachments/assets/ad0238cd-2a2c-45a3-b813-f064db99d6f8" />

# Data Source

The project uses the OpenSky Network REST API:

`https://opensky-network.org/api/states/all`

The API provides live aircraft state information, including attributes such as:

- ICAO24 aircraft identifier
- Callsign
- Origin country
- Longitude and latitude
- Barometric altitude
- Ground speed
- True track
- Vertical rate
- Time position
- Last contact
- Position source

The pipeline processes the available API response and stores the extracted data in the Bronze layer.

# Bronze Layer

The Bronze layer is responsible for extracting the raw flight information from the OpenSky Network API.

The extracted data is stored as CSV files without applying the main Silver-layer transformations.

This layer provides the initial representation of the source data and acts as the starting point for subsequent processing.

# Silver Layer

The Silver layer performs data cleaning, validation and transformation on the Bronze data.

In addition to cleaning the original fields, the pipeline derives additional attributes to make the flight data more useful for analysis.

Some of the derived fields include:

- **Velocity in KM/H:** Converts the available ground speed into kilometres per hour.
- **Altitude Difference:** Calculates the difference between the available altitude measurements.
- **Altitude Category:** Categorises aircraft based on their altitude.
- **Climb Status:** Identifies whether an aircraft is climbing, descending or maintaining its vertical position.
- **Flight Status:** Classifies the flight based on the available flight information.
- **Position Age:** Calculates the age of the aircraft position using `last_contact - time_position`.
- **Position Source:** Converts the position source value into a readable description.

The processed data is saved as a Silver CSV dataset.

# Gold Layer

The Gold layer generates analytical datasets from the cleaned Silver data.

Instead of working with individual aircraft records, this layer focuses on aggregations that can help understand flight activity and operational characteristics.

The project generates analytical datasets such as:

- **Country Summary:** Analyses flight distribution across origin countries.
- **Altitude Analysis:** Provides insights into aircraft altitude distribution.
- **Movement Analysis:** Analyses aircraft movement and flight characteristics.
- **Position Source Analysis:** Summarises the different sources of aircraft position information.
- **Operational Quality:** Provides insights into the quality and availability of flight data.
- **Geographical Distribution:** Analyses the geographical distribution of aircraft.
- **Time Analysis:** Aggregates flight information over time.
- **Heading Analysis:** Analyses aircraft distribution based on heading.

The Gold datasets are stored as CSV files before being loaded into Snowflake.

# Apache Airflow Orchestration

Apache Airflow is used to orchestrate the complete data pipeline.

The DAG manages the dependencies between Bronze ingestion, Silver processing, Gold transformations and Snowflake loading.

The pipeline is configured to run every 15 minutes:

`*/15 * * * *`

This allows the workflow to periodically retrieve new flight information and process it through the different layers.

Airflow also provides visibility into task execution, task dependencies, execution history and failures.

# Snowflake Integration

The Gold datasets are loaded into Snowflake for persistent storage and analytical querying.

The project uses SQL `MERGE` operations to load the aggregated data.

Using `MERGE` allows existing records to be updated when their matching keys already exist and new records to be inserted when they do not.

For example, the heading analysis uses a combination of `window_start` and `heading` as its matching key.

This makes repeated pipeline executions safer than blindly inserting the same aggregated records again.

# Technologies Used

Python, Apache Airflow, OpenSky Network REST API, Pandas, CSV, Docker, Docker Compose, PostgreSQL, Snowflake and SQL.

# Why CSV Instead of Parquet?

The project uses CSV files to store the Bronze, Silver and Gold datasets locally.

This keeps the data flow straightforward and makes it easier to inspect the extracted and transformed records during development.

The project focuses on understanding API ingestion, data transformation, pipeline orchestration and warehouse loading rather than introducing additional storage formats.

# Why PostgreSQL Is Used with Airflow

PostgreSQL is used as the Airflow metadata database.

It stores Airflow-related information such as DAG runs, task instances, execution states and other orchestration metadata.

It is not used as the main storage system for the flight datasets. The actual pipeline data is processed through CSV files and the analytical Gold datasets are loaded into Snowflake.

# Running the Project

1. Clone the repository:

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
```

Move into the project directory:

```bash
cd airflow_airplane
```

2. Configure environment variables.

Create a local `.env` file containing the required Snowflake connection details and other configuration values used by the project.

The `.env` file should not be uploaded to GitHub. An `.env.example` file can be used to document the required variables without exposing credentials.

3. Start the Docker services:

```bash
docker compose up -d
```

4. Access the Airflow web interface through the configured local Airflow webserver address.

5. Enable the `flights_ops_medallion_pipe` DAG if it is paused.

6. Trigger the DAG manually or allow the scheduler to execute it according to its configured schedule.

7. Monitor the task execution in the Airflow UI and verify the generated Bronze, Silver and Gold CSV datasets.

8. Verify that the Gold analytical datasets have been successfully loaded into Snowflake.

# What I Learned

Working on this project helped me understand:

- Extracting data from REST APIs.Working with JSON-based API responses.
- Building a Medallion Architecture using Python.Data cleaning and transformation using Pandas.
- Handling missing values and deriving new attributes.Working with CSV-based data pipelines.
- Apache Airflow DAGs and task dependencies.Scheduling and monitoring pipeline executions.
- Running Airflow services using Docker Compose.Understanding Airflow's metadata database.
- Loading analytical datasets into Snowflake.Using SQL MERGE operations for repeatable data loading.
- Building an end-to-end batch data engineering pipeline.

More importantly, this project helped me understand how data ingestion, transformation, orchestration and warehouse loading work together as a complete data engineering system.

# Future Scope

Some possible improvements for the project are:

- Implement better handling of API failures, retries and rate limits.
- Introduce more advanced analytical transformations.
- Add monitoring and alerting for pipeline failures.
- Explore cloud-based deployment and storage.

 Author

Baibhab Gupta

Feedback and suggestions are always welcome.
