import numpy as np
from airflow.hooks.base import BaseHook
import pandas as pd

def safe_float(value):
    if pd.isna(value):
        return None
    return float(value)

def safe_int(value):
    if pd.isna(value):
        return None
    return int(value)

def load_gold_to_snowflake(**context):
    import pandas as pd
    import snowflake.connector
       # Retrieve all generated Gold files from Airflow XCom
    gold_files = context["ti"].xcom_pull(
        key="gold_files",task_ids="gold_aggregate"
    )

    if not gold_files:
        raise ValueError("No Gold datasets found in XCom." )

    print("\nGold files received from XCom:\n")

    for dataset, file in gold_files.items():
        print(f"{dataset} --> {file}")

    # Execution timestamp.This identifies one pipeline execution (snapshot). It will be stored as WINDOW_START in every Snowflake table.
    execution_time = (
        context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
    )
    
    print(f"\nExecution Time : {execution_time}")

    # Read Snowflake connection from Airflow Connections
    # Airflow automatically stores:username,password,# account,warehouse, database, schema, role
    conn = BaseHook.get_connection("flight_snowflake" )  #go to admin->connection->new connectio(+)->connection id is this

    # Create Snowflake Connection
    sf_conn = snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson["account"],
        warehouse=conn.extra_dejson["warehouse"],
        database=conn.extra_dejson["database"],
        schema=conn.schema,
        role=conn.extra_dejson["role"]
    )
    print("\nSuccessfully connected to Snowflake.\n")

    # Cursor is responsible for executing SQL queries.
    # One cursor is reused for the entire loading process.
    cursor = sf_conn.cursor()
    try:

                                   # Start Transaction Either ALL tables are loaded successfully OR none of them are.This prevents partial data loading.
        cursor.execute("BEGIN")
        print("Transaction Started...\n")

        # GOLD DATASET 1        # COUNTRY_SUMMARY
        print("Loading COUNTRY_SUMMARY...\n")
        country_file = gold_files["country_summary"]
        country_df = pd.read_csv(country_file)
        country_df = country_df.head(50)#so large taking hours in compute(sometimes even hanging )
        country_df = country_df.replace({np.nan: None})
        #print(len(country_df))
        country_merge_sql = """
        MERGE INTO COUNTRY_SUMMARY tgt
        USING (
        SELECT
        TO_TIMESTAMP(%s) AS WINDOW_START,
        %s AS ORIGIN_COUNTRY,%s AS TOTAL_FLIGHTS,%s AS UNIQUE_CALLSIGNS,
        %s AS AVG_SPEED_KMH,%s AS MAX_SPEED_KMH,%s AS AVG_GEO_ALTITUDE,
        %s AS AVG_BARO_ALTITUDE,%s AS AVG_ALTITUDE_DIFFERENCE,%s AS GROUNDED_AIRCRAFT,
        %s AS AIRBORNE_AIRCRAFT,%s AS AVG_POSITION_AGE_SECONDS,%s AS SQUAWK_AVAILABLE,%s AS SPI_ENABLED
        ) src
        ON
        tgt.WINDOW_START = src.WINDOW_START
        AND tgt.ORIGIN_COUNTRY = src.ORIGIN_COUNTRY
        
        WHEN MATCHED THEN UPDATE SET
        TOTAL_FLIGHTS = src.TOTAL_FLIGHTS,
        UNIQUE_CALLSIGNS = src.UNIQUE_CALLSIGNS,
        AVG_SPEED_KMH = src.AVG_SPEED_KMH,
        MAX_SPEED_KMH = src.MAX_SPEED_KMH,
        AVG_GEO_ALTITUDE = src.AVG_GEO_ALTITUDE,
        AVG_BARO_ALTITUDE = src.AVG_BARO_ALTITUDE,
        AVG_ALTITUDE_DIFFERENCE = src.AVG_ALTITUDE_DIFFERENCE,
        GROUNDED_AIRCRAFT = src.GROUNDED_AIRCRAFT,
        AIRBORNE_AIRCRAFT = src.AIRBORNE_AIRCRAFT,
        AVG_POSITION_AGE_SECONDS = src.AVG_POSITION_AGE_SECONDS,
        SQUAWK_AVAILABLE = src.SQUAWK_AVAILABLE,
        SPI_ENABLED = src.SPI_ENABLED,
        LOAD_TIME = CURRENT_TIMESTAMP()
        
        WHEN NOT MATCHED THEN INSERT
        (
        WINDOW_START,ORIGIN_COUNTRY,TOTAL_FLIGHTS,
        UNIQUE_CALLSIGNS,AVG_SPEED_KMH,MAX_SPEED_KMH,
        AVG_GEO_ALTITUDE,AVG_BARO_ALTITUDE,AVG_ALTITUDE_DIFFERENCE,
        GROUNDED_AIRCRAFT,AIRBORNE_AIRCRAFT,AVG_POSITION_AGE_SECONDS,
        SQUAWK_AVAILABLE,SPI_ENABLED
        )
        VALUES
        (
        src.WINDOW_START,src.ORIGIN_COUNTRY,src.TOTAL_FLIGHTS,
        src.UNIQUE_CALLSIGNS,src.AVG_SPEED_KMH,src.MAX_SPEED_KMH,
        src.AVG_GEO_ALTITUDE,src.AVG_BARO_ALTITUDE,src.AVG_ALTITUDE_DIFFERENCE,
        src.GROUNDED_AIRCRAFT,src.AIRBORNE_AIRCRAFT,src.AVG_POSITION_AGE_SECONDS,
        src.SQUAWK_AVAILABLE,src.SPI_ENABLED
        );
        """
        for _, row in country_df.iterrows():
        
            cursor.execute(
                country_merge_sql,
                (       
                    execution_time,
                    row["origin_country"],safe_int(row["total_flights"]),int(row["unique_callsigns"]),
                    safe_float(row["avg_speed_kmh"]),safe_float(row["max_speed_kmh"]),safe_float(row["avg_geo_altitude"]),
                    safe_float(row["avg_baro_altitude"]),safe_float(row["avg_altitude_difference"]),safe_int(row["grounded_aircraft"]),
                    safe_int(row["airborne_aircraft"]),safe_float(row["avg_position_age_seconds"]),safe_int(row["squawk_available"]),
                    safe_int(row["spi_enabled"])
                )
            )
        
        print("COUNTRY_SUMMARY Loaded Successfully.\n")
        
        # GOLD DATASET 2  # ALTITUDE_ANALYSIS
        
        print("Loading ALTITUDE_ANALYSIS...\n")
        altitude_file = gold_files["altitude_analysis"]
        altitude_df = pd.read_csv(altitude_file)
        altitude_df = altitude_df.replace({np.nan: None})
        altitude_merge_sql = """
        MERGE INTO ALTITUDE_ANALYSIS tgt
        USING (
        SELECT
        TO_TIMESTAMP(%s) AS WINDOW_START,
        %s AS ALTITUDE_CATEGORY,%s AS AIRCRAFT_COUNT,%s AS AVG_GEO_ALTITUDE,
        %s AS AVG_BARO_ALTITUDE,%s AS AVG_SPEED_KMH,%s AS MAX_SPEED_KMH,
        %s AS AVG_VERTICAL_RATE,%s AS AVG_ALTITUDE_DIFFERENCE,%s AS AVG_POSITION_AGE_SECONDS,
        %s AS AIRBORNE_AIRCRAFT,%s AS GROUNDED_AIRCRAFT,%s AS AVG_HEADING_DEGREE
        ) src
        ON
        tgt.WINDOW_START = src.WINDOW_START
        AND tgt.ALTITUDE_CATEGORY = src.ALTITUDE_CATEGORY
        
        WHEN MATCHED THEN UPDATE SET
        
        AIRCRAFT_COUNT = src.AIRCRAFT_COUNT,
        AVG_GEO_ALTITUDE = src.AVG_GEO_ALTITUDE,
        AVG_BARO_ALTITUDE = src.AVG_BARO_ALTITUDE,
        AVG_SPEED_KMH = src.AVG_SPEED_KMH,
        MAX_SPEED_KMH = src.MAX_SPEED_KMH,
        AVG_VERTICAL_RATE = src.AVG_VERTICAL_RATE,
        AVG_ALTITUDE_DIFFERENCE = src.AVG_ALTITUDE_DIFFERENCE,
        AVG_POSITION_AGE_SECONDS = src.AVG_POSITION_AGE_SECONDS,
        AIRBORNE_AIRCRAFT = src.AIRBORNE_AIRCRAFT,
        GROUNDED_AIRCRAFT = src.GROUNDED_AIRCRAFT,
        AVG_HEADING_DEGREE = src.AVG_HEADING_DEGREE,
        LOAD_TIME = CURRENT_TIMESTAMP()
        
        WHEN NOT MATCHED THEN INSERT
        (
        WINDOW_START,ALTITUDE_CATEGORY,AIRCRAFT_COUNT,
        AVG_GEO_ALTITUDE,AVG_BARO_ALTITUDE,AVG_SPEED_KMH,
        MAX_SPEED_KMH,AVG_VERTICAL_RATE,AVG_ALTITUDE_DIFFERENCE,
        AVG_POSITION_AGE_SECONDS,AIRBORNE_AIRCRAFT,GROUNDED_AIRCRAFT,AVG_HEADING_DEGREE
        )
        VALUES
        (
        src.WINDOW_START,src.ALTITUDE_CATEGORY,src.AIRCRAFT_COUNT,
        src.AVG_GEO_ALTITUDE,src.AVG_BARO_ALTITUDE,src.AVG_SPEED_KMH,
        src.MAX_SPEED_KMH,src.AVG_VERTICAL_RATE,src.AVG_ALTITUDE_DIFFERENCE,
        src.AVG_POSITION_AGE_SECONDS,src.AIRBORNE_AIRCRAFT,src.GROUNDED_AIRCRAFT,src.AVG_HEADING_DEGREE
        );
        """
        for _, row in altitude_df.iterrows():
            cursor.execute(
                altitude_merge_sql,
                (
                    execution_time,
                    row["altitude_category"],safe_int(row["aircraft_count"]),
                    safe_float(row["avg_geo_altitude"]),safe_float(row["avg_baro_altitude"]),
                    safe_float(row["avg_speed_kmh"]),safe_float(row["max_speed_kmh"]),
                    safe_float(row["avg_vertical_rate"]),safe_float(row["avg_altitude_difference"]),
                    safe_float(row["avg_position_age_seconds"]),safe_int(row["airborne_aircraft"]),
                    safe_int(row["grounded_aircraft"]),safe_float(row["avg_heading_degree"])
                )
            )
        print("ALTITUDE_ANALYSIS Loaded Successfully.\n")

        # GOLD DATASET 3        # MOVEMENT_ANALYSIS
        
        print("Loading MOVEMENT_ANALYSIS...\n")
        movement_file = gold_files["movement_analysis"]
        movement_df = pd.read_csv(movement_file)
        movement_df = movement_df.replace({np.nan: None})
        movement_merge_sql = """
        MERGE INTO MOVEMENT_ANALYSIS tgt
        USING (
        SELECT
        TO_TIMESTAMP(%s) AS WINDOW_START,
        %s AS CLIMB_STATUS,%s AS AIRCRAFT_COUNT,%s AS AVG_SPEED_KMH,
        %s AS MAX_SPEED_KMH,%s AS AVG_GEO_ALTITUDE,%s AS AVG_BARO_ALTITUDE,
        %s AS AVG_ALTITUDE_DIFFERENCE,%s AS AVG_VERTICAL_RATE,%s AS AVG_POSITION_AGE_SECONDS,
        %s AS UNIQUE_COUNTRIES,%s AS AIRBORNE_AIRCRAFT,%s AS SQUAWK_AVAILABLE
        ) src
        ON
        tgt.WINDOW_START = src.WINDOW_START
        AND tgt.CLIMB_STATUS = src.CLIMB_STATUS
        
        WHEN MATCHED THEN UPDATE SET
        
        AIRCRAFT_COUNT = src.AIRCRAFT_COUNT,
        AVG_SPEED_KMH = src.AVG_SPEED_KMH,
        MAX_SPEED_KMH = src.MAX_SPEED_KMH,
        AVG_GEO_ALTITUDE = src.AVG_GEO_ALTITUDE,
        AVG_BARO_ALTITUDE = src.AVG_BARO_ALTITUDE,
        AVG_ALTITUDE_DIFFERENCE = src.AVG_ALTITUDE_DIFFERENCE,
        AVG_VERTICAL_RATE = src.AVG_VERTICAL_RATE,
        AVG_POSITION_AGE_SECONDS = src.AVG_POSITION_AGE_SECONDS,
        UNIQUE_COUNTRIES = src.UNIQUE_COUNTRIES,
        AIRBORNE_AIRCRAFT = src.AIRBORNE_AIRCRAFT,
        SQUAWK_AVAILABLE = src.SQUAWK_AVAILABLE,
        LOAD_TIME = CURRENT_TIMESTAMP()
        
        WHEN NOT MATCHED THEN INSERT
        (
        WINDOW_START,CLIMB_STATUS,AIRCRAFT_COUNT,
        AVG_SPEED_KMH,MAX_SPEED_KMH,AVG_GEO_ALTITUDE,
        AVG_BARO_ALTITUDE,AVG_ALTITUDE_DIFFERENCE,AVG_VERTICAL_RATE,
        AVG_POSITION_AGE_SECONDS,UNIQUE_COUNTRIES,AIRBORNE_AIRCRAFT,SQUAWK_AVAILABLE
        )
        VALUES
        (
        src.WINDOW_START,src.CLIMB_STATUS,src.AIRCRAFT_COUNT,
        src.AVG_SPEED_KMH,src.MAX_SPEED_KMH,src.AVG_GEO_ALTITUDE,
        src.AVG_BARO_ALTITUDE,src.AVG_ALTITUDE_DIFFERENCE,src.AVG_VERTICAL_RATE,
        src.AVG_POSITION_AGE_SECONDS,src.UNIQUE_COUNTRIES,src.AIRBORNE_AIRCRAFT,src.SQUAWK_AVAILABLE
        );
        """
        for _, row in movement_df.iterrows():
            cursor.execute(
                movement_merge_sql,
                (   execution_time,
                    row["climb_status"],safe_int(row["aircraft_count"]),
                    safe_float(row["avg_speed_kmh"]),safe_float(row["max_speed_kmh"]),
                    safe_float(row["avg_geo_altitude"]),safe_float(row["avg_baro_altitude"]),
                    safe_float(row["avg_altitude_difference"]),safe_float(row["avg_vertical_rate"]),
                    safe_float(row["avg_position_age_seconds"]),safe_int(row["unique_countries"]),
                    safe_int(row["airborne_aircraft"]),safe_int(row["squawk_available"])
                )
            )
        print("MOVEMENT_ANALYSIS Loaded Successfully.\n")

        # GOLD DATASET 4  # POSITION_SOURCE_ANALYSIS
        
        print("Loading POSITION_SOURCE_ANALYSIS...\n")
        position_source_file = gold_files["position_source_analysis"]
        position_source_df = pd.read_csv(position_source_file)
        position_source_df = position_source_df.replace({np.nan: None})
        position_source_merge_sql = """
        
        MERGE INTO POSITION_SOURCE_ANALYSIS tgt
        USING (
        SELECT
        TO_TIMESTAMP(%s) AS WINDOW_START,
        %s AS POSITION_SOURCE_NAME,%s AS AIRCRAFT_COUNT,%s AS UNIQUE_COUNTRIES,
        %s AS AVG_SPEED_KMH,%s AS AVG_GEO_ALTITUDE,%s AS AVG_BARO_ALTITUDE,
        %s AS AVG_ALTITUDE_DIFFERENCE,%s AS AVG_POSITION_AGE_SECONDS,%s AS GROUNDED_AIRCRAFT,
        %s AS AIRBORNE_AIRCRAFT,%s AS SQUAWK_AVAILABLE,%s AS SPI_ENABLED
        ) src
        ON
        tgt.WINDOW_START = src.WINDOW_START
        AND tgt.POSITION_SOURCE_NAME = src.POSITION_SOURCE_NAME
        
        WHEN MATCHED THEN UPDATE SET
        AIRCRAFT_COUNT = src.AIRCRAFT_COUNT,
        UNIQUE_COUNTRIES = src.UNIQUE_COUNTRIES,
        AVG_SPEED_KMH = src.AVG_SPEED_KMH,
        AVG_GEO_ALTITUDE = src.AVG_GEO_ALTITUDE,
        AVG_BARO_ALTITUDE = src.AVG_BARO_ALTITUDE,
        AVG_ALTITUDE_DIFFERENCE = src.AVG_ALTITUDE_DIFFERENCE,
        AVG_POSITION_AGE_SECONDS = src.AVG_POSITION_AGE_SECONDS,
        GROUNDED_AIRCRAFT = src.GROUNDED_AIRCRAFT,
        AIRBORNE_AIRCRAFT = src.AIRBORNE_AIRCRAFT,
        SQUAWK_AVAILABLE = src.SQUAWK_AVAILABLE,
        SPI_ENABLED = src.SPI_ENABLED,
        LOAD_TIME = CURRENT_TIMESTAMP()
        
        WHEN NOT MATCHED THEN INSERT
        (
        WINDOW_START,POSITION_SOURCE_NAME,AIRCRAFT_COUNT,
        UNIQUE_COUNTRIES,AVG_SPEED_KMH,AVG_GEO_ALTITUDE,
        AVG_BARO_ALTITUDE,AVG_ALTITUDE_DIFFERENCE,AVG_POSITION_AGE_SECONDS,
        GROUNDED_AIRCRAFT,AIRBORNE_AIRCRAFT,SQUAWK_AVAILABLE,SPI_ENABLED
        )
        VALUES
        (
        src.WINDOW_START,src.POSITION_SOURCE_NAME,src.AIRCRAFT_COUNT,
        src.UNIQUE_COUNTRIES,src.AVG_SPEED_KMH,src.AVG_GEO_ALTITUDE,
        src.AVG_BARO_ALTITUDE,src.AVG_ALTITUDE_DIFFERENCE,src.AVG_POSITION_AGE_SECONDS,
        src.GROUNDED_AIRCRAFT,src.AIRBORNE_AIRCRAFT,src.SQUAWK_AVAILABLE,src.SPI_ENABLED
        );
        """
        for _, row in position_source_df.iterrows():
            cursor.execute(
                position_source_merge_sql,
                (   execution_time,
                    row["position_source_name"],safe_int(row["aircraft_count"]),
                    safe_int(row["unique_countries"]),safe_float(row["avg_speed_kmh"]),
                    safe_float(row["avg_geo_altitude"]),safe_float(row["avg_baro_altitude"]),
                    safe_float(row["avg_altitude_difference"]),safe_float(row["avg_position_age_seconds"]),
                    safe_int(row["grounded_aircraft"]),safe_int(row["airborne_aircraft"]),
                    safe_int(row["squawk_available"]),safe_int(row["spi_enabled"])
                )
            )
        print("POSITION_SOURCE_ANALYSIS Loaded Successfully.\n")

        # GOLD DATASET 5    OPERATIONAL_QUALITY
        
        print("Loading OPERATIONAL_QUALITY...\n")
        quality_file = gold_files["operational_quality"]
        quality_df = pd.read_csv(quality_file)
        quality_df = quality_df.replace({np.nan: None})
        quality_merge_sql = """
        MERGE INTO OPERATIONAL_QUALITY tgt
        USING (
        SELECT
        TO_TIMESTAMP(%s) AS WINDOW_START,
        %s AS TOTAL_AIRCRAFT,%s AS UNIQUE_AIRCRAFT,%s AS UNIQUE_CALLSIGNS,
        %s AS COUNTRIES_OBSERVED,%s AS AVG_SPEED_KMH,%s AS MAX_SPEED_KMH,
        %s AS AVG_GEO_ALTITUDE,%s AS AVG_ALTITUDE_DIFFERENCE,%s AS AVG_POSITION_AGE_SECONDS,
        %s AS GROUNDED_AIRCRAFT,%s AS AIRBORNE_AIRCRAFT,%s AS CALLSIGNS_AVAILABLE,
        %s AS SQUAWK_AVAILABLE,%s AS SPI_ENABLED,%s AS POSITION_SOURCES
        ) src
        ON
        tgt.WINDOW_START = src.WINDOW_START
        
        WHEN MATCHED THEN UPDATE SET
        
        TOTAL_AIRCRAFT = src.TOTAL_AIRCRAFT,
        UNIQUE_AIRCRAFT = src.UNIQUE_AIRCRAFT,
        UNIQUE_CALLSIGNS = src.UNIQUE_CALLSIGNS,
        COUNTRIES_OBSERVED = src.COUNTRIES_OBSERVED,
        AVG_SPEED_KMH = src.AVG_SPEED_KMH,
        MAX_SPEED_KMH = src.MAX_SPEED_KMH,
        AVG_GEO_ALTITUDE = src.AVG_GEO_ALTITUDE,
        AVG_ALTITUDE_DIFFERENCE = src.AVG_ALTITUDE_DIFFERENCE,
        AVG_POSITION_AGE_SECONDS = src.AVG_POSITION_AGE_SECONDS,
        GROUNDED_AIRCRAFT = src.GROUNDED_AIRCRAFT,
        AIRBORNE_AIRCRAFT = src.AIRBORNE_AIRCRAFT,
        CALLSIGNS_AVAILABLE = src.CALLSIGNS_AVAILABLE,
        SQUAWK_AVAILABLE = src.SQUAWK_AVAILABLE,
        SPI_ENABLED = src.SPI_ENABLED,
        POSITION_SOURCES = src.POSITION_SOURCES,
        LOAD_TIME = CURRENT_TIMESTAMP()
        
        WHEN NOT MATCHED THEN INSERT
        (
        WINDOW_START,TOTAL_AIRCRAFT,UNIQUE_AIRCRAFT,
        UNIQUE_CALLSIGNS,COUNTRIES_OBSERVED,AVG_SPEED_KMH,
        MAX_SPEED_KMH,AVG_GEO_ALTITUDE,AVG_ALTITUDE_DIFFERENCE,
        AVG_POSITION_AGE_SECONDS,GROUNDED_AIRCRAFT,AIRBORNE_AIRCRAFT,
        CALLSIGNS_AVAILABLE,SQUAWK_AVAILABLE,SPI_ENABLED,POSITION_SOURCES
        )
        VALUES
        (
        src.WINDOW_START,src.TOTAL_AIRCRAFT,src.UNIQUE_AIRCRAFT,
        src.UNIQUE_CALLSIGNS,src.COUNTRIES_OBSERVED,src.AVG_SPEED_KMH,
        src.MAX_SPEED_KMH,src.AVG_GEO_ALTITUDE,src.AVG_ALTITUDE_DIFFERENCE,
        src.AVG_POSITION_AGE_SECONDS,src.GROUNDED_AIRCRAFT,src.AIRBORNE_AIRCRAFT,
        src.CALLSIGNS_AVAILABLE,src.SQUAWK_AVAILABLE,src.SPI_ENABLED,src.POSITION_SOURCES
        );
        """
        for _, row in quality_df.iterrows():
            cursor.execute(
                quality_merge_sql,
                (
                    execution_time,
                    safe_int(row["total_aircraft"]),safe_int(row["unique_aircraft"]),
                    safe_int(row["unique_callsigns"]),safe_int(row["countries_observed"]),
                    safe_float(row["avg_speed_kmh"]),safe_float(row["max_speed_kmh"]),
                    safe_float(row["avg_geo_altitude"]),safe_float(row["avg_altitude_difference"]),
                    safe_float(row["avg_position_age_seconds"]),safe_int(row["grounded_aircraft"]),
                    safe_int(row["airborne_aircraft"]),safe_int(row["callsigns_available"]),
                    safe_int(row["squawk_available"]),safe_int(row["spi_enabled"]),
                    safe_int(row["position_sources"])
                )
            )
        print("OPERATIONAL_QUALITY Loaded Successfully.\n")

        # GOLD DATASET 6 GEOGRAPHICAL_DISTRIBUTION
        
        print("Loading GEOGRAPHICAL_DISTRIBUTION...\n")
        geo_file = gold_files["geographical_distribution"]
        geo_df = pd.read_csv(geo_file)
        geo_df = geo_df.head(50)#so large taking hours in compute(sometimes even hanging )
        geo_df = geo_df.replace({np.nan: None})
        geo_merge_sql = """
        MERGE INTO GEOGRAPHICAL_DISTRIBUTION tgt
        USING (
        SELECT
        TO_TIMESTAMP(%s) AS WINDOW_START,
        %s AS LATITUDE_GRID,%s AS LONGITUDE_GRID,%s AS AIRCRAFT_COUNT,
        %s AS UNIQUE_COUNTRIES,%s AS UNIQUE_CALLSIGNS,%s AS AVG_SPEED_KMH,
        %s AS MAX_SPEED_KMH,%s AS AVG_GEO_ALTITUDE,%s AS AVG_BARO_ALTITUDE,
        %s AS AVG_ALTITUDE_DIFFERENCE,%s AS AVG_VERTICAL_RATE,%s AS AVG_POSITION_AGE_SECONDS,
        %s AS GROUNDED_AIRCRAFT,%s AS AIRBORNE_AIRCRAFT,%s AS SQUAWK_AVAILABLE,%s AS SPI_ENABLED
        ) src
        ON
        tgt.WINDOW_START = src.WINDOW_START
        AND tgt.LATITUDE_GRID = src.LATITUDE_GRID
        AND tgt.LONGITUDE_GRID = src.LONGITUDE_GRID
        
        WHEN MATCHED THEN UPDATE SET
        
        AIRCRAFT_COUNT = src.AIRCRAFT_COUNT,
        UNIQUE_COUNTRIES = src.UNIQUE_COUNTRIES,
        UNIQUE_CALLSIGNS = src.UNIQUE_CALLSIGNS,
        AVG_SPEED_KMH = src.AVG_SPEED_KMH,
        MAX_SPEED_KMH = src.MAX_SPEED_KMH,
        AVG_GEO_ALTITUDE = src.AVG_GEO_ALTITUDE,
        AVG_BARO_ALTITUDE = src.AVG_BARO_ALTITUDE,
        AVG_ALTITUDE_DIFFERENCE = src.AVG_ALTITUDE_DIFFERENCE,
        AVG_VERTICAL_RATE = src.AVG_VERTICAL_RATE,
        AVG_POSITION_AGE_SECONDS = src.AVG_POSITION_AGE_SECONDS,
        GROUNDED_AIRCRAFT = src.GROUNDED_AIRCRAFT,
        AIRBORNE_AIRCRAFT = src.AIRBORNE_AIRCRAFT,
        SQUAWK_AVAILABLE = src.SQUAWK_AVAILABLE,
        SPI_ENABLED = src.SPI_ENABLED,
        LOAD_TIME = CURRENT_TIMESTAMP()
        
        WHEN NOT MATCHED THEN INSERT 
        (WINDOW_START,LATITUDE_GRID,LONGITUDE_GRID,AIRCRAFT_COUNT,
        UNIQUE_COUNTRIES,UNIQUE_CALLSIGNS,AVG_SPEED_KMH,MAX_SPEED_KMH,
        AVG_GEO_ALTITUDE,AVG_BARO_ALTITUDE,AVG_ALTITUDE_DIFFERENCE,
        AVG_VERTICAL_RATE,AVG_POSITION_AGE_SECONDS,GROUNDED_AIRCRAFT,
        AIRBORNE_AIRCRAFT,SQUAWK_AVAILABLE,SPI_ENABLED
        )VALUES
        (
        src.WINDOW_START,src.LATITUDE_GRID,src.LONGITUDE_GRID,src.AIRCRAFT_COUNT,
        src.UNIQUE_COUNTRIES,src.UNIQUE_CALLSIGNS,src.AVG_SPEED_KMH,src.MAX_SPEED_KMH,
        src.AVG_GEO_ALTITUDE,src.AVG_BARO_ALTITUDE,src.AVG_ALTITUDE_DIFFERENCE,
        src.AVG_VERTICAL_RATE,src.AVG_POSITION_AGE_SECONDS,src.GROUNDED_AIRCRAFT,
        src.AIRBORNE_AIRCRAFT,src.SQUAWK_AVAILABLE,src.SPI_ENABLED
        );
        """
        for _, row in geo_df.iterrows():
            cursor.execute(
                geo_merge_sql,
                (
                    execution_time,
                    safe_int(row["latitude_grid"]),safe_int(row["longitude_grid"]),
                    safe_int(row["aircraft_count"]),safe_int(row["unique_countries"]),
                    safe_int(row["unique_callsigns"]),safe_float(row["avg_speed_kmh"]),
                    safe_float(row["max_speed_kmh"]),safe_float(row["avg_geo_altitude"]),
                    safe_float(row["avg_baro_altitude"]),safe_float(row["avg_altitude_difference"]),
                    safe_float(row["avg_vertical_rate"]),safe_float(row["avg_position_age_seconds"]),
                    safe_int(row["grounded_aircraft"]),safe_int(row["airborne_aircraft"]),
                    safe_int(row["squawk_available"]),safe_int(row["spi_enabled"])
                )
            )
        print("GEOGRAPHICAL_DISTRIBUTION Loaded Successfully.\n")

        #GOLD DATASET 7  TIME_ANALYSIS

        print("Loading TIME_ANALYSIS...\n")

        time_file = gold_files["time_analysis"]
        time_df = pd.read_csv(time_file)
        time_df = time_df.replace({np.nan: None})
        time_merge_sql = """
        MERGE INTO TIME_ANALYSIS tgt
        USING(
        SELECT
        TO_TIMESTAMP(%s) AS WINDOW_START,
        %s AS HOUR,%s AS AIRCRAFT_COUNT,%s AS UNIQUE_COUNTRIES,
        %s AS UNIQUE_CALLSIGNS,%s AS AVG_SPEED_KMH,%s AS MAX_SPEED_KMH,
        %s AS AVG_GEO_ALTITUDE,%s AS AVG_BARO_ALTITUDE,%s AS AVG_VERTICAL_RATE,
        %s AS AVG_ALTITUDE_DIFFERENCE,%s AS AVG_POSITION_AGE_SECONDS,%s AS GROUNDED_AIRCRAFT,
        %s AS AIRBORNE_AIRCRAFT,%s AS SQUAWK_AVAILABLE,%s AS SPI_ENABLED
        )src
        ON
        tgt.WINDOW_START=src.WINDOW_START
        AND tgt.HOUR=src.HOUR
        
        WHEN MATCHED THEN UPDATE SET
        
        AIRCRAFT_COUNT=src.AIRCRAFT_COUNT,
        UNIQUE_COUNTRIES=src.UNIQUE_COUNTRIES,
        UNIQUE_CALLSIGNS=src.UNIQUE_CALLSIGNS,
        AVG_SPEED_KMH=src.AVG_SPEED_KMH,
        MAX_SPEED_KMH=src.MAX_SPEED_KMH,
        AVG_GEO_ALTITUDE=src.AVG_GEO_ALTITUDE,
        AVG_BARO_ALTITUDE=src.AVG_BARO_ALTITUDE,
        AVG_VERTICAL_RATE=src.AVG_VERTICAL_RATE,
        AVG_ALTITUDE_DIFFERENCE=src.AVG_ALTITUDE_DIFFERENCE,
        AVG_POSITION_AGE_SECONDS=src.AVG_POSITION_AGE_SECONDS,
        GROUNDED_AIRCRAFT=src.GROUNDED_AIRCRAFT,
        AIRBORNE_AIRCRAFT=src.AIRBORNE_AIRCRAFT,
        SQUAWK_AVAILABLE=src.SQUAWK_AVAILABLE,
        SPI_ENABLED=src.SPI_ENABLED,
        LOAD_TIME=CURRENT_TIMESTAMP()
        
        WHEN NOT MATCHED THEN INSERT
         (
        WINDOW_START,HOUR,AIRCRAFT_COUNT,
        UNIQUE_COUNTRIES,UNIQUE_CALLSIGNS,AVG_SPEED_KMH,
        MAX_SPEED_KMH,AVG_GEO_ALTITUDE,AVG_BARO_ALTITUDE,
        AVG_VERTICAL_RATE,AVG_ALTITUDE_DIFFERENCE,AVG_POSITION_AGE_SECONDS,
        GROUNDED_AIRCRAFT,AIRBORNE_AIRCRAFT,SQUAWK_AVAILABLE,SPI_ENABLED            
         ) VALUES
        (
        src.WINDOW_START,src.HOUR,src.AIRCRAFT_COUNT,
        src.UNIQUE_COUNTRIES,src.UNIQUE_CALLSIGNS,src.AVG_SPEED_KMH,
        src.MAX_SPEED_KMH,src.AVG_GEO_ALTITUDE,src.AVG_BARO_ALTITUDE,
        src.AVG_VERTICAL_RATE,src.AVG_ALTITUDE_DIFFERENCE,src.AVG_POSITION_AGE_SECONDS,
        src.GROUNDED_AIRCRAFT,src.AIRBORNE_AIRCRAFT,src.SQUAWK_AVAILABLE,src.SPI_ENABLED
        );
        """
        for _,row in time_df.iterrows():
            cursor.execute(
                time_merge_sql,
                (
                    execution_time,
                    safe_int(row["hour"]),safe_int(row["aircraft_count"]),
                    safe_int(row["unique_countries"]),safe_int(row["unique_callsigns"]),
                    safe_float(row["avg_speed_kmh"]),safe_float(row["max_speed_kmh"]),
                    safe_float(row["avg_geo_altitude"]),safe_float(row["avg_baro_altitude"]),
                    safe_float(row["avg_vertical_rate"]),safe_float(row["avg_altitude_difference"]),
                    safe_float(row["avg_position_age_seconds"]),safe_int(row["grounded_aircraft"]),
                    safe_int(row["airborne_aircraft"]),safe_int(row["squawk_available"]),
                    safe_int(row["spi_enabled"])
                )
            )
        print("TIME_ANALYSIS Loaded Successfully.\n")

        #GOLD DATASET 8  HEADING_ANALYSIS
        
        heading_file = gold_files["heading_analysis"]
        heading_df = pd.read_csv(heading_file)
        heading_df = heading_df.replace({np.nan: None})
        heading_merge_sql = """
        MERGE INTO HEADING_ANALYSIS tgt
        USING(
        SELECT
        TO_TIMESTAMP(%s) AS WINDOW_START,
        %s AS HEADING,%s AS AIRCRAFT_COUNT,%s AS UNIQUE_COUNTRIES,
        %s AS AVG_SPEED_KMH,%s AS MAX_SPEED_KMH,%s AS AVG_GEO_ALTITUDE,
        %s AS AVG_VERTICAL_RATE, %s AS AVG_ALTITUDE_DIFFERENCE,%s AS AVG_POSITION_AGE_SECONDS,%s AS AVG_TRUE_TRACK,
        %s AS AIRBORNE_AIRCRAFT,%s AS SQUAWK_AVAILABLE
        )src
        ON
        tgt.WINDOW_START=src.WINDOW_START
        AND tgt.HEADING=src.HEADING
        
        WHEN MATCHED THEN UPDATE SET
        
        AIRCRAFT_COUNT=src.AIRCRAFT_COUNT,
        UNIQUE_COUNTRIES=src.UNIQUE_COUNTRIES,
        AVG_SPEED_KMH=src.AVG_SPEED_KMH,
        MAX_SPEED_KMH=src.MAX_SPEED_KMH,
        AVG_GEO_ALTITUDE=src.AVG_GEO_ALTITUDE,
        AVG_VERTICAL_RATE=src.AVG_VERTICAL_RATE,
        AVG_ALTITUDE_DIFFERENCE=src.AVG_ALTITUDE_DIFFERENCE,
        AVG_POSITION_AGE_SECONDS=src.AVG_POSITION_AGE_SECONDS,
        AVG_TRUE_TRACK=src.AVG_TRUE_TRACK,
        AIRBORNE_AIRCRAFT=src.AIRBORNE_AIRCRAFT,
        SQUAWK_AVAILABLE=src.SQUAWK_AVAILABLE,
        LOAD_TIME=CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
         (
         WINDOW_START,HEADING,AIRCRAFT_COUNT,
         UNIQUE_COUNTRIES,AVG_SPEED_KMH,MAX_SPEED_KMH,
         AVG_GEO_ALTITUDE,AVG_VERTICAL_RATE,AVG_ALTITUDE_DIFFERENCE,
         AVG_POSITION_AGE_SECONDS,AVG_TRUE_TRACK,AIRBORNE_AIRCRAFT,
         SQUAWK_AVAILABLE
         ) VALUES
        (
        src.WINDOW_START,src.HEADING,src.AIRCRAFT_COUNT,
        src.UNIQUE_COUNTRIES,src.AVG_SPEED_KMH,src.MAX_SPEED_KMH,
        src.AVG_GEO_ALTITUDE,src.AVG_VERTICAL_RATE,src.AVG_ALTITUDE_DIFFERENCE,
        src.AVG_POSITION_AGE_SECONDS,src.AVG_TRUE_TRACK,src.AIRBORNE_AIRCRAFT,src.SQUAWK_AVAILABLE
        );
        """
        for _,row in heading_df.iterrows():
            cursor.execute(
                heading_merge_sql,
                (
                    execution_time,
                    row["heading"],safe_int(row["aircraft_count"]),
                    safe_int(row["unique_countries"]),safe_float(row["avg_speed_kmh"]),
                    safe_float(row["max_speed_kmh"]),
                    safe_float(row["avg_geo_altitude"]),
                    safe_float(row["avg_vertical_rate"]),safe_float(row["avg_altitude_difference"]),
                    safe_float(row["avg_position_age_seconds"]),safe_float(row["avg_true_track"]),
                    safe_int(row["airborne_aircraft"]),safe_int(row["squawk_available"])
                )
            )
        print("HEADING_ANALYSIS Loaded Successfully.\n")
        sf_conn.commit()
        
        print("Transaction Committed.\n")
        context["ti"].xcom_push( key="snowflake_status",value="SUCCESS")
        print("\nAll Gold datasets loaded successfully.\n")

    except Exception as e:
        print("\nError occurred while loading Gold datasets.\n")
        print(e)
        print("\nRolling back transaction...\n")
        sf_conn.rollback()
        raise

    finally:
        cursor.close()
        sf_conn.close()
        print("\nSnowflake connection closed.\n")
