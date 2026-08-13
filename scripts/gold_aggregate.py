import pandas as pd
from pathlib import Path


def run_gold_aggregate(**context):

    silver_file = context["ti"].xcom_pull(
        key="silver_file",
        task_ids="silver_transform"
    )

    if not silver_file:
        raise ValueError("Silver file not found in XCom.")

    df = pd.read_csv(silver_file)

    #imp for understanding flight_status and code
    df["airborne"] = ~df["on_ground"]
    df["has_squawk"] = df["squawk"].notna()

    # Create Gold Directory

    gold_path = Path(str(silver_file).replace("silver", "gold")) #not used as last but for identification

    gold_directory = gold_path.parent  # gold_path.parent extracts the parent folder path from gold_path, isolates the base directory
                                       #path containing the target file or folder, allowing scripts to verify, create, or reference that parent folder before saving data.
    gold_directory.mkdir(parents=True, exist_ok=True)

    generated_files = {} #This is just an empty dictionary,later sends "all"(6) the generated files to Airflow.

    # GOLD DATASET 1
    # Country Summary

    country_summary = (
        df.groupby("origin_country")
        .agg(

            total_flights=("icao24", "count"),
            unique_callsigns=("callsign", "nunique"),  #nunique() means = Number of UNIQUE values.
            avg_speed_kmh=("velocity_kmh", "mean"),
            max_speed_kmh=("velocity_kmh", "max"),
            avg_geo_altitude=("geo_altitude", "mean"),
            avg_baro_altitude=("baro_altitude", "mean"),
            avg_altitude_difference=("altitude_difference", "mean"),
            grounded_aircraft=("on_ground", "sum"),
            airborne_aircraft=(
                "flight_status",
                lambda x: (x == "Airborne").sum()
            ),
            avg_position_age_seconds=(
                "position_age_seconds", "mean"
            ),
            squawk_available=(
                "squawk",
                lambda x: x.notna().sum()   #In Pandas, notna() is a function that checks for valid data. It returns True if a value exists (is not missing), and False if the value is empty, NaN, or None
            ),
            spi_enabled=("spi", "sum")

        )
        .reset_index()    #.reset_index() why is it req with group by[else create problem specially with csv indexes]
    )

    # Round decimal columns

    decimal_cols = [        #selecting attributes which have decimal values to make definite rouding offs[avg,max,min] not count,sum etc
        "avg_speed_kmh",
        "max_speed_kmh",
        "avg_geo_altitude",
        "avg_baro_altitude",
        "avg_altitude_difference",
        "avg_position_age_seconds"
    ]

    country_summary[decimal_cols] = (
        country_summary[decimal_cols]
        .round(2)      #rounding off to 2 decimal places
    )

    country_summary = country_summary.sort_values(
        by="total_flights", ascending=False
    )

    country_file = (                      # Equivalent to country_file = Path("/opt/airflow/data/gold/country_summary.csv")
        gold_directory / "country_summary.csv"
    )

    country_summary.to_csv(
        country_file,index=False
    )

    generated_files["country_summary"] = str(country_file)

    # GOLD DATASET 2
    # Altitude Analysis

    altitude_summary = (
        df.groupby("altitude_category")
        .agg(

            aircraft_count=("icao24", "count"),
            avg_geo_altitude=("geo_altitude", "mean"),
            avg_baro_altitude=("baro_altitude", "mean"),
            avg_speed_kmh=("velocity_kmh", "mean"),
            max_speed_kmh=("velocity_kmh", "max"),
            avg_vertical_rate=("vertical_rate", "mean"),
            avg_altitude_difference=("altitude_difference","mean"),
            avg_position_age_seconds=("position_age_seconds","mean"),
            airborne_aircraft=("flight_status",lambda x: (x == "Airborne").sum()),#see the use of lamda fnx=flight_status
            grounded_aircraft=("on_ground", "sum"),
            avg_heading_degree=("true_track", "mean")
        )
        .reset_index()
    )

    decimal_cols = [
        "avg_geo_altitude",
        "avg_baro_altitude",
        "avg_speed_kmh",
        "max_speed_kmh",
        "avg_vertical_rate",
        "avg_altitude_difference",
        "avg_position_age_seconds",
        "avg_heading_degree"
    ]

    altitude_summary[decimal_cols] = (
        altitude_summary[decimal_cols]
        .round(2)
    )

    altitude_summary = altitude_summary.sort_values(
        by="aircraft_count",
        ascending=False
    )

    altitude_file = ( gold_directory / "altitude_analysis.csv" )

    altitude_summary.to_csv(altitude_file,index=False)

    generated_files["altitude_analysis"] = str(altitude_file)

# GOLD DATASET 3
# Heading Analysis

    heading_summary = (
        df.groupby("heading")
        .agg(
            aircraft_count=("icao24", "count"),
            unique_countries=("origin_country", "nunique"),
            avg_speed_kmh=("velocity_kmh", "mean"),
            max_speed_kmh=("velocity_kmh", "max"),
            avg_geo_altitude=("geo_altitude", "mean"),
            avg_vertical_rate=("vertical_rate", "mean"),
            avg_altitude_difference=("altitude_difference", "mean"),
            avg_position_age_seconds=("position_age_seconds", "mean"),
            avg_true_track=("true_track", "mean"),
            airborne_aircraft=(
                "flight_status",
                lambda x: (x == "Airborne").sum()),
            squawk_available=(
                "squawk",
                lambda x: x.notna().sum())   #In Pandas, notna() is a function that checks for valid data. It returns True if a value exists (is not missing), and False if the value is empty, NaN, or None
        )
        .reset_index()
    )
    
    decimal_cols = [
        "avg_speed_kmh",
        "max_speed_kmh",
        "avg_geo_altitude",
        "avg_vertical_rate",
        "avg_altitude_difference",
        "avg_position_age_seconds",
        "avg_true_track"
    ]
    
    heading_summary[decimal_cols] = (
        heading_summary[decimal_cols]
        .round(2)
    )
    
    heading_summary = heading_summary.sort_values(
        by="aircraft_count",
        ascending=False
    )
    
    heading_file = (gold_directory /"heading_analysis.csv")
    
    heading_summary.to_csv(heading_file,index=False)
    
    generated_files["heading_analysis"] = str(heading_file)

# GOLD DATASET 4
    # Movement Analysis
    
    movement_summary = (
        df.groupby("climb_status")
        .agg(
            aircraft_count=("icao24", "count"),
            avg_speed_kmh=("velocity_kmh", "mean"),
            max_speed_kmh=("velocity_kmh", "max"),
            avg_geo_altitude=("geo_altitude", "mean"),
            avg_baro_altitude=("baro_altitude", "mean"),
            avg_altitude_difference=("altitude_difference", "mean"),
            avg_vertical_rate=("vertical_rate", "mean"),
            avg_position_age_seconds=("position_age_seconds", "mean"),
            unique_countries=("origin_country", "nunique"),
            airborne_aircraft=(
                    "flight_status",
                    lambda x: (x == "Airborne").sum()),
            squawk_available=(
                    "squawk",
                    lambda x: x.notna().sum())
        )
        .reset_index()
    )
    
    decimal_cols = [
        "avg_speed_kmh",
        "max_speed_kmh",
        "avg_geo_altitude",
        "avg_baro_altitude",
        "avg_altitude_difference",
        "avg_vertical_rate",
        "avg_position_age_seconds"
    ]
    
    movement_summary[decimal_cols] = (
        movement_summary[decimal_cols]
        .round(2)
    )
    
    movement_summary = movement_summary.sort_values(
        by="aircraft_count",
        ascending=False
    )
    
    movement_file = (
        gold_directory / "movement_analysis.csv"
    )
    
    movement_summary.to_csv(movement_file,index=False)
    
    generated_files["movement_analysis"] = str(movement_file)
# GOLD DATASET 5
# Position Source Analysis
    
    position_source_summary = (
        df.groupby("position_source_name")
        .agg(
            aircraft_count=("icao24", "count"),
            unique_countries=("origin_country", "nunique"),
            avg_speed_kmh=("velocity_kmh", "mean"),
            avg_geo_altitude=("geo_altitude", "mean"),
            avg_baro_altitude=("baro_altitude", "mean"),
            avg_altitude_difference=("altitude_difference", "mean"),
            avg_position_age_seconds=("position_age_seconds", "mean"),
            grounded_aircraft=("on_ground", "sum"),
            airborne_aircraft=("airborne", "sum"),
            squawk_available=("has_squawk", "sum"),
            spi_enabled=("spi", "sum")
        )
        .reset_index()
    )
    
    decimal_cols = [
        "avg_speed_kmh",
        "avg_geo_altitude",
        "avg_baro_altitude",
        "avg_altitude_difference",
        "avg_position_age_seconds"
    ]
    
    position_source_summary[decimal_cols] = (
        position_source_summary[decimal_cols]
        .round(2)
    )
    
    position_source_summary = position_source_summary.sort_values(
        by="aircraft_count",
        ascending=False
    )
    
    position_source_file = (
        gold_directory /"position_source_analysis.csv")
    
    position_source_summary.to_csv(
        position_source_file,index=False)
    
    generated_files["position_source_analysis"] = str(position_source_file)

# GOLD DATASET 6
# Operational Quality Dashboard
    
    quality_summary = pd.DataFrame({
        "snapshot_time":[df["last_contact"].max()],
        "total_aircraft":[df["icao24"].count()],
        "unique_aircraft":[df["icao24"].nunique()],
        "unique_callsigns":[df["callsign"].nunique()],
        "countries_observed":[df["origin_country"].nunique()],
        "avg_speed_kmh":[round(df["velocity_kmh"].mean(),2)],
        "max_speed_kmh":[round(df["velocity_kmh"].max(),2)],
        "avg_geo_altitude":[round(df["geo_altitude"].mean(),2)],
        "avg_altitude_difference":[round(df["altitude_difference"].mean(),2)],
        "avg_position_age_seconds":[round(df["position_age_seconds"].mean(),2)],
        "grounded_aircraft":[df["on_ground"].sum()],
        "airborne_aircraft":[df["airborne"].sum()],
        "callsigns_available":[df["has_callsign"].sum()],
        "squawk_available":[df["has_squawk"].sum()],
        "spi_enabled":[df["spi"].sum()],
        "position_sources":[df["position_source_name"].nunique()]    
    })
    
    quality_file = (
        gold_directory / "operational_quality.csv"
    )
    
    quality_summary.to_csv(
        quality_file,index=False
    )
    
    generated_files["operational_quality"] = str(quality_file)
# GOLD DATASET 7
# Geographical Distribution

    df["latitude_grid"] = (df["latitude"] // 1).fillna(0).astype(int)
    df["longitude_grid"] = (df["longitude"] // 1).fillna(0).astype(int)  #astype=to explicitly cast (convert) the data type of an object to another specified type
                                #can apply astype() to an entire DataFrame, a specific Series (column), or map multiple columns to different data types simultaneously
        #// 1 performs a floor division by 1, which effectively truncates a floating-point coordinate down to its integer floor value (e.g., -73.9425 // 1 becomes -74.0), removing the decimal precision
    geographical_distribution = (
        df.groupby(["latitude_grid", "longitude_grid"])
        .agg(
            aircraft_count=("icao24", "count"),
            unique_countries=("origin_country", "nunique"),
            unique_callsigns=("callsign", "nunique"),
            avg_speed_kmh=("velocity_kmh", "mean"),
            max_speed_kmh=("velocity_kmh", "max"),
            avg_geo_altitude=("geo_altitude", "mean"),
            avg_baro_altitude=("baro_altitude", "mean"),
            avg_altitude_difference=("altitude_difference", "mean"),
            avg_vertical_rate=("vertical_rate", "mean"),
            avg_position_age_seconds=("position_age_seconds", "mean"),
            grounded_aircraft=("on_ground", "sum"),
            airborne_aircraft=("airborne", "sum"),
            squawk_available=("has_squawk", "sum"),
            spi_enabled=("spi", "sum")
        )
        .reset_index()
    )
    
    decimal_cols = [
        "avg_speed_kmh",
        "max_speed_kmh",
        "avg_geo_altitude",
        "avg_baro_altitude",
        "avg_altitude_difference",
        "avg_vertical_rate",
        "avg_position_age_seconds"
    ]
    
    geographical_distribution[decimal_cols] = (
        geographical_distribution[decimal_cols]
        .round(2)
    )
    
    geographical_distribution = geographical_distribution.sort_values(
        by="aircraft_count",ascending=False)
    
    geo_file = (
        gold_directory / "geographical_distribution.csv")
    
    geographical_distribution.to_csv(
        geo_file,index=False)
    
    generated_files["geographical_distribution"] = str(geo_file)

# GOLD DATASET 8
# Time Analysis

    # Extract hour from the last contact timestamp
    df["hour"] = pd.to_datetime(df["last_contact"]).dt.hour     #The unit='s' argument in the pandas.to_datetime() function 
                                  # explicitly tells Python that your numeric input values represent time in seconds.By default, when 
                                  # you pass a number (like an integer or float) to pd.to_datetime(), Pandas assumes the value is in nanoseconds (unit='ns').
                                  # If you are parsing standard Unix timestamps (which measure the number of seconds since January 1, 1970), 
                             #.dt.hour: This pulls out only the hour part from the new time data.
            # It takes Unix timestamp numbers (measured in seconds), turns them into real date and time values, and saves just the hour number (from 0 to 23) for each row.
    time_analysis = (
        df.groupby("hour")
        .agg(
    
            aircraft_count=("icao24", "count"),
            unique_countries=("origin_country", "nunique"),
            unique_callsigns=("callsign", "nunique"),
            avg_speed_kmh=("velocity_kmh", "mean"),
            max_speed_kmh=("velocity_kmh", "max"),
            avg_geo_altitude=("geo_altitude", "mean"),
            avg_baro_altitude=("baro_altitude", "mean"),
            avg_vertical_rate=("vertical_rate", "mean"),
            avg_altitude_difference=("altitude_difference", "mean"),
            avg_position_age_seconds=("position_age_seconds", "mean"),
            grounded_aircraft=("on_ground", "sum"),
            airborne_aircraft=("airborne", "sum"),
            squawk_available=("has_squawk", "sum"),
            spi_enabled=("spi", "sum")
        )
        .reset_index()
    )
    
    decimal_cols = [
        "avg_speed_kmh",
        "max_speed_kmh",
        "avg_geo_altitude",
        "avg_baro_altitude",
        "avg_vertical_rate",
        "avg_altitude_difference",
        "avg_position_age_seconds"
    ]
    
    time_analysis[decimal_cols] = (
        time_analysis[decimal_cols]
        .round(2)
    )
    
    time_analysis = time_analysis.sort_values(
        by="hour"
    )
    
    time_file = (
        gold_directory / "time_analysis.csv"
    )
    
    time_analysis.to_csv(
        time_file,index=False )
    
    generated_files["time_analysis"] = str(time_file)

# Push Generated Files to XCom

    context["ti"].xcom_push(
        key="gold_files",
        value=generated_files
    )

    print("\nGold datasets generated successfully.\n")

    for name, file in generated_files.items():
     print(f"{name}: {file}")