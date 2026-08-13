import json
import pandas as pd
from pathlib import Path

# Helper Functions

def heading_direction(angle):  #Convert aircraft heading (0°-360°) into one of the   eight compass directions[N,N-E,E,S-E,S,S-W,W,N-W]
    
    if pd.isna(angle):   #checks if the input angle is NaN (Not a Number) or missing. 
        return "Unknown"

    directions = [
        "North",
        "North-East",
        "East",
        "South-East",
        "South",
        "South-West",
        "West",
        "North-West"
    ]
    return directions[int((angle + 22.5) // 45) % 8] #to get the correct compass direction based on the input angle.

def climb_status(rate):  #Categorize aircraft movement using vertical rate.
    
    if pd.isna(rate):
        return "Unknown"

    if rate > 1:
        return "Ascending"
    elif rate < -1:
        return "Descending"
    else:
        return "Level Flight"


def altitude_category(altitude): #Categorize aircraft altitude.

    if pd.isna(altitude):
        return "Unknown"

    if altitude < 1000:
        return "Low"

    elif altitude < 5000:
        return "Medium"

    elif altitude < 10000:
        return "High"

    else:
        return "Cruising"

# Silver Transformation

def run_silver_transform(**context):

    execution_date = context["ds_nodash"]  #pull execution date  from the current DAG run in YYYYMMDD (format without dashes) ,used for naming silve layer file later

    bronze_file = context["ti"].xcom_pull(  #getting it from bronze_ingest.py file from xcom_push()
        key="bronze_file",
        task_ids="bronze_ingest"   #from DAG file[pipeline.py]
    )

    if not bronze_file:    
        raise ValueError("Bronze file path not found in XCom")

    silver_path = Path("/opt/airflow/data/silver")
    silver_path.mkdir(parents=True, exist_ok=True) #create the directory if it doesn't exist, including any necessary parent 

    # Read Bronze JSON.
    with open(bronze_file) as f:  #not mentioning the operation mode, it defaults to read mode ("r"). 
        raw = json.load(f)     #json to Python object (like a dictionary or list)
 

    # Convert API response into DataFrame.
    df_raw = pd.DataFrame(raw["states"])

    # Assign column names based on the OpenSky REST API documentation.
    df_raw.columns = [ #naming all the attributes from the Opensky api documentation[https://openskynetwork.github.io/opensky-api/rest.html], so that we can easily access them by name instead of by index.
        "icao24", "callsign","origin_country",
        "time_position","last_contact","longitude",
        "latitude","baro_altitude","on_ground",
        "velocity","true_track","vertical_rate",
        "sensors","geo_altitude","squawk",
        "spi","position_source"
         #"category" #Not used as it is not present in the API response, but was in the API docs. This prevents errors when assigning column names to the DataFrame.

    ]
# Timestamp Conversion

    # Convert Unix timestamps into readable datetime.
    df_raw["time_position"] = pd.to_datetime(
        df_raw["time_position"],
        unit="s",
        errors="coerce"
    )

    df_raw["last_contact"] = pd.to_datetime(
        df_raw["last_contact"],
        unit="s",
        errors="coerce"
    )

# Standardization

    # Remove trailing spaces from callsigns.
    df_raw["callsign"] = (
        df_raw["callsign"]
        .fillna("UNKNOWN")    #Changes any empty or NaN values to the string "UNKNOWN" using fillna().
        .str.strip()         #Removes leading and trailing spaces from every text entry using .str.strip()
    )

    # Standardize country names.
    df_raw["origin_country"] = (
        df_raw["origin_country"]
        .fillna("Unknown")
        .str.strip()
        .str.title()        #.title(): Applies standard title-case formatting, turning the first character of every word to uppercase and the rest to lowercase
    )

# Data Cleaning

    # Remove duplicate aircraft states.
    df_raw = df_raw.drop_duplicates(
        subset=["icao24", "last_contact"]    #icao24 alone is not enough.It can happen the same aircraft, but observed at different times.So for uniqueness(key)
    )

    # Remove invalid latitude values.
    df_raw['latitude'] = pd.to_numeric(df_raw['latitude'], errors='coerce') #Force the column to be numeric (bad text becomes NaN).Force non-numeric or bad entries to turn into NaN using errors='coerce'.
    df_raw = df_raw[
        df_raw["latitude"].between(-90, 90)
    ]

    # Remove invalid longitude values.
    df_raw['longitude'] = pd.to_numeric(df_raw['longitude'], errors='coerce') #Force the column to be numeric (bad text becomes NaN).[non-numeric]
    df_raw = df_raw[
        df_raw["longitude"].between(-180, 180)
    ]

    # Remove unrealistic aircraft speeds.
    df_raw = df_raw[
        df_raw["velocity"].isna() |    #iit feel confusing but it is kept becouse speed may be NaN but other fields are there that will be used
        df_raw["velocity"].between(0, 600)    #Helicopter	60–90,,Small propeller aircraft	50–120,Passenger jet	200–270,Boeing 777 cruise	250–260,Fighter jet	300–700+
                                                #So a cutoff of 600 m/s (≈2160 km/h) is very generous.
    ]

    # Remove unrealistic altitudes.
    df_raw = df_raw[
        df_raw["geo_altitude"].isna() |    #Same reason as velocity
        (df_raw["geo_altitude"] >= -500)   #Dead Sea shoreline: about -430 m below mean sea level.Using -500 m provides a small buffer below the lowest expected terrain[GPS inaccuracies].
    ]

# Feature Engineering

    # Convert speed from m/s to km/h.
    df_raw["velocity_kmh"] = (
        df_raw["velocity"] * 3.6
    ).round(2)

    # Human-readable flight status.
    df_raw["flight_status"] = df_raw["on_ground"].map({ #mappping values for easy understanding
        True: "On Ground",
        False: "Airborne"
    })
    # Convert heading angle into compass direction.
    df_raw["heading"] = (
        df_raw["true_track"]
        .apply(heading_direction)  #apply helper functions for more info extraction
    )
    # Human-readable climb status.
    df_raw["climb_status"] = (
        df_raw["vertical_rate"]
        .apply(climb_status)     ##apply helper functions for more info extraction
    )
    # Categorize altitude.
    df_raw["altitude_category"] = (
        df_raw["geo_altitude"]
        .apply(altitude_category)  #apply helper functions for more info extraction
    )
    # Callsign availability
    df_raw["has_callsign"] = (
        df_raw["callsign"] != "UNKNOWN"
    )

    # Difference between GPS altitude and barometric altitude.
    df_raw["altitude_difference"] = (  #data quality monitoring like -:Average altitude difference,Maximum difference
        df_raw["geo_altitude"] -
        df_raw["baro_altitude"]
    ).round(2)

    # Time since the aircraft's position was last updated.This is NOT network latency. It measures how old/stale
    # the position information is compared to the latest.received aircraft message.
    df_raw["position_age_seconds"] = (   #data freshness monitoring like-:Average position freshness,Maximum stale aircraft,Aircraft with stale GPS
        df_raw["last_contact"] -
        df_raw["time_position"]
    ).dt.total_seconds()
    # Decode Special Position Indicator.
    df_raw["special_priority"] = (  #specialor operation related or emergency representation
        df_raw["spi"].map({
            True: "Yes",
            False: "No"
        })
    )
    # Decode Position Source.
    position_source_map = {  #4 types technical origin or tracking method used to determine and report an aircraft’s geographic location
        0: "ADS-B",
        1: "ASTERIX",
        2: "MLAT",
        3: "FLARM"
    }

    df_raw["position_source_name"] = (
        df_raw["position_source"]
        .map(position_source_map)
        .fillna("Unknown")
    )
# Final Silver Dataset

    # Keep both raw attributes + new aadded attributes.
    df = df_raw[
        [
            "icao24",
            "callsign",
            "origin_country",

            "has_callsign",
            "time_position",
            "last_contact",

            "latitude",
            "longitude",

            "baro_altitude",
            "geo_altitude",
            "altitude_difference",
            "altitude_category",

            "velocity",
            "velocity_kmh",

            "true_track",
            "heading",

            "vertical_rate",
            "climb_status",

            "on_ground",
            "flight_status",

            "sensors",

            "squawk",

            "spi",
            "special_priority",

            "position_source",
            "position_source_name",

            "position_age_seconds",

        ]
    ]

    # Save Silver layer as CSV.
    output_file = silver_path / f"flights_silver_{execution_date}.csv" #see from above,how the file name is constructed.
    df.to_csv(output_file, index=False)  #storing in CSV format, with index=False to exclude the DataFrame's noing of the rows.

    # Push Silver file path to XCom for Gold layer.
    context["ti"].xcom_push(
        key="silver_file",
        value=str(output_file)
    )

    print(f"Silver layer saved successfully: {output_file}")



############################################################################################
#import json  #loads the built-in module for working with JSON data. You use json.loads() to parse JSON strings into dictionaries
              #  and json.dumps() to convert Python objects back into JSON strings. 
#import pandas as pd #loads the Pandas data analysis library into your Python workspace,pandas.DataFrame()=create a table like structure,
         # pd.read_csv("")=load data,df.info()=is a method for concise summary of a DataFrame{mem usage, column dtypes, non-null counts},df.describe()=generates descriptive statistics of a DataFrame 
#from pathlib import Path #provides an object-oriented approach to handling file system paths across Windows, macOS, and Linux
#
#def run_silver_transform(**context):
#    execution_date = context["ds_nodash"] # Python dictionary key that returns the DAG run's logical date formatted as a continuous string of numbers without dashes (YYYYMMDD).
            #data Type: str Format Pattern: YYYYMMDD (e.g., 20260805)
            #Retrieved via kwargs['ds_nodash'] or context['ds_nodash'] inside a python_callable
            #File Naming:unique log/data file prefixes like report_20260805.csv.  Database Partitioning:table names or partition keys (e.g., sales_20260805).
            # API Payloads: Passing compact date strings into external request parameters where dashes are invalid.
#
#    bronze_file = context["ti"].xcom_pull(   #context is for dictionary containing metadata about the current Airflow DAG run, passed automatically to tasks
#        key="bronze_file",                     #xcom_pull() is a method of the TaskInstance object that retrieves data from Airflow's XCom system, allowing tasks to share information. Here, it fetches the path of the JSON file saved by the bronze_ingest task.
#        task_ids="bronze_ingest"          #Need of task_ids-:You need the task_ids parameter because XCom keys are not globally unique across a workflow.Multiple tasks within the same Directed Acyclic Graph (DAG) can push different data using the exact same key name
#    )
#
#    if not bronze_file:    #if file is not found in the XCom, raise an error
#        raise ValueError("Bronze file path not found in XCom")  #ValueError is a built-in exception in Python that is raised when a function receives an argument of the right type but an inappropriate value, indicating that the input does not meet the expected criteria.
                        # Why a ValueError is Used Here   1.The Type is Right: The variable bronze_file is likely expected to be a string (the file path). If XCom fails to find it, it returns None.
                        # The Value is Wrong: A value of None or an empty string means the downstream task cannot do its job because the file path is missing.
                        # Intentional Crash: By raising ValueError, you intentionally crash the Airflow task so it marks itself as Failed. This alerts engineers and prevents the script from crashing later with a confusing error (like trying to open a file named None).
                        #Why ValueError is correct on line 9:At this specific point in your code, you are not checking if a file exists on your hard drive. You are checking if Airflow's XCom database successfully handed you a text string. If bronze_file is empty (None), the value returned by XCom is missing/invalid. Python hasn't even looked at your file system yet.
                        #Where FileNotFoundError would happen:A FileNotFoundError is specifically for when you have a path string (like "/opt/airflow/data/bronze/data.csv"), you tell Python to open it, but the physical file does not exist on the disk.
#    silver_path = Path("/opt/airflow/data/silver") #Converts the raw text string into a robust Python Path object. This makes it easy to manipulate the path, check if it exists, or create directories in a cross-platform way.
#    silver_path.mkdir(parents=True, exist_ok=True) #mkdir(...): The method that actually tells the operating system to create the physical folder on the disk.
#                       #parents=True: Creates any missing folders along the path. If /opt/airflow/ exists but data/ does not, Python will automatically create both data/ and silver/. Without this, Python would crash if a middle folder was missing.
                        # exist_ok=True: Prevents Python from crashing if the folder already exists. Since this DAG will run repeatedly (e.g., daily), the folder will already be there on day two. This setting ensures the code runs smoothly without errors on subsequent runs.
#    with open(bronze_file) as f:  #here we are reading which is the default case so we are not mentioning the operation mode, it defaults to read mode ("r"). 
                                    # do not forget with is context manager.it guarantees that Python will automatically close the file when you are done, even if the code inside crashes. This prevents memory and file leaks.
#        raw = json.load(f)  #converts the JSON-formatted text in the file into a native Python object (like a dictionary or list). This allows you to work with the data in Python without manually parsing the JSON.
#               #Why r instead of w? In Python, open(bronze_file) defaults to mode="r" (read mode).r mode opens an existing file so Python can read its contents. It protects your data from being modified.
#               #w mode opens a file for writing. Crucially, w mode instantly deletes (overwrites) everything inside the file the moment it opens. If you used w here, your raw bronze data would be wiped out before you could read it
                #understand the diff b/w json.load() and json.loads(),json.dump() and json.dumps() 
                # :json.load() reads JSON data from a file-like object and converts it into a Python object, while json.loads() reads JSON data from a string. Conversely, json.dump() writes a Python object to a file-like object as JSON, while json.dumps() converts a Python object into a JSON-formatted string.
                #for json.dump() use w op and for json.load() use r op
#    df_raw = pd.DataFrame(raw["states"])  # converts a specific part of your JSON data into a Pandas DataFrame, which is the ideal data structure for cleaning and transforming data
#                           #pd.DataFrame(...): Takes that list of dictionaries and converts it into a structured table with labeled rows and columns. Each dictionary key becomes a column header, and each dictionary values becomes a row.
#    df_raw.columns = [  #naming all the attributes from the api documentation, so that we can easily access them by name instead of by index. This makes the DataFrame more readable and easier to work with.
#        "icao24", .... "position_source"
#    ]

#    df = df_raw[  #taking out only the items we need
#        [
#            "icao24","origin_country","velocity","on_ground"
#        ]
#    ]
#    output_file = silver_path / f"flights_silver_{execution_date}.csv" # uses two var from above silver_path and execution_date to construct a unique file path for the silver layer CSV file. 
#                       #The filename includes the execution date to ensure that each run of the DAG produces a separate file, preventing overwrites.
#    df.to_csv(output_file, index=False) #do not confuse attributes [like "icao24","origin_country" etc] with index=False, tells Pandas not to write the row numbers (index) into the CSV file. 
#                                        #This keeps the CSV clean and focused only on the actual data you care about.
#
#    context["ti"].xcom_push(# same as in bronze_ingest.py, automatically pushes the path of the saved CSV file to Airflow's XCom (cross-communication) system,for gold layer to access
                             #  this information later[with xcom_pull(key="silver_file")]. The key "silver_file" is used to identify this specific piece of data.
#        key="silver_file",
#        value=str(output_file)
#    )


#pd.isna()=etects missing or NA data in a flexible way across different data types  
      # .Matched Values: It returns True for None, numpy.nan (Not a Number), and pandas time/date missing markers like pd.NaT.Unmatched Values: It returns False for valid numbers, text strings, zero, or boolean values (True/False).

#directions[int((angle + 22.5) // 45) % 8]=The formula divides the 360° circle into 8 equal segments of 45° each, with an offset of 22.5° to center the ranges around the cardinal and intercardinal directions.
        #To make the mapping natural, North needs to be the center of its slice, spanning 22.5° to the left and 22.5° to the right (from 337.5° to 22.5°).By adding 22.5 to the angle before dividing, you effectively shift the entire circle backward. This centers each named direction over its numerical degree.

####Meaning of the terms of API response###

#ICAO24 code (Mode S hex code) is a unique 24-bit address assigned by national registries to individual aircraft
#An aviation callsign is a spoken, operational identifier used by pilots and air traffic control (ATC) over the radio[temporary].
       #  An ICAO24 address is a permanent, unique 24-bit hardware hex code hardcoded into an aircraft's transponder to identify the physical airframe digitally
#origin_country -:Country where the aircraft is registered (not current location).
#time_position (Unix Timestamp)Time when aircraft last transmitted its position. Can be null.Converted to datetime for easier analysis.
#Last Contact The final recorded moment the ground station or satellite received a raw data packet, status ping, or radio frequency response from the aircraft.
        #whereas last position(for time position) refers specifically to the last time valid geographic coordinate data (atitude, longitude, and altitude) was computed and updated
#longitude WGS-84 longitude in decimal degrees. Can be null.WGS-84 longitude in decimal degrees is expressed as a numerical value ranging from -180.000000° to +180.000000°, where positive values denote the Eastern Hemisphere and negative values denote the Western Hemisphere
#latitude WGS-84 latitude in decimal degrees. Can be null.nder the WGS-84 geodetic standard, latitude in decimal degrees is expressed as a signed numerical value ranging from -90.000000° to +90.000000°. Positive values indicate the Northern Hemisphere, and negative values indicate the Southern Hemisphere
#Baro Altitude?Measurement method: Uses a sensitive barometer to read local static air pressure and translates it into feet or meters
#WGS84 Altitude?Measurement method: Geometric height calculated via satellite navigation systems (GNSS/GPS).
#Key Differences b/w baro alti and WGS84: Reference base: Baro altitude relies on atmospheric pressure tied to sea level, whereas WGS84 relies on a geometric mathematical ellipsoid.Weather sensitivity: Baro altitude changes with wind, storms, and warm/cold air masses; WGS84 stays stable regardless of weather.
                  # Discrepancy: WGS84 height can differ from barometric/sea-level altitude by tens of meters because the actual ocean surface (geoid) does not perfectly match the flat mathematical WGS84 ellipsoid
#on_ground -:Boolean indicating whether the aircraft is on the ground (True) or in the air (False).
#velocity -:Speed of the aircraft in meters per second (m/s). Can be null.
#true_track=(Float)Aircraft heading measured clockwise from North (degrees).Used to derive compass direction.
        #0°   = North,90°  = East,180° = South,270° = West,360° = North,,Since we're using 8 compass directions, the ranges are:
        #Degrees	Direction,337.5°–360° and 0°–22.5°	North,22.5°–67.5°	North-East,67.5°–112.5°	East,112.5°–157.5°	South-East,157.5°–202.5°	South,202.5°–247.5°	South-West,247.5°–292.5°	West,292.5°–337.5°	North-West,
#vertical_rate(Float)Rate of climb/descent (m/s). Positive = climbing, Negative = descending.Used for climb status.
#sensors List IDs of OpenSky receivers that detected the aircraft.Used to calculate receiver count.Receiver coverage analysis.[no use here as all nulls]
#Geometric (Geo) altitude is the actual direct vertical distance (like a physical ruler) of an object above a defined reference surface. It is typically measured using satellite navigation (GNSS/GPS)
        #Search for geo_altitude[no dependencies accurate] vs baro_alti vs wgs84[type of geo altitude but have ref dependencies]
#squawk means setting a specific four-digit code into an aircraft's transponder so air traffic control can identify and track the plane on radar.
      #Assigned Codes: Unique numbers given to regular flights for tracking.7500: Hijacking or unlawful interference.7600: Lost radio communication.7700: General emergency
      #Serach for squawk(changes in every region) vs icao24(permanent on flight) vs callsign(temporary but for a full one flight same) [all are unique identifiers but for different purposes]
#spi=API most commonly stands for Special Position Identification (or Special Purpose Identification) pulse,Indicates special/emergency handling.
#position_source=In aviation and flight tracking data, a position_source defines the technical origin or tracking method used to determine and report an aircraft’s geographic location[4 types 0 = ADS-B,1 = ASTERIX,2 = MLAT,3 = FLARM]
        #ADS-B (Automatic Dependent Surveillance-Broadcast): The aircraft calculates its own precise position using an internal GNSS/GPS receiver and automatically broadcasts it via a transponder.
        # MLAT (Multilateration): Ground stations measure the time difference of arrival (TDOA) of a transponder signal to calculate the aircraft's position independently.
        # ASTERIX / Radar: Data derived or categorized from ground-based secondary or primary radar surveillance feeds.
        # FLARM: A specific traffic awareness and collision avoidance technology mostly used by gliders and light aviation