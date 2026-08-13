import requests #this is a popular Python library used for making HTTP requests to interact with web services and APIs.
import json #this is a built-in Python library for working with JSON data, allowing you to parse and generate JSON-formatted data.
from datetime import datetime,timezone #this is a built-in Python module that have classes for working on dates and times in both simple and complex ways.
from pathlib import Path #this is a built-in Python module that provides an object-oriented interface for working with file system paths, making it easier to manipulate and interact with files and directories.

URL="https://opensky-network.org/api/states/all" #this is the URL of the OpenSky Network API endpoint that provides real-time information about the states of aircraft in the airspace.
#It returns data in JSON format, including details such as aircraft positions, velocities, and other relevant information.
def run_bronze_ingestion(**context): # this is a function definition that takes a variable number of keyword arguments (context) 
    #While most developers use the conventional name **kwargs, the double asterisk (**) is the actual functional operator; 
    # you can name the variable whatever you like, and choosing context is highly popular in frameworks like Apache Airflow 
    #to capture execution metadata,,,Why It Is UsedWhen Airflow executes a PythonOperator or a @task decorated function,
    #  it automatically injects a dictionary containing environmental details. Accepting **context catches these variables so you
    #  can use them inside your logic
    #task_instance (or ti): The object representing the current task. Used to pull or push data via XComs.
    # ds: The logical date string of the execution run formatted as YYYY-MM-DD.
    # dag: The parent Directed Acyclic Graph object running this task.
    # data_interval_start / data_interval_end: The start and end timestamps of the data period this run covers
    try:

        response = requests.get(URL, timeout=30)#sends an HTTP GET request to the specified URL (OpenSky Network API endpoint) with a timeout of 30 seconds.
    #response = : Stores the server's return data (like HTML text, JSON data, or status codes) inside the response variable 
    #How the Timeout Works-:Passing a single number (30) splits the limit into two distinct stages:
    # Connect Timeout: It waits up to 30 seconds to establish a physical connection with the remote server.
    # Read Timeout: Once connected, it waits up to 30 seconds between incoming data packets from the server.
    # If either stage takes longer than 30 seconds, Python stops the execution and raises a "requests.exceptions.Timeout" error instead of hanging forever
        response.raise_for_status()
         #Successful Responses: If the HTTP status code is in the 2xx (Success) or 3xx (Redirection) range, the method does absolutely nothing and returns None.
         # Unsuccessful Responses: If the status code is a 4xx (Client Error like 404 Not Found) or 5xx (Server Error like 500 Internal Server Error), it immediately interrupts your code by raising an exception
        print("Success! Data received.")
        print(response.text[:100]) # Print first 100 characters
        
    except requests.exceptions.Timeout:
        print("Error: The request timed out after 30 seconds.")
        
    except requests.exceptions.RequestException as e:
        print(f"A network error occurred: {e}")

        
    data = response.json() #is used in Python to parse a JSON-formatted HTTP response body into a native Python data structure,
        #such as a dictionary or a list.[in the request library only]
        # It maps JSON objects to Python dict types and JSON arrays to Python list type

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") # This line generates a timestamp string representing the current UTC date and time,
        #formatted as "YYYYMMDDHHMMSS".Coordinated Universal Time (UTC) is the primary global time standard kept by precise atomic clocks, 
        #and India Standard Time (IST) is 5 hours and 30 minutes ahead of it (expressed as UTC+5:30).
        #now(datetime.timezone.utc) is used over depreciated function utcnow() of datetime() class to get the current UTC time as a timezone-aware datetime object.

    path = Path(f"/opt/airflow/data/bronze/flights_{timestamp}.json") #creates a file path for storing the JSON data. It uses the current UTC timestamp to create a unique filename, ensuring that each ingestion run produces a separate file. 
        #The Path class from the pathlib module is used to handle file system paths in an object-oriented way.
        #Syntax error prone-PATH,path not allowed only Path

    with open(path, "w") as f:# opens a file at the specified path in write mode ("w"). If the file doesn't exist, it will be created. If it does exist, its contents will be overwritten.
             json.dump(data, f)# converts a Python object (like a dictionary or list) into a JSON-formatted string and writes it to a file-like object (in this case, the file opened as f).
    
    context["ti"].xcom_push(key="bronze_file", value=str(path)) # automatically pushes[by xcom_push()] the path of the saved JSON file to Airflow's XCom (cross-communication) system,
        #allowing other tasks in the DAG to access this information later[with xcom_pull(key="bronze_file")]. The key "bronze_file" is used to identify this specific piece of data.
        # ti stands for "task instance" and represents the current execution of the task. It provides methods to interact with Airflow's XCom system, enabling tasks to share data between each other.
        #context["ti"] is a reference to the task instance object, which is part of the context dictionary[metadata,runtime data] passed to the function. It allows you to interact with the current task's execution context, including pushing and pulling data from XComs.
    print(f"Bronze file saved successfully: {path}")
