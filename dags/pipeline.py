import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = Path("/opt/airflow") ##Syntax error prone-PATH,path not allowed only Path

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_ingest import run_bronze_ingestion #This function is responsible for ingesting raw flight data from the OpenSky Network API and saving it to a JSON file in the bronze layer of the data pipeline.
from scripts.silver_transform import run_silver_transform #This function is responsible for transforming the raw flight data from the bronze layer into a structured format and saving it as a CSV file in the silver layer of the data pipeline.
from scripts.gold_aggregate import run_gold_aggregate
from scripts.gold_to_snowflake import load_gold_to_snowflake

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay" : timedelta(minutes=5),
}
#docker exec -it airflow-scheduler airflow dags trigger flights_ops_medallion_pipe[from cli not UI]for distributted big data ui not avaliable
with DAG(
    dag_id="flights_ops_medallion_pipe",#The unique identifier for your workflow in the Airflow UI.
    default_args=default_args,
    start_date=datetime(2025, 12, 10),
    schedule_interval="*/15 * * * *",#cron expression that represents a schedule that runs every 15 minutes. 
    catchup=False,#oes not disable automatic scheduling.It means Airflow won't go back and create all the missed runs between:Instead, it starts scheduling from the current/latest applicable interval.
    tags=["flights", "ops", "medallion", "pipeline"],
) as dag:

    bronze = PythonOperator( #TASK 1: This task is responsible for ingesting raw flight data from the OpenSky Network API and saving it to a JSON file in the bronze layer of the data pipeline.
        task_id="bronze_ingest", #will be seeing this in the UI
        python_callable=run_bronze_ingestion,
    )

    silver = PythonOperator(   #TASK 2: This task is responsible for transforming the raw flight data from the bronze layer into a structured format and saving it as a CSV file.
        task_id = "silver_transform",#will be seeing this in the UI
        python_callable=run_silver_transform,
    )

    gold = PythonOperator(
        task_id = "gold_aggregate",
        python_callable=run_gold_aggregate,
    )

    load_to_snowflake = PythonOperator(
        task_id = "load_gold_to_snowflake",
        python_callable=load_gold_to_snowflake,
    )

    bronze >> silver >> gold >> load_to_snowflake



#import sys-: is to load Python's built-in sys (System) module, which provides functions and variables to interact directly with the Python runtime environment and interpreter
    #sys.argv (Command-Line Arguments): Retrieves a list of arguments passed to a script when running it from the terminal.
    # sys.exit() (Terminating Scripts): Safely stops and exits a running Python script early.
    # sys.path (Module Search Path): Shows or modifies the list of directory paths where Python looks for modules to import.
    # sys.platform and sys.version (System Info): Identifies the operating system (e.g., win32, linux) and the current Python version.
    # sys.stdin / sys.stdout (Standard I/O Streams): Handles low-level reading of inputs and writing of outputs to the console.
    # sys.getsizeof() (Memory Management): Measures the exact memory size of an object in bytes.

#from pathlib import Path  # is for working with file system paths in an object-oriented way, making it easier to manipulate and interact with files and directories.
    #path.name: Extracts the full filename (e.g., data.csv).    path.stem: Extracts the filename without the extension (e.g., data).
    # path.suffix: Extracts only the file extension (e.g., .csv).  path.parent: Gets the immediate parent directory  .path.exists(): Returns True if the path physically exists.
    # path.is_file() / path.is_dir(): Checks the target type
    # Finding Files (Globbing)python# List all Python files in the current directory
    #for file in Path.cwd().glob("*.py"):
    #    print(file.name)

#from datetime import datetime, timedelta   #datetime represents a single point in time (e.g., August 4, 2026, at 6:19 PM).
    # timedelta represents a duration or the difference between two dates/times
    #Supported timedelta Arguments   timedelta constructor:weeks,days,hours,minutes,seconds,milliseconds,microseconds,
    # (Note: timedelta does not accept months or years because their lengths vary. For month/year arithmetic, you can use the external library dateutil.relativedelta.)
    #future date calculation: future_date = datetime.now() + timedelta(days=10)  # 10 days from now
    #past date calculation: past_date = datetime.now() - timedelta(hours=2,minutes=30)  # Subtract 2 hours and 30 minutes from the current time
    #difference between two dates: date1 = datetime(2023, 1, 1) date2 = datetime(2023, 1, 15) difference = date2 - date1  # Returns a timedelta object representing 14 days

#from airflow import DAG # Identification by Airflow as a DAG orchestrator without this is not possible: Airflow's parser scans Python files for specific keywords like airflow 
        # and DAG to register your workflows. 
        # Without this import, the system will not see or load your file as a valid job.

#from airflow.operators.python import PythonOperator  # used in Apache Airflow to execute custom Python functions as tasks within a Directed Acyclic Graph (DAG).
    #Core Parameters :python_callable: The name of the Python function you want to run.
    # op_kwargs: A dictionary of keyword arguments passed directly to your function[order does not matter].Eg-op_kwargs={"name": "Alice", "role": "Admin"}
    #  op_args: A list of positional arguments passed to your function[order matters].Eg-op_args=["Alice", "Admin"]

#AIRFLOW_HOME = Path("/opt/airflow") #is a constant that defines the root directory for all Airflow-related files, including configurations, logs, and DAGs. using Python's object-oriented pathlib library.
    #The specific path /opt/airflow comes from the Official Apache Airflow Docker Image. When the Airflow community built their official container, they chose /opt/airflow as the standard home directory (AIRFLOW_HOME) 
    # for running the application inside isolated environments.

#if str(AIRFLOW_HOME) not in sys.path: # checks if the string representation of the AIRFLOW_HOME path is already present in Python's module search path (sys.path). This ensures that any custom modules or scripts located in the AIRFLOW_HOME directory can be easily imported and used within your Airflow DAGs and tasks.
#    sys.path.insert(0, str(AIRFLOW_HOME)) # If it's not present, it adds it to the beginning of sys.path.
              # is to add the AIRFLOW_HOME directory to the beginning of Python's module search path (sys.path). This allows Python to locate and import custom modules or scripts stored in that directory, ensuring they can be used in your Airflow DAGs and tasks.
              # inserts the path at index 0 (the very beginning).: Python will look here first when you run import custom_xzy_module.
              #In Airflow, you often organize helper code, plugins, or custom operators in separate files within your Airflow directory. If Python cannot find these files, your DAGs will throw {ModuleNotFoundError} exceptions. This snippet fixes that issue dynamically at runtime.
              #Placement: Place this block at the very top of your DAG file, right before your custom import statements.
              #sys.path is the official list of directories where Python looks for modules and packages when you type an import statement.If your AIRFLOW_HOME directory is not in this list, Python remains completely blind to any custom files you write there.

#from scripts.bronze_ingest import run_bronze_ingestion #  By importing this custom function, you can use it as a callable within your Airflow DAG, allowing you to schedule and automate the ingestion process as part of your overall data workflow.

#from scripts.silver_transform import run_silver_transform
#from scripts.gold_aggregate import run_gold_aggregate
#from scripts.load_gold_to_snowflake import load_gold_to_snowflake
#
#default_args = {  #  acts as a central macro configuration. Instead of typing the same settings for every single task, you define them once here. Airflow automatically applies these settings to all tasks inside your workflow (DAG).
#    "owner": "airflow",#Why it is needed:Filtering: Allows users to filter the Airflow UI dashboard to see only their specific workflows. Alerts & Ownership: Helps DevOps and data teams instantly know who to contact if a pipeline breaks in production.
#    "retries": 3, #Resilience: Data networks, APIs, and databases experience brief, random blips (network drops, rate limits, temporary timeouts).Self-Healing: Instead of waking up an engineer at 2:00 AM for a temporary glitch, the system heals itself by trying again.
#    "retry_delay" : timedelta(minutes=5), #Backoff Strategy: Instead of hammering a server with immediate retries, this delay gives it a breather. It reduces the risk of cascading failures in distributed systems.
#}
#
#with DAG(
#    dag_id="flights_ops_medallion_pipe",#The unique identifier for your workflow in the Airflow UI. It should be descriptive enough to convey the purpose of the DAG, such as "flights_ops_medallion_pipe" for a flight operations medallion pipeline.
#    default_args=default_args, # using default_args ensures that all tasks within this DAG inherit the same retry and ownership settings, promoting consistency and reducing redundancy in task definitions.
#    start_date=datetime(2025, 12, 10),#The date when the DAG should start running.
#    schedule_interval="*/30 * * * *", #see cron expression eg .Operator Cheetsheet-:* (Asterisk) — Match any value / every interval., (Comma) — Value list separator (e.g., 1,3,5 for specific days).- (Hyphen) — Range of values (e.g., 1-5 for Mon-Fri)./ (Forward Slash) — Step increments (e.g., */15 for every 15 minutes)
#    catchup=False,#catchup=False tells the scheduler to skip all past missed time intervals and only run the single, most recent schedule. How Catchup Works 1.With catchup=True (default): Airflow creates and runs a backlog of every past interval from your start_date up to the present moment if the DAG was paused or newly unpaused.
                   # 2.With catchup=False: Airflow ignores all historical gaps and triggers a DAG run exclusively for the latest current interval.
                   #eg-schedule="@daily" or "0 0 * * *" & start=1Aug and unpause/start=3 Aug, catchup=True will run for 1Aug,2Aug,3Aug. catchup=False will only run for 3Aug.
#    tags=["flights", "ops", "medallion", "pipeline"],#Labels that help you filter and find this DAG quickly in the Airflow dashboard.[imp in large companies with a lot of DAGs]
#) as dag:  # Python, the with DAG(...) as dag: syntax uses what is called a Context Manager.Previously, you would have to manually create a DAG object[eg my_dag=DAG(dag_id="flights_ops_medallion_pipe", ...)] and then pass it to each task.task_1 = BashOperator(task_id="extract", bash_command="echo 1", dag=my_dag)
            #task_2 = BashOperator(task_id="transform", bash_command="echo 2", dag=my_dag) now, with the context manager syntax with DAG(..) as dag:, task_1 = BashOperator(task_id="extract", bash_command="echo 1") task_2 = BashOperator(task_id="transform", bash_command="echo 2")  # No "dag=my_dag" needed!
#
#    bronze = PythonOperator(  #TASK 1 SEE FOR THE INDENTATION AS PART OF DAG AS IF A PYTHON FUNCTION
#        task_id="bronze_ingest",#dag_id: The unique identifier for the "entire" pipeline (the collection of all tasks). Airflow uses this to track the workflow on the main web dashboard.
                                # task_id: The unique identifier for a single action "within" that pipeline. Airflow uses this to log status, retries, and errors for that specific step.
#        python_callable=run_bronze_ingestion, #is for specifying the Python function that will be executed when this task runs. In this case, it points to the run_bronze_ingestion function imported from the bronze_ingest.py script.
#    )
#
#    silver = PythonOperator(
#        task_id = "silver_transform",
#        python_callable=run_silver_transform,#is for specifying the Python function that will be executed when this task runs. In this case, it points to the run_silver_transform function imported from the silver_transform.py script.

#    )
#
#    gold = PythonOperator(
#        task_id = "gold_aggregate",
#        python_callable=run_gold_aggregate,
#    )
#
#    load_to_snowflake = PythonOperator(
#        task_id = "load_gold_to_snowflake",
#        python_callable=load_gold_to_snowflake,
#    )
#
#    bronze >> silver >> gold >> load_to_snowflake