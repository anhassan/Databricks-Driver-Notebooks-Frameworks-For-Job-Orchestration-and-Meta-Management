# Databricks notebook source
# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

# MAGIC %run ../../data_engineering/commons/utilities

# COMMAND ----------

!pip install SQLAlchemy

# COMMAND ----------

!pip install pymysql

# COMMAND ----------

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import pymysql

Base = declarative_base()

class DriverLogs(Base):
  __tablename__ = 'driver_logs'
  driver_name = Column(String(256),primary_key=True)
  driver_run_id = Column(String(256))
  driver_run_status = Column(String(256))
  driver_run_time = Column(Integer)
  driver_run_start_time = Column(DateTime)
  driver_run_end_time = Column(DateTime)

class DriverNotebookLogs(Base):
  __tablename__ = 'driver_notebook_logs'
  surrogate_key = Column(Integer,primary_key=True,autoincrement=True)
  driver_name = Column(String(256))
  notebook_name = Column(String(256))
  notebook_status = Column(String(256))
  notebook_run_time = Column(Integer)
  notebook_start_time = Column(DateTime)
  notebook_end_time = Column(DateTime)

# COMMAND ----------

def init_connection(database_type):
    if database_type == "mysql":
        connect_args = {}
        connect_args['ssl_ca'] = mysql_ssl_path
        username,password = mysql_user,mysql_password
        host,database_name = mysql_host,mysql_db
    connection_string = 'mysql+pymysql://{}:{}@{}/{}'.format(username,password,host,database_name)
    database_engine = create_engine(connection_string,connect_args = connect_args)
    return database_engine

def persist_metadata(metadata_obj,database_engine):
    Session = sessionmaker(bind = database_engine)
    session = Session()
    session.add(metadata_obj)
    session.commit()
    session.close()
    
def insert_driver_logs(driver_name,driver_run_id,driver_run_status,driver_run_time
                       ,driver_run_start_time,driver_run_end_time,database_type):
  
    database_engine = init_connection(database_type)
    driver_logs = DriverLogs(driver_name=driver_name,driver_run_id=driver_run_id,driver_run_status=driver_run_status,driver_run_time=driver_run_time,
                          driver_run_start_time=driver_run_start_time,driver_run_end_time=driver_run_end_time)
    persist_metadata(driver_logs,database_engine)

def insert_notebook_logs(driver_name,notebook_name,notebook_status,notebook_run_time,
                         notebook_start_time,notebook_end_time,database_type):
  
    database_engine = init_connection(database_type)
    driver_notebook_logs = DriverNotebookLogs(driver_name = driver_name,notebook_name=notebook_name,notebook_status=notebook_status,
                                         notebook_run_time=notebook_run_time,notebook_start_time=notebook_start_time,notebook_end_time=notebook_end_time)
    persist_metadata(driver_notebook_logs,database_engine)

Base.metadata.create_all(bind = init_connection("mysql"))

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pytz import timezone
import time
import json
import requests

def run_with_meta_management(index,arguments,notebook_path,base_path,driver_name,database_type,default_timezone,logging):
  start_time = time.time()
  start_datetime = datetime.now(default_timezone)
  try:
      notebook_start_time = time.time()
      notebook_start_datetime = datetime.now(default_timezone)
      notebook_arguments = arguments[index] if len(arguments) > 0 and len(arguments[index]) > 0 else {}
      notebook_full_path = "{}/{}".format(base_path,notebook_path)
      dbutils.notebook.run(notebook_full_path,0,notebook_arguments)
      
      if logging:
        notebook_end_time = time.time()
        notebook_end_datetime = datetime.now(default_timezone)
        notebook_run_duration = notebook_end_time - notebook_start_time
        print("Driver Name : {}, Notebook Name : {}, Time Taken : {} , Status : {}".format(driver_name,notebook_path,notebook_run_duration,True))
        insert_notebook_logs(driver_name,notebook_path,"SUCCESS",round(notebook_run_duration),notebook_start_datetime,notebook_end_datetime,database_type)
        
      print("{} Successfull in Time : {}".format(notebook_full_path,notebook_end_time-start_time))
     
      
  except:
      if logging:
        notebook_end_time = time.time()
        notebook_end_datetime = datetime.now(default_timezone)
        notebook_run_duration = notebook_end_time - start_time
        print("Driver Name : {}, Notebook Name : {}, Time Taken : {} , Status : {}".format(driver_name,notebook_path,notebook_run_duration,False))
        insert_notebook_logs(driver_name,notebook_path,"FAILURE",round(notebook_run_duration),start_datetime,notebook_end_datetime,database_type)
        
      print("{} Failed...".format(notebook_full_path))
      raise Exception("{} Failed...".format(notebook_full_path))

  
  

def run_parallel_notebooks(base_path,notebook_paths,num_parallel_jobs,driver_notebook_path,arguments=[],logging=True):
  
  driver_name = driver_notebook_path[driver_notebook_path.rfind("/")+1:]
  default_timezone = timezone('US/Central')
  database_type="mysql"
  driver_run_start_time = time.time()
  driver_run_start_datetime = datetime.now(default_timezone)
  
  with ThreadPoolExecutor(max_workers=num_parallel_jobs) as ec:
     notebooks_status = [ec.submit(run_with_meta_management,index,arguments,notebook_path,base_path,driver_name,database_type,default_timezone,logging) \
                        for index,notebook_path in enumerate(notebook_paths)]
      
  driver_run_end_time = time.time()
  driver_run_end_datetime = datetime.now(default_timezone)
  driver_run_time = driver_run_end_time - driver_run_start_time
  driver_status = [["FAILURE",index] for index,notebook_status in enumerate(notebooks_status) if "Exception" in str(notebook_status)]
  driver_run_status = driver_status[0][0] if len(driver_status) > 0 else "SUCCESS"
  
  if logging:
    driver_run_id = get_driver_run_id(driver_name)
    print("Driver Name : {} , Driver Run Id : {} , Time Taken : {}, Status : {}".format(driver_name,driver_run_id,driver_run_time,driver_run_status))
    insert_driver_logs(driver_name,driver_run_id,driver_run_status,round(driver_run_time),driver_run_start_datetime,driver_run_end_datetime,database_type)
  
  if len(driver_status) > 0:
    notebook_name = notebook_paths[driver_status[0][1]]
    raise Exception("Notebook : {} Failed...".format(notebook_name))
  
  return "Parallel Running took {} seconds".format(driver_run_end_time-driver_run_start_time)
    

def run_series_notebooks(base_path,notebook_paths,driver_notebook_path,arguments=[],logging=True):
  
  driver_name = driver_notebook_path[driver_notebook_path.rfind("/")+1:]
  default_timezone = timezone('US/Central')
  database_type = "mysql"

  execution_times = []
  driver_run_start_time = time.time()
  driver_run_start_datetime = datetime.now(default_timezone)
  
  for index,notebook_path in enumerate(notebook_paths):
    print("Running Notebook : {} ".format(notebook_path))
    start_time = time.time()
    start_datetime = datetime.now(default_timezone)
    try:
          notebook_start_time = time.time()
          notebook_start_datetime = datetime.now(default_timezone)
          notebook_arguments = arguments[index] if len(arguments) > 0 and len(arguments[index]) > 0 else {}
          dbutils.notebook.run("{}/{}".format(base_path,notebook_path),0,notebook_arguments)
          
          if logging:
            notebook_end_time = time.time()
            notebook_end_datetime = datetime.now(default_timezone)
            notebook_run_duration = notebook_end_time - notebook_start_time
            print("Driver Name : {}, Notebook Name : {}, Time Taken : {} , Status : {}".format(driver_name,notebook_path,notebook_run_duration,True))
            insert_notebook_logs(driver_name,notebook_path,"SUCCESS",round(notebook_run_duration),notebook_start_datetime,notebook_end_datetime,database_type)
            
          print("{}/{} Successfull...".format(base_path,notebook_path))
    except:
          if logging:
            notebook_end_time = time.time()
            notebook_end_datetime = datetime.now(default_timezone)
            notebook_run_duration = notebook_end_time - start_time
            print("Driver Name : {}, Notebook Name : {}, Time Taken : {} , Status : {}".format(driver_name,notebook_path,notebook_run_duration,False))
            insert_notebook_logs(driver_name,notebook_path,"FAILURE",round(notebook_run_duration),start_datetime,notebook_end_datetime,database_type)
            
            driver_run_end_time = time.time()
            driver_run_end_datetime = datetime.now(default_timezone)
            driver_run_time = driver_run_end_time - driver_run_start_time
            driver_run_status = "FAILURE"
            driver_run_id = get_driver_run_id(driver_name)
            print("Driver Name : {} , Driver Run Id : {} , Time Taken : {}, Status : {}".format(driver_name,driver_run_id,driver_run_time,driver_run_status))
            insert_driver_logs(driver_name,driver_run_id,driver_run_status,round(driver_run_time),driver_run_start_datetime,driver_run_end_datetime,database_type)
            
          print("{}/{} Failed...".format(base_path,notebook_path))
          raise Exception("{} Failed...".format(notebook_path))
    
    end_time = time.time()
    print("Notebook : {} took {} seconds".format(notebook_path,end_time-start_time))
    execution_times.append((notebook_path,abs(start_time-end_time)))
  
  driver_run_end_time = time.time()
  driver_run_end_datetime = datetime.now(default_timezone)
  driver_run_time = driver_run_end_time - driver_run_start_time
  driver_run_status = "SUCCESS"
  
  if logging:
    driver_run_id = get_driver_run_id(driver_name)
    print("Driver Name : {} , Driver Run Id : {} , Time Taken : {}, Status : {}".format(driver_name,driver_run_id,driver_run_time,driver_run_status))
    insert_driver_logs(driver_name,driver_run_id,driver_run_status,round(driver_run_time),driver_run_start_datetime,driver_run_end_datetime,database_type)
  
  return execution_times
  
def get_tables_from_database(database_name):
  return list(spark.sql("SHOW TABLES IN {}".format(database_name).select("tableName").toPandas()["tableName"]))

def get_table_row_counts(schema_name,tables):
  return [ ("{}.{}".format(schema_name,table),spark.sql("SELECT * FROM {}.{}".format(schema_name,table)).count()) for table in tables]

def keys(d, c = []):
  return [i for a, b in d.items() for i in ([c+[a]] if not isinstance(b, dict) else keys(b, c+[a]))]

def get_driver_run_id(driver_name):
  databricks_instance_name = dbutils.secrets.get(scope="databricks-credentials",key="databricks-instance-name")
  headers={"Authorization": "Bearer {}".format(dbutils.secrets.get(scope="switch-ownership",key="databricks-auth-token"))}
  request_jobs_url = "https://" + databricks_instance_name + "/api/2.0/jobs/list"
  jobs_list = json.loads(requests.get(request_jobs_url, headers=headers).content)['jobs']
  try:
      job_ids_list = [job['job_id'] for job in jobs_list if ['settings','notebook_task','notebook_path'] in keys(job) and \
                      driver_name == job['settings']['notebook_task']['notebook_path'] \
                      [job['settings']['notebook_task']['notebook_path'].rfind("/")+1:]]
  except :
      job_ids_list = []
  if len(job_ids_list) == 0:
    raise Exception("Driver Job Name should be equal to Notebook Name...")
  else:
    job_id = job_ids_list[0]
    request_runs_url = "https://" + databricks_instance_name + "/api/2.0/jobs/runs/list?job_id={}".format(job_id)
    try:
        runs_list = json.loads(requests.get(request_runs_url,headers=headers).content)['runs']
        driver_run_id = [run['run_id'] for run in runs_list]
    except:
        raise Exception("No Run corresponding to the Job Name : {} found".format(driver_name))
    #print("Notebook: {} has Run Ids: {}".format(driver_name,driver_run_id))
    return max(driver_run_id) if len(driver_run_id) > 0 else "No job exists"

print("Driver Utilities Loaded Successfully....")