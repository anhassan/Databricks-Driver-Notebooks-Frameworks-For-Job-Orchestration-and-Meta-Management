
# %run ../utils/driver_utils

base_path = "../"
driver_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
parallel_tasks = ["raw_curated_ingestion","curated_semantic_ingestion"]
series_tasks = ["raw_curated_ingestion","curated_semantic_ingestion"]

run_parallel_notebooks("{}/{}".format(base_path,"code"),parallel_tasks,len(parallel_tasks),driver_path)

run_series_notebooks("{}/{}".format(base_path,"code"),parallel_tasks,driver_path)