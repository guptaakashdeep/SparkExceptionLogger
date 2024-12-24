# SparkExceptionLogger

A Lightweight Custom Spark Exception Logger decorator class written in python to log the Exceptions details occurred during a job run.

## Motivation

The main motivation to create this Custom Spark Logger is: 

- To make the debugging of Spark Jobs faster.
- To log all the occured exceptions in a table that can be queried via any query engines like DuckDB, Athena, Trino, etc.
- Easier integration into already existing scripts with minimal code introduction.

Logger logs the below mentioned details:

- `process_name` - Functional Process Name. Can be defined on script level.
- `sub_process` - Functional Sub Process Name. Can be defined on script level. Default is empty string.
- `script_name` - Name of the .py script.
- `cluster_id` - Spark Cluster ID on which spark job is submitted. For e.g., for job running on EMR, emr_id will be here.
- `application_id` - Spark Application ID of the job.
- `status` - Final status of the spark job. `completed` or `failed`
- `start_ts` - Start timestamp of the spark job run.
- `end_ts` - End timestamp of the spark job run.
- `error` - In case job status is `failed`, short description of the error.
- `error_desc` - In case job status is `failed`, Full stacktrace of the error.
- `time_taken` - Total time taken by the script execution.
- `execution_dt` - Execution date of the spark job.

## Inital Setup

SparkExceptionLogger supports writing logs into:

- A Table, defined via `LOG_TABLE` parameter in Decorator class.
- A common path, defined via `LOG_PATH` parameter in Decorator class.

In case, users are interested to write into a table directly. Sample Create table statement for this table can be found in `batch_logs.hql`.

In case the log table is defined with some other name. `LOG_TABLE` defined in `SparkExceptionLogger.py` can be updated to the new log table name.

## Usage and Integration in Spark Python files

It's defined as a decorator class so to make it easier to integrate in the scripts with least amount of code introduction.

### Code Samples

#### Simple Integration by defining `process_name` and `sub_process` as a parameter

```python
# quaterly_sales.py
from pyspark.sql import SparkSession
from SparkExceptionLogger import SparkExceptionLogger

process_name = "sales"
app_name = "quaterly_region_sales"

spark = SparkSession.builder.master("yarn").enableHiveSupport().getOrCreate()

@SparkExceptionLogger(spark, process_name, __file__, sub_process=app_name)
def quaterly_sales_process():
    try:
        # all the spark code with transformation and stuffs
        print("Transformation and other code stuffs.")
    except Exception as e:
        raise e

if __name__ == "__main__":
    quaterly_sales_process()
```

```bash
# sample spark-submit
spark-submit --master yarn --deploy-mode cluster --conf spark.yarn.maxAttempts=1 --driver-memory=4g --conf spark.driver.cores=1 --executor-cores 5 --conf spark.executor.instances=4 --conf spark.dynamicAllocation.minExecutors=2 --conf spark.dynamicAllocation.maxExecutors=10 --executor-memory 14g --py-files <path-to-folder>/SparkExceptionLogger.py <path-to-pyfiles>/quaterly_sales.py
```

#### Integration by passing `process_name` and `sub_process` as  job parameters

To overwrite or define a custom process name by concatenating other parameters requires `args_tracking` to be set to `True`

```python
# quaterly_sales.py
import argparse
from pyspark.sql import SparkSession
from SparkExceptionLogger import SparkExceptionLogger

# process name will be overwritten passed in args
process_name = "dummy_process"

# sub process name if passed via as arguments will be overwritten
app_name = "quaterly_region_sales"

spark = SparkSession.builder.master("yarn").enableHiveSupport().getOrCreate()

@SparkExceptionLogger(spark, process_name, __file__, sub_process=app_name, args_tracking=True)
def quaterly_sales_process(args):
    try:
        # all the spark code with transformation and stuffs
        print("Transformation and other code stuffs.")
    except Exception as e:
        raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--process_name", type=str, required=True, help="Process Name")
    parser.add_argument("-sp", "--sub_process_name", type=str, required=False, help="Sub Process Name")
    parser.add_argument("-x", "--argument X", type=int, required=False, help="Other Argument X")
    args = parser.parse_args()

    # calling main function
    quaterly_sales_process(args)
```

```bash
# sample spark-submit
spark-submit --master yarn --deploy-mode cluster --conf spark.yarn.maxAttempts=1 --driver-memory=4g --conf spark.driver.cores=1 --executor-cores 5 --conf spark.executor.instances=4 --conf spark.dynamicAllocation.minExecutors=2 --conf spark.dynamicAllocation.maxExecutors=10 --executor-memory 14g --py-files <path-to-folder>/SparkExceptionLogger.py <path-to-pyfiles>/quaterly_sales.py -p sales -sp q_region_sales
```

### Additional Notes for expanding SparkExceptionLogger

- For defining custom `process_name` or `sub_process` name by concatenating some other fields can be achieved by modifying the `_update_process` method in `SparkExceptionLogger.py`
- `args_tracking` must be set to `True` for reading `process_name` and `sub_process` or any other argument from the method parameters.
- `@SparkExceptionLogger` only needs to be defined on the main function that is being called on script execution and **NOT** on the helper methods.

---

## V2 Updates

- `SparkExceptionLogger` now support `service_name` and `logging` to a path instead of table.
  - To enable logging to a path instead of on a table:
    - `log_to_path`: Set this to `True`. Default: `False`
    - `log_path`: Set this to path where logs will be written. Default: `"s3://spark-logs-bucket/logs/"`
      - Default `log_path` can be updated by updating `LOG_PATH` in `SparkExceptionLogger.py`
  - To running it for other service other than EMR `service_name`, can have other possible values:
    - Default: `EMR` => populates `cluster_id` column with `EMR ID`.
    - `local` => populate `cluster_id` column with Driver `hostname`.
      - Currently, any other value, just fetches the `hostname` for the driver.
    - Main idea to add `service_name` parameter is to extend the logger functionality to get the `cluster_id` for other services in future.
