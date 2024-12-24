from os import popen, path
from pyspark.sql import Row
from datetime import datetime
from inspect import getfullargspec
import argparse

# Logger Table where the logs will be written
LOG_TABLE = "control_db.batch_logs"
LOG_PATH = "s3://spark-logs-bucket/logs/"


class SparkExceptionLogger:
    """
    This decorator class is used to log the exception in spark application.
    It also log the arguments passed to the function,
    the time taken to execute the function,
    the Cluster ID,
    the application ID,
    the process name,
    the script name,
    the sub process name,
    the status of the process,
    the start timestamp,
    the end timestamp,
    the error,
    the error description,
    the time taken to execute the function.
    """

    def __init__(
        self,
        spark,
        process_name,
        script_name,
        sub_process="",
        arg_tracking=False,
        service_name="EMR",
        log_to_path=False,
        log_path="",
    ):
        self.spark = spark
        self.process_name = process_name
        self.script = script_name
        self.sprocess = sub_process
        self.app_id = spark.sparkContext.applicationId
        self.arg_track = arg_tracking
        self.log_table = LOG_TABLE
        self.service_name = service_name
        self.path_logging = log_to_path
        self.log_path = log_path if log_path else LOG_PATH
        self.record_dict = {
            "execution_dt": datetime.date(datetime.now()),
            "process_name": self.process_name,
            "sub_process": self.sprocess,
            "script_name": self.script,
            "cluster_id": (
                self._get_emrid()
                if service_name.upper() == "EMR"
                else self._get_hostname()
            ),
            "application_id": self.app_id,
            "start_ts": datetime.now(),
        }

    def __call__(self, func):
        def wrapper(*_args):
            try:
                if self.arg_track:
                    args_dict = self._track_args(func, _args)
                    self._update_process(args_dict)
                func(*_args)
                self.record_dict["status"] = "completed"
                self.record_dict["end_ts"] = datetime.now()
                self.record_dict["error"] = ""
                self.record_dict["error_desc"] = ""
                self._calculate_time_taken()
                # write into log table
                self._write_log()
            except Exception as e:
                self.record_dict["status"] = "failed"
                self.record_dict["end_ts"] = datetime.now()
                if hasattr(e, "getErrorClass"):
                    self.record_dict["error"] = (
                        e.getErrorClass() if e.getErrorClass() else e.__class__.__name__
                    )
                else:
                    self.record_dict["error"] = e.__class__.__name__
                if hasattr(e, "desc"):
                    self.record_dict["error_desc"] = e.desc
                else:
                    self.record_dict["error_desc"] = str(e)
                self._calculate_time_taken()
                # write into log table
                self._write_log()
                raise e

        return wrapper

    def _get_emrid(self):
        """Gets the EMR ID on which Spark job is running."""
        return str(
            popen('cat /mnt/var/lib/info/job-flow.json | jq -r ".jobFlowId"')
            .read()
            .strip()
        )

    def _get_script_path(self):
        """Gets the script path."""
        return path.realpath(self.script)

    def _calculate_time_taken(self):
        """Calculates the time taken to execute the function."""
        start = self.record_dict["start_ts"]
        end = self.record_dict["end_ts"]
        diff = end - start
        seconds_in_day = 24 * 60 * 60
        mins, secs = divmod(diff.days * seconds_in_day + diff.seconds, 60)
        time_taken = ""
        if mins:
            time_taken = time_taken + f"{mins} min"
        if secs:
            time_taken = time_taken + f"{secs} secs"
        self.record_dict["time_taken"] = time_taken

    def _create_log_record(self):
        """Creates the log Row Object that will be written into the log table"""
        return Row(**self.record_dict)

    def _write_log(self):
        """Writes the log record into the log table"""
        records = [self._create_log_record()]
        if not self.path_logging:
            col_order = self.spark.read.table(self.log_table).limit(1).columns
            self.spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
            self.spark.createDataFrame(records).select(*col_order).write.insertInto(
                self.log_table
            )
        else:
            self.spark.createDataFrame(records).write.mode("append").partitionBy(
                "execution_dt"
            ).parquet(self.log_path)

    def _track_args(self, func_obj, func_args):
        """Tracks the arguments passed to the function."""
        args_space = getfullargspec(func_obj)
        if args_space and func_args:
            arg_names = args_space.args
            args_dict = dict(zip(arg_names, func_args))
            return args_dict
        else:
            return None

    def _update_process(self, param_dict):
        """Updates the process name with the arguments passed to the function.
        This function can be modify to overwrite the process_name or the other parameters
        being written into table from the arguments passed in the function.
        """
        for arg, val in param_dict.items():
            if isinstance(val, argparse.Namespace):
                arg_val_dict = vars(val)
                arg_val_keys = arg_val_dict.keys()
                if "process_name" in arg_val_keys:
                    self.record_dict["process_name"] = arg_val_dict["process_name"]
                if "sub_process" in arg_val_keys:
                    self.record_dict["sub_process"] = arg_val_dict["sub_process"]

    def _get_hostname(self):
        """Gets the hostname on which Spark job is running."""
        return str(popen("hostname").read().strip())
