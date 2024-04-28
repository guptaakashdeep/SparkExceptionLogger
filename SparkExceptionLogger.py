from os import popen, path
from pyspark.sql import Row
from datetime import datetime
from inspect import getfullargspec
import argparse

LOG_TABLE = "control_db.batch_logs"

class SparkExceptionLogger:
    def __init__(self, spark, process_name, script_name, sub_process="", arg_tracking=False):
        self.spark = spark
        self.process_name = process_name
        self.script = script_name
        self.sprocess = sub_process
        self.appId = spark.sparkContext.applicationId
        self.argTrack = arg_tracking
        self.log_table = LOG_TABLE
        self.record_dict = {
                "execution_dt": datetime.date(datetime.now()),
                "process_name": self.process_name,
                "sub_process": self.sprocess,
                "script_name": self.script,
                "emr_id": self._get_emrid(),
                "application_id": self.appId,
                "start_ts": datetime.now()
            }
     
    def __call__(self, func):
        def wrapper(*_args):
            try:
                if self.argTrack:
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
                if hasattr(e, 'getErrorClass'):
                    self.record_dict["error"] = e.getErrorClass() if e.getErrorClass() else e.__class__.__name__
                else:
                    self.record_dict["error"] = e.__class__.__name__
                if hasattr(e, 'desc'):
                    self.record_dict["error_desc"] = e.desc
                else:
                    self.record_dict["error_desc"] = str(e)
                self._calculate_time_taken()
                # write into log table
                self._write_log()
                raise e
        return wrapper

    def _get_emrid(self):
        return str(
            popen('cat /mnt/var/lib/info/job-flow.json | jq -r ".jobFlowId"')\
                .read().strip()
            )

    def _get_script_path(self):
        return path.realpath(self.script)

    def _calculate_time_taken(self):
        start = self.record_dict["start_ts"]
        end = self.record_dict["end_ts"]
        diff = end - start
        seconds_in_day = 24*60*60
        mins, secs = divmod(diff.days * seconds_in_day + diff.seconds, 60)
        time_taken = ""
        if mins:
            time_taken = time_taken + f"{mins} min"
        if secs:
            time_taken = time_taken + f"{secs} secs"
        self.record_dict["time_taken"] = time_taken

    def _create_log_record(self):
        return Row(**self.record_dict)

    def _write_log(self):
        records = [self._create_log_record()]
        col_order = self.spark.read.table(self.log_table).limit(1).columns
        self.spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")
        self.spark.createDataFrame(records).select(*col_order).write.insertInto(self.log_table)

    def _track_args(self, func_obj, func_args):
        args_space = getfullargspec(func_obj)
        if args_space and func_args:
            arg_names = args_space.args
            args_dict = dict(zip(arg_names, func_args))
            return args_dict
        else:
            return None

    def _update_process(self, param_dict):
        for arg, val in param_dict.items():
            if isinstance(val, argparse.Namespace):
                arg_val_dict = vars(val)
                arg_val_keys = arg_val_dict.keys()
                if "process_name" in arg_val_keys:
                    self.record_dict["process_name"] = arg_val_dict["process_name"]

     
 
