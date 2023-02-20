import arcpy
import json
import os
import time
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
import logging
import logging.handlers


# ------------------------------------------------------------
#                         LOGGING
# ------------------------------------------------------------


class LXGLogging(logging.handlers.RotatingFileHandler):
    def emitlog(self, record):
        """
        Write the log message
        """
        try:
            msg = record.msg.format(record.args)
        except:
            msg = record.msg

        if record.levelno >= logging.ERROR:
            arcpy.AddError(msg)
        elif record.levelno >= logging.WARNING:
            arcpy.AddWarning(msg)
        elif record.levelno >= logging.INFO:
            arcpy.AddMessage(msg)

        super(LXGLogging, self).emitlog(record)


def delete_exist(filename):
    if os.path.isfile(filename):
        os.remove(filename)
    else:
        pass


if os.path.isdir("logs"):
    pass
else:
    os.mkdir("logs")
today = datetime.today().strftime("%Y%m%d_%H%M%S")
log_filename = f"logs/replication_{today}.log"
delete_exist(log_filename)

logger = logging.getLogger("MIGRATION")
handler = LXGLogging(
    log_filename,
    maxBytes=1024 * 1024 * 2,
    backupCount=10
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

# ------------------------------------------------------------
#                         UTILS
# ------------------------------------------------------------


def process(seconds):
    conversion = timedelta(seconds=seconds)
    converted_time = str(conversion)

    return converted_time

# ------------------------------------------------------------
#                         PROCESS
# ------------------------------------------------------------


class ReplicateSDE2GDB:
    def __init__(self, sde_instance, sde_username, sde_password, file_gdb, wildcard_datasets, wildcard_featureclass):
        self.instance = sde_instance
        self.usr = sde_username
        self.pwd = sde_password
        self.gdb = file_gdb
        self.wildcard_ds = wildcard_datasets
        self.wildcard_fc = wildcard_featureclass

        if self.wildcard_ds is None:
            self.wildcard_ds = ""
        if self.wildcard_fc is None:
            self.wildcard_fc = ""

        start0 = time.time()

        sde = self.temp_connection()

        current_dir = os.path.dirname(os.path.realpath(__file__))

        db_out = os.path.join(current_dir, self.gdb)
        if arcpy.Exists(db_out):
            arcpy.Delete_management(db_out)
            arcpy.CreateFileGDB_management(current_dir, self.gdb, "9.3")
        else:
            arcpy.CreateFileGDB_management(current_dir, self.gdb, "9.3")

        arcpy.env.workspace = sde

        logger.info(f'Workspace : {sde}')
        dss = sorted(arcpy.ListDatasets(self.wildcard_ds, "ALL"))
        pbar01 = tqdm(dss, position=0, colour='GREEN')
        for ds in pbar01:
            pbar01.set_description(ds)
            dsname = '%s' % self.newname('SDE.', ds)

            # copy everything in dataset
            try:
                copytime = time.time()
                out_data = os.path.join(db_out, dsname)
                arcpy.Copy_management(ds, out_data)
                logger.info(f"Successfully copy SDE dataset to GDB - {process(time.time() - copytime)}")
            except Exception as e:
                logger.error(e)

        stop0 = time.time()
        logger.info('Total processing time: ' + process(stop0 - start0))

    def temp_connection(self):
        # create connection parameters
        conn = {"out_folder_path": os.path.dirname(os.path.realpath(__file__)),
                "out_name": 'temp.sde',
                "database_platform": 'ORACLE',
                "instance": self.instance,
                "account_authentication": "DATABASE_AUTH",
                "database": "",
                "username": self.usr,
                "password": self.pwd,
                "save_user_pass": "SAVE_USERNAME"}

        temp_sde = os.path.join(conn["out_folder_path"], conn["out_name"])

        if arcpy.Exists(temp_sde):
            arcpy.Delete_management(temp_sde)

        logger.info(f'[INFO] Creating a temporary connection - {temp_sde}')
        logger.info(f'[INFO] Creating a temporary connection - {temp_sde}')
        arcpy.CreateDatabaseConnection_management(**conn)

        return temp_sde

    @staticmethod
    def newname(target_string, old_name):
        new_name = old_name.replace(target_string, "", 1)
        return new_name


if __name__ == "__main__":
    # sde_instance = arcpy.GetParameterAsText(0)
    # sde_username = arcpy.GetParameterAsText(1)
    # sde_password = arcpy.GetParameterAsText(2)
    # file_gdb = arcpy.GetParameterAsText(3)
    # wildcard_datasets = arcpy.GetParameterAsText(4)
    # wildcard_featureclass = arcpy.GetParameterAsText(5)

    params = {
        "sde_instance": "KUCHINGLXG",
        "sde_username": "sde",
        "sde_password": "sde",
        "file_gdb": "cms_replica.gdb",
        "wildcard_datasets": "*CMS_ADMINISTRATIVE*",
        "wildcard_featureclass": ""}

    ReplicateSDE2GDB(**params)
