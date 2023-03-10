import os
import errno
import arcpy
import tempfile
from datetime import datetime
import multiprocessing as mp
from tqdm import tqdm
import shutil
import logging
import logging.handlers
import numpy as np
from .assets import BRSO


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


class MigrationLog:
    def __init__(self, log_directory):
        self.dir = log_directory

        if os.path.isdir(self.dir):
            pass
        else:
            os.mkdir(self.dir)

        now = datetime.today().strftime("%Y%m%d%H%M%S")
        self.file = os.path.join(self.dir, f"migration-{now}.log")
        if os.path.isfile(self.file):
            os.remove(self.file)

    def create(self):
        logger = logging.getLogger("MIGRATION")
        handler = LXGLogging(
            self.file,
            maxBytes=1024 * 1024 * 2,
            backupCount=10
        )
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        return logger


class ReplicationLog:
    def __init__(self, log_directory):
        self.dir = log_directory

        if os.path.isdir(self.dir):
            pass
            # shutil.rmtree(self.dir)
            # os.mkdir(self.dir)
        else:
            os.mkdir(self.dir)

        now = datetime.today().strftime("%Y%m%d%H%M%S")
        self.file = os.path.join(self.dir, f"replication-{now}.log")
        if os.path.isfile(self.file):
            os.remove(self.file)

    def create(self):
        logger = logging.getLogger("REPLICATION")
        handler = LXGLogging(
            self.file,
            maxBytes=1024 * 1024 * 2,
            backupCount=10
        )
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        return logger


class ToBRSO:
    def __init__(self, file_geodatabase, projection=None):
        self.gdb = file_geodatabase
        self.crs = BRSO() if projection is None else projection

        arcpy.env.workspace = self.gdb

        dss = sorted(arcpy.ListDatasets("", "ALL"))
        if len(dss) > 0:
            ds_list = [os.path.join(self.gdb, ds) for ds in dss]

            with mp.Pool(processes=4) as pool:
                results = tqdm(pool.imap(self.define, ds_list),
                               total=len(ds_list),
                               desc='BRSO - Dataset',
                               position=0,
                               colour='GREEN')
                tuple(results)
                pool.close()
                pool.join()

        fclasses = sorted(arcpy.ListFeatureClasses("*", ""))
        if len(fclasses) > 0:
            fc_list = [os.path.join(self.gdb, fc) for fc in fclasses]

            with mp.Pool(processes=4) as pool:
                results = tqdm(pool.imap(self.define, fc_list),
                               total=len(fc_list),
                               desc='BRSO - Featureclass',
                               position=0,
                               colour='GREEN')
                tuple(results)
                pool.close()
                pool.join()

    def define(self, datasets):
        try:
            arcpy.DefineProjection_management(datasets, self.crs)
        except arcpy.ExecuteError as e:
            arcpy.AddError(e)


class ToShapefile:
    def __init__(self, geodatabase, output_directory, checklist=None):
        self.gdb = geodatabase
        self.dir = output_directory
        self.checklist = checklist

        os.makedirs(self.dir, exist_ok=True)

        if self.checklist:
            self.featureclass_list = np.array(self.checklist)
        else:
            self.featureclass_list = None

    def run(self):
        arcpy.env.workspace = self.gdb
        dss = sorted(arcpy.ListDatasets("", "feature"))
        # pbar01 = tqdm(dss, desc=f'{self.gdb}', position=0, colour='GREEN')
        if self.featureclass_list is None:
            for ds in dss:
                fcs = sorted(arcpy.ListFeatureClasses("", "All", ds))
                # pbar02 = tqdm(fcs, position=1, colour='YELLOW', leave=False)
                if len(fcs) > 0:
                    for fc in fcs:
                        # pbar02.set_description(fc)
                        desc = arcpy.Describe(fc)
                        if desc.FeatureType != 'Annotation':
                            try:
                                arcpy.FeatureClassToShapefile_conversion(fc, self.dir)
                            except arcpy.ExecuteError as e:
                                arcpy.AddError(e)
                            except IOError as e:
                                arcpy.AddError(e)
                            except Exception as e:
                                arcpy.AddError(e)

                            output_shapefile = os.path.join(self.dir, f"{fc}.shp")
                            try:
                                fc_tabs = self.column_names(fc)
                                shp_tabs = self.column_names(output_shapefile)
                                zip_list = zip(shp_tabs, fc_tabs)
                                try:
                                    list_file = open(os.path.join(self.dir, fc + ".txt"), "w")
                                    for x in zip_list:
                                        list_file.write(f"{x[0]} {x[1]}\n")
                                    list_file.close()
                                except Exception as e:
                                    arcpy.AddError(e)
                                except arcpy.ExecuteError as e:
                                    arcpy.AddError(e)
                            except IOError as e:
                                arcpy.AddError(e)
                            except Exception as e:
                                arcpy.AddError(e)
        else:
            for ds in dss:
                fcs = sorted(arcpy.ListFeatureClasses("", "All", ds))
                if len(fcs) > 0:
                    fc_list = [os.path.basename(fc) for fc in fcs if fc in self.featureclass_list[:, 0]]
                    # pbar02 = tqdm(fc_list, position=1, colour='YELLOW', leave=False)
                    for fc in fc_list:
                        # pbar02.set_description(fc)
                        desc = arcpy.Describe(fc)
                        if desc.FeatureType != 'Annotation':
                            try:
                                arcpy.FeatureClassToShapefile_conversion(fc, self.dir)
                            except arcpy.ExecuteError as e:
                                arcpy.AddError(e)
                            except IOError as e:
                                arcpy.AddError(e)
                            except Exception as e:
                                arcpy.AddError(e)

                            output_shapefile = os.path.join(self.dir, f"{fc}.shp")
                            try:
                                fc_tabs = self.column_names(fc)
                                shp_tabs = self.column_names(output_shapefile)
                                zip_list = zip(shp_tabs, fc_tabs)
                                try:
                                    list_file = open(os.path.join(self.dir, fc + ".txt"), "w")
                                    for x in zip_list:
                                        list_file.write(f"{x[0]} {x[1]}\n")
                                    list_file.close()
                                except Exception as e:
                                    arcpy.AddError(e)
                                except arcpy.ExecuteError as e:
                                    arcpy.AddError(e)
                            except IOError as e:
                                arcpy.AddError(e)
                            except Exception as e:
                                arcpy.AddError(e)

        arcpy.ClearWorkspaceCache_management()

    def column_names(self, featureclass):
        field_names = []
        avoid_this = ['GLOBALID',
                      'Shape',
                      'SHAPE',
                      'SHAPE_Leng',
                      'SHAPE_Length',
                      'SHAPE_Area']
        fields = arcpy.ListFields(featureclass, "")
        for field in fields:
            if field.editable is True and field.name not in avoid_this and field.type != "OID":
                try:
                    field_names.append(field.name)
                except Exception as e:
                    arcpy.AddError(e)
                except arcpy.ExecuteError as e:
                    arcpy.AddError(e)

        return field_names


class GenerateScript:
    def __init__(self, shapefile_directory, directory_in_server=None):
        self.shp_dir = shapefile_directory

        if directory_in_server is None:
            self.serverdir = "/home0/sde/lxg_spatial"
        else:
            self.serverdir = directory_in_server

        arcpy.env.workspace = self.shp_dir

        replica_script = open(os.path.join(self.shp_dir, "cms_replication.sh"), "w")

        replica_script.write("#!/bin/bash \n")
        replica_script.write(f"cd {self.serverdir}\n")
        replica_script.write("TruncateLogFile='/home0/sde/truncate_logger.log'\n")
        replica_script.write("AppendLogFile='/home0/sde/append_logger.log'\n")
        replica_script.write("ReplicationAdminLog='/home0/sde/replication_users_locks_logger.log'\n")
        replica_script.write("rm -f $TruncateLogFile\n")
        replica_script.write("rm -f $AppendLogFile\n")
        replica_script.write("rm -f $ReplicationAdminLog\n")
        replica_script.write("echo 'START: ' `date` >> $TruncateLogFile\n")
        replica_script.write("echo 'START: ' `date` >> $AppendLogFile\n")
        replica_script.write('sdemon -o info -I users >> $ReplicationAdminLog\n')
        replica_script.write('sdemon -o info -I locks >> $ReplicationAdminLog\n')
        replica_script.write('sdemon -o kill -t all -N -p sde >> $ReplicationAdminLog\n')
        shapefiles = sorted(arcpy.ListFeatureClasses())

        prog01 = tqdm(shapefiles, desc='Create .sh', position=0, colour='GREEN')
        for shp in prog01:
            shp_name = os.path.splitext(os.path.basename(shp))[0]
            replica_script.write("echo '----------' >>  $TruncateLogFile\n")
            replica_script.write("echo '" + shp_name + "' >>  $TruncateLogFile\n")
            replica_script.write("sdetable -o truncate -t " + shp_name + " -u sde -p sde -N  >> $TruncateLogFile\n")
            replica_script.write("echo '" + shp_name + " finish: ' `date` >> $TruncateLogFile\n")
            replica_script.write("echo '----------' >>  $TruncateLogFile\n")
            replica_script.write("echo '----------' >>  $AppendLogFile\n")
            replica_script.write("echo '" + shp_name + "' >>  $AppendLogFile\n")
            replica_script.write("shp2sde -o append -l " + shp_name + ",shape -f " + shp_name +
                                 " -a file=" + shp_name + ".txt -u sde -p sde >> $AppendLogFile\n")
            replica_script.write("echo '" + shp_name + " finish: ' `date` >> $AppendLogFile\n")
            replica_script.write("echo '----------' >>  $AppendLogFile\n")
        replica_script.write("echo 'END: ' `date` >> $TruncateLogFile\n")
        replica_script.write("echo 'END: ' `date` >> $AppendLogFile\n")
        replica_script.close()


def makedirs(folder, *args, **kwargs):
    try:
        return os.makedirs(folder, exist_ok=True, *args, **kwargs)
    except TypeError:
        # Unexpected arguments encountered
        pass

    try:
        # Should work is TypeError was caused by exist_ok, eg., Py2
        return os.makedirs(folder, *args, **kwargs)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
        if os.path.isfile(folder):
            # folder is a file, raise OSError just like os.makedirs() in Py3
            raise


def delete_workdir():
    _temp = os.path.join(tempfile.gettempdir(), "lxg_replica_temp")
    if os.path.isdir(_temp):
        shutil.rmtree(_temp)
        return print(f"[INFO] Temporary working directory deleted :)")
    else:
        return None


def temporary_workdir():
    _temp = os.path.join(tempfile.gettempdir(), "lxg_replica_temp")
    makedirs(_temp)

    return _temp


class TemporaryDirectory(object):
    """Context manager for tempfile.mkdtemp() so it's usable with "with" statement."""
    def __enter__(self):
        self.name = tempfile.mkdtemp()
        return self.name

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(self.name)

