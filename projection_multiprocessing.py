import arcpy
import multiprocessing as mp
import os
from tqdm import tqdm
import time
from datetime import timedelta


def process(seconds):
    conversion = timedelta(seconds=seconds)
    converted_time = str(conversion)

    return converted_time


class TOBRSO:
    def __init__(self, file_geodatabase, projection):
        self.gdb = file_geodatabase
        self.crs = projection
        arcpy.env.workspace = self.gdb
        dss = sorted(arcpy.ListDatasets("", "ALL"))
        ds_list = [os.path.join(self.gdb, ds) for ds in dss]

        with mp.Pool(processes=8) as pool:
            results = tqdm(pool.imap(self.define, ds_list),
                           total=len(ds_list),
                           desc='BRSO',
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


if __name__ == "__main__":
    start0 = time.time()
    gdb = r"C:\LXG\replication\workshop\test\replication_from_19c.gdb"
    crs = r"C:\LXG\replication\arcpro\BRSO_4.prj"

    TOBRSO(gdb, crs)

    stop0 = time.time()

    arcpy.AddMessage(f'[INFO]\t Processing Done. Total time {process(stop0 - start0)}s ...')
