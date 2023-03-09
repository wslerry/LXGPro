import os
import re
import sys
import time
import arcpy
from tqdm import tqdm
import tempfile
import shutil
import uuid
import multiprocessing as mp


class GDB2SDE:
    def __init__(self, geodatabase, sde_instance, sde_platform,
                 sde_username, sde_password, sde_database,
                 wildcard_datasets="*", wildcard_featureclass="*"):
        self.gdb = geodatabase
        self.platform = sde_platform
        self.instance = sde_instance
        self.usr = sde_username
        self.pwd = sde_password
        self.database = sde_database
        self.wildcard_ds = wildcard_datasets
        self.wildcard_fc = wildcard_featureclass

        self.temp_dir = tempfile.mkdtemp()

        self.UpgradeDatasets()

        sde = self.temp_connection()

        arcpy.env.workspace = self.gdb

        exist_ds = []
        nonexist_ds = []
        dss = sorted(arcpy.ListDatasets(self.wildcard_ds, "ALL"))
        pbar01 = tqdm(dss, position=0, colour='GREEN')
        for ds in pbar01:
            pbar01.set_description(ds)
            try:
                src_data = os.path.join(self.gdb, ds)
                out_data = os.path.join(sde,
                                        f"{self.database}.sde.{ds}" if self.platform == "POSTGRESQL" else f"SDE.{ds}")
                if arcpy.Exists(out_data):
                    exist_ds.append([src_data, out_data])
                    # feats = sorted(arcpy.ListFeatureClasses("", "All", ds))
                    # for fc in feats:
                    #     out_fc_name = os.path.join(
                    #         sde,
                    #         out_data,
                    #         f"{self.database}.sde.{fc}" if self.platform == "POSTGRESQL" else f"SDE.{fc}"
                    #     )
                    #     src_fc_data = os.path.join(src_data, fc)
                    #     tgt_fc_data = os.path.join(out_data, out_fc_name)
                    #     exist_ds.append([src_fc_data, tgt_fc_data])
                else:
                    nonexist_ds.append([src_data, out_data])
            except Exception as e:
                arcpy.AddError(e)

        # run multiprocessing
        if len(exist_ds) > 0:
            with mp.Pool(processes=4) as pool:
                results = tqdm(pool.imap(self.truncate_append, exist_ds),
                               total=len(exist_ds),
                               desc="Append",
                               position=1,
                               colour='YELLOW', leave=False)
                tuple(results)
                pool.close()
                pool.join()

                del results

        if len(nonexist_ds) > 0:
            with mp.Pool(processes=4) as pool2:
                results = tqdm(pool2.imap(self.copy_datasets, nonexist_ds),
                               total=len(nonexist_ds),
                               desc="Copy",
                               position=1,
                               colour='YELLOW', leave=False)
                tuple(results)
                pool2.close()
                pool2.join()

                del results

        time.sleep(2.0)
        try:
            arcpy.Delete_management(self.temp_dir)
            # shutil.rmtree(self.temp_dir)
        except arcpy.ExecuteError as e:
            arcpy.AddError(e)

    def temp_connection(self):
        # create connection parameters

        if self.platform == "oracle":
            platform = "ORACLE"
        elif self.platform == "postgres":
            platform = "POSTGRESQL"
        else:
            platform = self.platform

        assert self.platform is not None or self.platform != ""

        f_uid = uuid.uuid4()
        conn = {"out_folder_path": self.temp_dir,
                "out_name": f'temp_{str(f_uid.hex)}.sde',
                "database_platform": platform,
                "instance": self.instance,
                "account_authentication": "DATABASE_AUTH",
                "database": self.database if platform == "POSTGRESQL" else "",
                "username": self.usr,
                "password": self.pwd,
                "save_user_pass": "SAVE_USERNAME"}

        temp_sde = os.path.join(conn["out_folder_path"], conn["out_name"])

        if arcpy.Exists(temp_sde):
            arcpy.Delete_management(temp_sde)

        arcpy.CreateDatabaseConnection_management(**conn)

        return temp_sde

    def copy_datasets(self, dataset):
        try:
            arcpy.Copy_management(dataset[0], dataset[1])
        except arcpy.ExecuteError as e:
            arcpy.AddError(e)

    def truncate_append(self, dataset):
        arcpy.env.workspace = dataset[1]
        feats = sorted(arcpy.ListFeatureClasses("", "All", dataset[1]))
        for fc in feats:
            try:
                fc_uid = uuid.uuid4()
                arcpy.MakeTableView_management(os.path.join(dataset[1], fc), f"{fc}_tab_{str(fc_uid)}")
                query = f"OBJECTID IS NOT NULL"
                arcpy.SelectLayerByAttribute_management(f"{fc}_tab_{str(fc_uid)}", "NEW_SELECTION", query)
                arcpy.DeleteRows_management(f"{fc}_tab_{str(fc_uid)}")
                arcpy.SelectLayerByAttribute_management(f"{fc}_tab_{str(fc_uid)}", "CLEAR_SELECTION")
                arcpy.Delete_management(f"{fc}_tab_{str(fc_uid)}")

                del fc_uid
            except arcpy.ExecuteError as e:
                arcpy.AddError(e)

            try:
                fieldMappings = self.fieldmapping(os.path.join(dataset[0], fc),
                                                  os.path.join(dataset[1], fc))
                arcpy.Append_management(inputs=os.path.join(dataset[0], fc),
                                        target=os.path.join(dataset[1], fc),
                                        schema_type="NO_TEST",
                                        field_mapping=fieldMappings
                                        )
                del fieldMappings
            except arcpy.ExecuteError as e:
                arcpy.AddError(e)

    def fieldmapping(self, fc_source, fc_target):
        fieldMappings = arcpy.FieldMappings()
        field_tgt = arcpy.ListFields(fc_target)
        namelist_tgt = []
        exclude_list = ['OBJECT_ID',
                        'OBJECTID',
                        'OBJECT_ID1',
                        'OBJECT_ID2',
                        'SHAPE']

        # Creating field maps for the two files
        fieldMappings.addTable(fc_source)
        fieldMappings.addTable(fc_target)

        for fd in field_tgt:
            if fd.name not in exclude_list and fd.type != "OID" and fd.editable is True:
                namelist_tgt.append(fd.name)

        for field in fieldMappings.fields:
            if field.name not in namelist_tgt:
                fieldMappings.removeFieldMap(fieldMappings.findFieldMapIndex(field.name))

        return fieldMappings

    def UpgradeDatasets(self):
        if arcpy.Exists(self.gdb):
            arcpy.env.workspace = self.gdb

            is_current = arcpy.Describe(self.gdb).currentRelease

            if not is_current:
                try:
                    arcpy.UpgradeGDB_management(self.gdb, "PREREQUISITE_CHECK", "UPGRADE")
                    print(f'[INFO]\t{self.gdb} UPGRADED SUCCESSFULLY')
                except arcpy.ExecuteError as e:
                    arcpy.AddError(e)

                dss = sorted(arcpy.ListDatasets("", "Feature"))
                bar01 = tqdm(dss, position=0, colour='Green', leave=True)
                for ds in bar01:
                    bar01.set_description(ds)
                    fclass = sorted(arcpy.ListFeatureClasses("", "Annotation", ds))
                    bar02 = tqdm(fclass, position=1, colour='Yellow', leave=False)
                    for fc in bar02:
                        bar02.set_description(fc)
                        try:
                            arcpy.UpgradeDataset_management(fc)
                        except arcpy.ExecuteError as e:
                            arcpy.AddError(e)
            else:
                pass


class EnterpriseGDB:
    """
    Create a geodatabase enterprise
    """