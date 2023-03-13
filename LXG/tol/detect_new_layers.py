import sys
import arcpy
import re
import os
from tqdm import tqdm
import multiprocessing as mp
import pandas as pd
import uuid
import tempfile
import shutil
from ..utils import TemporaryDirectory


class TOLNewFeatures:
    """Detect a new created polygon from TOL *KPG_EXT* layer.

    Args:
        init_geodatabase: initial geodatabase (a replica geodatabase) before new changes
        latest_geodatabase: latest geodatabase (SDE) which contains *KPG_EXT* layers
        division: LASIS divisional abbreviation eg. KCH, BTU, LBG ...
        datasets_wildcard (optional): Query for interested dataset layer(s)

    Usage:
        ```
        init = r"C:\Datasets\TNT\TEST\OLD\TNT.gdb"

        new = r"C:\LXG\replication\ARC\replication_test\127.0.0.1.sde"

        check_db = TOLNewFeatures(init_geodatabase=init,
                                  latest_geodatabase=new,
                                  division="KCH")
        ```

    Returns:
        None

    Note:
        Make sure SDE connected or create a new SDE connection before running the script.
    """
    def __init__(self, init_geodatabase, latest_geodatabase, division, datasets_wildcard=None,
                 featureclass_wildcard=None):
        self.init = init_geodatabase
        self.new = latest_geodatabase
        self.div = division
        self.ds_wildcard = f"*{self.div}*KPG_EXT*" if datasets_wildcard is None or datasets_wildcard == "" else datasets_wildcard
        self.fc_wildcard = f"*KPG_EXT*{self.div}*" if featureclass_wildcard is None or featureclass_wildcard == "" else featureclass_wildcard

        self.processor_num = 4 if mp.cpu_count() >= 4 else (2 if mp.cpu_count() == 2 else 1)

        self.scratch = "in_memory"
        arcpy.env.workspace = self.scratch

        try:
            self.prepare_features(self.init, "init")
            self.prepare_features(self.new, "new")

            _ = self.check_differences()

            arcpy.ClearWorkspaceCache_management()
            arcpy.Delete_management(self.scratch)
        except arcpy.ExecuteError as e:
            print(e)

    def prepare_features(self, geodatabase, name):
        fc_class_name = name

        arcpy.env.workspace = geodatabase
        dss = sorted(arcpy.ListDatasets(self.ds_wildcard, "Feature"))
        pbar01 = tqdm(dss, desc=f'Analyze {fc_class_name}', position=0, colour='GREEN')
        for ds in pbar01:
            fc_poly = sorted(arcpy.ListFeatureClasses(self.fc_wildcard, "Polygon", ds))
            if len(fc_poly) > 0:
                pbar03 = tqdm(fc_poly, position=1, colour='Yellow', leave=False)
                for poly in pbar03:
                    if re.search('sde.', poly):
                        poly_name = poly.split(os.extsep)[-1]
                    elif re.search('SDE.', poly):
                        poly_name = poly.split(os.extsep)[-1]
                    else:
                        poly_name = poly

                    pts = os.path.join(self.scratch, f'{fc_class_name}_{poly_name}_pts')
                    buf = os.path.join(self.scratch, f'{fc_class_name}_{poly_name}_buf')
                    self._polys([os.path.join(geodatabase, ds, poly),
                                 geodatabase,
                                 pts,
                                 buf
                                 ])

    def get_scratch_features(self):
        arcpy.env.workspace = self.scratch

        print(arcpy.ListFeatureClasses("", "All"))
        all_feats = [fc for fc in sorted(arcpy.ListFeatureClasses("", ""))]

        return all_feats

    def _polys(self, fc_list):
        geodatabase = fc_list[1]
        pts_feat_poly = fc_list[2]
        buf_feat_poly = fc_list[3]
        try:
            arcpy.FeatureToPoint_management(fc_list[0], pts_feat_poly, "CENTROID")
            if geodatabase == self.init:
                # Execute the Buffer tool if initial geodatabase
                arcpy.Buffer_analysis(pts_feat_poly,
                                      buf_feat_poly,
                                      "0.02 meters")
                arcpy.Delete_management(pts_feat_poly)
        except arcpy.ExecuteError as e:
            arcpy.AddError(e)

        del geodatabase, pts_feat_poly, buf_feat_poly

    def check_differences(self):
        arcpy.env.workspace = self.new
        new_features_list = []

        dss = sorted(arcpy.ListDatasets(self.ds_wildcard, "Feature"))
        pbar01 = tqdm(dss, desc='Detect changes', position=0, colour='GREEN')
        for ds in pbar01:
            fcs = sorted(arcpy.ListFeatureClasses(self.fc_wildcard, "Polygon", ds))
            pbar02 = tqdm(fcs, position=1, colour='Yellow', leave=False)
            for fc in pbar02:
                pbar02.set_description(fc)
                if re.search('sde.', fc):
                    fc = fc.split(os.extsep)[-1]
                elif re.search('SDE.', fc):
                    fc = fc.split(os.extsep)[-1]
                else:
                    fc = fc
                fc_pts = os.path.join(self.scratch, f'new_{fc}_pts')

                old_fc_buf = os.path.join(self.scratch, f'init_{fc}_buf')
                try:
                    if arcpy.Exists(fc_pts):
                        if arcpy.Exists(old_fc_buf):
                            get_count = self.intersect(fc_pts, old_fc_buf)
                            if get_count > 0:
                                new_features_list.append((fc, get_count))
                            # start copy/append
                            target_point_fc = os.path.join(self.new, ds, f"KPG_EXT_POINT_{self.div}")
                            if arcpy.Exists(target_point_fc):
                                self.truncate_append(target_point_fc, fc_pts)
                            else:
                                arcpy.CopyFeatures_management(fc_pts, target_point_fc)
                except arcpy.ExecuteError as e:
                    arcpy.AddError(e)
        if len(new_features_list) > 0:
            df = pd.DataFrame(new_features_list, columns=["FeatureClasses", "Count"])
            report_dir = os.path.join(os.path.expanduser('~'), "Documents", "GIS_Reports", "KPG_EXT")
            os.makedirs(report_dir, exist_ok=True)
            df.to_csv(os.path.join(report_dir, f"{self.div}_KPG_EXT_new_layers.csv"), index=False)

        return new_features_list

    def intersect(self, in_layer, select_features):
        _, _, selected_count = arcpy.SelectLayerByLocation_management(
            in_layer=in_layer,
            overlap_type="INTERSECT",
            select_features=select_features,
            selection_type="NEW_SELECTION",
            invert_spatial_relationship="INVERT")

        arcpy.SelectLayerByAttribute_management(select_features, "CLEAR_SELECTION")

        selected_layer, _, _ = arcpy.SelectLayerByLocation_management(
            in_layer=in_layer,
            overlap_type="INTERSECT",
            select_features=select_features,
            selection_type="NEW_SELECTION",
            invert_spatial_relationship="NOT_INVERT")

        arcpy.DeleteRows_management(selected_layer)

        arcpy.SelectLayerByAttribute_management(in_layer, "CLEAR_SELECTION")

        return int(selected_count)

    def truncate_append(self, featureclass_target, feature_point):
        try:
            fc_uid = uuid.uuid4()
            arcpy.MakeTableView_management(featureclass_target, f"tab_{str(fc_uid)}")
            query = f"OBJECTID IS NOT NULL"
            arcpy.SelectLayerByAttribute_management(f"tab_{str(fc_uid)}", "NEW_SELECTION", query)
            arcpy.DeleteRows_management(f"tab_{str(fc_uid)}")
            arcpy.SelectLayerByAttribute_management(f"tab_{str(fc_uid)}", "CLEAR_SELECTION")
            arcpy.Delete_management(f"tab_{str(fc_uid)}")
        except arcpy.ExecuteError as e:
            arcpy.AddError(e)

        try:
            fieldMappings = self.fieldmapping(feature_point,
                                              featureclass_target)
            arcpy.Append_management(inputs=feature_point,
                                    target=featureclass_target,
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
        # Creating field maps for the two files
        fieldMappings.addTable(fc_source)
        fieldMappings.addTable(fc_target)

        for fd in field_tgt:
            if fd.type != "OID" and fd.editable is False:
                namelist_tgt.append(fd.name)

        for field in fieldMappings.fields:
            if field.name not in namelist_tgt:
                fieldMappings.removeFieldMap(fieldMappings.findFieldMapIndex(field.name))

        return fieldMappings


class ReplicateTOL:
    """Replicate TOL dataset into a replica for dynamic layers.
    This is one way replication from SDE to local geodatabase.

    Args:
        sde_connection: SDE Connection file, a temporary SDE connection will be created if not provide.
        sde_instance: Database Instance
        sde_platform: ORACLE,POSTGRESQL
        sde_username (Optional): Query for interested layers
        sde_password (Optional): SDE Password
        sde_database (Optional): SDE database name - only for POSTGRESQL
        division (Optional): LASIS divisional abbreviation eg. KCH, BTU, LBG ...
        wildcard_datasets (Optional): SQL Query to select table for interested dataset layer
        wildcard_featureclass (Optional): SQL Query to select table for interested featureclass layer
        replica (Optional): Geodatabase file for replica
        replica_name (Optional): Replica name.

    Usage:
        ```
        init = r"C:\Datasets\TNT\TEST\OLD\TNT.gdb"

        new = r"C:\LXG\replication\ARC\replication_test\127.0.0.1.sde"

        ReplicateTOL(init_geodatabase=init, latest_geodatabase=new, division="KCH")
        ```
    """
    def __init__(self, sde_connection=None, sde_instance=None, sde_platform=None,
                 sde_username=None, sde_password=None, sde_database=None, division=None,
                 wildcard_datasets=None, wildcard_featureclass=None, replica=None, replica_name=None):
        self.tempdir = tempfile.mkdtemp()
        self.platform = sde_platform
        self.instance = "127.0.0.1" if sde_instance is None else sde_instance
        self.usr = "sde" if sde_username is None else sde_username
        self.pwd = "sde" if sde_password is None else sde_password
        self.database = "" if sde_database is None else sde_database
        self.sde = self.temp_connection() if sde_connection is None else sde_connection
        self.division = division
        self.wildcard_ds = "*" if wildcard_datasets is None else wildcard_datasets
        self.wildcard_fc = "*" if wildcard_featureclass is None else wildcard_featureclass
        work_dir = os.path.join(os.path.expanduser('~'), ".LXG_WORKSPACE")
        if not os.path.isdir(work_dir):
            os.makedirs(work_dir)
        else:
            pass
        replica_gdb = os.path.join(work_dir, "tol_replica.gdb")
        if arcpy.Exists(replica_gdb):
            # if file exist, keep it. It is a replica geodatabase by the way.
            self.replica = replica_gdb if replica is None else replica
        else:
            # create a new replica if not exist in the system.
            self.replica = arcpy.CreateFileGDB_management(work_dir, "tol_replica.gdb") if replica is None else replica

        self.replica_name = "tol_replication" if replica_name is None else replica_name

        try:
            self.run()
            self.check()
            self.sync()
        except arcpy.ExecuteError as e:
            arcpy.AddError(e)

        shutil.rmtree(self.tempdir)

    def run(self):
        """"Run ReplicateTOL"""
        repl_name = []
        for replica in arcpy.da.ListReplicas(self.replica):
            repl_name.append(replica.name)

        if self.replica_name in repl_name:
            arcpy.AddWarning(f"[WARNING]\tReplica name '{self.replica_name}' already exist, continue to sync...")
            pass
        else:
            kpg_ext = f"{self.database}.sde.{self.division}_LAND_KPG_EXT" if self.platform == "POSTGRESQL" \
                else f"SDE.{self.division}_LAND_KPG_EXT"
            try:
                arcpy.CreateReplica_management(
                    os.path.join(self.sde, kpg_ext),
                    "ONE_WAY_REPLICA",
                    self.replica,
                    self.replica_name,
                    "FULL",
                    "PARENT_DATA_SENDER",
                    "USE_DEFAULTS",
                    "DO_NOT_REUSE",
                    "GET_RELATED",
                    None,
                    "DO_NOT_USE_ARCHIVING",
                    "DO_NOT_USE_REGISTER_EXISTING_DATA",
                    "GEODATABASE",
                    None)
                arcpy.AddMessage(f"[INFO]\tReplica created, continue to sync...")
            except arcpy.ExecuteError as e:
                arcpy.AddError(e)

    def sync(self):
        """Sync replica geodatabase to SDE database"""
        try:
            arcpy.SynchronizeChanges_management(
                self.replica,
                self.replica_name,
                self.sde,
                "FROM_GEODATABASE2_TO_1",
                "IN_FAVOR_OF_GDB2",
                "BY_OBJECT",
                "RECONCILE ")
        except arcpy.ExecuteError as e:
            arcpy.AddError(e)

    def check(self):
        repl_name = []
        for replica in arcpy.da.ListReplicas(self.replica):
            repl_name.append(replica.name)
        try:
            print_out = 'Replica name list:\n'+'\u25A0 '+'\n\u25A0 '.join(repl_name)
            print(print_out)
        except UnicodeDecodeError:
            pass

    def temp_connection(self):
        """Create a temporary connection to SDE"""
        if self.platform == "oracle":
            platform = "ORACLE"
        elif self.platform == "postgres":
            platform = "POSTGRESQL"
        else:
            platform = self.platform

        assert self.platform is not None or self.platform != ""

        f_uid = uuid.uuid4()

        conn = {"out_folder_path": self.tempdir,
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

