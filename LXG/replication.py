"""
Author : Lerry William
"""
import sys
import arcpy
import os
from tqdm import tqdm
import multiprocessing as mp
import numpy as np
import pandas as pd
import tempfile
import shutil
from jinja2 import Environment, FileSystemLoader
from xhtml2pdf import pisa
import time
import uuid
from datetime import date, timedelta
from .utils import temporary_workdir, delete_workdir


def process(seconds):
    conversion = timedelta(seconds=seconds)
    converted_time = str(conversion)

    return converted_time


def your_existance_in_questions(directory, geodatabase_name):
    gdb_filename = os.path.join(directory, geodatabase_name)
    if arcpy.Exists(gdb_filename):
        arcpy.Delete_management(gdb_filename)
        arcpy.CreateFileGDB_management(directory, geodatabase_name, "9.3")
    else:
        arcpy.CreateFileGDB_management(directory, geodatabase_name, "9.3")

    return gdb_filename


class BatchImportXML:
    def __init__(self, geodatabase, xml_directory):
        self.gdb = geodatabase
        self.xml_dir = xml_directory

        current = os.path.dirname(os.path.realpath(__file__))
        out_gdb = os.path.join(current, self.gdb)
        if arcpy.Exists(out_gdb):
            arcpy.Delete_management(out_gdb)
            arcpy.CreateFileGDB_management(current, self.gdb, "9.3")
        else:
            arcpy.CreateFileGDB_management(current, self.gdb, "9.3")

        xml_list = []
        for root, dirs, files in os.walk(self.xml_dir):
            for file in files:
                xml_list.append(os.path.join(root, file))

        for xml in xml_list:
            try:
                arcpy.env.workspace = out_gdb
                arcpy.ImportXMLWorkspaceDocument_management(out_gdb, xml, 'SCHEMA_ONLY')
                print(f"{xml} success")
            except arcpy.ExecuteError as e:
                arcpy.AddError(e)


class AppendNewFeatures:
    def __init__(self, init_geodatabase,
                 latest_geodatabase,
                 division,
                 datasets_wildcard=None,
                 feature_wildcard=None,
                 report=False,
                 report_output_directory=None):
        self.gdb1 = init_geodatabase
        self.gdb2 = latest_geodatabase
        self.div = division
        self.ds_wildcard = f"*" if datasets_wildcard is None or datasets_wildcard == "" else datasets_wildcard
        self.fc_wildcard = f"*" if feature_wildcard is None or feature_wildcard == "" else feature_wildcard
        self.create_report = report
        self.report_out_dir = report_output_directory
        self.processor_num = 4 if mp.cpu_count() >= 4 else (2 if mp.cpu_count() == 2 else 1)
        self.new_ds_list = list()
        start0 = time.time()
        arcpy.AddMessage(f'[INFO]\tProcessing start at {start0} ...')

        delete_workdir()

        if arcpy.Exists(self.gdb1) or arcpy.Exists(self.gdb2):
            pass
        else:
            sys.exit("Invalid init_geodatabase or latest_geodatabase input. System exit...")

        self.memory = "in_memory"
        arcpy.Delete_management(self.memory)
        self.memory = "in_memory"

        try:
            self.prepare_features(self.gdb1)
            self.prepare_features(self.gdb2)

            check_list = self.check_differences()

            self.append_latest(check_list)

            if self.create_report:
                if self.report_out_dir is not None:
                    os.makedirs(self.report_out_dir, exist_ok=True)
                    self.report(check_list)
                else:
                    report_dir = os.path.join(os.path.expanduser('~'), "Documents", "GIS_Reports", "LXG_New_Features")
                    self.report_out_dir = report_dir
                    os.makedirs(self.report_out_dir, exist_ok=True)
                    self.report(check_list)
            else:
                pass

            arcpy.Delete_management(self.memory)
        except arcpy.ExecuteError as e:
            arcpy.Delete_management(self.memory)
            arcpy.AddError(e)

        arcpy.Compact_management(self.gdb1)
        arcpy.Compact_management(self.gdb2)

        stop0 = time.time()
        arcpy.AddMessage(f'[INFO]\tProcessing Done. Total time {process(stop0 - start0)}s ...')

    def prepare_features(self, geodatabase):
        fc_class_name = None
        if geodatabase == self.gdb1:
            fc_class_name = 'gdb1'
        elif geodatabase == self.gdb2:
            fc_class_name = 'gdb2'
        else:
            pass

        arcpy.env.workspace = geodatabase
        dss = sorted(arcpy.ListDatasets("", "Feature"))
        pbar01 = tqdm(dss, desc=f'{geodatabase}', position=0, colour='GREEN')
        for ds in pbar01:
            # Polygon
            fc_poly = sorted(arcpy.ListFeatureClasses("", "Polygon", ds))
            if len(fc_poly) > 0:
                pbar03 = tqdm(fc_poly, desc="Polygon", position=1, colour='Yellow', leave=False)
                for poly in pbar03:
                    self._polys([os.path.join(geodatabase, ds, poly),
                                 geodatabase,
                                 os.path.join(self.memory, f'{fc_class_name}_{poly}_pts'),
                                 os.path.join(self.memory, f'{fc_class_name}_{poly}_buf')
                                 ])

            # Polyline
            fc_lines = sorted(arcpy.ListFeatureClasses("", "Polyline", ds))
            if len(fc_lines) > 0:
                pbar03 = tqdm(fc_lines, desc="Polyline", position=1, colour='Yellow', leave=False)
                for line in pbar03:
                    # pbar03.set_description(line)
                    input_line = os.path.join(geodatabase, ds, line)
                    pts_feat_line = os.path.join(self.memory, f'{fc_class_name}_{line}_pts')
                    if arcpy.Exists(pts_feat_line):
                        pass
                    else:
                        arcpy.GeneratePointsAlongLines_management(Input_Features=input_line,
                                                                  Output_Feature_Class=pts_feat_line,
                                                                  Point_Placement="PERCENTAGE",
                                                                  Distance="",
                                                                  Percentage=50,
                                                                  Include_End_Points="")
                    try:
                        if geodatabase == self.gdb1:
                            # Execute the Buffer tool of initial geodatabase
                            arcpy.Buffer_analysis(pts_feat_line,
                                                  os.path.join(self.memory, f'{fc_class_name}_{line}_buf'),
                                                  "0.5 meters")
                            arcpy.Delete_management(pts_feat_line)
                    except arcpy.ExecuteError as e:
                        arcpy.AddError(e)

                    del pts_feat_line

            # Points
            fc_points = sorted(arcpy.ListFeatureClasses("", "Point", ds))
            if len(fc_points) > 0:
                fc_points_list = [[os.path.join(geodatabase, ds, pts),
                                   geodatabase,
                                   os.path.join(self.memory, f'{fc_class_name}_{pts}_pts'),
                                   os.path.join(self.memory, f'{fc_class_name}_{pts}_buf')] for pts in fc_points]
                with mp.Pool(processes=self.processor_num) as pool3:
                    results = tqdm(pool3.imap(self._points, fc_points_list),
                                   total=len(fc_points_list),
                                   desc="Points",
                                   position=1,
                                   colour='YELLOW', leave=False)
                    tuple(results)
                    pool3.close()
                    pool3.join()

                    del results

        del fc_class_name

        return None

    def _polys(self, fc_list):
        geodatabase = fc_list[1]
        pts_feat_poly = fc_list[2]
        buf_feat_poly = fc_list[3]

        try:
            arcpy.FeatureToPoint_management(fc_list[0], pts_feat_poly, "CENTROID")
            if geodatabase == self.gdb1:
                # Execute the Buffer tool if initial geodatabase
                arcpy.Buffer_analysis(pts_feat_poly,
                                      buf_feat_poly,
                                      "0.5 meters")
                # arcpy.Delete_management(pts_feat_poly)
        except arcpy.ExecuteError as e:
            arcpy.AddError(e)

    def _lines(self, fc_list):
        input_line = fc_list[0]
        geodatabase = fc_list[1]
        pts_feat_line = fc_list[2]
        buf_feat_line = fc_list[3]

        try:
            arcpy.GeneratePointsAlongLines_management(Input_Features=input_line,
                                                      Output_Feature_Class=pts_feat_line,
                                                      Point_Placement="PERCENTAGE",
                                                      Distance="",
                                                      Percentage=50,
                                                      Include_End_Points="")
            if geodatabase == self.gdb1:
                # Execute the Buffer tool if initial geodatabase
                arcpy.Buffer_analysis(pts_feat_line,
                                      buf_feat_line,
                                      "0.5 meters")
                # arcpy.Delete_management(pts_feat_line)
        except arcpy.ExecuteError as e:
            arcpy.AddError(e)

    def _points(self, fc_list):
        geodatabase = fc_list[1]
        pts_feat_pts = fc_list[2]
        buf_feat_pts = fc_list[3]

        try:
            arcpy.CopyFeatures_management(fc_list[0], pts_feat_pts)
            if geodatabase == self.gdb1:
                # Execute the Buffer tool if initial geodatabase
                arcpy.Buffer_analysis(fc_list[0],
                                      buf_feat_pts,
                                      "0.5 meters")
                # arcpy.Delete_management(pts_feat_pts)
        except arcpy.ExecuteError as e:
            arcpy.AddError(e)

    def intersect(self, in_layer, select_features):
        _, _, selected_count = arcpy.SelectLayerByLocation_management(
            in_layer=in_layer,
            overlap_type="INTERSECT",
            select_features=select_features,
            selection_type="NEW_SELECTION",
            invert_spatial_relationship="INVERT")

        arcpy.SelectLayerByAttribute_management(select_features, "CLEAR_SELECTION")

        # int(arcpy.GetCount_management(fc_pts)[0])
        selected_layer, _, _ = arcpy.SelectLayerByLocation_management(
            in_layer=in_layer,
            overlap_type="INTERSECT",
            select_features=select_features,
            selection_type="NEW_SELECTION",
            invert_spatial_relationship="NOT_INVERT")

        arcpy.DeleteRows_management(selected_layer)

        arcpy.SelectLayerByAttribute_management(in_layer, "CLEAR_SELECTION")

        return int(selected_count)

    def check_differences(self):
        arcpy.env.workspace = self.gdb2
        new_features_list = []
        dss = sorted(arcpy.ListDatasets("", "Feature"))
        pbar01 = tqdm(dss, desc='Detect changes', position=0, colour='GREEN')
        for ds in pbar01:
            fcs = sorted(arcpy.ListFeatureClasses("", "Polygon", ds))
            pbar02 = tqdm(fcs, position=1, colour='Yellow', leave=False)
            for fc in pbar02:
                pbar02.set_description(fc)
                fc_pts = os.path.join(self.memory, f'gdb2_{fc}_pts')
                old_fc_buf = os.path.join(self.memory, f'gdb1_{fc}_buf')
                try:
                    if arcpy.Exists(fc_pts):
                        if arcpy.Exists(old_fc_buf):
                            get_count = self.intersect(fc_pts, old_fc_buf)
                            if get_count > 0:
                                new_features_list.append((fc, get_count))
                except arcpy.ExecuteError as e:
                    arcpy.AddError(e)

            fcs = sorted(arcpy.ListFeatureClasses("", "Polyline", ds))
            pbar03 = tqdm(fcs, position=1, colour='Yellow', leave=False)
            for fc in pbar03:
                pbar03.set_description(fc)
                fc_pts = os.path.join(self.memory, f'gdb2_{fc}_pts')
                old_fc_buf = os.path.join(self.memory, f'gdb1_{fc}_buf')
                try:
                    if arcpy.Exists(fc_pts):
                        if arcpy.Exists(old_fc_buf):
                            get_count = self.intersect(fc_pts, old_fc_buf)
                            if get_count > 0:
                                new_features_list.append((fc, get_count))
                except arcpy.ExecuteError as e:
                    arcpy.AddError(e)

            fcs = sorted(arcpy.ListFeatureClasses("", "Point", ds))
            pbar04 = tqdm(fcs, position=1, colour='Yellow', leave=False)
            for fc in pbar04:
                pbar04.set_description(fc)
                fc_pts = os.path.join(self.memory, f'gdb2_{fc}_pts')
                old_fc_buf = os.path.join(self.memory, f'gdb1_{fc}_buf')
                try:
                    if arcpy.Exists(fc_pts):
                        if arcpy.Exists(old_fc_buf):
                            get_count = self.intersect(fc_pts, old_fc_buf)
                            if get_count > 0:
                                new_features_list.append((fc, get_count))
                except arcpy.ExecuteError as e:
                    arcpy.AddError(e)

        return new_features_list

    def field_mappings(self, source_feature, target_feature, point_feat_dir):
        field_src = arcpy.ListFields(source_feature)
        field_tgt = arcpy.ListFields(target_feature)
        namelist_tgt = []
        namelist_src = []
        exclude_list = ['OBJECT_ID', 'OBJECTID', 'OBJECT_ID1', 'OBJECT_ID2', 'SHAPE']

        for fd in field_tgt:
            if fd.name not in exclude_list:
                namelist_tgt.append(fd.name)

        for fd2 in field_src:
            if fd2.name in namelist_tgt:
                namelist_src.append((fd2.name, fd2.aliasName, fd2.type, fd2.length))

        fd_mapping = ""
        for name, alias, _type, _len in namelist_src:
            fd_mapping += f'{name} "{alias}" true true false {_len} {_type} 0 0,First,#,{point_feat_dir},{name},0,{_len};'

        return fd_mapping

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

    def append_latest(self, featureclass_list):
        featureclass_list = np.array(featureclass_list)
        arcpy.env.workspace = self.gdb2

        dss = sorted(arcpy.ListDatasets("", "ALL"))
        pbar01 = tqdm(dss, desc='Append', position=0, colour='GREEN')
        for ds in pbar01:
            fcs = sorted(arcpy.ListFeatureClasses("", "Polygon", ds))
            if len(fcs) > 0:
                try:
                    fc_list = [[os.path.join(self.gdb1, ds, fc),
                                os.path.join(self.gdb2, ds, fc),
                                self.memory] for fc in fcs if fc in featureclass_list[:, 0]]
                    for fc in fc_list:
                        self.fast_poly_append(fc)
                    # with mp.Pool(processes=self.processor_num) as pool1:
                    #     results = tqdm(pool1.imap(self.fast_poly_append, fc_list),
                    #                    total=len(fc_list),
                    #                    desc='Append Polygons',
                    #                    colour='Yellow',
                    #                    position=1,
                    #                    leave=False)
                    #     tuple(results)
                    #     pool1.close()
                    #     pool1.join()
                    #
                    #     del results
                except Exception as e:
                    arcpy.AddError(e)

            del fcs

            fcs = sorted(arcpy.ListFeatureClasses("", "Polyline", ds))
            if len(fcs) > 0:
                fc_list = [[os.path.join(self.gdb1, ds, fc),
                            os.path.join(self.gdb2, ds, fc),
                            self.memory] for fc in fcs if fc in featureclass_list[:, 0]]
                for fc in fc_list:
                    self.fast_append(fc)
                # with mp.Pool(processes=self.processor_num) as pool2:
                #     results = tqdm(pool2.imap(self.fast_append, fc_list),
                #                    total=len(fc_list),
                #                    desc='Append Polylines',
                #                    colour='Yellow',
                #                    position=1,
                #                    leave=False)
                #     tuple(results)
                #     pool2.close()
                #     pool2.join()
                #
                #     del results

            del fcs

            fcs = sorted(arcpy.ListFeatureClasses("", "Point", ds))
            if len(fcs) > 0:
                fc_list = [[os.path.join(self.gdb1, ds, fc),
                            os.path.join(self.gdb2, ds, fc),
                            self.memory] for fc in fcs if fc in featureclass_list[:, 0]]
                with mp.Pool(processes=self.processor_num) as pool3:
                    results = tqdm(pool3.imap(self.fast_append, fc_list),
                                   total=len(fc_list),
                                   desc='Append Points',
                                   colour='Yellow',
                                   position=1,
                                   leave=False)
                    tuple(results)
                    pool3.close()
                    pool3.join()

                    del results

            del fcs

    def fast_append(self, fc):
        _fc = os.path.basename(fc[1])

        fc_pts = os.path.join(fc[2], f'gdb2_{_fc}_pts')

        # Select the points that are touching the buffer using the Select Layer By Location tool
        selected_layer, _, _ = arcpy.SelectLayerByLocation_management(fc[1], "INTERSECT", fc_pts)

        _params = self.fieldmapping(selected_layer, fc[1])

        try:
            # Append selected points to a old version of feature class
            arcpy.Append_management(selected_layer, fc[0], "NO_TEST", field_mapping=_params)
        except arcpy.ExecuteError as e:
            arcpy.AddError(e)

        arcpy.SelectLayerByAttribute_management(selected_layer, "CLEAR_SELECTION")

    def fast_poly_append(self, fc):
        _fc = os.path.basename(fc[1])

        fc_pts = os.path.join(fc[2], f'gdb2_{_fc}_pts')

        arcpy.SelectLayerByAttribute_management(fc[0], "CLEAR_SELECTION")
        arcpy.SelectLayerByAttribute_management(fc[1], "CLEAR_SELECTION")

        selected_layer01, _, _ = arcpy.SelectLayerByLocation_management(fc[1],
                                                                        "INTERSECT",
                                                                        fc_pts,
                                                                        None,
                                                                        "NEW_SELECTION",
                                                                        "NOT_INVERT")
        # second round to avoid duplicate at the same place
        selected_layer02, _, _ = arcpy.SelectLayerByLocation_management(fc[0],
                                                                        "HAVE_THEIR_CENTER_IN",
                                                                        selected_layer01,
                                                                        "-0.2 Meters",
                                                                        "NEW_SELECTION",
                                                                        "NOT_INVERT")
        # delete selected layer for old data
        oid_nums = [int(fid) for fid in arcpy.Describe(selected_layer02).FIDSet.split(";") if fid != '']
        oid_fieldname = arcpy.Describe(selected_layer02).OIDFieldName
        # edit.startOperation()
        if len(oid_nums) > 0:
            try:
                # Start an edit operation
                query = f"{oid_fieldname} IN ({', '.join(str(x) for x in oid_nums)})"
                with arcpy.da.UpdateCursor(fc[0], [f"{oid_fieldname}"], where_clause=query) as rows:
                    for row in rows:
                        rows.deleteRow()
            except Exception as e:
                arcpy.AddError(e)
            except arcpy.ExecuteError as e:
                arcpy.AddError(e)
        else:
            pass

        _params = self.field_mappings(source_feature=selected_layer01,
                                      target_feature=fc[1],
                                      point_feat_dir=fc_pts)

        try:
            # Append selected points to a old version of feature class
            arcpy.Append_management(selected_layer01,
                                    fc[0],
                                    "NO_TEST",
                                    _params
                                    )
        except arcpy.ExecuteError as e:
            arcpy.AddError(e)

        del _params

        arcpy.SelectLayerByAttribute_management(selected_layer01, "CLEAR_SELECTION")
        arcpy.SelectLayerByAttribute_management(selected_layer02, "CLEAR_SELECTION")

    def report(self, featureclass_list):
        """
        Generate PDF Report for total count of new feature added from 19C
        References - https://gist.github.com/blakebjorn/de24417bb032c71c3901a82c79bcf926
                   - https://codereview.stackexchange.com/questions/273767/resume-builder-using-jinja-templates-and-html
                   - https://stackoverflow.com/questions/63833699/building-a-dynamic-table-with-jinja2-html-and-flask
        Args:
            featureclass_list:

        Returns:

        """
        file_dir = os.path.dirname(os.path.realpath(__file__))
        # df = pd.DataFrame(featureclass_list)
        # df.columns = ['FeatureClasses', 'Count']

        df = pd.DataFrame(featureclass_list, columns=["FeatureClasses", "Count"])
        # report_dir = os.path.join(os.path.expanduser('~'), "Documents", "GIS_Reports", "Test")
        # os.makedirs(report_dir, exist_ok=True)
        df.to_csv(os.path.join(self.report_out_dir, f"{self.div}_KPG_EXT.csv"), index=False)

        # df_html = df.to_html(index=False, classes="table-title")
        # env = Environment(loader=FileSystemLoader(os.path.join(file_dir, "assets", "report")))
        # template = env.get_template("replication_report.html")
        # generated_html = template.render(date=str(date.today().strftime('%d, %b %Y')),
        #                                  title='Total new features from 19C for each featureclass',
        #                                  df=df_html)
        #
        # pdf_out = os.path.join(self.report_out_dir, "latest_features.pdf")
        #
        # with open(pdf_out, "w+b") as out_pdf:
        #     try:
        #         pisa.CreatePDF(src=generated_html, dest=out_pdf)
        #     except Exception as e:
        #         arcpy.AddError(e)

    def __repr__(self):
        cls = self.__class__.__name__
        return f'{cls}(init_geodatabase={self.gdb1}, ' \
               f'latest_geodatabase={self.gdb2},' \
               f'report={self.create_report},' \
               f'report_output_directory={self.report_out_dir})\n' \
               f'Processor number : {self.processor_num}\n'


class ReplicateSDE2GDB:
    def __init__(self, sde_instance, sde_username, sde_password,
                 output_directory, file_gdb, wildcard_datasets=None, wildcard_featureclass=None):
        self.instance = sde_instance
        self.usr = sde_username
        self.pwd = sde_password
        self.out_dir = output_directory
        self.gdb = file_gdb
        self.wildcard_ds = wildcard_datasets
        self.wildcard_fc = wildcard_featureclass
        self.temp_dir = tempfile.mkdtemp()

        if self.wildcard_ds is None:
            self.wildcard_ds = ""
        if self.wildcard_fc is None:
            self.wildcard_fc = ""

        sde = self.temp_connection()

        db_out = os.path.join(self.out_dir, self.gdb)
        if arcpy.Exists(db_out):
            arcpy.Delete_management(db_out)
            arcpy.CreateFileGDB_management(self.out_dir, self.gdb, "9.3")
        else:
            arcpy.CreateFileGDB_management(self.out_dir, self.gdb, "9.3")

        arcpy.env.workspace = sde

        dss = sorted(arcpy.ListDatasets(self.wildcard_ds, "ALL"))
        pbar01 = tqdm(dss, position=0, colour='GREEN')
        for ds in pbar01:
            pbar01.set_description(ds)
            dsname = '%s' % self.newname('SDE.', ds)
            # copy everything in dataset
            try:
                out_data = os.path.join(db_out, dsname)
                arcpy.Copy_management(ds, out_data)
            except Exception as e:
                arcpy.AddError(e)

        shutil.rmtree(self.temp_dir)

    def temp_connection(self):
        # create connection parameters
        conn = {"out_folder_path": self.temp_dir,
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

        arcpy.CreateDatabaseConnection_management(**conn)

        return temp_sde

    @staticmethod
    def newname(target_string, old_name):
        new_name = old_name.replace(target_string, "", 1)
        return new_name


