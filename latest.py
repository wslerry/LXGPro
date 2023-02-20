# ------------------------------
# Description : Detect latest feature classes which not exist in older version, and append to target @ older version
# version : 2.0.1
# created : 13/02/2023
# updated : 19/02/2023
# Author : Lerry William Seling
# Copyright : Sarawak Information System Sdn Bhd
# Client : Sarawak Land and Survey
# Arcgis version : ArcgisPro 2.8
# Python version : 3.7.10
# ------------------------------

import arcpy
import os
import time
from datetime import datetime, date, timedelta
import multiprocessing as mp
from tqdm import tqdm
import numpy as np
import pandas as pd
from jinja2 import Environment, FileSystemLoader
# from weasyprint import HTML
from xhtml2pdf import pisa
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
log_filename = f"logs/new_feature_detection_{today}.log"
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


current_dir = os.path.dirname(os.path.realpath(__file__))


def your_existance_in_questions(geodatabase_name):
    gdb_filename = os.path.join(current_dir, geodatabase_name)
    if arcpy.Exists(gdb_filename):
        arcpy.Delete_management(gdb_filename)
        arcpy.CreateFileGDB_management(current_dir, geodatabase_name, "9.3")
    else:
        arcpy.CreateFileGDB_management(current_dir, geodatabase_name, "9.3")

    return gdb_filename


# noinspection PyMethodMayBeStatic
class NewFeatures:
    def __init__(self, init_geodatabase, latest_geodatabase, report=False):
        self.gdb1 = init_geodatabase
        self.gdb2 = latest_geodatabase
        self.create_report = report

        self.new_ds_list = list()
        start0 = time.time()

        # self.gdb_poly = os.path.join(current_dir, 'polys.gdb')
        self.gdb_poly = your_existance_in_questions('polys.gdb')
        self.gdb_lines = your_existance_in_questions('lines.gdb')
        self.gdb_points = your_existance_in_questions('points.gdb')

        logger.info(f'Data preparation for {self.gdb1}')
        self.prepare_features(self.gdb1)
        logger.info(f'Data preparation for {self.gdb2}')
        self.prepare_features(self.gdb2)

        logger.info(f'Difference checking')
        check_list = self.check_differences()

        if self.create_report:
            logger.info(f'Create report')
            self.report(check_list)
        else:
            pass

        logger.info(f'Append to {self.gdb1}')
        self.append_latest(check_list)

        stop0 = time.time()
        logger.info('Total processing time: ' + process(stop0 - start0))
        arcpy.AddMessage(f'[INFO]\t Processing Done. Total time {process(stop0 - start0)}s ...')

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
                                 os.path.join(self.gdb_poly, f'{fc_class_name}_{poly}_pts'),
                                 os.path.join(self.gdb_poly, f'{fc_class_name}_{poly}_buf')
                                 ])
                # fc_list = [[os.path.join(geodatabase, ds, poly),
                #             geodatabase,
                #             os.path.join(self.gdb_poly, f'{fc_class_name}_{poly}_pts'),
                #             os.path.join(self.gdb_poly, f'{fc_class_name}_{poly}_buf')] for poly in fc_poly]

                # with mp.Pool(processes=4) as pool1:
                #     results = tqdm(pool1.imap(self._polys, fc_list),
                #                    desc="Polygon",
                #                    total=len(fc_list),
                #                    position=1,
                #                    colour='YELLOW', leave=False)
                #     tuple(results)
                #     pool1.close()
                #     pool1.join()
                #
                #     del results

            fc_lines = sorted(arcpy.ListFeatureClasses("", "Polyline", ds))
            if len(fc_lines) > 0:
                pbar03 = tqdm(fc_lines, desc="Polyline", position=1, colour='Yellow', leave=False)
                for line in pbar03:
                    # pbar03.set_description(line)
                    input_line = os.path.join(geodatabase, ds, line)
                    pts_feat_line = os.path.join(self.gdb_lines, f'{fc_class_name}_{line}_pts')
                    arcpy.GeneratePointsAlongLines_management(Input_Features=input_line,
                                                              Output_Feature_Class=pts_feat_line,
                                                              Point_Placement="PERCENTAGE",
                                                              Distance="",
                                                              Percentage=50,
                                                              Include_End_Points="")
                    try:
                        if geodatabase == self.gdb1:
                            # Execute the Buffer tool if initial geodatabase
                            arcpy.Buffer_analysis(pts_feat_line,
                                                  os.path.join(self.gdb_lines, f'{fc_class_name}_{line}_buf'),
                                                  "0.5 meters")
                            arcpy.Delete_management(pts_feat_line)
                    except arcpy.ExecuteError as e:
                        arcpy.AddError(e)
                        logger.error(e)

                    del pts_feat_line

            # points
            fc_points = sorted(arcpy.ListFeatureClasses("", "Point", ds))
            if len(fc_points) > 0:
                fc_points_list = [[os.path.join(geodatabase, ds, pts),
                                   geodatabase,
                                   os.path.join(self.gdb_points, f'{fc_class_name}_{pts}_pts'),
                                   os.path.join(self.gdb_points, f'{fc_class_name}_{pts}_buf')] for pts in fc_points]
                with mp.Pool(processes=4) as pool3:
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

    def _polys(self, fc_list):
        geodatabase = fc_list[1]
        pts_feat_poly = fc_list[2]
        buf_feat_poly = fc_list[3]

        arcpy.FeatureToPoint_management(fc_list[0], pts_feat_poly, "CENTROID")
        try:
            if geodatabase == self.gdb1:
                # Execute the Buffer tool if initial geodatabase
                arcpy.Buffer_analysis(pts_feat_poly,
                                      buf_feat_poly,
                                      "0.5 meters")
                arcpy.Delete_management(pts_feat_poly)
        except arcpy.ExecuteError as e:
            arcpy.AddError(e)
            logger.error(e)

        del geodatabase, pts_feat_poly, buf_feat_poly

    def _lines(self, fc_list):
        input_line = fc_list[0]
        geodatabase = fc_list[1]
        pts_feat_line = fc_list[2]
        buf_feat_line = fc_list[3]

        arcpy.GeneratePointsAlongLines_management(Input_Features=input_line,
                                                  Output_Feature_Class=pts_feat_line,
                                                  Point_Placement="PERCENTAGE",
                                                  Distance="",
                                                  Percentage=50,
                                                  Include_End_Points="")
        try:
            if geodatabase == self.gdb1:
                # Execute the Buffer tool if initial geodatabase
                arcpy.Buffer_analysis(pts_feat_line,
                                      buf_feat_line,
                                      "0.5 meters")
                arcpy.Delete_management(pts_feat_line)
        except arcpy.ExecuteError as e:
            arcpy.AddError(e)
            logger.error(e)

        del geodatabase, pts_feat_line, buf_feat_line

    def _points(self, fc_list):
        geodatabase = fc_list[1]
        pts_feat_pts = fc_list[2]
        buf_feat_pts = fc_list[3]

        arcpy.CopyFeatures_management(fc_list[0], pts_feat_pts)
        try:
            if geodatabase == self.gdb1:
                # Execute the Buffer tool if initial geodatabase
                arcpy.Buffer_analysis(fc_list[0],
                                      buf_feat_pts,
                                      "0.5 meters")
                arcpy.Delete_management(pts_feat_pts)
        except arcpy.ExecuteError as e:
            arcpy.AddError(e)
            logger.error(e)

        del geodatabase, pts_feat_pts, buf_feat_pts

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

        return int(selected_count)

    def check_differences(self):
        arcpy.env.workspace = self.gdb2
        new_features_list = []
        dss = sorted(arcpy.ListDatasets("", "ALL"))
        pbar01 = tqdm(dss, desc='Detect changes', position=0, colour='GREEN')
        for ds in pbar01:
            fcs = sorted(arcpy.ListFeatureClasses("", "Polygon", ds))
            pbar02 = tqdm(fcs, position=1, colour='Yellow', leave=False)
            for fc in pbar02:
                pbar02.set_description(fc)
                fc_pts = os.path.join(self.gdb_poly, f'gdb2_{fc}_pts')
                old_fc_buf = os.path.join(self.gdb_poly, f'gdb1_{fc}_buf')
                try:
                    if arcpy.Exists(fc_pts):
                        if arcpy.Exists(old_fc_buf):
                            get_count = self.intersect(fc_pts, old_fc_buf)
                            if get_count > 0:
                                new_features_list.append((fc, get_count))
                except arcpy.ExecuteError as e:
                    arcpy.AddError(e)
                    logger.error(e)

            fcs = sorted(arcpy.ListFeatureClasses("", "Polyline", ds))
            pbar03 = tqdm(fcs, position=1, colour='Yellow', leave=False)
            for fc in pbar03:
                pbar03.set_description(fc)
                fc_pts = os.path.join(self.gdb_lines, f'gdb2_{fc}_pts')
                old_fc_buf = os.path.join(self.gdb_lines, f'gdb1_{fc}_buf')
                try:
                    if arcpy.Exists(fc_pts):
                        if arcpy.Exists(old_fc_buf):
                            get_count = self.intersect(fc_pts, old_fc_buf)
                            if get_count > 0:
                                new_features_list.append((fc, get_count))
                except arcpy.ExecuteError as e:
                    arcpy.AddError(e)
                    logger.error(e)

            fcs = sorted(arcpy.ListFeatureClasses("", "Point", ds))
            pbar04 = tqdm(fcs, position=1, colour='Yellow', leave=False)
            for fc in pbar04:
                pbar04.set_description(fc)
                fc_pts = os.path.join(self.gdb_points, f'gdb2_{fc}_pts')
                old_fc_buf = os.path.join(self.gdb_points, f'gdb1_{fc}_buf')
                try:
                    if arcpy.Exists(fc_pts):
                        if arcpy.Exists(old_fc_buf):
                            get_count = self.intersect(fc_pts, old_fc_buf)
                            if get_count > 0:
                                new_features_list.append((fc, get_count))
                except arcpy.ExecuteError as e:
                    arcpy.AddError(e)
                    logger.error(e)

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
                                self.gdb_poly] for fc in fcs if fc in featureclass_list[:, 0]]
                    with mp.Pool(processes=4) as pool1:
                        results = tqdm(pool1.imap(self.fast_poly_append, fc_list),
                                       total=len(fc_list),
                                       desc='Append FeatureClasses',
                                       colour='Yellow',
                                       leave=False)
                        tuple(results)
                        pool1.close()
                        pool1.join()
                except Exception as e:
                    arcpy.AddError(e)

                # pbar02 = tqdm(fcs, position=1, desc="Append FeatureClasses", colour='Yellow', leave=False)
                # for fc in pbar02:
                #     if fc in featureclass_list[:, 0]:
                #         self.fast_poly_append([os.path.join(self.gdb1, ds, fc),
                #                                os.path.join(self.gdb2, ds, fc),
                #                                self.gdb_poly]
                #                               )

            del fcs

            fcs = sorted(arcpy.ListFeatureClasses("", "Polyline", ds))
            if len(fcs) > 0:
                fc_list = [[os.path.join(self.gdb1, ds, fc),
                            os.path.join(self.gdb2, ds, fc),
                            self.gdb_lines] for fc in fcs if fc in featureclass_list[:, 0]]
                with mp.Pool(processes=4) as pool2:
                    results = tqdm(pool2.imap(self.fast_append, fc_list),
                                   total=len(fc_list),
                                   desc='Append FeatureClasses',
                                   colour='Yellow',
                                   position=1,
                                   leave=False)
                    tuple(results)
                    pool2.close()
                    pool2.join()

            del fcs

            fcs = sorted(arcpy.ListFeatureClasses("", "Point", ds))
            if len(fcs) > 0:
                fc_list = [[os.path.join(self.gdb1, ds, fc),
                            os.path.join(self.gdb2, ds, fc),
                            self.gdb_points] for fc in fcs if fc in featureclass_list[:, 0]]
                with mp.Pool(processes=4) as pool3:
                    results = tqdm(pool3.imap(self.fast_append, fc_list),
                                   total=len(fc_list),
                                   desc='Append FeatureClasses',
                                   colour='Yellow',
                                   position=1,
                                   leave=False)
                    tuple(results)
                    pool3.close()
                    pool3.join()

            del fcs

    def fast_append(self, fc):
        _fc = os.path.basename(fc[1])

        fc_pts = os.path.join(fc[2], f'gdb2_{_fc}_pts')

        # Select the points that are touching the buffer using the Select Layer By Location tool
        selected_layer, _, _ = arcpy.SelectLayerByLocation_management(fc[1],
                                                                      "INTERSECT",
                                                                      fc_pts)
        _params = self.field_mappings(source_feature=selected_layer,
                                      target_feature=fc[1],
                                      point_feat_dir=fc_pts)

        try:
            # Append selected points to a old version of feature class
            arcpy.Append_management(selected_layer,
                                    fc[0],
                                    "NO_TEST",
                                    _params
                                    )
        except arcpy.ExecuteError as e:
            arcpy.AddError(e)
            logger.error(e)

        del _params

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

        # Start an edit session. Must provide the workspace.
        edit = arcpy.da.Editor(self.gdb1)
        edit.startEditing(False, False)

        # delete selected layer for old data
        oid_nums = [int(fid) for fid in arcpy.Describe(selected_layer02).FIDSet.split(";") if fid != '']
        oid_fieldname = arcpy.Describe(selected_layer02).OIDFieldName
        edit.startOperation()
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
            logger.error(e)

        # # second round to avoid duplicate at the same place
        # selected_layer02, _, _ = arcpy.SelectLayerByLocation_management(fc[0],
        #                                                                 "HAVE_THEIR_CENTER_IN",
        #                                                                 selected_layer01,
        #                                                                 "-0.2 Meters",
        #                                                                 "NEW_SELECTION",
        #                                                                 "NOT_INVERT")
        #
        # # Start an edit session. Must provide the workspace.
        # edit = arcpy.da.Editor(self.gdb1)
        # edit.startEditing(False, False)
        #
        # # delete selected layer for old data
        # oid_nums = [int(fid) for fid in arcpy.Describe(selected_layer02).FIDSet.split(";") if fid != '']
        # oid_fieldname = arcpy.Describe(selected_layer02).OIDFieldName
        # try:
        #     # Start an edit operation
        #     edit.startOperation()
        #     if len(oid_nums) > 0:
        #         query = f"{oid_fieldname} IN ({', '.join(str(x) for x in oid_nums)})"
        #         with arcpy.da.UpdateCursor(fc[0], [f"{oid_fieldname}"], where_clause=query) as rows:
        #             for row in rows:
        #                 rows.deleteRow()
        #     else:
        #         pass
        # except Exception as e:
        #     arcpy.AddError(e)
        # except arcpy.ExecuteError as e:
        #     arcpy.AddError(e)
        #
        # _params = self.field_mappings(source_feature=selected_layer01,
        #                               target_feature=fc[1],
        #                               point_feat_dir=fc_pts)
        #
        # try:
        #     # Append selected points to a old version of feature class
        #     arcpy.Append_management(selected_layer01,
        #                             fc[0],
        #                             "NO_TEST",
        #                             _params
        #                             )
        # except arcpy.ExecuteError as e:
        #     arcpy.AddError(e)
        #     logger.error(e)

        del _params
        # Stop the edit operation.
        edit.stopOperation()
        # Stop the edit session and save the changes
        edit.stopEditing(True)

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
        df = pd.DataFrame(featureclass_list)
        df.columns = ['FeatureClasses', 'Count']
        df_html = df.to_html(index=False, classes="table-title")
        env = Environment(loader=FileSystemLoader('./report'))
        template = env.get_template("replication_report.html")
        generated_html = template.render(date=date.today().strftime('%d, %b %Y'),
                                         title='Total new features added from 19C for each featureclass',
                                         df=df_html)

        # Convert HTML to PDF
        with open('./report/replication_report.pdf', "w+b") as out_pdf:
            pisa.CreatePDF(src=generated_html, dest=out_pdf)


if __name__ == "__main__":
    # params = {
    #     "init_geodatabase": r"C:\LXG\replication\workshop\8i_2023.gdb",
    #     "latest_geodatabase": r"C:\LXG\replication\workshop\replication_from_19c.gdb",
    #     "report": False
    # }

    params = {
        "init_geodatabase": r"C:\LXG\replication\ARC\replication_test\OLD.gdb",
        "latest_geodatabase": r"C:\LXG\replication\ARC\replication_test\NEW.gdb",
        "report": False
    }

    # params = {
    #     "init_geodatabase": r"C:\LXG\replication\arcpro\data\old.gdb",
    #     "latest_geodatabase": r"C:\LXG\replication\arcpro\data\new.gdb",
    #     "report": False
    # }

    NewFeatures(**params)

