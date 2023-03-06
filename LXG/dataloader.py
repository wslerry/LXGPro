import os
import sys
import arcpy
import re
import pandas as pd
from osgeo import ogr

ogr.UseExceptions()


class DataLoader:
    """
    Dataloader for extracting information from geodatabase, output as a dataframe
    """
    def __init__(self, geodatabase):
        self.gdb = geodatabase
        arcpy.env.workspace = self.gdb

        self.datasets_list = list()
        self.featureclass_list = list()
        self.table_list = list()

        self.datatypes = ["FeatureClass", "FeatureDataset", "Table"]

        for d_type in self.datatypes:
            idx = 0
            for root, folder, files in arcpy.da.Walk(arcpy.env.workspace, datatype=d_type, type=None):
                for f in files:
                    idx += 1
                    if d_type == "FeatureClass":
                        self.featureclass_list.append([idx, os.path.join(root, f)])
                    elif d_type == "FeatureDataset":
                        self.datasets_list.append([idx, os.path.join(root, f)])
                    elif d_type == "Table":
                        self.table_list.append([idx, os.path.join(root, f)])
                    else:
                        pass

    def featureclasses(self):
        _feat_list = [fc for fc in self.featureclass_list]
        if len(_feat_list) > 0:
            recs = [self.getcount(i[0], os.path.basename(i[1])) for i in _feat_list]
            recs.sort()
            df = pd.DataFrame(recs)
            df.columns = ['FeatureClasses', 'Count']
            return df
        else:
            return None

    def datasets(self):
        _ds_list = [ds for ds in self.datasets_list]
        if len(_ds_list) > 0:
            df = pd.DataFrame(_ds_list)
            df.columns = ['Datasets']
            return df
        else:
            return None

    def tables(self):
        _tab_list = [tab for tab in self.table_list]
        if len(_tab_list) > 0:
            df = pd.DataFrame(_tab_list)
            df.columns = ['Table']
            return df
        else:
            return None

    def getcount(self, idx, feat):
        arcpy.MakeTableView_management(feat, f"temp_tab_{idx}")
        count = int(arcpy.GetCount_management(f"temp_tab_{idx}").getOutput(0))
        arcpy.Delete_management(f"temp_tab_{idx}")

        return feat, count

    def get_datasetname(self, feature):
        """
        Get FileGDB datasets name from feature name
        Only work for full featureclass name with file directory
        example:
            > gdb_file_directory = C:/My/File/hello.gdb/dataset_name/featureclass_name
            > get_datasetname(gdb_file_directory)
        result:
            > dataset_name
        """
        namesplit = os.path.normpath(feature).split(os.sep)
        # Find datasets
        idx = 0
        gdb_idx = None
        for x in namesplit:
            idx += 1
            if re.search(".gdb", x):
                gdb_idx = idx

        dif = len(namesplit) - gdb_idx
        if dif == 2:
            # return os.path.join(self.gdb, namesplit[-2], os.path.basename(feature))
            return namesplit[-2]
        else:
            return None

    def records(self):
        return self.datasets_list, \
               self.featureclass_list, \
               self.table_list


class OGRDataLoader:
    """
    Dataloader will return dataframe for all feature class record in geodatabase (*.gdb or *.mdb)

    Usage:
        OGRDataLoader(geodatabase='c:/path/to/helloworld.gdb')
        OGRDataLoader(geodatabase='c:/path/to/helloworld.mdb')

    example:
        data = OGRDataLoader(geodatabase='c:/path/to/helloworld.gdb')
        df = data.features()

    Reference :
        1. https://pcjericks.github.io/py-gdalogr-cookbook/vector_layers.html
        2. https://gis.stackexchange.com/questions/41324/read-an-arcgis-feature-dataset-using-gdal
    """
    def __init__(self, geodatabase):
        self.gdb = geodatabase
        if os.path.splitext(os.path.basename(self.gdb))[1] == '.mdb':
            self.gdb_type = "MDB"
            self.driver = ogr.GetDriverByName('PGeo')
        elif os.path.splitext(os.path.basename(self.gdb))[1] == '.gdb':
            self.gdb_type = "GDB"
            self.driver = ogr.GetDriverByName('OpenFileGDB')
        else:
            sys.exit("Not a valid geodatabase")

        self.datasets = self.driver.Open(self.gdb)

    def features_count(self):
        check_list = self.esri_features_fullname()
        feat_list = list()

        numLayers = self.datasets.GetLayerCount()

        if self.gdb_type == "GDB":
            for f_name in check_list:
                for layerNum in range(numLayers):
                    feat = self.datasets.GetLayerByIndex(layerNum)
                    name = feat.GetName()
                    if re.search("DirtyAreas", name) \
                            or re.search("PolyErrors", name) \
                            or re.search("LineErrors", name) \
                            or re.search("PointErrors", name):
                        pass
                    elif os.path.basename(f_name) == name:
                        feat_list.append((name, feat.GetFeatureCount()))
                    else:
                        pass
        elif self.gdb_type == "MDB":
            for layerNum in range(numLayers):
                feat = self.datasets.GetLayerByIndex(layerNum)
                name = feat.GetName()
                if re.search("DirtyAreas", name) \
                        or re.search("PolyErrors", name) \
                        or re.search("LineErrors", name) \
                        or re.search("PointErrors", name) \
                        or feat.GetGeomType() == 100:
                    pass
                else:
                    feat_list.append((feat.GetName(), feat.GetFeatureCount()))
        else:
            pass

        feat_list.sort()
        df = pd.DataFrame(feat_list)
        df.columns = ['FeatureClasses', 'Count']

        return df

    def esri_features_fullname(self):
        tem_feat_list = list()
        arcpy.env.workspace = self.gdb
        idx = 0
        working_files = arcpy.da.Walk(arcpy.env.workspace, datatype="FeatureClass", type=None)
        for root, folder, files in working_files:
            for f in files:
                idx += 1
                tem_feat_list.append(os.path.join(root, f))

        return tem_feat_list

    def get_datasetname(self, feature):
        """
        Get FileGDB datasets name from feature name
        Only work for full featureclass name with file directory
        example:
            gdb_file_directory = C:/My/File/hello.gdb/dataset_name/featureclass_name
        """
        namesplit = os.path.normpath(feature).split(os.sep)
        # Find datasets
        idx = 0
        gdb_idx = None
        for x in namesplit:
            idx += 1
            if re.search(".gdb", x):
                gdb_idx = idx

        dif = len(namesplit) - gdb_idx
        if dif == 2:
            # return os.path.join(self.gdb, namesplit[-2], os.path.basename(feature))
            return namesplit[-2]
        else:
            return None