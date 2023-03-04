"""
Author : Lerry William
"""

import os
import sys
import arcpy
import pandas as pd
import numpy as np
from osgeo import ogr
import re

ogr.UseExceptions()


class DataLoader:
    """
    Dataloader for extracting information from geodatabase
    """
    def __init__(self, geodatabase):
        self.gdb = geodatabase
        arcpy.env.workspace = self.gdb

        self.datasets_list = list()
        self.featureclass_list = list()
        self.geom_net_list = list()
        self.las_list = list()
        self.mosaic_ds_list = list()
        self.net_ds_list = list()
        self.rast_cat_list = list()
        self.rast_ds_list = list()
        self.relation_list = list()
        self.represent_list = list()
        self.table_list = list()
        self.terrain_list = list()
        self.tin_list = list()
        self.topology_list = list()

        self.datatypes = ["FeatureClass", "FeatureDataset",
                          "GeometricNetwork", "LasDataset",
                          "MosaicDataset", "NetworkDataset",
                          "RasterCatalog", "RasterDataset",
                          "RelationshipClass", "RepresentationClass",
                          "Table", "Terrain", "Tin", "Topology"
                          ]

        for d_type in self.datatypes:
            idx = 0
            for root, folder, files in arcpy.da.Walk(arcpy.env.workspace, datatype=d_type, type=None):
                for f in files:
                    idx += 1
                    if d_type == "FeatureClass":
                        self.featureclass_list.append([idx, os.path.join(root, f)])
                    elif d_type == "FeatureDataset":
                        self.datasets_list.append([idx, os.path.join(root, f)])
                    elif d_type == "GeometricNetwork":
                        self.geom_net_list.append([idx, os.path.join(root, f)])
                    elif d_type == "LasDataset":
                        self.las_list.append([idx, os.path.join(root, f)])
                    elif d_type == "MosaicDataset":
                        self.mosaic_ds_list.append([idx, os.path.join(root, f)])
                    elif d_type == "NetworkDataset":
                        self.net_ds_list.append([idx, os.path.join(root, f)])
                    elif d_type == "RasterCatalog":
                        self.rast_cat_list.append([idx, os.path.join(root, f)])
                    elif d_type == "RasterDataset":
                        self.rast_ds_list.append([idx, os.path.join(root, f)])
                    elif d_type == "RelationshipClass":
                        self.relation_list.append([idx, os.path.join(root, f)])
                    elif d_type == "RepresentationClass":
                        self.represent_list.append([idx, os.path.join(root, f)])
                    elif d_type == "Table":
                        self.table_list.append([idx, os.path.join(root, f)])
                    elif d_type == "Terrain":
                        self.terrain_list.append([idx, os.path.join(root, f)])
                    elif d_type == "Tin":
                        self.tin_list.append([idx, os.path.join(root, f)])
                    elif d_type == "Topology":
                        self.topology_list.append([idx, os.path.join(root, f)])
                    else:
                        pass

        # dss = arcpy.ListDatasets("", "Feature")
        # _feats = arcpy.ListFeatureClasses("", "")
        #
        # idx = 0
        # for root, folder, files in arcpy.da.Walk(arcpy.env.workspace, datatype="FeatureDataset", type=None):
        #     for f in files:
        #         self.datasets_list.append(os.path.join(root, f))
        #
        # for root, folder, files in arcpy.da.Walk(arcpy.env.workspace, datatype="FeatureClass", type=None):
        #     for f in files:
        #         self.featureclass_list.append(os.path.join(root, f))
        #
        #
        # idx = 0
        # if len(dss) > 0:
        #     for ds in dss:
        #         self.datasets_list.append(os.path.join(self.gdb, ds))
        #         all_feats = arcpy.ListFeatureClasses("", "All", ds)
        #         for feat in all_feats:
        #             idx += 1
        #             self.featureclass_list.append((os.path.join(self.gdb, ds, feat), idx))
        # else:
        #     pass
        #
        # if len(_feats) > 0:
        #     for feat in _feats:
        #         idx += 1
        #         self.featureclass_list.append((os.path.join(self.gdb, feat), idx))
        # else:
        #     pass
    def features(self):
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

        df = pd.DataFrame(_ds_list)
        df.columns = ['Datasets']

        return df

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

    def records(self):
        return self.datasets_list, \
               self.featureclass_list, \
               self.geom_net_list,\
               self.las_list,\
               self.mosaic_ds_list,\
               self.net_ds_list,\
               self.rast_cat_list,\
               self.rast_ds_list,\
               self.relation_list,\
               self.represent_list,\
               self.table_list,\
               self.terrain_list,\
               self.tin_list,\
               self.topology_list

        # return self.datasets_list, self.featureclass_list

    # def __repr__(self):
    #     """SOMETHING"""
    #     cls = self.__class__.__name__
    #     return f"{cls}(GDB={os.path.basename(self.gdb)})"
    #
    # def __str__(self):
    #     """SOMETHING"""
    #
    # def __len__(self):
    #     """SOMETHING"""


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


class CheckDifferences:
    """
    dataframe input should be a merge between old data and new data
    """

    def __init__(self, dataframe_init, dataframe_new):
        self.df1 = dataframe_init
        self.df2 = dataframe_new

    def missing(self):
        merge = pd.merge(self.df1, self.df2, on='FeatureClasses', how='right')
        merge['nan'] = merge.apply(lambda r: pd.isna(r['Count_x']), axis=1)
        miss_df = merge[merge['nan'] == True]

        if len(miss_df) > 0:
            return miss_df.drop(['Count_x', 'Count_y', 'nan'], axis=1)
        else:
            return None

    def change(self):
        merge = pd.merge(self.df1, self.df2, on='FeatureClasses', how='right')
        merge['nan'] = merge.apply(lambda r: pd.isna(r['Count_x']), axis=1)
        merge['variance'] = merge.apply(lambda r: np.var([r['Count_x'], r['Count_y']], dtype=np.float64), axis=1)
        change_df = merge[merge['nan'] == False]
        change_df = change_df[change_df['variance'] > 0.0]

        if len(change_df) > 0:
            return change_df.drop(['Count_x', 'Count_y', 'nan', 'variance'], axis=1)
        else:
            return None

    def nochange(self):
        """
        zero value in latest geodatabase, means no changes and there will be no replication
        """
        merge = pd.merge(self.df1, self.df2, on='FeatureClasses', how='right')
        nochange_df = merge[merge['Count_x'] == 0.0]
        nochange_df = nochange_df[nochange_df['Count_y'] == 0.0]

        if len(nochange_df) > 0:
            return nochange_df.drop(['Count_x', 'Count_y'], axis=1)
        else:
            return None

    def empty(self):
        merge = pd.merge(self.df1, self.df2, on='FeatureClasses', how='right')
        zero_df = merge[merge['Count_x'] == 0.0]
        zero_df = zero_df[zero_df['Count_y'] > 0.0]

        if len(zero_df) > 0:
            return zero_df.drop(['Count_x', 'Count_y'], axis=1)
        else:
            return None