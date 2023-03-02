"""
Author : Lerry William
"""

import os
import sys
import arcpy
import pandas as pd
from osgeo import ogr
import re


class DataLoader:
    """
    Dataloader for extracting information from geodatabase
    """
    def __init__(self, geodatabase):
        self.gdb = geodatabase
        arcpy.env.workspace = self.gdb

        self.datasets_list = list()
        self.featureclass_list = list()

        dss = arcpy.ListDatasets("", "Feature")
        _feats = arcpy.ListFeatureClasses("", "")

        idx = 0
        if len(dss) > 0:
            for ds in dss:
                self.datasets_list.append(os.path.join(self.gdb, ds))
                all_feats = arcpy.ListFeatureClasses("", "All", ds)
                for feat in all_feats:
                    idx += 1
                    self.featureclass_list.append((os.path.join(self.gdb, ds, feat), idx))
        else:
            pass

        if len(_feats) > 0:
            for feat in _feats:
                idx += 1
                self.featureclass_list.append((os.path.join(self.gdb, feat), idx))
        else:
            pass

    def features(self):
        _feat_list = [fc for fc in self.featureclass_list]

        recs = [self.getcount(i[0], i[1]) for i in _feat_list]

        df = pd.DataFrame(recs)
        df.columns = ['FeatureClasses', 'Count']

        return df

    def datasets(self):
        _ds_list = [ds for ds in self.datasets_list]

        df = pd.DataFrame(_ds_list)
        df.columns = ['Datasets']

        return df

    def getcount(self, feat, idx):
        arcpy.MakeTableView_management(feat, f"temp_tab_{idx}")
        count = int(arcpy.GetCount_management(f"temp_tab_{idx}").getOutput(0))

        arcpy.Delete_management(f"temp_tab_{idx}")

        return feat, count

    def records(self):
        return self.datasets_list, self.featureclass_list

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
            self.driver = ogr.GetDriverByName('PGeo')
        elif os.path.splitext(os.path.basename(self.gdb))[1] == '.gdb':
            self.driver = ogr.GetDriverByName('OpenFileGDB')
        else:
            sys.exit("Not a valid geodatabase")

        self.datasets = self.driver.Open(self.gdb)

    def features(self):
        feat_list = list()
        numLayers = self.datasets.GetLayerCount()
        for layerNum in range(numLayers):
            feat = self.datasets.GetLayerByIndex(layerNum)
            name = feat.GetName()
            if re.search("DirtyAreas", name) \
                    or re.search("PolyErrors", name) \
                    or re.search("LineErrors", name) \
                    or re.search("PointErrors", name):
                pass
            else:
                feat_list.append((feat.GetName(), feat.GetFeatureCount()))
        feat_list.sort()
        df = pd.DataFrame(feat_list)
        df.columns = ['FeatureClasses', 'Count']

        return df

