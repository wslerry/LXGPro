"""
Author : Lerry William
"""

import os
import sys
import arcpy
import pandas as pd
import numpy as np


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