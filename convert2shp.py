import arcpy
from tqdm import tqdm
import os
import time
from datetime import datetime, date, timedelta

arcpy.env.overwriteOutput = True


def process(seconds):
    conversion = timedelta(seconds=seconds)
    converted_time = str(conversion)

    return converted_time


class Convert:
    def __init__(self, geodatabase, output_directory):
        self.gdb = geodatabase
        self.dir = output_directory

        arcpy.env.workspace = self.gdb

        dss = sorted(arcpy.ListDatasets("", "feature"))
        pbar01 = tqdm(dss, desc=f'{self.gdb}', position=0, colour='GREEN')
        for ds in pbar01:
            fcs = sorted(arcpy.ListFeatureClasses("", "All", ds))
            pbar02 = tqdm(fcs, position=1, colour='YELLOW', leave=False)
            for fc in pbar02:
                pbar02.set_description(fc)
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
                    fc_tabs = None
                    shp_tabs = None
                    try:
                        fc_tabs = self.column_names(fc)
                        shp_tabs = self.column_names(output_shapefile)
                    except IOError as e:
                        arcpy.AddError(e)
                    except Exception as e:
                        arcpy.AddError(e)

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
            if field.editable is True and field.name not in avoid_this:
                try:
                    field_names.append(field.name)
                except Exception as e:
                    arcpy.AddError(e)
                except arcpy.ExecuteError as e:
                    arcpy.AddError(e)

        return field_names


if __name__ == "__main__":
    gdb = r"C:\LXG\replication\workshop\8i_2023.gdb"
    directory = r"C:\LXG\replication\workshop\shp"

    start0 = time.time()
    Convert(geodatabase=gdb, output_directory=directory)
    arcpy.AddMessage(f'[INFO]\t Processing Done. Total time {process(time.time() - start0)}s ...')
