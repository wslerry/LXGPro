import arcpy
import os
dir_f = os.path.abspath(os.path.dirname(__file__))

def BRSO():
    crs = arcpy.SpatialReference(os.path.join(dir_f, 'BRSO_4.prj'))
    return crs
