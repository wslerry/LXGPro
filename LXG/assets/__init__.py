def BRSO():
    import arcpy
    import os
    dir_f = os.path.abspath(os.path.dirname(__file__))
    crs = arcpy.SpatialReference(os.path.join(dir_f, 'projection', 'BRSO_4.prj'))

    return crs
