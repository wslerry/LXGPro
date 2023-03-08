def BRSO():
    import arcpy
    import os
    dir_f = os.path.abspath(os.path.dirname(__file__))
    crs = arcpy.SpatialReference(os.path.join(dir_f, 'projection', 'BRSO_4.prj'))

    return crs


def HQ():
    import arcpy
    import os
    dir_f = os.path.abspath(os.path.dirname(__file__))
    kch_dir = os.path.join(dir_f, "workspace_xml", "hq")
    kch_xml_list = []
    for root, dirs, files in os.walk(kch_dir):
        for file in files:
            kch_xml_list.append(os.path.join(root, file))

    return kch_xml_list


def KCH():
    import arcpy
    import os
    dir_f = os.path.abspath(os.path.dirname(__file__))
    div_dir = os.path.join(dir_f, "workspace_xml", "kch")
    xml_list = []
    for root, dirs, files in os.walk(div_dir):
        for file in files:
            xml_list.append(os.path.join(root, file))

    return xml_list


def BTG():
    import arcpy
    import os
    dir_f = os.path.abspath(os.path.dirname(__file__))
    div_dir = os.path.join(dir_f, "workspace_xml", "btg")
    xml_list = []
    for root, dirs, files in os.walk(div_dir):
        for file in files:
            xml_list.append(os.path.join(root, file))

    return xml_list


def BTU():
    import arcpy
    import os
    dir_f = os.path.abspath(os.path.dirname(__file__))
    div_dir = os.path.join(dir_f, "workspace_xml", "btu")
    xml_list = []
    for root, dirs, files in os.walk(div_dir):
        for file in files:
            xml_list.append(os.path.join(root, file))

    return xml_list


def KPT():
    import arcpy
    import os
    dir_f = os.path.abspath(os.path.dirname(__file__))
    div_dir = os.path.join(dir_f, "workspace_xml", "kpt")
    xml_list = []
    for root, dirs, files in os.walk(div_dir):
        for file in files:
            xml_list.append(os.path.join(root, file))

    return xml_list


def LBG():
    import arcpy
    import os
    dir_f = os.path.abspath(os.path.dirname(__file__))
    div_dir = os.path.join(dir_f, "workspace_xml", "lbg")
    xml_list = []
    for root, dirs, files in os.walk(div_dir):
        for file in files:
            xml_list.append(os.path.join(root, file))

    return xml_list


def MKH():
    import arcpy
    import os
    dir_f = os.path.abspath(os.path.dirname(__file__))
    div_dir = os.path.join(dir_f, "workspace_xml", "mkh")
    xml_list = []
    for root, dirs, files in os.walk(div_dir):
        for file in files:
            xml_list.append(os.path.join(root, file))

    return xml_list


def MRI():
    import arcpy
    import os
    dir_f = os.path.abspath(os.path.dirname(__file__))
    div_dir = os.path.join(dir_f, "workspace_xml", "mri")
    xml_list = []
    for root, dirs, files in os.walk(div_dir):
        for file in files:
            xml_list.append(os.path.join(root, file))

    return xml_list


def SBU():
    import arcpy
    import os
    dir_f = os.path.abspath(os.path.dirname(__file__))
    div_dir = os.path.join(dir_f, "workspace_xml", "sbu")
    xml_list = []
    for root, dirs, files in os.walk(div_dir):
        for file in files:
            xml_list.append(os.path.join(root, file))

    return xml_list


def SMH():
    import arcpy
    import os
    dir_f = os.path.abspath(os.path.dirname(__file__))
    div_dir = os.path.join(dir_f, "workspace_xml", "smh")
    xml_list = []
    for root, dirs, files in os.walk(div_dir):
        for file in files:
            xml_list.append(os.path.join(root, file))

    return xml_list


def SRI():
    import arcpy
    import os
    dir_f = os.path.abspath(os.path.dirname(__file__))
    div_dir = os.path.join(dir_f, "workspace_xml", "sri")
    xml_list = []
    for root, dirs, files in os.walk(div_dir):
        for file in files:
            xml_list.append(os.path.join(root, file))

    return xml_list


def SRK():
    import arcpy
    import os
    dir_f = os.path.abspath(os.path.dirname(__file__))
    div_dir = os.path.join(dir_f, "workspace_xml", "srk")
    xml_list = []
    for root, dirs, files in os.walk(div_dir):
        for file in files:
            xml_list.append(os.path.join(root, file))

    return xml_list


def SRN():
    import arcpy
    import os
    dir_f = os.path.abspath(os.path.dirname(__file__))
    div_dir = os.path.join(dir_f, "workspace_xml", "srn")
    xml_list = []
    for root, dirs, files in os.walk(div_dir):
        for file in files:
            xml_list.append(os.path.join(root, file))

    return xml_list
