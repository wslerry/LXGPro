import arcpy
import os
import multiprocessing as mp
from tqdm import tqdm


class ExportXML:
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

        # with mp.Pool(4) as pool:
        #     x = tqdm(pool.imap(self.func, xml_list), total=len(xml_list), colour="green", position=0)
        #     tuple(x)
        #     pool.close()
        #     pool.join()

        for xml in xml_list:
            try:
                arcpy.env.workspace = out_gdb
                arcpy.ImportXMLWorkspaceDocument_management(out_gdb, xml, 'SCHEMA_ONLY')
                print(f"{xml} success")
            except arcpy.ExecuteError as e:
                arcpy.AddError(e)

    def func(self, xml):
        try:
            arcpy.ImportXMLWorkspaceDocument_management(self.gdb, xml, 'SCHEMA_ONLY')
            print(f"{xml} success")
        except arcpy.ExecuteError as e:
            arcpy.AddError(e)

    def printme(self, xml):
        print(xml)


if __name__ == "__main__":
    gdb = 'CMS.gdb'
    xml_dir = r'C:\LXG\replication\workshop\Workspace'
    ExportXML(gdb, xml_dir)
