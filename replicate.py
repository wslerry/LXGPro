import os
from LXG import AppendNewFeatures
import tempfile
import shutil

if __name__ == "__main__":
    params1 = {
        "init_geodatabase": r"C:\LXG\replication\arcpro\data\old.gdb",
        "latest_geodatabase": r"C:\LXG\replication\arcpro\data\new.gdb",
        "report": False
    }

    AppendNewFeatures(**params1)

    # current_ = os.path.join(tempfile.gettempdir(), "lxg_replica_temp")
    # if os.path.isdir(current_):
    #     shutil.rmtree(current_)
    # else:
    #     pass
