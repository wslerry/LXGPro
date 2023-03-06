from .migration import Migration
from .replication import ToBRSO, ToShapefile, AppendNewFeatures, BatchImportXML, ReplicateSDE2GDB
from .analysis import CheckDifferences
from .dataloader import OGRDataLoader, DataLoader
from .assets import BRSO
from . import _version
__version__ = _version.get_versions()['version']

# import sys
# # import os
# # dir_f = os.path.abspath(os.path.dirname(__file__))
# sys.path.append("../")
# from assets import BRSO

