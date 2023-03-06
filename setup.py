import sys
import versioneer
from glob import glob

try:
    import arcpy
except ImportError:
    sys.exit("LXGPro requires Arcpy/Arcgispro to be installed")

if sys.version_info < (3, 7):
    sys.exit("LXGPro requires Python 3.7 or newer")

try:
    import setuptools
    from setuptools import setup
    from setuptools import Extension
    from Cython.Distutils import build_ext
except ImportError:
    from distutils.core import setup
    from distutils.extension import Extension
    from Cython.Distutils import build_ext

# ext_modules = [
#     Extension("LXGMIG", ["LXG/migration.py"]),
#     Extension("LXGREP", ["LXG/replication.py"]),
#     Extension("LXGAnalysis", ["LXG/analysis.py"])
# ]

ext_modules = [
    Extension("LXG", ["LXG/migration.py", "LXG/replication.py", "LXG/dataloader.py", "LXG/analysis.py"]),
]

_data_files = [
    ('LXG', glob('assets/**/*', recursive=True))
]

package_data = {
    '': ['*.prj', '*.XML']
}

with open("README.md", "r") as fh:
    long_description = fh.read()
with open("requirements.txt", "r") as fh:
    requirements = [line.strip() for line in fh]

setup(
    name=f'LXG',
    version=versioneer.get_version(),
    cmdclass={'build_ext': build_ext, 'versioning': versioneer.get_cmdclass()},
    ext_modules=ext_modules,
    data_files=_data_files,
    package_data=package_data,
    author="Lerry William",
    author_email="lerryws@sains.com.my",
    description="A Python library to migrate FileGDB to ArcSDE and replicate ArcSDE to FileGDB",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    install_requires=requirements,
)
