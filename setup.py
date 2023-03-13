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
    from setuptools import setup, Extension, find_packages
    from Cython.Distutils import build_ext
except ImportError:
    from distutils.core import setup
    from distutils.extension import Extension
    from Cython.Distutils import build_ext

ext_modules = [
    Extension("LXG", ["LXG/migration.py", "LXG/replication.py", "LXG/dataloader.py", "LXG/analysis.py"]),
]

_data_files = [
    ('LXG', glob('assets/**/*', recursive=True)),
    ('LXG.apis', glob('apis/**/*', recursive=True)),
    ('LXG.tol', glob('tol/**/*', recursive=True)),
    ('LXG.verification', glob('verification/**/*', recursive=True)),
]

package_data = {
    '': ['*.prj', '*.XML', '*.otf', '*.ttf', '*.html', '*.css']
}

with open("README.md", "r") as fh:
    long_description = fh.read()
with open("requirements.txt", "r") as fh:
    requirements = [line.strip() for line in fh]

setup(
    name=f'LXG',
    version=versioneer.get_version(),
    cmdclass={'versioning': versioneer.get_cmdclass()},
    # cmdclass={'build_ext': build_ext, 'versioning': versioneer.get_cmdclass()},
    # ext_modules=ext_modules,
    data_files=_data_files,
    package_data=package_data,
    author="Lerry William",
    author_email="lerryws@sains.com.my",
    description="A Python library to migrate FileGDB to ArcSDE and replicate ArcSDE to FileGDB",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    python_requires='>=3.7',
    install_requires=requirements,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Python Software Foundation License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Scientific/Engineering :: GIS"
        "Topic :: Software Development :: ARCGIS Pro",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ],
)
