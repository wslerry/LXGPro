import sys
import versioneer
try:
    import arcpy
except ImportError:
    sys.exit("lxg_replication requires Arcpy/Arcgispro to be installed")
if sys.version_info < (3, 7):
    sys.exit("doctr requires Python 3.7 or newer")

try:
    import setuptools
    from setuptools import setup
    from setuptools import Extension
except ImportError:
    from distutils.core import setup
    from distutils.extension import Extension

from Cython.Distutils import build_ext


ext_modules = [
    Extension("LXG",  ["replication.py"]),
]

with open("README.md", "r") as fh:
    long_description = fh.read()
with open("requirements.txt", "r") as fh:
    requirements = [line.strip() for line in fh]

setup(
    name='lxg_replication',
    version=versioneer.get_version(),
    cmdclass={'build_ext': build_ext, 'versioning': versioneer.get_cmdclass()},
    ext_modules=ext_modules,
    author="Lerry William",
    author_email="lerryws@sains.com.my",
    description="A Python library to replicate ArcSDE to FileGDB",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=requirements,
)
