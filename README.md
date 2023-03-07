# LXG PRO : Migration & Replication

## Build and installation

Install master repo

`pip install -U git+https://github.com/wslerry/LXGPro.git`



Install development repo

`pip install -U git+https://github.com/wslerry/LXGPro.git@dev`



To install from repo

`git clone https://github.com/wslerry/LXGPro.git`

`cd LXGPro`

then follow as below as your choice of installation.

To install as python extension module:

`python setup.py build_ext --inplace`



To install as python module

`python setup.py install`



To save as wheel for pip installation

`python setup.py bdist_wheel`

## Usage

1. For migration or replication

    ```python
    from LXG import Migration, Replication
    
    Migration()
    Replication()
    ```


2. For detecting changes of two different version of database

   ```python
   as
   ```

   

## Dev

**Versioning**

[ref 1](https://jacobtomlinson.dev/posts/2020/versioning-and-formatting-your-python-code/)

```bash
git add -A
git commit -m "Add versioneer"
git tag 0.0.1
```

