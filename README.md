# LXG PRO : Migration & Replication

## Build and installation

Install master repo

`pip install -U git+https://github.com/wslerry/LXGPro.git`



Install development repo

`pip install -U git+https://github.com/wslerry/LXGPro.git@dev`





To install as python extension module:

`python setup.py build_ext --inplace`



To install as python module

`python setup.py install`



To save as wheel for pip installation

`python setup.py bdist_wheel`

## Usage

```python
from LXG import Migration, Replication

Migration()
Replication()
```

## Dev

**Versioning**

[ref 1](https://jacobtomlinson.dev/posts/2020/versioning-and-formatting-your-python-code/)

```bash
git add -A
git commit -m "Add versioneer"
git tag 0.0.1
```

