# LXG PRO : Migration & Replication

## Build and installation

`python setup.py build_ext --inplace`

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
