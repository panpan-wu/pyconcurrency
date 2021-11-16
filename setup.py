import os

from setuptools import setup


here = os.path.dirname(os.path.abspath(__file__))
about = {}
with open(os.path.join(here, "pyconcurrency", "__version__.py")) as f:
    exec(f.read(), about)


setup(
    name="pyconcurrency",
    version=about["__version__"],
    url="https://github.com/panpan-wu/pyconcurrency",
    author="Panpan Wu",
    author_email="wupanpan8@163.com",
    license="Apache Software License",
    packages=["pyconcurrency"],
)
