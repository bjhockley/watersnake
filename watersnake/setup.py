"""Cython setup file: to build the cython module do:
python setup.py build_ext
cp build/lib.linux-x86_64-2.7/watersnake/membership.so .
"""

from distutils.core import setup
from Cython.Build import cythonize

setup(
    name='SWIM cython app',
    ext_modules=cythonize("membership.pyx"),
)
