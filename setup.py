"""Legacy setup.py so `pip install -e .` works offline on old pip/setuptools.

Metadata lives in setup.cfg. No dependencies (stdlib only).
"""
from setuptools import setup

setup()
