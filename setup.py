#!/usr/bin/env python
# encoding: utf-8

__version__ = "1.0"

# $Source$
from sys import version
import os
from setuptools import setup

requires=[]

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='pymc',
    version=__version__,
    description='Python interface to Memcached.',
    long_description=read('README.md'),
    author='Tyler Williams',
    author_email='williams.tyler@echonest.com',
    maintainer='Tyler Williams',
    maintainer_email='williams.tyler@echonest.com',
    url='https://github.com/tylerwilliams/pymc',
    download_url='https://github.com/tylerwilliams/pymc',
    package_dir={'pymc':'pymc'},
    packages=['pymc'],
    requires=requires
)
