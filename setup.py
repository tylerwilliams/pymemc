#!/usr/bin/env python
# encoding: utf-8

__version__ = "1.1"

# $Source$
from sys import version
import os
from setuptools import setup

requires=[]

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='pymemc',
    version=__version__,
    description='Python interface to Memcached.',
    long_description=read('README.md'),
    author='Tyler Williams',
    author_email='williams.tyler@gmail.com',
    maintainer='Tyler Williams',
    maintainer_email='williams.tyler@gmail.com',
    url='https://github.com/tylerwilliams/pymemc',
    download_url='https://github.com/tylerwilliams/pymemc',
    package_dir={'pymemc':'pymemc'},
    packages=['pymemc'],
    requires=requires
)
