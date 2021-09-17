#!/usr/bin/env python3

from distutils.core import setup
from catkin_pkg.python_setup import generate_distutils_setup

d = generate_distutils_setup(
    packages=['migrave_knowledge_base'],
    package_dir={'migrave_knowledge_base': 'common/src/migrave_knowledge_base'}
)

setup(**d)
