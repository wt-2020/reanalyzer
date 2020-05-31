# -*- coding: utf-8 -*-
import subprocess

from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='reanalyzer',
    version='0.1.0',
    description='Real Estate Analyzer',
    long_description=readme,
    author='wt-2020',
    author_email='wutongjushi2020@gmail.com',
    url='https://github.com/wt-2020/reanalyzer.git',
    license=license,
    packages=find_packages(exclude=('tests', 'docs')),
    entry_points={
        'console_scripts': [
            'reanalyzer = reanalyzer.reanalyzer:main',
        ]
    }
)

subprocess.call(['scripts/dbsetup.py'])