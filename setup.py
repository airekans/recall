from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(name='recall',
      version=version,
      description="Python High performance RPC framework based on protobuf",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='rpc gevent',
      author='Yaolong Huang',
      author_email='airekans@gmail.com',
      url='https://github.com/airekans/recall',
      license='MIT',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'gevent',
          'protobuf>=2.3'
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
