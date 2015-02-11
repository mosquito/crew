# encoding: utf-8
from __future__ import absolute_import, print_function
import sys


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


__version__ = '0.8.7'
__author__ = 'Dmitry Orlov <me@mosquito.su>'


supports = {
    'install_requires': [
        'shortuuid',
    ]
}
if sys.version_info >= (3,):
    supports['install_requires'].append('python3-pika')
else:
    supports['install_requires'].append('pika')

setup(
    name='crew',
    version=__version__,
    author=__author__,
    author_email='me@mosquito.su',
    license="MIT",
    description="AMQP based worker/master pattern framework",
    platforms="all",
    url="http://github.com/mosquito/crew",
    classifiers=[
        'Environment :: Console',
        'Programming Language :: Python',
    ],
    long_description=open('README.rst').read(),
    packages=['crew', 'crew.worker', 'crew.master', 'crew.master.tornado'],
    **supports
)
