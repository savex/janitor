import glob
import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README')).read()

DATA = [
    ('etc', [f for f in glob.glob(os.path.join('etc', '*'))]),
    ('res', [f for f in glob.glob(os.path.join('res', '*'))])
]

dependencies = [
    'six'
]

entry_points = {
    "console_scripts":
        "sweeper = janitor.sweep:sweeper_cli"
}


setup(
    name="Janitor",
    version="0.1",
    author="Alex Savatieiev",
    author_email="a.savex@gmail.com",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7"
    ],
    keywords="QA, infrastructure, openstack, cleaner",
    entry_points=entry_points,
    url="https://github.com/savex/janitor",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        '': ['*.conf', '*.list', '*.profile']
    },
    zip_safe=False,
    install_requires=dependencies,
    data_files=DATA,
    license="GNU General Public License v3.0",
    description="Janitor is a console util to create profiles "
                "to execute simple dependent chained actions, like "
                "list/delete, list/create and others ",
    long_description=README
)

