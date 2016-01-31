from setuptools import setup, find_packages
from codecs import open  # To use a consistent encoding
from os import path
import json

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except(IOError, ImportError) as e:
    print "Warning:", e
    print "Using pre-made DESCRIPTION.rst"
    long_description = open('DESCRIPTION.rst').read()


def setup_package():

    build_requires = []
    try:
        import numpy
    except ImportError:
        build_requires.append('numpy')

    try:
        import pandas
    except ImportError:
        build_requires.append('pandas')

    with open('csrec/pkg_info.json') as fp:
        _info = json.load(fp)

    metadata = dict(
        name='cold-start-recommender',

        # Versions should comply with PEP440.  For a discussion on single-sourcing
        # the version across setup.py and the project code, see
        # https://packaging.python.org/en/latest/single_source_version.html
        version=_info['version'],
        description='In-memory recommender for recommendations produced on-the-fly',
        long_description=long_description,

        # The project's main homepage.
        url='https://github.com/elegans-io/cold-start-recommender',

        # Author details
        author=_info['author'],
        author_email=_info['email'],

        # Choose your license
        license=_info['license'],

        # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
        classifiers=[
            # How mature is this project? Common values are
            #   3 - Alpha
            #   4 - Beta
            #   5 - Production/Stable
            'Development Status :: 4 - Beta',

            # Indicate who your project is intended for
            'Intended Audience :: Information Technology',
            'Topic :: Scientific/Engineering :: Artificial Intelligence',

            # Pick your license as you wish (should match "license" above)
            'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',

            # Specify the Python versions you support here. In particular, ensure
            # that you indicate whether you support Python 2, Python 3 or both.
            'Programming Language :: Python :: 2.7',
        ],

        # What does your project relate to?
        keywords='recommendations, recommender,recommendation engine',

        # You can just specify the packages manually here if your project is
        # simple. Or you can use find_packages().
        packages=find_packages(exclude=['contrib', 'docs', 'tests*']),

        # List run-time dependencies here.  These will be installed by pip when your
        # project is installed. For an analysis of "install_requires" vs pip's
        # requirements files see:
        # https://packaging.python.org/en/latest/requirements.html
        # See also https://github.com/scipy/scipy/blob/master/setup.py (malemi)
        setup_requires=build_requires,
        install_requires=build_requires,

        # List additional groups of dependencies here (e.g. development dependencies).
        # You can install these using the following syntax, for example:
        # $ pip install -e .[dev,test]
        # extras_require={
        #     'dev': ['check-manifest'],
        #     'test': ['coverage'],
        # },

        # If there are data files included in your packages that need to be
        # installed, specify them here.  If using Python 2.6 or less, then these
        # have to be included in MANIFEST.in as well.
        package_data={
            'csrec': ['*.cl', '*.py', '*.json']
        },

#        data_files=[('config', ['config/csrec.config'])],

        include_package_data=True

        # To provide executable scripts, use entry points in preference to the
        # "scripts" keyword. Entry points provide cross-platform support and allow
        # pip to create the appropriate form of executable for the target platform.
        # entry_points={
        #     'console_scripts': [
        #         'sample=sample:main',
        #     ],
        # },
    )

    setup(**metadata)

if __name__ == '__main__':
    setup_package()
