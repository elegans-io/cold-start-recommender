from setuptools import setup

# see https://github.com/pypa/sampleproject/blob/master/setup.py

setup(name='cold-start-recommender',
      description='In-memory recommender for recommendations produced on-the-fly',
      keywords='recommendations, recommender,recommendation engine',
      author='elegans.io',
      author_email='info@elegans.io',
      version='0.5.0',
      packages=['csrec'],
      url='https://github.com/elegans-io/cold-start-recommender',
      license='LICENSE.txt',
      install_requires=['pandas', 'numpy'],
      # List additional groups of dependencies.
      # Install these using the following syntax:
      # $ pip install -e .[webapp, mongo]
      data_files=[('config', ['config/csrec.config'])],
      )
