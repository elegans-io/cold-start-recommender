from setuptools import setup

# see https://github.com/pypa/sampleproject/blob/master/setup.py

setup(name='cold-start-recommender',
      description='In-memory recommender for recommendations produced on-the-fly',
      keywords='recommendations, recommender,recommendation engine',
      author='elegans.io',
      author_email='elegans.io Ltd',
      version='0.4.0',
      packages=['csrec'],
      url='https://github.com/elegans-io/cold-start-recommender',
      license='LICENSE.txt',
      scripts=['bin/recommender_api.py'],
      install_requires=['pandas', 'numpy'],
      # List additional groups of dependencies.
      # Install these using the following syntax:
      # $ pip install -e .[webapp, mongo]
      extras_require={
          'webapp': ['webapp2', 'paste']
      },
      data_files=[('config', ['config/csrec.config'])],
      )
