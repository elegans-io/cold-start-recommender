from distutils.core import setup

setup(name='cold-start-recommender',
      description='In-memory recommender for recommendations produced on-the-fly',
      author='elegans.io',
      author_email='elegans.io Ltd',
      version='0.4.0',
      py_modules=['csrec.Recommender', 'tools.Singleton'],
      url='https://github.com/elegans-io/cold-start-recommender',
      license='LICENSE.txt',
      scripts=['bin/recommender_api.py'],
      )
