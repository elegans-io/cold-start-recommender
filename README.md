****************************************************
Easy, fast, greedy recommender to avoid a cold start
****************************************************
        
"Will it scale?" is a less important question than "will it ever matter?" [kadavy.net]

We developed Cold Start Recommender because we needed a recommender
with the following characteristics:

* **Greedy.** Useful in situations where no previous data on Items or
    Users are available, therefore *any* information must be used
    --not just which Item a User likes, but also --in the case of a
    book-- the corresponding category, author etc.

* **Fast.** Any information on Users and Item should be stored and
    used immediately. A rating by any User should improve
    recommendations for this User, but also for other Users. This
    means  in-memory database and no batch computations.

* **Ready to use.** Take a look at recommender_api.py to start
    a webapp that POSTs information and GETs recommendations.


CSRec should not (yet) be used for production systems, but only for
pilots, where statistics are so low that filters (e.g. loglikelihood
filter on the co-occurence matrix) are premature. It aims to
*gather data* in order to immediately personalise the user experience.

TODO Future releases will include state of the art algorithms (availability planned for
October 2014, a few months before our product goes
public).

CSRec is written in Python, and under the hood it uses the `Pandas`_
library. 

**Table of Contents**

.. contents::
    :local:
    :depth: 1
    :backlinks: none


A simple script
---------------

    from csrec.Recommender import Recommender
    
    engine = Recommender()

    # Insert Item with it properties (e.g. author, category...)
    # NB lists can be passed as json-parseable strings
    engine.insert_item({'_id': 'an_item', 'author': 'The Author', 'tags': '["nice", "good"]'})

    # Insert rating, indicating wich property of the Item should be used for producing recs

    engine.insert_rating(user_id='a_user', item_id='an_item', rating=4, item_info=['author', 'tags'])

    # Insert rating, indicating that only the property should be used for recs (e.g. initial users' profiling)

    engine.insert_rating(user_id='another_user', item_id='an_item', rating=3, item_info=['author'], only_info=True)


Dependencies
============

The following python packages are needed in order to run the recommender:

#unittest
#abc
#pickle
#math
#pandas
#numpy

If you want to run the webservice then you also need:

#webapp2
#webob
#paste

Features
========

Persistence
-----------

You can use CSRec purely in-memory for testing or with MongoDB, which
you can install on a tmpfs filesystem created in your RAM (on Linux,
see
http://edgystuff.tumblr.com/post/49304254688/how-to-use-mongodb-as-a-pure-in-memory-db-redis-style). If using a RAM partition, please make a replica set!

(Why use a replica set? Because you can have the primary DB in
memory, and two other secondaries on disk. If the primary goes down,
you still can use CSRec at lower performances, but without any data
loss.)

Examples:

	engine = Recommender()  # Start in-memory recommender for testing
	
	engine = Recommender(mongo_host='localhost', mongo_db_name='my_cold_rec')  # ...with MongoDB, collections are created automatically
	
	engine = Recommender(mongo_host='localhost', mongo_db_name='my_cold_rec', mongo_replica_set='recommender_replica')  # as above, with replica
	

The Cold Start Problem
----------------------

The Cold Start Problem originates from the fact that collaborative
filtering recommenders need data to build recommendations. Typically,
if Users who liked item 'A' also liked item 'B', the recommender would
recommend 'B' to a user who just liked 'A'. But if you have no
previous rating by any User, you cannot make any recommendations.

CSRec tackles the issue in various ways:

1. It allows profiling with well-known Items without biasing the
results. For instance, if a call to insert_rating is done in this way:

   engine.insert_rating(user_id='another_user', item_id='an_item', rating=3, item_info=['author'], only_info=True)

CSRec will only register that 'another_user' likes a certain author,
but not that s/he might like 'an_item'. This is of fundamental
importance when profiling Users with a "profiling page" on your
website.  If you ask Users whether they prefer "Harry Potter" or "The
Better Angels of Our Nature", and most of them choose Harry Potter, you would not 
want to make the Item "Harry Potter" even more popular. You might just want to record
that those users like children's books marketed as adult literature.

CSRec does that because, unless you are Amazon or a similar brand, the
co-occurence matrix is often too sparse to compute decent
recommendations. In this way you start building multiple, denser,
co-occurence matrices and use them from the very beginning.

2. **Any information is used.** You decide which information you should
record about a User rating an Item. This is similar to the previous
point, but you also register the item_id.

3. **Any information is used *immediately*.** The co-occurence matrix is
updated as soon as a rating is inserted.

4. **It tracks anonymous users,** e.g. random visitors of a website
before the sign in/ sign up process. After sign up/ sign in the
information can be reconciled --information relative to the session ID
is moved into the correspondent user ID entry.

Mix Recommended with Popular Items
----------------------------------

What about users who would only receive a couple of recommended items?
No problem! We'll fill the list with the most popular items that were not
recommended (nor rated by such users).

Algorithms
----------

At the moment CSRec only provides purely item-based recommendations
(co-occurence matrix dot the User's ratings array). In this way we can
provide recommendations in less than 200msec for a matrix of about
10,000 items.


Versions
--------

**v 3.15**
It is now a singleton, improved performance when used with, eg, Pyramid

**v 3.14**
Minor bugs

**v 3.13**

Added self.drop_db

**v 3.12**

* Bug fixed

**v 3.11**

* Some debugs messsages added

**v 3.10**

* Categories can now be a list (or passed as json-parseable string).
  This is important for, eg, tags which can now be passed in a REST API as:

      curl -X POST "http://127.0.0.1:8081/insertitem?id=Boo2&author=TheAuthor&cathegory=Horror&tags=scary,terror"

* Fixed bug in recommender_api example file

**v 3.8**

* Sync categories' users and items collections in get_recommendations

**v 3.7**

Bug fixing for in-memory

**v 3.5**

* Added logging
* Added creation of collections for super-cold start (not even one rating, and still user asking for recommendations...)
* Additional info used for recommendations (eg Authors etc) are now stored in the DB
* _sync_user_item_ratings now syncs addition info's collections too
* popular_items now are always returned, even in case of no rating done, and get_recommendations eventually adjusts the order if some profiling has been done 


.. _Pandas: http://pandas.pydata.org
