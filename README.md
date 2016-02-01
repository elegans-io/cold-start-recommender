****************************************************
Easy, fast, greedy recommender to avoid a cold start
****************************************************

"Will it scale?" is a less important question than "will it ever matter?" ([David Kadavy](http://kadavy.net))


**!!README!!**

We have re-written good part of the recommender.

The API have changed, and the webapp is now a separate package,
called cold-start-recommender-webapp, which can be installed via
`pip`.

You can still access the old version with:

`pip install cold-start-recommender==0.3.15`

Any comment sent to info@elegans.io will be appreciated.

**END**

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

CSRec is written in Python, and under the hood it uses the `Pandas`_
library. 

**Table of Contents**

.. contents::
    :local:
    :depth: 1
    :backlinks: none


Dependencies
============

The following python packages are needed in order to run the recommender:

* pickle
* pandas
* numpy

Since version 4, the web service has been taken out of the package.
You need to install elegans.io's package [csrec-webapp](https://github.com/elegans-io/csrec-webapp)

Features
========

The Cold Start Problem
----------------------

The Cold Start Problem originates from the fact that collaborative
filtering recommenders need data to build recommendations. Typically,
if Users who liked item 'A' also liked item 'B', the recommender would
recommend 'B' to a user who just liked 'A'. But if you have no
previous rating by any User, you cannot make any recommendation.

CSRec tackles the issue in various ways.

###Selective profiling

CSRec allows **profiling with well-known Items without biasing the results**.

For instance, if a call to insert_rating is done in this way:

   `engine.db.insert_item_action(user_id='user1', item_id='item1', code=4, item_meaningful_info=['author', 'tags'], only_info=True)`

CSRec will only register that `user1` likes a certain author, certain tags,
but not that s/he might like `item1`. This is of fundamental
importance when profiling users through a "profiling page" on your
website.  If you ask users whether they prefer "Harry Potter" or "The
Better Angels of Our Nature", and most of them choose Harry Potter, you would not 
want to make the Item "Harry Potter" even more popular. You might just want to record
that those users like children's books marketed as adult literature.

CSRec does that because, unless you are Amazon or a similar brand, the
co-occurence matrix is often too sparse to compute decent
recommendations. In this way you start building multiple, denser,
co-occurence matrices and use them from the very beginning.

###Store any possible information

Any information is used. You decide which information you should
record about a User rating an Item. This is similar to the previous
point, but you also register the item_id.

###Use everything you can, now

Any information is used *immediately*. The co-occurence matrix is
updated as soon as a rating is inserted.

###Efficient users' tracking

It tracks anonymous users and merges their preferences into profiles. E.g. an anonymous visitors of a website
likes a few items before the sign in/ sign up process. After sign up/ sign in the
information can be reconciled --information relative to the session ID
is moved into the correspondent user ID entry.

###Mix recommended items and popular items

What about users who would only receive a couple of recommendations?
No problem! CSRec will fill the list with the most popular items (nor rated by such users).

###Algorithms

At the moment CSRec only provides purely item-based recommendations
(co-occurence matrix dot the User's ratings array). In this way we can
provide recommendations in less than 200msec for a matrix of about
10,000 items.

A simple script
---------------


```python
from csrec import Recommender
engine = Recommender()

# Insert items with their properties (e.g. author, tags...)
# NB lists can be passed as json-parseable strings or strings
engine.db.insert_item(item_id='item1', attributes={'author': 'Author A', 'tags': '["nice", "good", "new"]'})

# The author field is a list, even if it was passed as a simple string:
assert engine.db.items_tbl['item1']['author'] ==  ['Author A']

engine.db.insert_item(item_id='item2', attributes={'author': '["Author B", "Author Z"]', 'tags': '["nice", "fair"]'})
engine.db.insert_item(item_id='item3', attributes={'author': 'Author B', 'tags': '["nice", "good"]'})
engine.db.insert_item(item_id='item4', attributes={'author': 'Author C', 'tags': '["new", "fashion"]'})

# The following lines tell the recommender that user1 likes items 1 and 2 but also "Author A", "B", "Z"
# and tags "nice", "good" and "fair"

engine.db.insert_item_action(user_id='user1', item_id='item1', code=4, item_meaningful_info=['author', 'tags'])
engine.db.insert_item_action(user_id='user1', item_id='item2', code=5, item_meaningful_info=['author', 'tags'])

# user1 has given a total of 4 points to Author A, 5 to Author B and Z, 4 to tag good, 5 to fair, and 9 to nice:
assert engine.db.tot_categories_user_ratings == {'author': {'user1': {'Author A': 4, 'Author B': 5, 'Author Z': 5}},
'tags': {'user1': {'fair': 5, 'good': 4, 'new': 4, 'nice': 9}}}

# ...and user2 likes item3, "Author B", "nice" and "good" items:
engine.db.insert_item_action(user_id='user2', item_id='item3', code=5, item_meaningful_info=['author', 'tags'])

# ...and user3 likes item4, "Author C", but we give no information about the tag!
engine.db.insert_item_action(user_id='user3', item_id='item4', code=5, item_meaningful_info=['author'])

# ...and user4 only goes through the profiling page, and say she likes books tagged as 'new' and 'fashion'
engine.db.insert_item_action(user_id='user4', item_id='item4', code=5, item_meaningful_info=['tags'], only_info=True)

# We should recommend to user1 items 3 and then 4, etc etc
assert engine.get_recommendations('user1') == ['item3', 'item4']

# 'user2' signs in and we discover that it's 'user1' who was browsing anonymously
engine.db.reconcile_user('user2', 'user1')

# now we know user1 liked item1, 2, 3
assert engine.db.users_ratings_tbl['user1'] == {'item1': 4, 'item2': 5, 'item3': 5}

# so we can only recommend item4
assert engine.get_recommendations('user1') == ['item4']
```

Remember that the cold start recommender is now only in memory, which means that you must implement a
 periodic saving of the data:

```python
# Save the data from the engine from above
engine.db.serialize('pippo.db')

# create a new engine with the same data:
new_engine = Recommender()
new_engine.db.restore('pippo.db')
```


Versions
--------
**v 4.1 No backward compatibility with 3**

* Action of users on users can be saved (see `insert_social_action` in dal.py)
* Various new metrics to monitor users' interaction (see e.g. `get_social_actions` in dal.py)
* No more embedded web service: use [csrec-webapp](https://github.com/elegans-io/csrec-webapp)
* TODO: make "social" recommendations based on users saving actions on each other
* Heavy refactoring
* Serialization and de-serialization of the data in a file for backup

**v 4.0**

* Data Abstraction Layers for memory and mongo.
* NB Not compatible with 3.15

**v 3.15**

* It is now a singleton, improved performance when used with, eg, Pyramid

**v 3.14**

* Minor bugs

**v 3.13**

* Added self.drop_db

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

* Bug fixing for in-memory

**v 3.5**

* Added logging
* Added creation of collections for super-cold start (not even one rating, and still user asking for recommendations...)
* Additional info used for recommendations (eg Authors etc) are now stored in the DB
* _sync_user_item_ratings now syncs addition info's collections too
* popular_items now are always returned, even in case of no rating done, and get_recommendations eventually adjusts the order if some profiling has been done 


.. _Pandas: http://pandas.pydata.org
