import random
from csrec.Recommender import Recommender
from csrec.DALFactory import DALFactory
import math
import numpy as np
import sys
import timeit
import logging

print "Creating http service"
service_host='localhost'
service_port=27017

print ("Info: starting recommender service on %s:%d" % (service_host, service_port))
db = DALFactory(name='mem', params = {}) # instantiate an in memory database
engine = Recommender(db)

# Montecarlo:
n_books = 1000
n_users = 1000
n_purchases = 10000
n_authors = 100
n_publishers = 10
authors = ['A'+str(i) for i in range(1, n_authors+1)]
publishers = ['P'+str(i) for i in range(1, n_publishers+1)]


print ("Info: insertion of random generated items: %d" % (n_books))
# generate books
for b in range(0, n_books + 1):
    # Author "AnN" is n^2 times more productive than "AN".
    attributes = {'author': authors[int(math.sqrt(random.randrange(0, n_authors)**2))], 'publisher': publishers[int(math.sqrt(random.randrange(0, n_publishers)**2))]}
    db.insert_or_update_item( item_id=str(b), attributes=attributes)

print ("Info: generation and insert of random generated preferences: %d" % (n_purchases))
purchase = 0
while(purchase < n_purchases):
    book_n = np.random.zipf(1.05)
    user_n = np.random.zipf(1.5)
    if book_n <= n_books and user_n <= n_users:
        purchase += 1
        user_id = str(user_n)
        item_id = str(book_n)
        rating = random.randrange(1, 6)
        #print 'user', user_id, 'rated', rating, 'stars item', item_id
        engine.insert_rating(user_id=user_id, item_id=item_id, rating=3, item_info=['author', 'publisher'], only_info=False)

engine.get_recommendations('1')

engine.compute_items_by_popularity()
print engine.items_by_popularity

print "End"

