import random
from csrec import Recommender
from csrec import DALFactory
import math
import numpy as np
import sys
import timeit
import logging

db = DALFactory(name='mem')  # instantiate an in memory database
engine = Recommender(db)

# Monte Carlo:
n_books = 10000
n_users = 10000
n_purchases = 5000
n_authors = 100
n_publishers = 10
authors = ['A'+str(i) for i in range(1, n_authors+1)]
publishers = ['P'+str(i) for i in range(1, n_publishers+1)]


print ("Info: insertion of random generated items: %d" % n_books)
# generate books
for b in range(0, n_books + 1):
    # Author "AnN" is n^2 times more productive than "AN".
    attributes = {'author': authors[int(math.sqrt(random.randrange(0, n_authors)**2))], 'publisher': publishers[int(math.sqrt(random.randrange(0, n_publishers)**2))]}
    db.insert_or_update_item(item_id=str(b), attributes=attributes)

print ("Info: generation and insert of random generated preferences: %d" % n_purchases)
purchase = 0

engine.set_item_info(['author', 'publisher'])
engine.set_only_info(False)

while purchase < n_purchases:
    book_n = np.random.zipf(1.05)
    user_n = np.random.zipf(1.5)
    if book_n <= n_books and user_n <= n_users:
        purchase += 1
        user_id = str(user_n)
        item_id = str(book_n)
        rating = random.randrange(1, 6)
        #print 'user', user_id, 'rated', rating, 'stars item', item_id
        db.insert_or_update_item_action(user_id = user_id, item_id = item_id, rating=3.0)

print ("Info: compute_items_by_popularity")
engine.compute_items_by_popularity()

for i in [1, 10, 100, 1000, 10000]:
    print ("Info: generating recommendations for user: " + str(i))
    print engine.get_recommendations(str(i))

print ("Serialization")
db.serialize(filepath="database.bin")

print ("Restore")
db.restore(filepath="database.bin")

print "End"

