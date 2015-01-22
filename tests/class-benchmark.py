import random
from csrec.Recommender import Recommender
import math
import numpy as np
import time

engine = Recommender(mongo_host="localhost:27017", mongo_db_name="csrec", log_level=30)

print "Engine created"

engine.insert_item({'_id': 'an_item', 'author': 'The Author', 'tags': '["nice", "good"]'})

engine.drop_db()

# Montecarlo:
n_books = 1000
n_users = 1000
n_purchases = 10000
n_authors = 50
n_publishers = 10
authors = ['A'+str(i) for i in range(1, n_authors+1)]
publishers = ['P'+str(i) for i in range(1, n_publishers+1)]
# generate books
for b in range(0, n_books + 1):
    # Author "AnN" is n^2 times more productive than "AN".
    book = {'uid': 'b'+str(b), 'author': authors[int(math.sqrt(random.randrange(0, n_authors)**2))], 'publisher': publishers[int(math.sqrt(random.randrange(0, n_publishers)**2))]}
    engine.insert_item(book, _id='uid')

purchase = 0
while(purchase < n_purchases):
    book_n = np.random.zipf(1.05)
    user_n = np.random.zipf(1.5)
    if book_n <= n_books and user_n <= n_users:
        purchase += 1
        user_id = 'u'+str(user_n)
        item_id = 'b'+str(book_n)
        rating = random.randrange(1, 6)
        #print 'user', user_id, 'rated', rating, 'stars item', item_id
        engine.insert_rating(user_id=user_id, item_id=item_id, rating=3, item_info=['author', 'publisher'], only_info=False)

start = time.time()
for i in xrange(n_users):
    engine.get_recommendations("u"+str(i))
end = time.time()

av = (end - start) / n_users

print "Average time, normal:", av

start = time.time()
for i in xrange(n_users):
    engine.get_recommendations("u"+str(i), fast=True)
end = time.time()

av = (end - start) / n_users

print "Average time, fast:", av
