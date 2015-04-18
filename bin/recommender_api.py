#!/usr/bin/env python

import webapp2
from csrec.Recommender import Recommender
from csrec.DALFactory import DALFactory
import csrec

"""
Usage:
python recommender_api.py

Start a webapp for testing the recommender.

"""

db = DALFactory(name='mem', params = {}) # instantiate an in memory database
engine = Recommender(db)

class MainPage(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write("Cold Start Recommender v. " + str(csrec.__version__) + "\n")


class InsertRating(webapp2.RequestHandler):
    """
    e.g.:
    curl -X POST  'localhost:8081/insertrating?item=Book1&user=User1&rating=4'
    """
    def post(self):
        db.insert_or_update_item_rating(
            user_id = self.request.get('user'),
            item_id = self.request.get('item'),
            rating = float(self.request.get('rating'))
        )

class InsertItem(webapp2.RequestHandler):
    """
    e.g.:
    curl -X POST  'localhost:8081/insertitem?id=Book1&author=TheAuthor&cathegory=Horror&tags=scary,terror'
    """
    def post(self):
        item_fields = {}
        for i in self.request.params.items():
            item_fields[i[0]] = i[1]

        item_id = item_fields['id']
        del item_fields['id']
        db.insert_or_update_item(item_id = item_id, attributes = item_fields)

class GetItems(webapp2.RequestHandler):
    """
    curl -X GET  'localhost:8081/items?id=Book1'
    """
    def get(self):
        item_id = self.request.get('id')
        item_record = db.get_item(item_id = item_id)
        self.response.write(item_record)

class Recommend(webapp2.RequestHandler):
    """
    curl -X GET  'localhost:8081/recommend?user=User1&max_recs=10&fast=False'
    """
    def get(self):
        user = self.request.get('user')
        max_recs = self.request.get('max_recs', 10)
        fast = self.request.get('fast', False)
        recomms = engine.get_recommendations(user, max_recs=max_recs, fast=fast)
        self.response.write(recomms)


class Reconcile(webapp2.RequestHandler):
    def post(self):
        old_user_id = self.request.get('old')
        new_user_id = self.request.get('new')
        db.reconcile_user(
            old_user_id = old_user_id,
            new_user_id = new_user_id
        )

class Info(webapp2.RequestHandler):
    """
    curl -X GET  'localhost:8081/info?user=User1'
    """
    def get(self):
        user_id = self.request.get('user')
        recomms = db.get_user_ratings(user_id=user_id)
        self.response.write(recomms)


app = webapp2.WSGIApplication([
    ('/', MainPage),
    ('/insertrating', InsertRating),
    ('/insertitem', InsertItem),
    ('/items', GetItems),
    ('/recommend', Recommend),
    ('/reconcile', Reconcile),
    ('/info', Info)
], debug=False)


def main():
    from paste import httpserver
    httpserver.serve(app, host='127.0.0.1', port='8081', use_threadpool=True, threadpool_workers=10)

if __name__ == '__main__':
    main()
