#!/usr/bin/env python

import webapp2
from csrec import Recommender
from csrec import DALFactory
import csrec
import json
import argparse
from paste import httpserver

"""
Usage:
python recommender_api.py

Start a webapp for testing the recommender.

"""

db = DALFactory(name='mem', params={})  # instantiate an in memory database
engine = Recommender(db, log_level=True)


class MainPage(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write("Cold Start Recommender v. " + str(csrec.__version__) + "\n")


class UpdateItems(webapp2.RequestHandler):
    """
    Update (or insert) item. The unique_id must be given as param
    e.g.:
    curl -X POST -H "Content-Type: application/json" -d '[{ "_id" : "123", "type": "lady", "category" : "romance"}, { "_id" : "124", "type": "male", "category" : "hardcore"}]' 'http://localhost:8081/updateitems?unique_id=_id'
    """
    def post(self):
        items = json.loads(self.request.body)
        item_id = self.request.params['unique_id']
        for i in items:
            db.insert_or_update_item(item_id=i[item_id], attributes=i)


class ItemAction(webapp2.RequestHandler):
    """
    e.g.:
    curl -X POST  'http://localhost:8081/itemaction?item=Book1&user=User1&code=4'
    """
    def post(self):
        db.insert_or_update_item_action(
            user_id=self.request.get('user'),
            item_id=self.request.get('item'),
            code=float(self.request.get('code'))
        )


class SocialAction(webapp2.RequestHandler):
    """
    e.g.:
    curl -X POST  'http://localhost:8081/socialaction?user=User1&user_to=User2&code=4'
    """
    def post(self):
        db.insert_or_update_social_action(
            user_id=self.request.get('user'),
            user_id_to=self.request.get('user_to'),
            code=float(self.request.get('code'))
        )


class GetItem(webapp2.RequestHandler):
    """
    curl -X GET  'http://localhost:8081/item?id=Book1'
    """
    def get(self):
        item_id = self.request.get('id')
        item_record = db.get_item(item_id=item_id)
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
            old_user_id=old_user_id,
            new_user_id=new_user_id
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
    ('/itemaction', ItemAction),
    ('/updateitems', UpdateItems),
    ('/socialaction', SocialAction),
    ('/item', GetItem),
    ('/recommend', Recommend),
    ('/reconcile', Reconcile),
    ('/info', Info)
], debug=False)


def main():
    parser = argparse.ArgumentParser(description="Launch a webapp with the recommender. Only in-memory for now")
    parser.add_argument('--port', type=str, default='8081', help='Port, default 8081')
    parser.add_argument('--host', type=str, default='localhost', help='hostname, default localhost')
    parser.add_argument('--debug', action='store_true', help='Print debug info')
    #TODO parser.add_argument('-d', '--db', type=str, default='mem')
    parser.parse_args()
    args = parser.parse_args()
    httpserver.serve(app, host=args.host, port=args.port, use_threadpool=True, threadpool_workers=10)

if __name__ == '__main__':
    main()
