from locust import HttpLocust, Locust, TaskSet, task
import random
import json
import logging

#random.seed(0)

class HttpRecommTaskSet(TaskSet):
    min_wait = 2000
    max_wait = 5000

    @task(5)
    def insert_items(self):
        """
        insert item and rating
        :return:
        """
        item_num = random.randint(0, 10000)
        author_num = random.randint(0, 10000)
        publisher_num = random.randint(0, 100000)

        user_rating_num = random.randint(0, 6)
        user_to_rating_num = random.randint(0, 6)

        item_name = 'item_' + str(item_num)
        item_author = 'author_' + str(author_num)
        publisher = 'publisher_' + str(publisher_num)

        user_id = 'user_' + str(random.randint(0, 10000))

        user_to_id = 'user_' + str(random.randint(0, 10000))

        self.client.post("/insertitems?unique_id=id",
             data=json.dumps([{'id': item_name, 'author': item_author, 'publisher': publisher}],
                             separators=(',', ':')))

        self.client.post("/itemaction", data={'item': item_name, 'user': user_id, 'code': user_rating_num})

        self.client.post("/socialaction", data={'user_to': user_to_id, 'user': user_id, 'code': user_to_rating_num})

        #response = self.client.get("/recommend",data={'user':user_id, 'max_recs':10, 'fast':True})
        #print "Response status code:", response.status_code
        #print "Response content:", response.content

    @task(20)
    def get_item(self):
        item_num = random.randint(0, 10000)
        item_id = 'item_' + str(item_num)
        response = self.client.get("/item", data={'item': item_id})

    @task(20)
    def get_info(self):
        user_num = random.randint(0, 10000)
        user_id = 'item_' + str(user_num)
        response = self.client.get("/info", data={'user': user_id})

    @task(20)
    def recommend_fast(self):
        user_num = random.randint(0, 10000)
        user_id = 'user_' + str(user_num)
        response = self.client.get("/recommend", data={'user': user_id, 'max_recs': 10, 'fast': True})

    @task(20)
    def recommend_slow(self):
        user_num = random.randint(0,10000)
        user_id = 'user_' + str(user_num)
        response = self.client.get("/recommend", data={'user': user_id, 'max_recs': 10, 'fast': False})

class HttpRecommTest(HttpLocust):
    task_set = HttpRecommTaskSet

