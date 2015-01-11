#!/usr/bin/env bash

locust -f test0.py HttpRecommTest --host http://127.0.0.1:8081
#locust -f test0.py HttpRecommTest --host http://127.0.0.1:8000/csrec_w2p/recomm
#locust -f test0.py RecommTest

