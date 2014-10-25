#!/usr/bin/env python
# encoding: utf-8
import os
import time
from subprocess import Popen, PIPE
import socket
import requests
import thread
import uuid

PROJECT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'example')

os.chdir(PROJECT_ROOT)
print ("Running master")
master_process = Popen(['python', 'master.py'], stdin=PIPE, stderr=PIPE, stdout=PIPE)
print ("Running worker")
worker_process = Popen(['python', 'worker.py'], stdin=PIPE, stderr=PIPE, stdout=PIPE)


class TestCrew(object):
    ADDRESS = '127.0.0.1'
    PORT = 8888
    ALIVE = False
    result = None

    def setUp(self):
        self.multiplier = 1

    def _make_socket(self, address='127.0.0.1', port=8888):
        conn = socket.socket()
        conn.connect((address, port))
        return conn

    def _http_get(self, uri):
        assert uri.startswith("/")
        return requests.get("http://{0}:{1}{2}".format(self.ADDRESS, self.PORT, uri)).text

    def _http_post(self, uri, data):
        assert uri.startswith("/")
        return requests.post("http://{0}:{1}{2}".format(self.ADDRESS, self.PORT, uri), str(data)).text

    def test_00_run(self):
        for i in range(5):
            try:
                self._make_socket()
                self.ALIVE = True
            except:
                time.sleep(1)

        assert self.ALIVE

    def test_01_root(self):
        assert "Wake up Neo" in self._http_get('/')

    def test_02_dead(self):
        data = self._http_get('/fast')
        assert "Timeout" in data or "All workers are gone" in data

    def test_03_stat(self):
        assert "I'm worker" in self._http_get('/stat')

    def test_04_stat2(self):
        assert "I'm worker" in self._http_get('/stat2')

    def test_05_parallel(self):
        data = self._http_get('/parallel')
        assert "I'm worker" in data and "Wake up Neo" in data

    def test_06_publish(self):
        def thread_inner():
            self.result = self._http_get("/subscribe")
            print ("Got result:", self.result)

        thread.start_new_thread(thread_inner, ())

        uid = str(uuid.uuid4())
        assert self._http_post('/publish', uid) == 'None'
        assert self.result == uid
        self.result = None

    def test_07_publish2(self):
        def thread_inner():
            self.result = self._http_get("/subscribe")
            print ("Got result:", self.result)

        thread.start_new_thread(thread_inner, ())

        uid = str(uuid.uuid4())
        self._http_post('/publish2', uid)
        time.sleep(1)
        assert self.result == uid
        self.result = None