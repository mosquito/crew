#!/usr/bin/env python
# encoding: utf-8
import os
import time
from subprocess import Popen, PIPE
import socket
import requests
import threading
import uuid

PROJECT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'example')

def get_free_tcp_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("",0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port

PORT = get_free_tcp_port()

os.chdir(PROJECT_ROOT)
print ("Running master")
master_process = Popen(['python', 'master.py', '--port={0}'.format(PORT)], stdin=PIPE, stderr=PIPE, stdout=PIPE)
print ("Running worker")
worker_process = Popen(['python', 'worker.py'], stdin=PIPE, stderr=PIPE, stdout=PIPE)

alive = False
for i in range(20):
    try:
        conn = socket.socket()
        conn.connect(('127.0.0.1', PORT))
        alive = True
    except:
        time.sleep(1)

assert alive

time.sleep(5)

class TestCrew(object):
    ADDRESS = '127.0.0.1'
    PORT = PORT
    result = None

    def setUp(self):
        self.multiplier = 1

    def _http_get(self, uri):
        assert uri.startswith("/")
        try:
            return requests.get("http://{0}:{1}{2}".format(self.ADDRESS, self.PORT, uri), timeout=10).text
        except requests.HTTPError as e:
            print ("HTTP ERROR:", e.response)
            raise

    def _http_post(self, uri, data):
        assert uri.startswith("/")
        return requests.post("http://{0}:{1}{2}".format(self.ADDRESS, self.PORT, uri), str(data), timeout=10).text

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
            self.lock = False

        self.lock = True
        threading.Thread(target=thread_inner).start()

        uid = str(uuid.uuid4())
        self._http_post('/publish', uid)

        while self.lock:
            time.sleep(0.5)

        print (self.result, '==', uid)
        assert self.result == uid

    def test_07_publish2(self):
        def thread_inner():
            self.result = self._http_get("/subscribe")
            self.lock = False

        threading.Thread(target=thread_inner).start()

        uid = str(uuid.uuid4())
        self._http_post('/publish2', uid)

        while self.lock:
            time.sleep(0.5)

        print (self.result, '==', uid)
        assert self.result == uid
