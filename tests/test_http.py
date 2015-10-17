# encoding: utf-8
from . import TestBaseHandler
from tornado.testing import gen_test


class TestHttp(TestBaseHandler):
    WORKERS = 1

    @gen_test
    def test_task_test(self):
        response = yield self.client('/')

        self.assertEqual(response.body, 'str: Wake up Neo.\n')
