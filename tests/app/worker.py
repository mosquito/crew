# encoding: utf-8
from crew.worker import context, Task
from time import sleep


@Task('test')
def long_task(req):
    context.settings.counter += 1
    return 'Wake up Neo.\n'


@Task('stat')
def get_counter(req):
    context.settings.counter += 1
    return 'I\'m worker "%s". And I serve %s tasks' % (context.settings.uuid, context.settings.counter)


@Task('dead')
def infinite_loop_task(req):
    while True:
        pass


@Task('publish')
def publish(req):
    context.pubsub.publish('test', req)


@Task('sleep')
def sleeper(rew):
    sleep(3)
    return 1
