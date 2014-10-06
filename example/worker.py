# encoding: utf-8
from crew.worker import run, context, Task


@Task('test')
def long_task(req):
    context.settings.counter += 1
    return 'Wake up Neo.\n' + (" " * 10000)


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

run(
    counter=0,      # This is a part of this worker context
)
