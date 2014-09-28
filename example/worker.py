# encoding: utf-8
from crew.worker import run, context, Task
from time import sleep

@Task('test')
def long_task(req):
    context.settings.counter += 1
    return 'Wake up Neo.\n' + (" " * 10000)

@Task('stat')
def get_counter(req):
    context.settings.counter += 1
    return 'I\'m worker "%s". And I serve %s tasks' % (context.settings.uuid, context.settings.counter)

@Task('dead')
def timeout_task(req):
    sleep(60)
    return "I'm still alive."

run(
    counter=0,      # This is a part of this worker context
)
