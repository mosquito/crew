from crew.worker.run import run
from crew.worker.listener import context
from time import sleep

def long_task(req):
    context.settings.counter += 1
    return 'Wake up Neo.\n' + (" " * 10000)

def get_counter(req):
    context.settings.counter += 1
    return "I serve %s tasks" % context.settings.counter

def timeout_task(req):
    sleep(60)
    return "I'm still alive."

run(
    handlers={
        'test': long_task,
        'stat': get_counter,
        'dead': timeout_task,
    },
    counter=0   # This is a part of this worker context
)
