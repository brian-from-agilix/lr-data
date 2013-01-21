from celery.task import task
from celery.execute import send_task
from celery.log import get_default_logger
log = get_default_logger()
import redis


@task
def emptyValidate(envelope, config):
    for task in config['insertTask']:
        send_task(task, (envelope, config))


@task
def validateLRMI(envelope, config):
    r = config['redis']
    r = redis.StrictRedis(host=r['host'], port=r['port'], db=r['db'])
    r.incr(config['semaphore'])
    if "lrmi" in [x.upper() for x in envelope['keys']]:
        for task in config['insertTask']:
            send_task(task, (envelope, config))
