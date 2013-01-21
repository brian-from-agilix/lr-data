import pyes
from celery.task import task
from celery.log import get_default_logger
import redis
log = get_default_logger()


@task
def insertDocumentElasticSearch(envelope, config):
        r = config['redis']
        r = redis.StrictRedis(host=r['host'], port=r['port'], db=r['db'])
        conf = config['elasticsearch']
        es = pyes.ES("{0}:{1}".format(conf['host'], conf['port']))
        r.decr(config['semaphore'])
        for item in envelope['resource_data']['items']:
            count = r.incr('esid')
            es.index(item, conf['index'], conf['index-type'], count)


@task
def insertDocumentLRB(envelope, config):
    pass
