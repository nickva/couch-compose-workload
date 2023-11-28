#!/usr/bin/env python
#
# ./workload.py -x 2 -n 10 -t 2 -w 10

import copy
import sys
import time
import threading
import os
import argparse
import uuid
import traceback
import random

import requests
from requests.exceptions import HTTPError
from requests.adapters import HTTPAdapter, Retry

import multiprocessing as mp
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool

BATCH = 1000
VIEW_BATCH = 500

# Revs limit will be lowered and increased between these values
MIN_REVS_LIMIT=900
MAX_REVS_LIMIT=1000

# Q the default is 2
Q = 8
# When using a cluster N=#nodes
N = 3

USER='adm'
PASS='pass'
VIEW = 'v1'
WITH_REDUCE = False
TIMEOUT = 60
REQUESTS_RETRY_COUNT = 9
REQUESTS_RETRY_CODES = [500, 502, 503, 504]
REQUESTS_RETRY_BACKOFF_FACTOR = 0.1

CONFIG = {
    'couchdb':{
        'max_dbs_open': '5000'
    },
    'cluster':{
        'q': str(Q),
        'n': str(N)
    },
    'log': {
        'level': 'notice'
    },
    'query_server_config': {
        'os_process_timeout': 'infinity',
        'os_process_limit': '500'
    }
}


SETUP_NODES = [
    'couchdb@10.0.0.11',
    'couchdb@10.0.0.12',
    'couchdb@10.0.0.13'
]

DB_URLS = [
    'http://10.0.0.11:5984',
    'http://10.0.0.12:5984',
    'http://10.0.0.13:5984'
]

DB_NAME = 'db'

def log(*args):
    sargs = []
    for a in args:
        try:
            sargs.append(str(a))
        except:
            sargs.append('?')
    msg = " ".join(sargs)
    sys.stderr.write(msg + '\n')
    sys.stderr.flush()


def pick_server(urls):
    if isinstance(urls, list):
        return random.choice(urls)
    return urls


def get_design_doc(docnum, key):
    ddoc = {
        "_id": "_design/%s" % docnum,
        "autoupdate": False,
        "views": {VIEW: {"map": 'function(d){emit(1,1);}'}}
    }
    if WITH_REDUCE:
        ddoc['views'][VIEW]['reduce'] = '''
             function(keys, values, rereduce) {
                 if (rereduce) {
                    return sum(values);
                 } else {
                    return 2 * values.length;
                 }
              }
        '''
    return ddoc


class Server:

    def __init__(self, args, url, timeout=TIMEOUT):
        self.sess = requests.Session()
        retries = Retry(
            total = REQUESTS_RETRY_COUNT,
            backoff_factor = REQUESTS_RETRY_BACKOFF_FACTOR,
            status_forcelist = REQUESTS_RETRY_CODES
        )
        self.sess.mount('http://', HTTPAdapter(max_retries = retries))
        self.sess.auth = args.auth
        self.url = url.rstrip('/')
        self.timeout = timeout

    def _apply_timeout(self, kw):
        if self.timeout is not None and 'timeout' not in kw:
            kw['timeout'] = self.timeout
        return kw

    def get(self, path = '', **kw):
        kw = self._apply_timeout(kw)
        r = self.sess.get(f'{self.url}/{path}', **kw)
        r.raise_for_status()
        return r.json()

    def post(self, path, **kw):
        kw = self._apply_timeout(kw)
        r = self.sess.post(f'{self.url}/{path}', **kw)
        r.raise_for_status()
        return r.json()

    def put(self, path, **kw):
        kw = self._apply_timeout(kw)
        r = self.sess.put(f'{self.url}/{path}', **kw)
        r.raise_for_status()
        return r.json()

    def delete(self, path, **kw):
        kw = self._apply_timeout(kw)
        r = self.sess.delete(f'{self.url}/{path}', **kw)
        r.raise_for_status()
        return r.json()

    def head(self, path, **kw):
        kw = self._apply_timeout(kw)
        r = self.sess.head(f'{self.url}/{path}', **kw)
        return r.status_code

    def version(self):
        return self.get()['version']

    def membership(self):
        return self.get('_membership')

    def cluster_setup(self, req):
        return self.post('_cluster_setup', json = req)

    def create_db(self, dbname):
        if dbname not in self:
            try:
                self.put(dbname, timeout=TIMEOUT)
            except HTTPError as err:
                response = err.response
                if not response:
                    Exception(f"{dbname} could not be created")
                if response.status_code == 412:
                    log(f" -> {dbname} PUT returned a 412. DB is already created")
                    return True
                raise err
        if dbname not in self:
            raise Exception(f"{dbname} could not be created")
        else:
            return True

    def bulk_docs(self, dbname, docs, timeout=TIMEOUT):
        return self.post(f'{dbname}/_bulk_docs', json = {'docs': docs})

    def bulk_get(self, dbname, docs, timeout=TIMEOUT):
        return self.post(f'{dbname}/_bulk_get', json = {'docs': docs})

    def compact(self, dbname, **kw):
        r = self.sess.post(f'{self.url}/{dbname}/_compact', json = {},  **kw)
        r.raise_for_status()
        return r.json()

    def set_revs_limit(self, dbname, revs_limit):
        if not isinstance(revs_limit, str):
            revs_limit = str(revs_limit)
        r = self.sess.put(f'{self.url}/{dbname}/_revs_limit', data = revs_limit)
        r.raise_for_status()
        return r.json()

    def get_revs_limit(self, dbname):
        r = self.sess.get(f'{self.url}/{dbname}/_revs_limit')
        r.raise_for_status()
        return int(r.json())

    def config_set(self, section, key, val):
        url = f'_node/_local/_config/{section}/{key}'
        return self.put(url, data='"'+val+'"')

    def config_get(self, section, key):
        url = f'_node/_local/_config/{section}/{key}'
        return self.get(url)

    def __iter__(self):
        dbs = self.get('_all_dbs')
        return iter(dbs)

    def __str__(self):
        return "<Server:%s>" % self.url

    def __contains__(self, dbname):
        res = self.head(dbname)
        if res == 200:
            return True
        if res == 404:
            return False
        raise Exception(f"Unexpected head status code {res}")

def execute_query_fun(args, dbname, ddoc, i, srv):
    params = {'limit': str(VIEW_BATCH)}
    try:
        res = srv.get(f'{dbname}/_design/{ddoc}/_view/{VIEW}', params=params)
        count = len(res['rows'])
        log(" **** %s/%s rows: %s" % (dbname, ddoc, count))
    except HTTPError as e:
        log(" ***** %s/%s view call failed with :%s" % (dbname, ddoc, e))

def execute_create_fun(args, pid, tid, i, srv):
    dbname = "%s_%s_%s_%s" % (args.dbname, pid, tid, i)
    revs_limit = MAX_REVS_LIMIT
    num_docs = args.num_docs
    srv.create_db(dbname)
    update_revs_limit(srv, dbname, revs_limit)
    key = [pid, tid, i]
    doc_id = i * num_docs
    all_docs = {}
    ddocs = [get_design_doc(docnum, key) for docnum in range(args.design_docs)]
    bulk_docs_with_retry(srv, dbname, ddocs)
    batches = num_docs // BATCH
    for b in range(batches):
        (doc_id, docs) = generate_docs(doc_id, BATCH, all_docs)
        bulk_docs_with_retry(srv, dbname, docs)
        if args.compact:
            srv.compact(dbname)
        revs_limit = update_revs_limit(srv, dbname, revs_limit)
        log(f" -> batch pid:{pid} tid:{tid} i:{i} batch:{b} revs_limit:{revs_limit}")
    left = num_docs - batches * BATCH
    (doc_id, docs) = generate_docs(doc_id, left, all_docs)
    bulk_docs_with_retry(srv, dbname, docs)
    verify_docs(srv, dbname, all_docs)
    if args.compact:
        srv.compact(dbname)

def bulk_docs_with_retry(srv, dbname, docs):
    try:
        r = srv.bulk_docs(dbname, docs)
        r.raise_for_status()
    except HTTPError as err:
        response = err.response
        if response and response.status_code == 500:
            # We expect a {{badmatch,{error,timeout}},[{ddoc_cache_entry_validation_funs,recover,..}
            # so we try to mask that
            log(f" -> retrying _bulk_docs due to a 500 error {dbname} {len(docs)} {response}")
            time.sleep(1.0)
            return bulk_docs_with_retry(srv, dbname, docs)
        raise err

def update_revs_limit(srv, dbname, revs_limit):
    cur_revs_limit = srv.get_revs_limit(dbname)
    if cur_revs_limit != revs_limit:
        Exception(f" !!! Error revs_limit not persisted {cur_revs_limit} != {revs_limit}")
    if revs_limit >= MAX_REVS_LIMIT:
        revs_limit = MIN_REVS_LIMIT
    else:
        revs_limit += 1
    srv.set_revs_limit(dbname, revs_limit)
    return revs_limit

def generate_docs(doc_id, batch, all_docs):
    docs = []
    for i in range(batch):
        doc_id += 1
        doc = generate_doc(doc_id)
        docs.append(doc)
        all_docs[doc['_id']] = doc
    return (doc_id, docs)

def generate_doc(doc_id):
    doc_id_str = '%012d' % doc_id
    return {
        '_id': doc_id_str,
        'data': doc_id
    }

def verify_docs(srv, dbname, all_docs):
    batch_size = 0
    batch = {}
    for doc_id in all_docs:
        doc = all_docs[doc_id]
        batch[doc_id] = doc
        if len(batch) >= BATCH:
            verify_batch(srv, dbname, batch)
            batch = {}
    verify_batch(srv, dbname, batch)

def verify_batch(srv, dbname, docs, retries = 100, sleep = 2.0):
    getdocs = [{'id': doc['_id']} for doc in docs.values()]
    res = srv.bulk_get(dbname, getdocs)
    if 'results' not in res:
        raise Exception("!!! Invalid _bulk_ response no 'results' ***")
    results = res['results']
    # row example might be:
    #  {
    #    'id': '000000000001',
    #    'docs': [
    #       {'ok': {'_id': '000000000001', '_rev': '1-13..', 'data': 1}}
    #     ]
    #  }
    retry_docs = {}
    for row in results:
        row_id = row['id']
        if 'docs' not in row:
            raise Exception(f"!!! Invalid docs for row {row_id}")
        row_docs = row['docs']
        if len(row_docs) != 1:
            raise Exception(f"!!! Row docs length != 1 {row_docs}")
        doc_res = row_docs[0]
        if 'error' in doc_res:
            errobj = doc_res['error']
            is_timeout = errobj.get('reason') == 'timeout'
            is_internal_fabric_error = errobj.get('error') == 'internal_fabric_error'
            if is_timeout or is_internal_fabric_error:
                retry_docs[row_id] = docs[row_id]
            else:
                raise Exception(f"!!! Doc error for row {doc_res}")
        if 'ok' not in doc_res:
            raise Exception(f"!!! Doc error for row {doc_res}")
        doc_body = doc_res['ok']
        all_docs_doc = docs[row_id]
        if all_docs_doc['data'] != doc_body['data']:
            raise Exception(f"!!! Error data doesn't match {all_docs_doc} != {doc_body}")
    if retry_docs:
        log(" Found some doc timeouts during _bulk_get fetch, retrying: ", len(d))
        if retries <= 0:
            raise Exception(f"!!! Ran out of {retries} with these docs {retry_docs}")
        time.sleep(sleep)
        verify_batch(srv, dbname, retry_docs, retries - 1, sleep = sleep)

def thread_worker(args):
    pid = os.getpid()
    tid = args.tid
    url = pick_server(args.urls)
    srv = Server(args, url)
    ver = srv.version()
    tries = args.tries
    query = args.query
    if query:
        dbnames = [dbname for dbname in srv if dbname.startswith(args.dbname + '_')]
        if not dbnames:
            log(" >>> no databases matching prefix",args.dbname, "found")
            return
        for i in range(tries):
            dbname = random.choice(dbnames)
            ddoc = random.randrange(0, args.design_docs)
            try:
                execute_query_fun(args, dbname, ddoc, i, srv)
            except Exception as e:
                log(" >>> Query worker exception", e)
                traceback.print_exc(file=sys.stderr)
                continue
    else:
        for i in range(tries):
            #  try:
                execute_create_fun(args, pid, tid, i, srv)
            # except Exception as e:
            #     log(" >>> Worker exception caught", e)
            #     traceback.print_exc(file=sys.stderr)
            #     continue
    return tid


def set_worker_id(args, tid):
    args = copy.deepcopy(args)
    args.tid = tid
    return args

def process_worker(args):
    wcount = args.worker_count
    tpool = ThreadPool(wcount)
    worker_args = [set_worker_id(args, i) for i in range(wcount)]
    list(tpool.imap_unordered(thread_worker, worker_args))
    return True

def wait_urls(args):
    srvs = []
    for url in args.urls:
        srv = wait_url(args, url)
        log(" >> Server up", url, srv.version())
        srvs.append(srv)
    return srvs

def wait_url(args, url):
    while True:
        srv = Server(args, url)
        try:
            srv.version()
            return srv
        except Exception as e:
            log(">>> Waiting for server", url)
            time.sleep(1.0)

def setup_cluster(args):
    srvs = wait_urls(args)
    srv = srvs[0]
    for srv in srvs:
        membership = srv.membership()
        cluster_nodes = membership['cluster_nodes']
        for node in args.setup:
            if node not in cluster_nodes:
                try:
                    srv.put(f'_node/_local/_nodes/{node}', json = {})
                except HTTPError as e:
                    if e.response.status_code == 409:
                        log(">> Node already set up", node, srv.url)
                    else:
                        raise e
    srv.create_db('_users')

def config_nodes(args):
    if args.setup != []:
        setup_cluster(args)
    for url in args.urls:
        srv = Server(args, url)
        for section in CONFIG:
            for (key, val) in CONFIG[section].items():
                srv.config_set(section, key, val)

def clear(args):
    prefix = args.dbname + '_'
    srv = Server(args, args.urls[0])
    dbnames = [srv.delete(db) for db in srv if db.startswith(prefix)]

def main(args):
    if args.urls == []:
        args.urls = DB_URLS
    args = _get_auth(args)
    config_nodes(args)
    clear(args)
    ppool = Pool(processes=args.processes)
    pool_args = [args for pnum in range(args.processes)]
    log()
    log("********************* START ******************")
    log()
    list(ppool.imap_unordered(process_worker, pool_args))
    log()
    log("********************* DONE *******************")
    log()

def _get_auth(args):
    if args.auth:
        args.auth = tuple(args.auth.split(':'))
    elif 'AUTH' in os.environ:
        authstr = os.environ['AUTH']
        args.auth = tuple(authstr.split(':'))
        log(" ! using auth", username," from AUTH env var")
    else:
        args.auth = (USER, PASS)
    return args

def _args():
    description = "Do a few crud operations as a stampede"
    p = argparse.ArgumentParser(description = description)

    # list of couchdb server nodes to set up initially
    p.add_argument('-s', '--setup', action="append", default=[], help = "Server nodes")

    # list of couchdb server urls to connect to (usually one, but can have multiple
    # in that case they are randomly picked)
    p.add_argument('-u', '--urls', action="append", default=[], help = "Server URL(s)")

    # db name prefix databases will be created with db_pid_threadid_.... pattern
    p.add_argument('-d', '--dbname', default=DB_NAME, help = "DB name")

    # how many design docs (indexes) to create
    p.add_argument('-x', '--design_docs', type=int, default=1)

    # how many regular docs to insert, docs are inserted in a batch
    # default batch size is BATCH
    p.add_argument('-n', '--num_docs', type=int, default=10)

    # how many worker threads to start per process
    p.add_argument('-w', '--worker-count', type=int, default=1)

    # how many times to repeat the operation per-tread (create docs, query)
    p.add_argument('-t', '--tries', type=int, default=1)

    # how many processes to start, each process will start `worker-count` threads
    p.add_argument('-p', '--processes', type=int, default=1)

    # user:pass basic auth creds
    p.add_argument('-a', '--auth', default=None)

    # run queries on dbs instead of inserting data
    p.add_argument('-q', '--query', action="store_true", default=False)

    # clear all the database starting with out DB_NAME prefix (db_)
    p.add_argument('-c', '--clear', action="store_true", default=False)

    # compact after inserting every batch
    p.add_argument('-k', '--compact', action="store_true", default=False)
    res = p.parse_args()

    if res.setup != [] and res.urls != []:
       if len(res.setup) != len(res.urls):
            raise Exception("Setup nodes number must match number of URLs")

    return res

if __name__=='__main__':
    mp.set_start_method('spawn')
    args = _args()
    main(_args())
