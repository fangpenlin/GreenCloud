#!/usr/bin/env python
'''
Created on 2010/9/5

@author: Victor-mortal
'''
import types
import logging

import wsgi_xmlrpc
import gevent
from gevent import queue, pywsgi
from gevent.server import StreamServer

import pipeline

log = logging.getLogger(__name__)
        
class WorkerClient(object):
    
    def __init__(self, socket, address):
        self.socket = socket
        self.address = address
        self.pipeline = pipeline.Pipeline(socket)
    
    def runTask(self, code, *args, **kwargs):
        """Run a task on this worker client
        
        """
        task = dict(code=code, args=args, kwargs=kwargs)
        self.pipeline.write(task)
        return self.pipeline.read()

class GreenCloudManager(object):
    def __init__(self, ):
        self.idle_queue = queue.Queue()
        self.working_list = set()
        
    def _runOneTask(self, code, *args, **kwargs):
        # get a worker from queue
        worker = self.idle_queue.get()
        self.working_list.add(worker)
        # XXX we should handle connection lost problem here
        result = worker.runTask(code, *args, **kwargs)
        self.working_list.remove(worker)
        self.idle_queue.put(worker)
        return result
        
    def runTask(self, code, arg_list):
        jobs = [gevent.spawn(self._runOneTask, code, *args, **kwargs) 
                for args, kwargs in arg_list]
        gevent.joinall(jobs)
        return [job.value for job in jobs]
    
    def handleRequest(self, socket, address):
        log.info('Request from %s:%s', address[0], address[1])
        p = pipeline.Pipeline(socket)
        while True:
            try:
                task = p.read()
            except pipeline.ConnectionLost:
                break
            result = self.runTask(task['code'], task['arg_list'])
            p.write(result)
    
    def handleWorker(self, socket, address):
        log.info('Add worker from %s:%s', address[0], address[1])
        self.idle_queue.put(WorkerClient(socket, address))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    manager = GreenCloudManager()
    cloud_server = StreamServer(('0.0.0.0', 6000), manager.handleWorker)
    request_server = StreamServer(('0.0.0.0', 6001), manager.handleRequest)
    
    def runCloud():
        print 'Starting GreenCloud server on port 6000'
        cloud_server.serve_forever()
        
    def runRequest():
        print 'Starting Request server on port 6001'
        request_server.serve_forever()
        
    servers = map(gevent.spawn, [runCloud, runRequest])
    gevent.joinall(servers)