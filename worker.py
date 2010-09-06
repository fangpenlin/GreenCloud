'''
Created on 2010/9/5

@author: Victor-mortal
'''
import types
import socket
import marshal
import logging

import pipeline

log = logging.getLogger(__name__)

class Worker(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.pipeline = None
    
    def connect(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))
        self.pipeline = pipeline.Pipeline(s)
    
    def close(self):
        self.pipeline.close()

    def run(self):
        while True:
            task = self.pipeline.read()
            code = marshal.loads(task['code'])
            func = types.FunctionType(code, globals(), 'remote_function')
            args = task['args']
            kwargs = task['kwargs']
            try:
                result = func(*args, **kwargs)
            except Exception, e:
                result = e
            self.pipeline.write(result)
        
if __name__ == '__main__':
    worker = Worker('127.0.0.1', 6000)
    worker.connect()
    worker.run()