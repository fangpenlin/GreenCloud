'''
Created on 2010/9/5

@author: Victor-mortal
'''
import socket
import marshal
import logging

import pipeline

log = logging.getLogger(__name__)

class Client(object):
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

    def runTask(self, func, arg_list):
        task = dict(code=marshal.dumps(func.func_code), arg_list=arg_list)
        self.pipeline.write(task)
        result = self.pipeline.read()
        return result
    
    def runCall(self, call):
        return self.runTask(call.func, call.arg_list)
    
class FunctionCall(object):
    def __init__(self, func):
        self.func = func
        self.arg_list = []
    
    def __call__(self, *args, **kwargs):
        self.arg_list.append((args, kwargs))
        
if __name__ == '__main__':
    client = Client('127.0.0.1', 6001)
    client.connect()
    
    def sum(n):
        print 'Sum from 0 to', n
        s = 0
        for i in range(n):
            s += i
        return s
    
    call = FunctionCall(sum)
    for i in range(1000):
        call(i)
    print client.runCall(call)