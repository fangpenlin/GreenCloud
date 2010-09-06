'''
Created on 2010/9/6

@author: Victor-mortal
'''
import cPickle as pickle

import int_prefix

class ConnectionLost(Exception):
    """Connection lost 
    
    """

class Pipeline(object):
    
    def __init__(self, socket):
        self.socket = socket
        self.parser = int_prefix.IntPrefixedDataParser()
        
    def read(self):
        """Read a python object from peer
        
        """
        frame = None
        while not frame:
            data = self.socket.recv(2048)
            # connection lost
            if not data:
                raise ConnectionLost()
            self.parser.feed(data)
            frame = self.parser.getFrame()
        return pickle.loads(frame)
    
    def write(self, obj):
        """Write a python object to peer
        
        """
        data = pickle.dumps(obj)
        frame = int_prefix.makeFrame(data)
        self.socket.send(frame)
        
    def close(self):
        self.socket.close()