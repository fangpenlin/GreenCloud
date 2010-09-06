'''
Created on 2010/9/5

@author: Victor-mortal
'''
import struct

__all__ = [
    'makeFrame',
    'IntPrefixedDataParser'
]

def makeFrame(data):
    """Make an integer prefixed data frame
    
    """
    return struct.pack('I', len(data)) + data

class IntPrefixedDataParser(object):
    """It is a very simple protocol, only for passing pickled object between
    Python server/client
    
    The idea of protocol is very simple, an int32 integer prefixed data body
    sequence. 
    
    For example:
    <int32: length of data1>
    <data1 body>
    <int32: length of data2>
    <data2 body>
    ...
    
    """
    
    def __init__(self):
        self.buffer = []
    
    def feed(self, data):
        """Feed data to parser
        
        """
        self.buffer.append(data)
    
    def getFrame(self):
        """Return a frame, if there is no complete frame, return None
        
        """
        frame = None
        # get whole data in buffer
        chunk = ''.join(self.buffer)
        # we have at last one integer in chunk?
        if len(chunk) >= 4:
            length = struct.unpack('I', chunk[:4])[0]
            # the complete body of data is in chunk
            if len(chunk) - 4 >= length:
                frame = chunk[4:length + 4]
                chunk = chunk[length + 4:]
        self.buffer = [chunk]
        return frame
       
import unittest
class TestFrame(unittest.TestCase):
    
    def testMakeFrame(self):
        data = '\0\1\2\3'
        frame = makeFrame(data)
        # NOTICE: we assume the machine is little-endian here
        self.assertEqual(frame, '\4\0\0\0\0\1\2\3')

    def testFeedData(self):
        p = IntPrefixedDataParser()
        data = '1234567890'
        p.feed(makeFrame(data))

        result = p.getFrame()
        self.assertEqual(data, result)
        self.assertEqual(p.getFrame(), None)
        
        data = 'abcdefg'
        frame = makeFrame(data)
        for c in frame[:-1]:
            p.feed(c)
            self.assertEqual(p.getFrame(), None)
        p.feed(frame[-1])
        self.assertEqual(p.getFrame(), data)
        
    def testZeroLengthData(self):
        data = ''
        p = IntPrefixedDataParser()
        p.feed(makeFrame(data))
        self.assertEqual(p.getFrame(), '')