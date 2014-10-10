
# some quick examples of pygochan usage

import pygochan
import threading
import time
import functools
import random
import datetime


def main():
    # some of these can produce quiet a bit of output
    # the output can be quiet jumbled due to asyncronous printing
    
    example001()
    example002()
    example003()
    example004()
    example005()
    example006()
    return


def example001():
    # very simple example. each will probably
    # put all its data in the unbounded queue
    # before the next even starts
    
    print 'RUNNING EXAMPLE001'
    
    channel = pygochan.Channel()
    
    @background
    def sender( name ):
        for x in xrange( 100 ):
            channel.put( (name, x) )
        
        channel.put( (name, None) )
        
    @background
    def receiver():
        finished = 0
        while finished < 100:
            message = channel.get()
            if message[1] == None:
                print 'FINISHED', message[0]
                finished += 1
            else:
                print 'RECEIVED', message[0], message[1]
    
    for x in range( 100 ):
        sender( str( x ) )
    
    receiverThread = receiver()
    
    receiverThread.join()
    
    print 'EXAMPLE001 COMPLETE'


def example002():
    # add a small time delay so each channel
    # doesn't fully flood the queue with its
    # data before the next even begins
    
    print 'RUNNING EXAMPLE002'
    
    channel = pygochan.Channel()
    
    @background
    def sender( name ):
        for x in xrange( 100 ):
            time.sleep( 0.1 )
            channel.put( (name, x) )
        
        channel.put( (name, None) )
        
    @background
    def receiver():
        finished = 0
        while finished < 100:
            message = channel.get()
            if message[1] == None:
                print 'FINISHED', message[0]
                finished += 1
            else:
                print 'RECEIVED', message[0], message[1]
    
    for x in range( 100 ):
        sender( str( x ) )
    
    receiverThread = receiver()
    
    receiverThread.join()
    
    print 'EXAMPLE002 COMPLETE'


def example003():
    # getting a value from an iterable of channels
    # also, demonstrating a queue of 0 size, forcing
    # all writers to wait for a reader before continuing
    
    print 'RUNNING EXAMPLE003'
    
    @background
    def sender( name, channel ):
        for x in xrange( 100 ):
            time.sleep( 0.1 )
            print 'SENDING', name, x
            channel.put( (name, x) )
            print 'SENT', name, x
            
        channel.put( (name, None) )
    
    channels = []
    for x in xrange( 100 ):
        # 0 size channel
        channel = pygochan.Channel( size = 0 )
        channels.append( channel )
        sender( str( x ), channel )
    
    finished = 0
    while finished < 100:
        result = pygochan.channel_select( channels )
        if result[1] == None:
            finished += 1
            print 'FINISHED', result[0]
        else:
            print 'RECEIVED', result[0], result[1]
    
    print 'EXAMPLE003 COMPLETE'


def example004():
    # this time shuffle the channels to prevent the earlier
    # channels in the list from having a read advantage
    
    print 'RUNNING EXAMPLE004'
    
    @background
    def sender( name, channel ):
        for x in xrange( 100 ):
            time.sleep( 0.1 )
            print 'SENDING', name, x
            channel.put( (name, x) )
            print 'SENT', name, x
            
        channel.put( (name, None) )
    
    channels = []
    for x in xrange( 100 ):
        # 0 size channel
        channel = pygochan.Channel( size = 0 )
        channels.append( channel )
        sender( str( x ), channel )
    
    finished = 0
    while finished < 100:
        random.shuffle( channels )
        result = pygochan.channel_select( channels )
        if result[1] == None:
            finished += 1
            print 'FINISHED', result[0]
        else:
            print 'RECEIVED', result[0], result[1]
    
    print 'EXAMPLE004 COMPLETE'


def example005():
    # non-blocking operation of readers and writers
    # busy workers attempt to push to channel, on fail
    # they just loop around and try again.
    # a middle group of passers pull from one channel
    # and push the next. the passers die as they forward
    # completion messages from the senders.
    
    print 'RUNNING EXAMPLE005'
    
    firstChannel  = pygochan.Channel( size = 5 )
    secondChannel = pygochan.Channel( size = 0 )
    
    @background
    def sender( name ):
        for x in xrange( 100 ):
            while True:
                try:
                    print 'TRYING-SEND', name, x
                    firstChannel.put( (name, x), blocking = False )
                    break
                except firstChannel.Full:
                    continue
        
        while True:
            try:
                firstChannel.put( (name, None) )
                break
            except firstChannel.Full:
                continue
    
    @background
    def passer( name ):
        while True:
            try:
                print 'TRYING-PASS', name
                message = firstChannel.get( blocking = False )
                print 'PASSING', name, message
                secondChannel.put( message )
                if message[1] == None:
                    print 'STOPPING', name
                    break
            except firstChannel.Empty:
                print 'NOTHING-TO-PASS', name
                pass
    
    for x in xrange( 100 ):
        sender( 'sender %s' % str( x ) )
        passer( 'passer %s' % str( x ) )
    
    finished = 0
    while finished < 100:
        message = secondChannel.get()
        if message[1] == None:
            finished += 1
            print 'FINISHED', message[0]
        else:
            print 'RECEIVED', message[0], message[1]


def example006():
    """
    thread-ring from the debian language shootout
    
    this takes as long as it takes your hardware to perform
    5,000,000 python thread context switches, roughly
    """
    channels = [ pygochan.Channel( 0 ) for _ in xrange( 503 ) ]
    doneChan = pygochan.Channel()
    
    @background
    def worker( workerNo, inChan, outChan ):
        while True:
            value = inChan.get()
            if value == 0:
                doneChan.put( workerNo )
            else:
                outChan.put( value - 1 )
    
    workers = []
    for x in xrange( 503 ):
        worker( x + 1 , channels[ x - 1 ], channels[ x ] )
    
    
    initialValue = 5000000
    print 'PUSHING INITIAL VALUE', str( datetime.datetime.now() )
    
    # we push into the final because its what the first pulls from
    channels[ -1 ].put( initialValue )
    
    finalWorker = doneChan.get()
    print 'RECEIVED FINAL WORKER', str( datetime.datetime.now() )
    print finalWorker

def background( fn ):
    """ run a function as a daemon thread """
    @functools.wraps( fn )
    def backgrounding( *args, **kwargs ):
        # print 'RUNNING', fn, 'ARGS', args, 'KWARGS', kwargs
        thread = threading.Thread(
            target = fn     ,
            args   = args   ,
            kwargs = kwargs ,
            )
        thread.isDaemon = True
        thread.start()
        return thread
        
    return backgrounding


if __name__ == '__main__':
    main()
