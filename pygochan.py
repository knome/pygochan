
import threading
import collections

class Channel():
    """
    Go-style channels for python
    
    by Michael Speer
    """
    
    class Full    ( Exception ) : pass
    class Empty   ( Exception ) : pass
    
    def __init__( self, size = None ):
        
        self._lock = threading.Lock()
        
        # {} is just something that python
        # will always return True for when
        # asking if it's greater than the
        # any integer
        self._max = (
            {} if size == None else size
            )
        
        # values placed in the channel
        self._pending = collections.deque()
        
        # blocking writers awaiting writing a value
        # [ ( OnWriteEvent, writtenValue ), ... ]
        self._waiting_writers = collections.deque()
        
        # blocking readers awaiting reading a value
        # [ Selector, ... ]
        # the empty list is a box used to pass the
        # value from the writer to the reader
        self._waiting_readers = collections.deque()
        
        # oft-read and rarely written queues can
        # build up stale selector entries
        # if this is over the max, flush out the
        # stale entries. the check is done each
        # time the queue is called with an
        # external selector ( if select() is never
        # used this is never used either
        self._stale = 0
        self._max_stale = 256
        
    def put( self, value, blocking = True ):
        """
        put a value into the channel
        if the queue is full, block until the value is written
        """
        # if set, we must wait for this event before leaving the function
        putEvent = None
        
        with self._lock:
            # there cannot be waiting readers and a pending queue
            # of messages.  if it happens, it's a deadly error
            
            if self._waiting_readers and self._pending:
                raise Exception( 'impossible error: cannot be waiting readers and pending messages' )
            
            while self._waiting_readers:
                # if there is a waiting reader, pass our value to it directly
                selector = self._waiting_readers.popleft()
                
                if not selector.offer( value ):
                    # that reader already received a different value
                    continue
                else:
                    # we've successfully passed on our value, leave the function
                    return
            
            # there were no waiting readers, or all had already accepted a value
            # ( meaning now there are no waiting readers )
            
            # put the value in the queue
            
            # if the queue is full, we'll have to block ( if we're blocking )
            if len( self._pending ) >= self._max:
                # the queue is full
                if blocking:
                    # if blocking, we must block awaiting a reader
                    putEvent = threading.Event()
                    self._waiting_writers.append( (putEvent, value ) )
                else:
                    raise Channel.Full
                
            else:
                # there's room in the queue
                self._pending.append( value )
        
        # if the queue was full, we passed in an event to await a reader
        # wait on the reader to trigger our event before leaving the function
        if putEvent != None:
            putEvent.wait()
        
        return
    
    def get( self, blocking = True, selector = None ):
        """
        get a value from the channel
        
        if blocking, this won't return until a value is available
        if nonblocking, this will raise Empty if no value is available
        
        if selector is provided, blocking must be true, selector will be
        offered the value when it is ready. if selector refuses the value
        it must not be removed from the lists
        
        this function returns None when a selector is passed into it
        """
        
        # used internally to the function to handle queuing
        internal_selector = None
        
        with self._lock:
            
            # we flush the deque of stale readers every
            # _max_stale'th time an external selector is
            # pushed into the queue
            # this keeps underwritten queues from having
            # an infinitely growing _waiting_reader queue
            # ( at least an unintended one )
            
            if selector:
                self._stale += 1
                if self._stale > self._max_stale:
                    self._waiting_readers = collections.deque(
                        wr for wr in self._waiting_readers
                        if not wr.stale()
                        )
                    self._stale = 0
            
            # this makes the code the same regardless of 
            # whether the selector is in function or passed
            if not selector:
                selector = internal_selector = Selector()
            
            if self._pending:
                # just grab a value off of the pending queue
                queue_value = self._pending.popleft()
                
                # if the selector rejects a value, it is not
                # the internal selector and it has already
                # received a value. put the data back on the
                # queue and return nothing
                if not selector.offer( queue_value ):
                    self._pending.appendleft( queue_value )
                    return
                
                # our data was accepted, now we do some housecleaning
                # if there are blocked writers awaiting space in the
                # queue, take the first ones value into the queue and
                # signal the threads event so it stops blocking
                if self._waiting_writers:
                    ( writer_event, writer_value ) = self._waiting_writers.popleft()
                    self._pending.append( writer_value )
                    writer_event.set()
            
            elif self._waiting_writers:
                # if there's nothing in the queue, but there are waiting writers,
                # we're just dealing with a queue of size 0. take a value directly
                # from the first writer and release it
                ( writer_event, writer_value ) = self._waiting_writers.popleft()
                
                # if we've received a selector, see if it wants the
                # value, if not, put it back and exit the get
                
                # if the selector rejects our offer, it is not
                # the internal selector and it has already
                # received a value. put the writer back on the
                # queue and return nothing
                if not selector.offer( writer_value ):
                    self._waiting_writers.appendleft( ( writer_event, writer_value ) )
                    return
                
                # signal the writer that it can unblock
                writer_event.set()
            
            else:
                # there's nothing in the queue and no pending writers, we'll have
                # to go into the reader queue and await a writer to pass us a value
                # unless we're nonblocking, then we just throw an Empty exception
                if blocking:
                    self._waiting_readers.append( selector )
                else:
                    raise Channel.Empty
        
        if internal_selector:
            # we're in the reader queue, wait on a writer to offer a value
            return internal_selector.get()
        
        # we return if we were dealing with an external selector to allow the
        # thread passing it in to place its selector in as many queues as it
        # desires. it is the callers duty to call get() on the passed in selector

class Selector():
    """
    returns first offered value from
    get(). get() blocks till a value
    is offered. offer() returns true
    if the value is first, otherwise
    it returns false.
    """
    
    def __init__( self ):
        self._lock  = threading.Lock()
        self._event = threading.Event()
        self._set   = False
        self._value = None
        return
    
    def stale( self ):
        # do not use this to see if the
        # selector has a value yet and
        # then offer. just offer and 
        # check the return
        # this is for flushing stale 
        # selectors out of queue deque
        with self._lock:
            return self._set
        
    def offer( self, value ):
        with self._lock:
            if self._set:
                return False
            else:
                self._set   = True
                self._value = value
                self._event.set()
                return True
    
    def get( self ):
        self._event.wait()
        return self._value

def channel_select( channels, blocking = True ):
    """
    take the first value available from any of a list of channels
    
    the channels are tried in order, so earlier channels have priority
    over later channels ( randomize the channel list if you're
    concerned )
    """
    assert channels
    
    if blocking:
        selector = Selector()
        
        for channel in channels:
            channel.get(
                selector = selector ,
                )
        
        return selector.get()
    
    else:
        # nonblocking multiple channel get
        for channel in channels:
            try:
                return channel.get( blocking = False )
            except Channel.Empty:
                pass
            
        # if no channel returns a value, raise Empty
        raise Channel.Empty
