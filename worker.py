__author__ = 'Pranjal Pandit <pranjal5215@gmail.com>'
__copyright__ = 'Copyright (c) 2012-2013 Pranjal Pandit'
__license__ = 'BSD Licence'
__version__ = '0.1'

"""
fastQ worker implementation

"""

import __future__ import absolute_import
from multiprocessing import Pool, Process, Queue
from .queue import Queue, Task
from .datastore import DataStore
import time

class Worker(object):
    """
    Worker class Implementation. 
    Pick up values from Queue provided by Queue API.
    Process using multiprocessing and store the 
    value in datastore.
    """
    #TODO : Break this into worker/master to make it distributed.
    def __init__(self):
        """
        
        """
        self.queue = Queue()
        self.ds = DataStore()

    def _get_from_queue(self):
        """
        Pull data out from queue, this should return instantly 
        with None if queue is empty or value if queue has data.
        """
        # Task instance from queue.task
        task = self.queue.get()
        assert isinstance(task, Task)
        return tsk
        
    def _put_to_process(self):
        """
        Put data to process through multiprocessing.
        """
        

    def _store_in_ds(self):
        """
        After completion of process put the result with
        status in datastore.
        """
        

    def run_worker(self):
        """
        Replace this with higher level async mechanism
        to get data from queue, like signals etc.
        """
        while 1:

            #Should return instantly.
            task = self._get_from_queue()

            if item:
                self._put_to_process(task)
            
            time.sleep(0.15)
        
