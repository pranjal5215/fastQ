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
import importlib

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
        Initialize worker.
        """
        self.queue = Queue()
        self.ds = DataStore()
        try:
            self.num_cores = multiprocessing.cpu_count()
        except NotImplementedError:
            self.num_cores = 4

    def _get_from_queue(self, items_to_fetch):
        """
        Pull data out from queue, this should return instantly 
        with None if queue is empty or task object if data in queue.
        Return : List of tasks.
        """
        # Task instance from queue.task
        task_list = self.queue.get(items_to_fetch)
        for task in task_list:
            assert isinstance(task, Task)
        return task_list
        
    def _put_to_process(self, task_list):
        """
        Put data to process through multiprocessing.
        """
        task_length = len(task_list)
        pool = Pool(task_length)
        [pool.apply_async(self._exec_func, task) for task in task_list]

        
    def _exec_func(self, task):
        """
        Parallel calls with multiprocess.
        """
        func = task.data['func']
        args = task.data['args']
        kwargs = task.data['kwargs']

        impt = func.split('.')[-1]
        
        mod = __import__(('.'.join(func.split('.')[:-1])), fromlist=[impt])
        clazz = getattr(mod, impt)
        instance = clazz(kwargs.get('options'))
        result = instance.func(*args, **kwargs)

        self._store_in_ds(task, result)

    def _store_in_ds(self, task, result):
        """
        After completion of process put the result with
        status in datastore.
        """
        self.queue.send(task, result)

    def run_worker(self):
        """
        Replace this with higher level async mechanism
        to get data from queue, like signals etc.
        """
        while 1:
            try:
                #Should return instantly.
                task_list = self._get_from_queue(self.num_cores)
            
                if task_list:
                    self._put_to_process(task_list)
                
                time.sleep(0.15)
            except KeyboardInterrupt:
                break
            
        
