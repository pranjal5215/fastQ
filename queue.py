# -*- coding: utf-8 -*-

from __future__ import absolute_import
from multiprocessing import Pool
from Queue import Empty as EmptyQueueException


class QueueException(Exception):
    pass


class Queue(object):

    def __init__(self, name):
        if isinstance(name, basestring) and name.strip():
            self.name = name
            self._initialize()
        else:
            raise QueueException(
                "Bad value for 'name'. 'name' has to be a non empty string.")

    def _initialize(self):
        raise NotImplementedError

    def get(self, count=1, wait=False):
        raise NotImplementedError

    def put(self, data):
        raise NotImplementedError

    def send(self, task, data):
        raise NotImplementedError

    def __unicode__(self):
        return self.name


class RetaskQueue(Queue):
    """
    A custom Queue API for our workers so that we can plug an play with
    multiple Queue implementations.
    """

    def _initialize(self):
        from retask import Queue
        self.q = Queue(self.name)
        self.q.connect()

    def get(self, count=1, wait=False):
        """
        Get task from Queue.

        Args:
            wait: A boolean deciding whether to retrieve a task
                  from queue in a blocking fashion or not.
        Returns:
            A list of retask.Task instances.
        """
        tasks = []
        for n in range(count or 1):
            if wait:
                task = self.q.wait()
            else:
                task = self.q.dequeue()
            tasks.append(task)
        return tasks

    def put(self, data):
        """
        Create a task from data and put it to Queue.

        Args:
            data: A dictionary.
        """
        from retask import Task
        task = Task(data)
        job = self.q.enqueue(task)
        return job

    def send(self, task, data):
        """
        Update result for a task and send it to the producer.

        Args:
            task: A retask.Task instance
            data: A string.
        """
        self.q.send(task, data)


class MultiprocessingQueue(Queue):

    def _initialize(self):
        from multiprocessing import Queue
        self.work_queue = Queue()
        self.done_queue = Queue()

    def get(self, wait=False, timeout=None):
        try:
            task = self.work_queue.get(wait, timeout)
            return task
        except EmptyQueueException as e:
            pass

    def put(self, data):
        self.work_queue.put(data)

    def send(self, data):
        self.done_queue.put(data)

    def get_result(self):
        try:
            return self.done_queue.get(True, 0.5)
        except Exception:
            pass

    def qsize(self, type='work'):
        if type == 'work':
            return self.work_queue.qsize()
        elif type == 'result':
            return self.done_queue.qsize()
        return 0


def worker(queue):
    """
    Sample worker.
    """
    import time
    import random
    t = queue.get(True, 0.01)
    sleep_time = random.randint(1, 4)
    print 'Sleep time: %d' % sleep_time
    time.sleep(sleep_time)
    queue.send(t)


class Master(object):

    def __init__(self, queue, worker, job_args=None):
        self.queue = queue
        self.worker = worker
        self.job_count = self.queue.qsize()
        self.processes = []
        self.job_args = job_args

    def schedule_tasks(self):
        from multiprocessing import Process
        if self.job_args is not None:
            self.job_count = 0
            for job in self.job_args:
                p = Process(target=self.worker, args=(self.queue, job))
                self.processes.append(p)
                self.job_count += 1
                p.start()
        else:
            for i in range(self.job_count):
                p = Process(target=self.worker, args=(self.queue,))
                self.processes.append(p)
                p.start()

    def results(self):
        for i in range(self.job_count):
            yield self.queue.get_result()
        for p in self.processes:
            p.join()

