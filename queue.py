# -*- coding: utf-8 -*-

from __future__ import absolute_import
from retask import Queue as RetaskQueue
from retask import Task


class QueueException(Exception):
    pass


class Queue(object):
    """
    A custom Queue API for our workers so that we can plug an play with
    multiple Queue implementations.
    """

    def __init__(self, name):
        if isinstance(name, basestring) and name.strip():
            self.q = RetaskQueue(name)
            self.q.connect()
            self.name = name
        else:
            raise QueueException(
                "Bad value for 'name'. 'name' has to be a non empty string.")

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

    def __unicode__(self):
         return self.name

