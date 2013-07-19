__author__ = 'Pranjal Pandit <pranjal5215@gmail.com>'
__copyright__ = 'Copyright (c) 2012-2013 Pranjal Pandit'
__license__ = 'BSD Licence'
__version__ = '0.1'

"""
fastQ worker implementation

"""

import multiprocessing
from multiprocessing import Pool, Process, Queue
from queue import RedisQueue, RedisDataStore
import time
import os, sys
import datetime
import pdb

PROCESS_MUTEX = '/tmp/process_mutex'

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
        self.queue = RedisQueue('TASK_QUEUE')
        self.ds = RedisDataStore('RESULT_STORE')
        self.processes = []
        try:
            self.num_cores = multiprocessing.cpu_count()
        except NotImplementedError:
            self.num_cores = 4

    def _get_from_queue(self):
        """
        Pull data out from queue, this should return instantly 
        with None if queue is empty or task object if data in queue.
        Return : List of tasks.
        """
        # Task instance from queue.task
        task_dict = self.queue.get()
        return task_dict
        
    def _put_to_process(self, task_dict):
        """
        Put data to process through multiprocessing.
        """
        #Work through Process(), p.start() but process.join() in a thread.

        #task_length = len(task_list)
        #pool = Pool(task_length)
        #[pool.apply_async(self._exec_func, task) for task in task_list]
        
        p = Process(target=self._exec_func, args=(task_dict, ))
        p.start()
        self.processes.append(p)
        
    def _exec_func(self, task_dict):
        """
        Parallel calls with multiprocess.
        """
        task_dict = eval(task_dict)
        task_id = task_dict['taskid']
        exec_data = task_dict['data']
        export_list = exec_data['export_list']
        function = exec_data['function']
        args = exec_data['args']
        object_instance = exec_data.get('object_instance')
        file_path = exec_data.get('file_path')
        
        
        for export_item in export_list:
            sys.path.append(export_item)
        from Flight.QueryEngine.FlightQueryEngineConnection import FlightQueryEngineConnection
        from Flight.QueryEngine.FlightQueryEngineCursor import FlightQueryEngineCursor
        conn = FlightQueryEngineConnection()
        baseArgs = {'callBackAdd': '', 'Cache': True, 'flavour': 'v2', 'carriers': ['6E', 'NSG', 'G8', '9W', 'S2', 'IT', 'AI', 'SG'], 'Route': {'Source': 'DEL', 'DateOfDeparture': '20130723', 'Destination': 'BOM', 'DateOfArrival': '20130730'}, 'cache': True, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'application': 'fbs', 'CarrierPreference': {'Counter': '100'}, 'callBackEnd': '', 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'transaction_required': 123, 'qtype': 'fbs'}
        dataList = [{'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['SG'], 'Route': {'Source': 'DEL', 'DateOfDeparture': '20130723', 'Destination': 'BOM', 'DateOfArrival': '20130730'}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'qtype': 'fbs'}, {'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['G8'], 'Route': {'Source': 'DEL', 'DateOfDeparture': '20130723', 'Destination': 'BOM', 'DateOfArrival': '20130730'}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'qtype': 'fbs'}, {'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['6E'], 'Route': {'Source': 'DEL', 'DateOfDeparture': '20130723', 'Destination': 'BOM', 'DateOfArrival': '20130730'}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'qtype': 'fbs'}, {'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['9W', 'S2'], 'Route': {'Source': 'DEL', 'DateOfDeparture': '20130723', 'Destination': 'BOM', 'DateOfArrival': ''}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'qtype': 'fbs'}, {'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['AI'], 'Route': {'Source': 'DEL', 'DateOfDeparture': '20130723', 'Destination': 'BOM', 'DateOfArrival': ''}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'qtype': 'fbs'}, {'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['SG'], 'Route': {'Source': 'DEL', 'DateOfDeparture': '20130723', 'Destination': 'BOM', 'DateOfArrival': '20130730'}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'DiscountedReturnFareSearch': 'Y', 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'qtype': 'fbs'}, {'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['6E'], 'Route': {'Source': 'DEL', 'DateOfDeparture': '20130723', 'Destination': 'BOM', 'DateOfArrival': '20130730'}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'DiscountedReturnFareSearch': 'Y', 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'qtype': 'fbs'}, {'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['AI', 'S2', '9W'], 'Route': {'Source': 'DEL', 'DateOfDeparture': '20130723', 'Destination': 'BOM', 'DateOfArrival': '20130730'}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'DiscountedReturnFareSearch': 'Y', 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'qtype': 'fbs'}, {'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['9W', 'S2'], 'qtype': 'fbs', 'Route': {'Source': 'BOM', 'DateOfDeparture': '20130730', 'Destination': 'DEL', 'DateOfArrival': ''}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'round_flipped': 'true'}, {'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['AI'], 'qtype': 'fbs', 'Route': {'Source': 'BOM', 'DateOfDeparture': '20130730', 'Destination': 'DEL', 'DateOfArrival': ''}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'round_flipped': 'true'}]
        vendorsList = [u'spicejetnew', u'goair', u'indigo', u'amadeus', u'amadeus', u'spicejetnew', u'indigo', u'amadeus', 'amadeus', 'amadeus']
        originalVendorsList = [u'spicejetnew', u'goair', u'indigo', u'amadeus', u'amadeus', u'spicejetnew', u'indigo', u'amadeus', 'amadeus', 'amadeus']
        originalDataList = [{'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['SG'], 'Route': {'Source': 'DEL', 'DateOfDeparture': '20130723', 'Destination': 'BOM', 'DateOfArrival': '20130730'}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'qtype': 'fbs'}, {'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['G8'], 'Route': {'Source': 'DEL', 'DateOfDeparture': '20130723', 'Destination': 'BOM', 'DateOfArrival': '20130730'}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'qtype': 'fbs'}, {'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['6E'], 'Route': {'Source': 'DEL', 'DateOfDeparture': '20130723', 'Destination': 'BOM', 'DateOfArrival': '20130730'}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'qtype': 'fbs'}, {'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['9W', 'S2'], 'Route': {'Source': 'DEL', 'DateOfDeparture': '20130723', 'Destination': 'BOM', 'DateOfArrival': ''}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'qtype': 'fbs'}, {'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['AI'], 'Route': {'Source': 'DEL', 'DateOfDeparture': '20130723', 'Destination': 'BOM', 'DateOfArrival': ''}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'qtype': 'fbs'}, {'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['SG'], 'Route': {'Source': 'DEL', 'DateOfDeparture': '20130723', 'Destination': 'BOM', 'DateOfArrival': '20130730'}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'DiscountedReturnFareSearch': 'Y', 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'qtype': 'fbs'}, {'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['6E'], 'Route': {'Source': 'DEL', 'DateOfDeparture': '20130723', 'Destination': 'BOM', 'DateOfArrival': '20130730'}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'DiscountedReturnFareSearch': 'Y', 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'qtype': 'fbs'}, {'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['AI', 'S2', '9W'], 'Route': {'Source': 'DEL', 'DateOfDeparture': '20130723', 'Destination': 'BOM', 'DateOfArrival': '20130730'}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'DiscountedReturnFareSearch': 'Y', 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'qtype': 'fbs'}, {'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['9W', 'S2'], 'qtype': 'fbs', 'Route': {'Source': 'BOM', 'DateOfDeparture': '20130730', 'Destination': 'DEL', 'DateOfArrival': ''}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'round_flipped': 'true'}, {'callBackAdd': '', 'callBackEnd': '', 'flavour': 'v2', 'carriers': ['AI'], 'qtype': 'fbs', 'Route': {'Source': 'BOM', 'DateOfDeparture': '20130730', 'Destination': 'DEL', 'DateOfArrival': ''}, 'Cache': 'True', 'hitlist': {u'spicejetnew': ['SG'], u'goair': ['G8'], u'indigo': ['6E'], u'amadeus': ['9W,S2', 'AI']}, 'cache': 'True', 'CarrierPreference': {'Counter': '100'}, 'PassengerCount': {'Infants': '0', 'Adults': '1', 'Children': '0'}, 'actionId': 'AirFareSearchRequest', 'CabinPreference': {'SeatingClass': 'E'}, 'application': 'fbs', 'transaction_required': '123', 'round_flipped': 'true'}]
        
        passDict = {
            'dataList':dataList, 
            'vendorsList':vendorsList,
            'originalDataList':originalDataList,
            'originalVendorsList':originalVendorsList,
            'args' : baseArgs,
            }
        cursor = FlightQueryEngineCursor(conn, passDict)
        print args[0]
        result = cursor.processControllerData(args[0])

        """
        
        import_str = file_str[len(export_list):]
        import_str = ".".join(import_str.split('/'))
        import_str = import_str[:-3] if import_str.endswith('.py') else import_str

        #Put object_instance and export_list ans lists
        'from %s import %s' %(import_str, object_instance)
        mod = __import__('import_str', fromlist=[object_instance])
        clazz = getattr(mod, object_instance)
        conn = FlightQueryEngineConnection()
        instance = clazz(conn)
        exec_str = 'result = instance.' + function + args
        exec(exec_str)

        # from Flight.QueryEngine.FlightQueryEngineCursor import FlightQueryEngineCursor
        """
        self._store_in_ds(task_id, result)

    def _store_in_ds(self, task_id, result):
        """
        After completion of process put the result with
        status in datastore.
        """
        self.ds.put(task_id, result)

    def run_worker(self):
        """
        Replace this with higher level async mechanism
        to get data from queue, like signals etc.
        """
        task_dict = self._get_from_queue()
        self._put_to_process(task_dict)

def join_workers(worker):

    if os.path.exists(PROCESS_MUTEX):
        return
        open(PROCESS_MUTEX, 'w').close()

    num_processes = len(worker.processes)
    processes_started = worker.processes
    worker.processes = worker.processes[num_processes:]
    
    for process_started in processes_started:
        process_started.join()

    os.remove(PROCESS_MUTEX)

if __name__ == "__main__":
    pdb.set_trace()
    worker = Worker()
    worker.run_worker()

