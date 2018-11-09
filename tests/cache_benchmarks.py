from multiprocessing import Process
from multiprocessing.connection import Client
import numpy as np
import os
import time

def put(client, key, randarr):
    arr = np.memmap("/tmp/" + key, dtype='float64', mode='w+', shape=(4096,4096))
    arr[:] = randarr[:]
    client.send((1, key)) # put
    res = client.recv()

def run_simple(id_, num_repeat, log_info):
    tick = time.time()
    key_pre = "test{}"
    client = Client(address='local_cache')
    # this benchmark assumes that the cache is the size of a single entry

    warm_pins = []
    warm_gets = []
    cold_pins = []
    cold_gets = []
    puts_no_evict = []
    puts_with_evict = []

    # put with no eviction
    for _ in range(num_repeat):
        key = key_pre.format(0)
        randarr = np.random.rand(4096, 4096)
        start_time = time.time()
        put(client, "0", randarr)
        puts_no_evict.append(time.time() - start_time)
    
    # put with eviction
    i = 1
    for _ in range(num_repeat):
        key = key_pre.format(i)
        randarr = np.random.rand(4096, 4096)
        start_time = time.time()
        put(client, str(i), randarr)
        puts_with_evict.append(time.time() - start_time)
        i = 1 - i

    # warm get and pin
    for _ in range(num_repeat):
        key = key_pre.format(0)
        start_time = time.time()
        client.send((0, key, True)) # get and pin
        res = client.recv()
        arr = np.memmap(res, dtype='float64', mode='r', shape=(4096,4096))
        # print(arr)
        client.send((2, key)) # release
        res = client.recv()
        warm_pins.append(time.time() - start_time)
        # print("get {}: {}".format(i, time.time() - start_time))

    # warm get
    for _ in range(num_repeat):
        key = key_pre.format(0)
        start_time = time.time()
        client.send((0, key))
        res = client.recv()
        arr = np.frombuffer(res, dtype='float64').reshape((4096, 4096))
        # print(arr)
        warm_gets.append(time.time() - start_time)
        # print("get {}: {}".format(i, time.time() - start_time))

    #cold get and pin
    i = 1
    for _ in range(num_repeat):
        key = key_pre.format(i)
        start_time = time.time()
        client.send((0, key, True)) # get and pin
        res = client.recv()
        arr = np.memmap(res, dtype='float64', mode='r', shape=(4096,4096))
        # print(arr)
        client.send((2, key)) # release
        res = client.recv()
        cold_pins.append(time.time() - start_time)
        i = 1 - i

    #cold get
    i = 1
    for _ in range(num_repeat):
        key = key_pre.format(i)
        start_time = time.time()
        client.send((0, key))
        res = client.recv()
        arr = np.frombuffer(res, dtype='float64').reshape((4096, 4096))
        # print(arr)
        cold_gets.append(time.time() - start_time)
        i = 1 - i


    tock = time.time()
    #log_info[id_] = {'total_time': (tock-tick), 'warm_pins': warm_pins, 'warm_gets': warm_gets, 'cold_pins': cold_pins, 'cold_gets': cold_gets}

   # print("warm_pins: {}".format(warm_pins))
   # print("warm_gets: {}".format(warm_gets))
   # print("cold_pins: {}".format(cold_pins))
   # print("cold_gets: {}".format(cold_gets))

    np.savez("cache_benchmarks_simple", no_evict_puts=puts_no_evict, evict_puts=puts_with_evict, warm_pins=warm_pins, warm_gets=warm_gets, cold_pins=cold_pins, cold_gets=cold_gets)


if __name__ == '__main__':
    num_clients = 1
    num_repeat = 1000

    clients = []

    log_info = {}
    for i in range(num_clients):
        clients.append(Process(target=run_simple, args=(i,num_repeat,log_info)))

    tick = time.time()
    for client in clients:
        client.start()

    for client in clients:
        client.join()

    tock = time.time()

    print("outer time = %0.4f seconds" % (tock-tick))
    
