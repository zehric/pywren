import numpy as np
import matplotlib.pyplot as plt
import argparse
import boto3
import io
import os
import hashlib
import aiobotocore
import asyncio
import time
import math
import pywren
import concurrent.futures as fs

parser = argparse.ArgumentParser(description="S3 IO benchmarks")
parser.add_argument('data_size', type=str)
parser.add_argument('num_keys', type=int)
parser.add_argument('method', type=str)

# DATA_SIZE = 134217728
BUCKET = "s3iops-benchmarks"
args = parser.parse_args()
DATA_SIZE = float(args.data_size)
NUM_KEYS = args.num_keys
METHOD = args.method
OUTPUT_NAME = "{}_{}_{}".format(METHOD, args.data_size, NUM_KEYS)

def write_keys(prefix, size=DATA_SIZE, num_keys=10):
    write_bytes = b"\x00"+os.urandom(int(size))+ b"\x00"
    client = boto3.client('s3')
    times = []
    for i in range(num_keys):
        key = "{0}_{1}".format(prefix, i)
        sha1 = hashlib.sha1()
        sha1.update(key.encode())
        key = sha1.hexdigest()
        t = time.time()
        client.put_object(Key=key, Body=write_bytes, Bucket=BUCKET)
        e = time.time()
        times.append((t, e))
    return times

def put_object(client, key, data, bucket):
    backoff = 1
    t = time.time()
    while True:
        try:
            client.put_object(Key=key, Body=data, Bucket=BUCKET)
            e = time.time()
            return (t, e)
        except:
            return (t, -1.0)
            time.sleep(backoff)
            backoff *= 2

def get_object(client, key, bucket):
    backoff = 1
    t = time.time()
    while True:
        try:
            return client.get_object(Key=key, Bucket=BUCKET)['ContentLength']
            e = time.time()
            return (t, e)
        except:
            time.sleep(backoff)
            backoff *= 2

def write_keys_threaded(prefix, size=DATA_SIZE, num_keys=10, threads=10):
    write_bytes = b"\x00"+os.urandom(int(size))+ b"\x00"
    t = time.time()
    executor = fs.ThreadPoolExecutor(threads)
    client = boto3.client('s3')
    futures = []
    for i in range(num_keys):
        key = "{0}_{1}".format(prefix, i)
        sha1 = hashlib.sha1()
        sha1.update(key.encode())
        key = sha1.hexdigest()
        futures.append(executor.submit(put_object, client, key, write_bytes, BUCKET))
    fs.wait(futures)
    times = [f.result() for f in futures]
    e = time.time()
    return times

def read_keys_threaded(prefix, num_keys=10, threads=10):
    t = time.time()
    executor = fs.ThreadPoolExecutor(threads)
    client = boto3.client('s3')
    futures = []
    for i in range(num_keys):
        key = "{0}_{1}".format(prefix, i)
        sha1 = hashlib.sha1()
        sha1.update(key.encode())
        key = sha1.hexdigest()
        futures.append(executor.submit(get_object, client, key, BUCKET))
    fs.wait(futures)
    sizes = [f.result() for f in futures]
    e = time.time()
    return sizes

async def put_object_backoff(client, key, bucket, data, backoff_start=1):
    backoff = backoff_start
    t = time.time()
    while True:
        try:
            await client.put_object(Key=key, Bucket=bucket, Body=data)
            e = time.time()
            return (t, e)
        except:
            await asyncio.sleep(backoff)
            backoff *= 2
            
async def _write_keys_async(loop, keys_data, bucket=BUCKET):
    session = aiobotocore.get_session(loop=loop)
    async with session.create_client('s3', use_ssl=False, verify=False, region_name='us-west-2') as client:
        tasks = []
        for key, data in keys_data:
            tasks.append(asyncio.ensure_future(put_object_backoff(client, key, bucket, data)))
        return await asyncio.gather(*tasks)
    return 

def write_keys_async(prefix, size=DATA_SIZE, num_keys=100):
    client = boto3.client('s3')
    write_bytes = b"\x00"+os.urandom(int(size))+ b"\x00" 
    keys_data = []
    t = time.time()
    for i in range(num_keys):
        key = "{0}_{1}".format(prefix, i)
        sha1 = hashlib.sha1()
        sha1.update(key.encode())
        key = sha1.hexdigest()
        keys_data.append((key, write_bytes))
    loop = asyncio.get_event_loop()
    times = loop.run_until_complete(_write_keys_async(loop, keys_data))
    e = time.time() 
    return times

def profile_iops(results):
    results = [x for y in results for x in y]
    print(len(results))
    min_time = min(results, key=lambda x: x[0])[0]
    max_time = max(results, key=lambda x: x[1])[1]
    tot_time = (max_time - min_time)

    bins = np.linspace(min_time, max_time, (max_time - min_time) * 10)
    # bins = np.linspace(min_time, max_time, (max_time - min_time))
    #return bins, min_time, max_time
    iops = np.zeros(len(bins))

    fail_cnt = 0
    for start_time, end_time in results:
        if end_time < 0:
            fail_cnt += 1
        start_bin, end_bin = np.searchsorted(bins, [round(start_time, 1), round(end_time, 1)])
        # start_bin, end_bin = np.searchsorted(bins, [int(start_time), int(end_time)])
        iops[start_bin:(end_bin+1)] += (1 / (end_time - start_time))
    print("fail_cnt: {}".format(fail_cnt))
    return iops, bins


# w = write_keys_threaded("poop", num_keys=100, threads=100)
# print(w)
# exit()
# r = write_keys_async("poop", num_keys=100)
# print(r[1] - r[0])
# exit()

config = pywren.wrenconfig.default()
config['runtime']['s3_bucket'] = 'numpywrenpublic'
key = "pywren.runtime/pywren_runtime-3.6-numpywren.tar.gz"
config['runtime']['s3_key'] = key

pwex = pywren.default_executor(config=config)

if METHOD == "threaded":
    futures = pwex.map(lambda x: write_keys_threaded(x, num_keys=NUM_KEYS, threads=NUM_KEYS), range(36*25))
elif METHOD == "async":
    futures = pwex.map(lambda x: write_keys_async(x, num_keys=NUM_KEYS), range(36*25))
elif METHOD == "test_t":
    futures = pwex.map(lambda x: write_keys_threaded(x, num_keys=NUM_KEYS, threads=NUM_KEYS), range(36))
elif METHOD == "test_a":
    futures = pwex.map(lambda x: write_keys_async(x, num_keys=NUM_KEYS), range(36))
else:
    print("Input a valid method")
    exit()


pywren.wait(futures)

results = []
for i,f in enumerate(futures):
    try:
        if (f.done()):
            results.append(f.result())
    except Exception as e:
        print(e)
        pass

print(len(results))
if len(results) == 0:
    exit()
iops, bins = profile_iops(results)

np.savez("s3iops_lambda_python_write_{}".format(OUTPUT_NAME), iops=iops, bins=bins)

plt.plot(bins - min(bins), iops)
plt.ylabel("(write) IOPS")
plt.xlabel("time")
plt.savefig('s3iops_lambda_python_write_{}.png'.format(OUTPUT_NAME))

futures = pwex.map(lambda x: read_keys_threaded(x, num_keys=NUM_KEYS, threads=100), range(36*25))
pywren.wait(futures)
results = []
for i,f in enumerate(futures):
    try:
        if (f.done()):
            results.append(f.result())
    except Exception as e:
        print(e)
        pass
results = [x for y in results for x in y]
if len(results) != 36*25*NUM_KEYS:
    print("{} does not match {}".format(len(results), 36*25*NUM_KEYS))
    exit()
fail_cnt = 0
for result in results:
    if result != DATA_SIZE + 2:
        print("{} does not match size {}".format(result, DATA_SIZE))
        fail_cnt += 1

print("fail count: {}".format(fail_cnt))
