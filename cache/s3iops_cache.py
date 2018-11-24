import numpy as np
import matplotlib.pyplot as plt
import ctypes
import argparse
import boto3
import botocore.session
import os
import hashlib
import time
import math
import pywren
from fastclient import FastClient

parser = argparse.ArgumentParser(description="S3 IO benchmarks")
parser.add_argument('data_size', type=float)
parser.add_argument('num_keys', type=int)

# DATA_SIZE = 134217728
args = parser.parse_args()
DATA_SIZE = args.data_size
NUM_KEYS = args.num_keys
OUTPUT_NAME = "e{}_{}".format(int(math.log10(DATA_SIZE)), NUM_KEYS)
bucket = "uric-cache-benchmarks"

def read_keys_pin(prefix, size=DATA_SIZE, num_keys=10):
    fclient = FastClient(so_bucket="zehric-pywren-149")
    fclient.cache_so()
    t = time.time()
    # TODO: make this multithreaded so it can compete with the normal benchmarks

    fclient.run_benchmark_pins(num_keys)

    e = time.time()
    return t,e,num_keys

def read_keys_get(prefix, size=DATA_SIZE, num_keys=10):
    fclient = FastClient(so_bucket="zehric-pywren-149")
    fclient.cache_so()
    t = time.time()
    
    fclient.run_benchmark_gets(num_keys)

    e = time.time()
    return t,e,num_keys

def write_keys(prefix, size=DATA_SIZE, num_keys=10):
    fclient = FastClient(so_bucket="zehric-pywren-149")
    fclient.cache_so()
    t = time.time()
    
    fclient.run_benchmark_puts(num_keys, int(size))

    e = time.time()
    return t,e,num_keys

def profile_iops(results):
    min_time = min(results, key=lambda x: x[0])[0]
    max_time = max(results, key=lambda x: x[1])[1]
    tot_time = (max_time - min_time)

    bins = np.linspace(min_time, max_time, max_time - min_time)
    #return bins, min_time, max_time
    iops = np.zeros(len(bins))

    for start_time, end_time, num_ops in results:
        start_bin, end_bin = np.searchsorted(bins, [int(start_time), int(end_time)])
        iops[start_bin:((end_bin)+1)]  += num_ops/(end_time - start_time)
    return iops, bins

# w = read_keys_get("poop", num_keys=10)
# print(w[1] - w[0])
# r = read_keys_pin("poop", num_keys=100)
# print(r[1] - r[0])
# exit()


session = botocore.session.get_session()

extra_env = {"AWS_DEFAULT_REGION": "us-west-2", "AWS_ACCESS_KEY_ID": session.get_credentials().access_key, "AWS_SECRET_ACCESS_KEY": session.get_credentials().secret_key}

config = pywren.wrenconfig.default()
config['runtime']['s3_bucket'] = 'numpywrenpublic'
key = "pywren.runtime/pywren_runtime-3.6-numpywren.tar.gz"
config['runtime']['s3_key'] = key

pwex = pywren.standalone_executor(config=config)

print("mapping {} keys of size {}".format(NUM_KEYS, DATA_SIZE))
futures = pwex.map(lambda x: read_keys_pin(x, size=DATA_SIZE, num_keys=NUM_KEYS), range(25*36), extra_env=extra_env)

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
print(results[0][1] - results[0][0])
iops, bins = profile_iops(results)

np.savez("s3iops_cache_read_{}".format(OUTPUT_NAME), iops=iops, bins=bins)

plt.plot(bins - min(bins), iops)
plt.ylabel("(read) IOPS/s")
plt.xlabel("time")
plt.savefig('s3iops_cache_read_{}.png'.format(OUTPUT_NAME))
