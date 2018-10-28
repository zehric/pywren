#
# Copyright 2018 PyWren Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import absolute_import


import collections
import os
import mmap
import tempfile

import boto3

TEMP_DIR = tempfile.gettempdir()

class Entry(object):

    def __init__(self, key, no_mmap=False):
        self.ref_cnt = 0
        self.dirty = False
        self.file_path = os.path.join(TEMP_DIR, key)
        self.fd = os.open(self.file_path, os.O_RDWR | os.O_CREAT)
        self.data = None
        if not no_mmap:
            self.memmap()

    def memmap(self):
        self.data = mmap.mmap(self.fd, 0)

    def close(self):
        self.data.close()
        os.ftruncate(self.fd, 0)
        os.close(self.fd)
        os.unlink(self.file_path)


class Cache(object):
    """
    A Cache object caches data from S3.
    """
    # TODO: separate notion of S3 keys from file name
    def __init__(self, config):
        self.storage = Storage(config)
        self.cache = collections.OrderedDict()
        self.max_entries = 10 # TODO: to change this

    def release(self, key):
        """
        Decrements the reference counter for the cache entry.
        :param key: data key
        :return: bool success
        """
        entry = self.cache.pop(key)
        entry.ref_cnt -= 1
        self.cache[key] = entry
        return True

    def evict_if_full(self):
        if len(self.cache) >= self.max_entries:
            for key, entry in self.cache.items():
                if entry.ref_cnt == 0:
                    del self.cache[key]
                    if entry.dirty:
                        self.storage.store(key, entry)
                    entry.close()
                    return
        # TODO: do something if full

    def get(self, key):
        """
        Requests key from the cache. If it does not exist, request the data
        from the backing store. Pins the cache entry as well.
        :param key: data key
        :return: str path to shared data
        """
        if key in self.cache:
            entry = self.cache[key]
        else:
            entry = Entry(key, True)
            self.storage.load(key, entry)
            self.evict_if_full()
            self.cache[key] = entry

        entry.ref_cnt += 1
        return entry.file_path

    def put(self, key):
        """
        Marks data as dirty. On eviction, the cache writes the data to
        the backing store. Does not pin the cache entry, but this counts
        as an access.
        :param key: data key
        :return: bool success
        """
        if key in self.cache:
            entry = self.cache.pop(key)
            self.cache[key] = entry
        else:
            self.evict_if_full()
            entry = Entry(key)
            self.cache[key] = entry

        entry.dirty = True
        return True

class Storage(object):
    """
    A Storage object interfaces between the cache and the backing store.
    """

    def __init__(self, s3_bucket):
        # if config['storage_backend'] == 's3':
        #     self.s3_bucket = config['backend_config']['bucket']
        #     self.s3client = boto3.client('s3')
        #     # self.session = botocore.session.get_session()
        #     # self.s3client = self.session.create_client(
        #     #     's3', config=botocore.client.Config(max_pool_connections=200))
        # else:
        #     raise NotImplementedError(("Using {} as storage backend is" +
        #                                "not supported yet").format(config['storage_backend']))
        self.s3_bucket = s3_bucket
        self.s3client = boto3.client('s3')

    def load(self, key, cache_entry):
        with open(cache_entry.file_path, 'wb+') as f:
            self.s3client.download_fileobj(self.s3_bucket, key, f)
            cache_entry.memmap()

    def store(self, key, cache_entry):
        self.s3client.upload_fileobj(cache_entry.data, self.s3_bucket, key)

# if __name__ == "__main__":
#     import pywren
#     import pywren.wrenconfig as wrenconfig
#     import time
#     import numpy as np

#     storage_config = wrenconfig.extract_storage_config(wrenconfig.default())
#     cache = Cache(storage_config)
#     key_pre = "test{}"
#     for i in range(10):
#         key = key_pre.format(i)
#         randarr = np.full((4096, 4096), i, dtype='float64')
#         arr = np.memmap("/tmp/" + key, dtype='float64', mode='w+', shape=(4096,4096))
#         arr[:] = randarr[:]
#         start_time = time.time()
#         cache.put(key)
#         print("put: {}".format(time.time() - start_time))

#     for key, value in cache.cache.items():
#         print("cache entry: {}".format(key))

#     for i in range(10):
#         key = key_pre.format(i)
#         start_time = time.time()
#         cache.get(key)
#         print("{} get: ".format(time.time() - start_time))
#         arr = np.memmap("/tmp/" + key, dtype='float64', mode='r', shape=(4096,4096))
#         print(arr)
#         cache.release(key)
