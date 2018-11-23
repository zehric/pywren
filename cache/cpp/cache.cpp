/*
Copyright 2018 Vaishaal Shankar

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

#include <aws/core/Aws.h>
#include <aws/core/Region.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <fstream>
#include <streambuf>
#include <string>
#include "bufferstream.hpp"
#include <time.h>
#include "threadpool.h"
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <list>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <utility>
#include <tuple>

#include <stdlib.h>

#define ALLOCATION_TAG "NUMPYWREN_FASTIO"
extern "C" {
   int put_object(void* buffer, long buffer_size, const char* bucket, const char* key);
   int get_object(void* buffer, long buffer_size, const char* bucket, const char* key);
   int put_objects(void**obj_buffers, long num_objects, long* buffer_sizes, const char** buckets, const char** keys, int num_threads);
   int get_objects(void**obj_buffers, long num_objects, long* buffer_sizes, const char** buckets, const char** keys, int num_threads);
   void start_api();
   void stop_api();
}

class CacheEntry {
public:
    std::string file_path;
    int fd;
    int ref_cnt;
    bool dirty;
    int size;
    bool loading;
    void *data;
    std::condition_variable cv;
    std::list<std::string>::iterator position;
    CacheEntry(std::string, std::list<std::string>::iterator pos); // constructor
    ~CacheEntry();      // destructor
    void download();
    void memmap();
};

CacheEntry::CacheEntry(std::string path, std::list<std::string>::iterator pos) {
    ref_cnt = 0;
    dirty = false;
    file_path = path;
    position = pos;
    loading = false;
}

CacheEntry::~CacheEntry() {
    /* std::cerr << "evict " << file_path << "\n"; */
    if (dirty) {
        std::cerr << "upload to s3: " << file_path << "\n";
        // upload();
    }
    munmap(data, size);
    ftruncate(fd, 0);
    close(fd);
    remove(file_path.c_str());
}

void CacheEntry::download() {
    // TODO: download from s3();
    std::cerr << "download from s3: " << file_path << "\n";
    memmap();
}

void CacheEntry::memmap() {
    std::cerr << "mmap: " << file_path.c_str() << "\n";
    struct stat stat;
    fd = open(file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666); // TODO: remove
    /* fd = open(file_path.c_str(), O_RDWR); */
    ftruncate(fd, 100); // TODO: remove
    fstat(fd, &stat);
    size = stat.st_size;
    data = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
}

class Cache {
private:
    std::list<std::string> keys;
    std::unordered_map<std::string, CacheEntry *> entries;
    int max_size;
    std::mutex mutex;
    void evict_if_full();
public:
    Cache();
    void get(std::string key);
    void put(std::string key);
    void release(std::string key);
};

Cache::Cache() {
    max_size = 1; // TODO: change this
}

void Cache::get(std::string key) {
    std::unique_lock<std::mutex> lock(mutex);
    std::cerr << "get " << key << "\n";
    CacheEntry *entry;
    if (entries.find(key) != entries.end()) {
        std::cerr << "found " << key << "\n";
        entry = entries[key];
        entry->ref_cnt += 1;
        while (entry->loading) {
            entry->cv.wait(lock);
        }
    } else {
        keys.push_back(key);
        auto it = keys.end();
        entry = new CacheEntry("/tmp/" + key, --it);
        entries[key] = entry;
        entry->loading = true;
        entry->ref_cnt += 1;
        lock.unlock();
        evict_if_full();
        entry->download();
        lock.lock();
        entry->loading = false;
        entry->cv.notify_all();
    }
}

void Cache::release(std::string key) {
    std::unique_lock<std::mutex> lock(mutex);
    std::cerr << "release " << key << "\n";
    CacheEntry *entry = entries[key];
    entry->ref_cnt -= 1;

    keys.erase(entry->position);
    keys.push_back(key);
    auto it = keys.end();
    entry->position = --it;
    lock.unlock();
    evict_if_full();
}

void Cache::put(std::string key) {
    std::unique_lock<std::mutex> lock(mutex);
    std::cerr << "put " << key << "\n";
    CacheEntry *entry;
    if (entries.find(key) != entries.end()) {
        entry = entries[key];
        keys.erase(entry->position);
        keys.push_back(key);
        auto it = keys.end();
        entry->position = --it;
        entry->dirty = true;
    } else {
        keys.push_back(key);
        auto it = keys.end();
        entry = new CacheEntry("/tmp/" + key, --it);
        entry->memmap();
        entries[key] = entry;
        entry->dirty = true;
        lock.unlock();
        evict_if_full();
    }
}

void Cache::evict_if_full() {
    std::unique_lock<std::mutex> lock(mutex);
    if (keys.size() > max_size) {
        for (auto &key : keys) {
            CacheEntry *entry = entries[key];
            if (entry->ref_cnt < 1) {
                std::cerr << "evict " << key << "\n";
                entries.erase(key);
                keys.erase(entry->position);
                lock.unlock();
                delete entry; // should call destructor
                return;
            }
        }
    }
}


typedef Aws::S3::S3Client S3Client;

int _put_object_internal(Aws::S3::S3Client &client, char* &buffer, long buffer_size, const char* bucket, const char* key) {
    Aws::S3::Model::PutObjectRequest request;
    auto bstream = new boost::interprocess::bufferstream((char*) buffer, buffer_size);
    std::shared_ptr<Aws::IOStream> objBuffer =  std::shared_ptr<Aws::IOStream>(bstream);
    request.WithBucket(bucket).WithKey(key).SetBody(objBuffer);
    auto put_object_response = client.PutObject(request);
    if (!put_object_response.IsSuccess())
    {
        std::cout << "PutObject error: " <<
            put_object_response.GetError().GetExceptionName() << " " <<
            put_object_response.GetError().GetMessage() << std::endl;
        return -1;
    } else {
        return 0;
    }
}

int _get_object_internal(Aws::S3::S3Client &client, char* &buffer, long buffer_size, const char* bucket, const char* key) {

    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(bucket).WithKey(key);
    request.SetResponseStreamFactory(
        [buffer, buffer_size]()
        {
            return Aws::New<boost::interprocess::bufferstream>(ALLOCATION_TAG, (char*) buffer, buffer_size);
        });
    auto get_object_response = client.GetObject(request);
    if (!get_object_response.IsSuccess())
    {
        std::cout << "GetObject error: " << (int) get_object_response.GetError().GetResponseCode() << " " << get_object_response.GetError().GetMessage() << std::endl;
        std::cout << "BUCKET" << bucket  << std::endl;
        std::cout << "key" << key << std::endl;
        return -1;
    } else {
        return 0;
    }
}

int put_objects(void**obj_buffers, long num_objects, long* buffer_sizes, const char** buckets, const char** keys, int num_threads) {
    ThreadPool threadpool(num_threads);
    std::vector<std::future<int>> put_futures;
    for (int i = 0; i < num_objects; i++) {
        char* buffer_to_use = (char*) obj_buffers[i];
        auto future = threadpool.enqueue(put_object, buffer_to_use, buffer_sizes[i], buckets[i], keys[i]);
        put_futures.push_back(std::move(future));
    }

    for (int i = 0; i < num_objects; i++) {
        auto res = put_futures[i].get();
    }

}

int get_objects(void**obj_buffers, long num_objects, long* buffer_sizes, const char** buckets, const char** keys, int num_threads) {
    ThreadPool threadpool(num_threads);
    std::vector<std::future<int>> get_futures;
    for (int i = 0; i < num_objects; i++) {
        char* buffer_to_use = (char*) obj_buffers[i];
        auto future = threadpool.enqueue(get_object, buffer_to_use, buffer_sizes[i], buckets[i], keys[i]);
        get_futures.push_back(std::move(future));
    }

    for (int i = 0; i < num_objects; i++) {
        auto res = get_futures[i].get();
    }
}

int put_object(void* buffer, long buffer_size, const char* bucket, const char* key) {
    auto region = Aws::Region::US_WEST_2;
    Aws::Client::ClientConfiguration cfg;
    cfg.region = region;
    Aws::S3::S3Client s3_client(cfg);
    char* char_buffer = (char*) buffer;
    int ret = _put_object_internal(s3_client, char_buffer, buffer_size, bucket, key);
    return ret;
}

int get_object(void* buffer, long buffer_size, const char* bucket, const char* key) {
    auto region = Aws::Region::US_WEST_2;
    Aws::Client::ClientConfiguration cfg;
    cfg.region = region;
    Aws::S3::S3Client s3_client(cfg);
    char* char_buffer = (char*) buffer;
    int ret = _get_object_internal(s3_client, char_buffer, buffer_size, bucket, key);
    return ret;
}

void start_api() {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
}

void stop_api() {
    Aws::SDKOptions options;
    Aws::ShutdownAPI(options);
}

int test_cache_threadfunc(Cache *cache, std::string tid) {
    for (int i = 0; i < 5; i++) {
        int r = rand() % 5;
        cache->get(std::to_string(r));
        cache->put(std::to_string(r));
        cache->release(std::to_string(r));
    }
    return 0;
}

int main(int argc, char** argv)
{
    if (argc < 5)
    {
        std::cout << std::endl <<
            "This benchmark will upload data to s3 and then download it "
            << std::endl << "" << std::endl << std::endl <<
            "Ex: fastio <objsizebytes> <num_objects> <bucketname> <prefix>\n" << std::endl;
        exit(1);
    }

    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        auto objsizebytes = std::stol(argv[1]);
        auto num_objects = std::stol(argv[2]);
        auto bucket = std::string(argv[3]);
        auto prefix = std::string(argv[4]);

        std::cout << "Object Size " << argv[1] << std::endl;
        std::cout << "num_objects " << argv[2] << std::endl;
        std::cout << "buffersize " << objsizebytes*num_objects << std::endl;

        ThreadPool pool(num_objects);
        Cache *cache = new Cache();
        
        std::vector<std::future<int>> get_futures;
        for (int i = 0; i < num_objects; i++) {
            auto future = pool.enqueue(test_cache_threadfunc, cache, std::to_string(i));
            get_futures.push_back(std::move(future));
        }

        for (int i = 0; i < num_objects; i++) {
            auto res = get_futures[i].get();
        }

        delete cache;
    }

    Aws::ShutdownAPI(options);

}

