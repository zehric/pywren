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
#include <unistd.h>
#include <pthread.h>
#include <zmq.hpp>

#define ALLOCATION_TAG "CACHE"
int get_object(void* buffer, long buffer_size, const char* bucket, const char* key);
int put_object(void* buffer, long buffer_size, const char* bucket, const char* key);

static char *bucket;
static long objsizebytes;

static std::string random_string(int size) {
    std::string str;
    for (size_t i = 0; i < size; i++) {
         int randomChar = rand()%(26+26+10);
         if (randomChar < 26)
             str.push_back('a' + randomChar);
         else if (randomChar < 26+26)
             str.push_back('A' + randomChar - 26);
         else
             str.push_back('0' + randomChar - 26 - 26);
    }
    return str;
}

class CacheEntry {
public:
    std::string key;
    std::string file_path;
    int fd;
    int ref_cnt;
    bool dirty;
    long size;
    bool loading;
    void *data;
    std::condition_variable cv;
    std::list<std::string>::iterator position;
    CacheEntry(std::string, std::string path); // constructor
    ~CacheEntry();      // destructor
    void download(long);
    void memmap(long);
};

CacheEntry::CacheEntry(std::string k, std::string path) {
    ref_cnt = 0;
    dirty = false;
    key = k;
    file_path = path;
    loading = false;
}

CacheEntry::~CacheEntry() {
    if (dirty) {
        /* std::cerr << "upload to s3: " << file_path << "\n"; */
        put_object(data, size, bucket, key.c_str());
    }
    munmap(data, size);
    ftruncate(fd, 0);
    close(fd);
    remove(file_path.c_str());
}

void CacheEntry::download(long _size) {
    memmap(_size);
    // TODO: download from s3();
    get_object(data, size, bucket, key.c_str());
    /* std::cerr << "download from s3: " << file_path << "\n"; */
}

void CacheEntry::memmap(long _size) {
    if (_size != -1) {
        fd = open(file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
        size = _size;
        ftruncate(fd, size);
    } else {
        fd = open(file_path.c_str(), O_RDWR);
        struct stat stat;
        fstat(fd, &stat);
        size = stat.st_size;
    }
    data = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
}

class Cache {
private:
    std::list<std::string> keys;
    std::unordered_map<std::string, CacheEntry *> entries;
    int max_size;
    int hits;
    std::mutex mutex;
    std::condition_variable evict_cv;
    void evict_if_full(std::unique_lock<std::mutex>& lock);
public:
    Cache(int size);
    std::string get(std::string key);
    void put(std::string key);
    void release(std::string key);
};

Cache::Cache(int size) {
    max_size = size;
    hits = 0;
}

std::string Cache::get(std::string key) {
    std::unique_lock<std::mutex> lock(mutex);
    CacheEntry *entry;
    if (entries.find(key) != entries.end()) {
        entry = entries[key];
        entry->ref_cnt += 1;
        while (entry->loading) {
            entry->cv.wait(lock);
        }
        /* hits++; */
        /* std::cout << "hit " << hits << std::endl; */
    } else {
        auto file_path = "/tmp/" + key + "-" + random_string(20);
        entry = new CacheEntry(key, file_path);
        entries[key] = entry;
        entry->loading = true;
        entry->ref_cnt += 1;
        evict_if_full(lock);
        keys.push_back(key);
        auto it = keys.end();
        entry->position = --it;
        lock.unlock();
        entry->download(objsizebytes);
        lock.lock();
        entry->loading = false;
        entry->cv.notify_all();
    }
    return entry->file_path;
}

void Cache::release(std::string key) {
    std::unique_lock<std::mutex> lock(mutex);
    CacheEntry *entry = entries[key];
    entry->ref_cnt -= 1;

    keys.erase(entry->position);
    keys.push_back(key);
    auto it = keys.end();
    entry->position = --it;
    if (entry->ref_cnt == 0) {
        evict_cv.notify_one();
    }
    /* lock.unlock(); */
    /* evict_if_full(); */
}

/* FIXME: The cache currently cannot handle simultaneous gets and puts to
 * the same key. This is fine for numpywren but should be changed for general
 * purpose usage. */
void Cache::put(std::string key) {
    std::unique_lock<std::mutex> lock(mutex);
    CacheEntry *entry;
    if (entries.find(key) != entries.end()) {
        entry = entries[key];
        keys.erase(entry->position);
        keys.push_back(key);
        auto it = keys.end();
        entry->position = --it;
        entry->dirty = true;
    } else {
        entry = new CacheEntry(key, "/tmp/" + key); // TODO: change this to receive path from client
        entries[key] = entry;
        evict_if_full(lock);
        keys.push_back(key);
        auto it = keys.end();
        entry->position = --it;
        entry->memmap(-1);
        entry->dirty = true;
    }
}

void Cache::evict_if_full(std::unique_lock<std::mutex> &lock) {
    if (keys.size() >= max_size) {
        while (1) {
            for (auto &key : keys) {
                CacheEntry *entry = entries[key];
                if (entry->ref_cnt < 1) {
                    entries.erase(key);
                    keys.erase(entry->position);
                    lock.unlock();
                    delete entry; // should call destructor
                    lock.lock();
                    return;
                }
            }
            evict_cv.wait(lock);
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

/* int test_cache_threadfunc(Cache *cache, std::string tid) { */
/*     for (int i = 0; i < 5; i++) { */
/*         int r = rand() % 5; */
/*         cache->get(std::to_string(r)); */
/*         cache->release(std::to_string(r)); */
/*     } */
/*     return 0; */
/* } */
static Cache *cache;

void *worker_func (void *arg) {
    zmq::context_t *context = (zmq::context_t *) arg;

    zmq::socket_t socket(*context, ZMQ_REP);
    socket.connect("inproc://workers");

    while (true) {
        //  Wait for next request from client
        zmq::message_t request;
        socket.recv(&request);

        char *data = (char *) request.data();
        char op = data[0];
        std::string key = std::string(data + 1);
        /* std::cerr << "received key " << key << std::endl; */
        std::string retmsg = "success";
        switch (op) {
            case 0:
                retmsg = cache->get(key);
                break;
            case 1: // Nothing
                /* cache->release(key); */
                /* break; */
            case 2:
                cache->put(key);
                break;
            default:
                throw std::runtime_error("unsupported opcode" + std::to_string((int) op));
        }

        //  Send reply back to client
        int len = strlen(retmsg.c_str()) + 1;
        zmq::message_t reply(len);
        memcpy((void *) reply.data (), retmsg.c_str(), len);
        socket.send(reply);
    }
    return (NULL);
}

void *releaser_func (void *arg) {
    zmq::context_t *context = (zmq::context_t *) arg;

    zmq::socket_t socket(*context, ZMQ_REP);
    socket.bind("ipc:///tmp/local_cache_release");

    while (true) {
        //  Wait for next request from client
        zmq::message_t request;
        socket.recv(&request);

        char *data = (char *) request.data();
        char op = data[0];
        std::string key = std::string(data + 1);

        if (op != 1) {
            throw std::runtime_error("unsupported opcode" + std::to_string((int) op));
        }

        cache->release(key);

        //  Send reply back to client
        std::string retmsg = "success";
        int len = strlen(retmsg.c_str()) + 1;
        zmq::message_t reply(len);
        memcpy((void *) reply.data (), retmsg.c_str(), len);
        socket.send(reply);
    }
    return (NULL);
}

int main(int argc, char** argv)
{
    if (argc < 5)
    {
        std::cout << std::endl <<
            "This benchmark will upload data to s3 and then download it "
            << std::endl << "" << std::endl << std::endl <<
            "Ex: cache <objsizebytes> <num_threads> <bucketname> <cache_size>\n" << std::endl;
        exit(1);
    }
    srand(time(0));

    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        objsizebytes = std::stol(argv[1]); // FIXME: this is a hack
        auto num_threads = std::stol(argv[2]);
        bucket = argv[3];
        auto cache_size = std::stoi(argv[4]);

        std::cout << "Object Size " << argv[1] << std::endl;
        std::cout << "num_threads " << argv[2] << std::endl;
        std::cout << "bucket " << argv[3] << std::endl;
        std::cout << "cache size " << argv[4] << std::endl;

        /* for (int i = 0; i < 5; i++) { */
        /*     std::string path = "/tmp/" + std::to_string(i); */
        /*     int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666); */
        /*     ftruncate(fd, objsizebytes); */
        /*     close(fd); */
        /*     cache->put(std::to_string(i)); */
        /* } */
        /* cache->get(std::to_string(2)); */
        /* cache->get(std::to_string(3)); */

        cache = new Cache(cache_size);

        // Prepare our context and sockets
        zmq::context_t context(1);
        zmq::socket_t clients(context, ZMQ_ROUTER);
        clients.bind("ipc:///tmp/local_cache");
        zmq::socket_t workers(context, ZMQ_DEALER);
        workers.bind("inproc://workers");

        //  Launch pool of worker threads
        for (int thread_nbr = 0; thread_nbr < num_threads; thread_nbr++) {
            pthread_t worker;
            pthread_create (&worker, NULL, worker_func, (void *) &context);
        }
        pthread_t releaser;
        pthread_create(&releaser, NULL, releaser_func, (void *) &context);
        //  Connect work threads to client threads via a queue
        zmq::proxy (clients, workers, NULL);

        /* std::vector<std::future<int>> get_futures; */
        /* for (int i = 0; i < num_threads; i++) { */
        /*     auto future = pool.enqueue(cache_threadfunc, cache; */
        /*     get_futures.push_back(std::move(future)); */
        /* } */

        /* for (int i = 0; i < num_threads; i++) { */
        /*     auto res = get_futures[i].get(); */
        /* } */

        delete cache;
    }

    Aws::ShutdownAPI(options);

}

