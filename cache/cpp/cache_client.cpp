#include <fstream>
#include <string>
#include <zmq.hpp>
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>

extern "C" {
    int run_benchmark_gets(int num);
    int run_benchmark_pins(int num);
    int run_benchmark_puts(int num, long size);
}

int run_benchmark_pins(int num)
{
    srand(time(0));
    std::string prefix = std::string("test");
    //  Prepare our context and socket
    zmq::context_t context (1);
    zmq::socket_t socket (context, ZMQ_REQ);

    socket.connect ("ipc:///tmp/local_cache");
    struct stat stat;
    int fd;
    long size;
    void *data;

    for (int request_nbr = 0; request_nbr < num; request_nbr++) {
        std::string key = prefix + std::to_string(rand() % num);
        int len = strlen(key.c_str()) + 1;
        zmq::message_t request (len + 1);
        char *msg = (char *) request.data();
        msg[0] = 0;
        memcpy (msg + 1, key.c_str(), len);
        socket.send (request);

        //  Get the reply.
        zmq::message_t reply;
        socket.recv (&reply);

        std::string path = "/tmp/" + key;
        fd = open(path.c_str(), O_RDWR);
        fstat(fd, &stat);
        size = stat.st_size;
        data = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        munmap(data, size);

        zmq::message_t request2(len + 1);
        msg = (char *) request2.data();
        msg[0] = 1;
        memcpy (msg + 1, key.c_str(), len);
        socket.send (request2);

        //  Get the reply.
        socket.recv (&reply);
    }
    return 0;
}

int run_benchmark_gets(int num)
{
    srand(time(0));
    std::string prefix = std::string("test");
    //  Prepare our context and socket
    zmq::context_t context (1);
    zmq::socket_t socket (context, ZMQ_REQ);

    socket.connect ("ipc://local_cache");
    struct stat stat;
    int fd;
    long size;
    void *data;

    for (int request_nbr = 0; request_nbr < num; request_nbr++) {
        std::string key = prefix + std::to_string(rand() % num);
        int len = strlen(key.c_str()) + 1;
        zmq::message_t request (len + 1);
        char *msg = (char *) request.data();
        msg[0] = 0;
        memcpy (msg + 1, key.c_str(), len);
        socket.send (request);

        //  Get the reply.
        zmq::message_t reply;
        socket.recv (&reply);

        std::string path = "/tmp/" + key;
        fd = open(path.c_str(), O_RDWR);
        fstat(fd, &stat);
        size = stat.st_size;
        data = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        void *local = malloc(size);
        memcpy(local, data, size);
        munmap(data, size);

        zmq::message_t request2(len + 1);
        msg = (char *) request2.data();
        msg[0] = 1;
        memcpy (msg + 1, key.c_str(), len);
        socket.send (request2);

        //  Get the reply.
        socket.recv (&reply);
	free(local);
    }
    return 0;
}

int run_benchmark_puts(int num, long size)
{
    std::string prefix = std::string("test");
    //  Prepare our context and socket
    zmq::context_t context (1);
    zmq::socket_t socket (context, ZMQ_REQ);

    socket.connect ("ipc://local_cache");
    int fd;
    void *data;


    for (int request_nbr = 0; request_nbr < num; request_nbr++) {

        std::string key = prefix + std::to_string(request_nbr);
        std::string path = "/tmp/" + key;
        FILE *fp = fopen(path.c_str(), "w+");
        for (int i = 0; i < size; i++) {
            fputc(rand() % (1 << 8), fp);
        }
        fclose(fp);
        fd = open(path.c_str(), O_RDWR);
        data = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        int len = strlen(key.c_str()) + 1;
        zmq::message_t request (len + 1);
        char *msg = (char *) request.data();
        msg[0] = 2;
        memcpy (msg + 1, key.c_str(), len);
        socket.send (request);

        //  Get the reply.
        zmq::message_t reply;
        socket.recv (&reply);

        munmap(data, size);
    }
    return 0;
}


