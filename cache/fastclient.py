import ctypes
import load_shared_lib


class FastClient(object):

    def __init__(self, so_bucket="zehric-pywren-149"):
        self.so_bucket = so_bucket
        self.so_key = "cache_client"
        self.__so_cached = None
        self.__api_started = False

    @property
    def so(self):
        if (self.__so_cached) == None:
            return load_shared_lib.load_shared_lib(key=self.so_key, bucket=self.so_bucket)
        else:
            return self.__so_cached

    def run_benchmark_pins(self, num, unique, d_ptr_s, d_ptr_f):
        so = self.so
        so.run_benchmark_pins.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.POINTER(ctypes.c_double), ctypes.POINTER(ctypes.c_double)];
        return so.run_benchmark_pins(num, unique, d_ptr_s, d_ptr_f)

    def run_benchmark_gets(self, num):
        so = self.so
        so.run_benchmark_gets.argtypes = [ctypes.c_int];
        return so.run_benchmark_gets(num)

    def run_benchmark_puts(self, num, size):
        so = self.so
        so.run_benchmark_gets.argtypes = [ctypes.c_int, ctypes.c_long];
        return so.run_benchmark_gets(num, size)

    def pin_objects(self, keys, d_ptr_s, d_ptr_f, num_threads=10):
        so = self.so
        num_objects = len(keys)

        char_star_star = ctypes.c_char_p * num_objects
        c_keys_array = char_star_star(*keys)

        so.pin_objects.argtypes = [ctypes.c_long, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(ctypes.c_double), ctypes.POINTER(ctypes.c_double), ctypes.c_int]
        return so.pin_objects(num_objects, c_keys_array, d_ptr_s, d_ptr_f, num_threads)


    def cache_so(self):
        ''' Cache the fastio.so in memory
          NOTE: THIS WILL MAKE THIS OBJECT UNSERIALIZABLE PLEASE CALL
          .uncache_so(self) if you want to send this object over the wire
         '''
        self.__so_cached = self.so

    def uncache_so(self):
        ''' Uncache the fastio.so from memory so object is serializable
         '''
        self.__so_cached = None

if __name__=='__main__':
    import numpy as np
    fclient = FastClient(so_bucket="zehric-pywren-149")
    fclient.cache_so()
    key_list=['broadcast_benchmark/19d65ee5c55b95aa2108ea41908dbe03395b8ee1', 'cache_client.so']
    doubles_s = np.zeros(len(key_list), np.float64)
    doubles_f = np.zeros(len(key_list), np.float64)
    d_ptr_s = doubles_s.ctypes.data_as(ctypes.POINTER(ctypes.c_double))
    d_ptr_f = doubles_f.ctypes.data_as(ctypes.POINTER(ctypes.c_double))
    keys = []
    for key in key_list:
        keys.append(ctypes.c_char_p(key.encode()))

    fclient.pin_objects(keys, d_ptr_s, d_ptr_f, num_threads=len(keys))
    print(list(zip(doubles_s, doubles_f)))
