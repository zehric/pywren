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
        so.run_benchmark_pins.arg_types = [ctypes.c_int, ctypes.c_int, ctypes.POINTER(ctypes.c_double), ctypes.POINTER(ctypes.c_double)];
        return so.run_benchmark_pins(num, unique, d_ptr_s, d_ptr_f)

    def run_benchmark_gets(self, num):
        so = self.so
        so.run_benchmark_gets.arg_types = [ctypes.c_int];
        return so.run_benchmark_gets(num)

    def run_benchmark_puts(self, num, size):
        so = self.so
        so.run_benchmark_gets.arg_types = [ctypes.c_int, ctypes.c_long];
        return so.run_benchmark_gets(num, size)

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
