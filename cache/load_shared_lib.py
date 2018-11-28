import ctypes
import sys
import boto3
import os

if (sys.platform == "linux"):
    END = ".so"
elif (sys.platform == "darwin"):
    END = ".dylib"
else:
    raise Exception("unsupported platform")

def load_shared_lib(key="fastio", bucket="zehric-pywren-149", worker_id=0):
    key += END
    path = '/tmp/sos{}/'.format(worker_id)
    if not os.path.exists(path):
        os.mkdir(path)
    local_path = path + key
    if not os.path.exists(local_path):
        s3 = boto3.resource('s3')
        s3.Bucket(bucket).download_file(key, local_path)
    # sos = ['libaws-cpp-sdk-core.so', 'libaws-cpp-sdk-s3.so', 'libaws-c-event-stream.so',
    #        'libaws-c-common.so.0unstable']
    # for so in sos:
    #     s3.Bucket(bucket).download_file(so, path + so)
    # os.environ['LD_LIBRARY_PATH'] = "{}:{}".format(os.environ.get('LD_LIBRARY_PATH', ''), path)
    return ctypes.cdll.LoadLibrary(local_path)
