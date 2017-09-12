from __future__ import absolute_import

import json
import os
import threading

import botocore
import botocore.session
from pywren import local


SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))


class LambdaInvoker(object):
    def __init__(self, region_name, lambda_function_name):

        self.session = botocore.session.get_session()

        self.region_name = region_name
        self.lambda_function_name = lambda_function_name
        self.lambclient = self.session.create_client('lambda',
                                                     region_name=region_name)
        self.TIME_LIMIT = True

    def invoke(self, payload):
        """
        Invoke -- return information about this invocation
        """
        self.lambclient.invoke(FunctionName=self.lambda_function_name,
                               Payload=json.dumps(payload),
                               InvocationType='Event')
        # FIXME check response
        return {}

    def config(self):
        """
        Return config dict
        """
        return {'lambda_function_name' : self.lambda_function_name,
                'region_name' : self.region_name}


class DummyInvoker(object):
    """
    A mock invoker that simply appends payloads to a list. You must then
    call run()

    does not delete left-behind jobs

    """

    DEFAULT_RUN_DIR = "/tmp/task"
    def __init__(self):
        self.payloads = []
        self.TIME_LIMIT = False
        self.thread = None

    def invoke(self, payload):
        self.payloads.append(payload)

    def config(self): # pylint: disable=no-self-use
        return {}


    def run_jobs(self, MAXJOBS=-1, run_dir=DEFAULT_RUN_DIR):
        """
        run MAXJOBS in the queue
        MAXJOBS = -1  to run all

        # FIXME not multithreaded safe
        """

        jobn = len(self.payloads)
        if MAXJOBS != -1:
            jobn = MAXJOBS
        jobs = self.payloads[:jobn]

        local.local_handler(jobs, run_dir,
                            {'invoker' : 'DummyInvoker'})

        self.payloads = self.payloads[jobn:]

    def run_jobs_threaded(self, MAXJOBS=-1, run_dir=DEFAULT_RUN_DIR):
        """
        Just like run_jobs but in a separate thread
        (so it's non-blocking)
        """

        self.thread = threading.Thread(target=self.run_jobs,
                                       args=(MAXJOBS, run_dir))
        self.thread.start()
