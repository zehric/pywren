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

import glob
import os
import shutil

import pywren
from . import wrenhandler

def local_handler(payload, run_dir, extra_context=None):
    """
    Run a list of (deserialized) jobs locally inside of
    run_dir

    Just for debugging
    """

    def copy_runtime(tgt_dir):
        files = glob.glob(os.path.join(pywren.SOURCE_DIR, "./*.py"))
        files = glob.glob(os.path.join(pywren.SOURCE_DIR, "jobrunner/*.py"))
        for f in files:
            shutil.copy(f, os.path.join(tgt_dir, os.path.basename(f)))

    original_dir = os.getcwd()

    local_task_run_dir = os.path.join(run_dir, str(os.getpid()))
    if not os.path.exists(local_task_run_dir):
        os.makedirs(local_task_run_dir)
        copy_runtime(local_task_run_dir)


    context = {'jobnum' : os.getpid()}
    if extra_context is not None:
        context.update(extra_context)

    os.chdir(local_task_run_dir)
    # FIXME debug
    wrenhandler.generic_handler(payload, context)

    os.chdir(original_dir)

def clean(local_dir):
    dirs = glob.glob(os.path.join(local_dir, 'pymodules*'))
    dirs.append(os.path.join(local_dir, 'task'))
    dirs.append(os.path.join(local_dir, 'runtimes'))
    for d in dirs:
        shutil.rmtree(d, ignore_errors=True)
    files = [os.path.join(local_dir, 'runtime_download_lock')]
    files += glob.glob(os.path.join(local_dir, 'jobrunner*'))
    files += glob.glob(os.path.join(local_dir, 'condaruntime*'))
    for f in files:
        if os.path.exists(f) or os.path.islink(f):
            os.unlink(f)
