# -*- coding: utf-8 -*-
"""
Created on Fri May 20 13:44:06 2016

@author: aagnone3-gtri
"""

import os
import time
from threading import Timer
from datetime import datetime


def timestamped_file_name(parent_dir_name=None):
    local_name = datetime.now().__str__().replace(':', '-')
    if parent_dir_name:
        return os.sep.join([parent_dir_name, local_name])
    return local_name
    
                       
def get_dir_size_gb(dir_name):
    size = 0
    for path, dirs, files in os.walk(dir_name):
        for file in files:
            filename = os.path.join(path, file)
            size += os.path.getsize(filename)
    return size / 1e9


class DurationTaskRunner(object):

    SECONDS_PER_HOUR = 3600
    ITERATION_BREAK_INTERVAL = 10
    ITERATION_BREAK_LENGTH = 2

    def __init__(self, duration, task):
        self.duration = duration
        self.start_time = datetime.now()
        self.task = task
        self.iteration_count = 0

    def elapsed_time_hours(self):
        return (datetime.now() - self.start_time).seconds / DurationTaskRunner.SECONDS_PER_HOUR

    def allow_user_safe_exit(self):
        self.iteration_count += 1
        if self.iteration_count == DurationTaskRunner.ITERATION_BREAK_INTERVAL:
            print("Allowing {} seconds to end program...".format(DurationTaskRunner.ITERATION_BREAK_LENGTH))
            time.sleep(2)
            print("Continuing data dump")
            self.iteration_count = 0

    def start(self):
        # log for the desired number of hours before closing.
        # when iteration_count reaches DurationTask.ITERATION_BREAK_INTERVAL, allow the user a few seconds to
        # close the program without risk of closing during an I/O process
        while self.elapsed_time_hours() < self.duration:
            # invoke the task function
            self.task()
            # give the user a chance to safely break the program every once in a while, as defined by the constant
            # ITERATION_BREAK_INTERVAL
            self.allow_user_safe_exit()

        print("Completed desired data log time of {} hours".format(self.duration))


class RepeatedTimer(Timer):
    """
    See: https://hg.python.org/cpython/file/2.7/Lib/threading.py#l1079
    """
    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            self.function(*self.args, **self.kwargs)
        self.finished.set()
