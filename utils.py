# -*- coding: utf-8 -*-
"""
Created on Fri May 20 13:44:06 2016

@author: aagnone3-gtri
"""

import os
import datetime


def timestamped_file_name(parent_dir_name=None):
    local_name = datetime.datetime.now().__str__().replace(':', '-')
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
