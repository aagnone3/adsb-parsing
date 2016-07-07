# -*- coding: utf-8 -*-
"""
ADSB Virtual Radar Server (VRS) parser.
Follows the protocol defined below
    http://www.virtualradarserver.co.uk/documentation/Formats/AircraftList.aspx
"""

import logging
import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
import requests
import simplejson

from adsb_parsing.utils import timestamped_file_name

TIME_STR = timestamped_file_name()
DUMP_PATH = "../data/adsb_exchange_data_{}.h5".format(TIME_STR)

logging.basicConfig(filename="../logs/munge_{}.log".format(TIME_STR),
                    level=logging.DEBUG)


class DurationTask(object):

    SECONDS_PER_HOUR = 3600
    ITERATION_BREAK_INTERVAL = 10
    ITERATION_BREAK_LENGTH = 2

    def __init__(self, duration, task):
        self.duration = duration
        self.start_time = datetime.now()
        self.task = task
        self.iteration_count = 0

    def elapsed_time_hours(self):
        return (datetime.now() - self.start_time).seconds / DurationTask.SECONDS_PER_HOUR

    def allow_user_safe_exit(self):
        self.iteration_count += 1
        if self.iteration_count == DurationTask.ITERATION_BREAK_INTERVAL:
            print("Allowing {} seconds to end program...".format(DurationTask.ITERATION_BREAK_LENGTH))
            time.sleep(2)
            print("Continuing dump to {}".format(DUMP_PATH))
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


class ADSBLogger(object):
    # URI for data
    LNK = "http://global.adsbexchange.com/VirtualRadar/AircraftList.json"
    # expected column fields and data types for the received data in the aircraft list
    DATA_SPECS = pd.DataFrame([
        ['Id', int, -1],
        ['TSecs', int, -1],
        ['Rcvr', int, -1],
        ['Icao', str, ""],
        ['Bad', bool, False],
        ['Reg', str, ""],
        ['Alt', int, -1],
        ['AltT', int, -1],
        ['TAlt', int, -1],
        ['Call', str, ""],
        ['CallSus', bool, False],
        ['Lat', float, -1.0],
        ['Long', float, -1.0],
        ['PosTime', int, -1],
        ['Spd', int, -1],
        ['SpdTyp', int, -1],
        ['Vsi', int, -1],
        ['VsiT', int, -1],
        ['Trak', int, -1],
        ['TrkH', bool, False],
        ['TTrk', int, -1],
        ['Mdl', str, ""],
        ['Type', str, ""],
        ['From', str, ""],
        ['To', str, ""],
        # ['Stops', str, ""],
        ['Op', str, ""],
        ['OpCode', str, ""],
        ['Sqk', int, -1],
        ['Help', bool, False],
        ['Dst', int, -1],
        ['Brng', int, -1],
        ['WTC', int, -1],
        ['Engines', str, ""],
        ['EngType', int, -1],
        ['Species', int, -1],
        ['Mil', bool, False],
        ['Cou', str, ""],
        ['HasPic', bool, False],
        ['PicX', int, -1],
        ['PicY', int, -1],
        ['FlightsCount', int, -1],
        ['CMsgs', int, -1],
        ['Gnd', bool, False],
        ['Tag', str, ""],
        ['Interested', bool, False],
        ['TT', str, ""],
        ['Trt', int, -1],
        # ['Cos', ],
        # ['Cot', ],
        ['ResetTrail', bool, False],
        ['HasSig', bool, False],
        ['Sig', int, -1],
    ], columns=["name", "type", "default"])
    FIELDS = DATA_SPECS["name"].tolist()
    TYPES = DATA_SPECS["type"].tolist()
    DEFAULTS = pd.Series(DATA_SPECS["default"].tolist(), index=FIELDS)
    # minimum string sizes for each field, helps to detect corrupt data sent from the source
    MIN_STR_SIZES = {
        'Icao': 32,
        'Reg': 32,
        'Call': 48,
        'Mdl': 32,
        'Type': 32,
        'From': 4,
        'To': 4,
        'Op': 32,
        'OpCode': 32,
        'Engines': 32,
        'Cou': 32,
        'Tag': 32,
        'TT': 32,
    }

    def __init__(self, dump_file, file_exists):
        self.dump_file = dump_file
        self.file_exists = file_exists
        self.lastDataId = ""
        self.num_entries = 0
        self.formatted_df = None

    def more(self):
        # get new data, formatted as a DataFrame with no missing values
        data = ADSBLogger.get_data()

        # ensure that a new version of the aircraft list has been supplied
        if data and data["lastDv"] != self.lastDataId:
            # format the data and append it to the file
            self.formatted_df = ADSBLogger.format_data(data)
            self.append_to_file(data["totalAc"])

            # update the version of the aircraft list to avoid duplicate data
            self.lastDataId = data["lastDv"]
        elif not data:
            print("Error retrieving data from source.")

    def append_to_file(self, num_entries):
        # create/append to existing data file1
        try:
            with pd.HDFStore(self.dump_file) as hdf:
                if not self.file_exists:
                    hdf.put('/data/formatted', self.formatted_df, format='table', data_columns=True,
                            min_itemsize=ADSBLogger.MIN_STR_SIZES)
                    self.file_exists = True
                else:
                    hdf.append('/data/formatted', self.formatted_df, format='table', data_columns=True,
                               min_itemsize=ADSBLogger.MIN_STR_SIZES)
        except Exception as e:
            print("Error writing to HDF5 file. Saving df off to data/exception_info.pkl")
            print(e.args)
            self.formatted_df.to_pickle("../data/exception_info.pkl")

        # maintain the size of the data set, and report a rough MB file size estimate
        self.num_entries += num_entries
        print("Saved {} entries so far, total file size of ~{} MB".format(self.num_entries,
                                                                          os.path.getsize(self.dump_file) >> 20))

    @staticmethod
    def get_data():
        data = None
        try:
            data = requests.get(ADSBLogger.LNK).json()
        except simplejson.scanner.JSONDecodeError:
            print("""Invalid response received, and could not decode the JSON as a result.
            Ignoring this iteration of received data.""")
        finally:
            return data

    @staticmethod
    def format_data(data):
        # ensure no missing values, and avoid string of overflow length
        df = pd.DataFrame(data["acList"], columns=ADSBLogger.FIELDS)\
            .applymap(lambda x: np.nan if isinstance(x, str) and x == "" else x)\
            .applymap(lambda x: x[:47] if isinstance(x, str) and len(x) > 48 else x)\
            .fillna(ADSBLogger.DEFAULTS)
        df.to_pickle("../data/exception_info.pkl")

        # coerce data types
        for i, col in enumerate(df.columns):
            df[col] = df[col].astype(ADSBLogger.TYPES[i])
        return df

    @staticmethod
    def get_formatted_data():
        return ADSBLogger.format_data(ADSBLogger.get_data())


if __name__ == '__main__':
    print(DUMP_PATH)
    adsb_logger = ADSBLogger(DUMP_PATH, file_exists=False)

    task_runner = DurationTask(3.95, adsb_logger.more)
    task_runner.start()
