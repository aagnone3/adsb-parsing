"""
ADSB SBS-1 parsing utilties
Derived from the protocol defined at the link below
    http://woodair.net/sbs/Article/Barebones42_Socket_Data.htm
"""
import os
import threading
import pandas as pd
import logging
import time

log = logging.getLogger(__name__)
# Specify the field names and data types
DATA_SPECS = pd.DataFrame([["message_type", str],
                           ["transmission_type", str],
                           ["session_id", int],
                           ["aircraft_id", int],
                           ["hex_ident", str],
                           ["flight_id", int],
                           ["date_message_generated", str],
                           ["time_message_generated", str],
                           ["date_message_logged", str],
                           ["time_message_logged", str],
                           ["call_sign", str],
                           ["altitude", int],
                           ["ground_speed", float],
                           ["track", float],
                           ["latitude", float],
                           ["longitude", float],
                           ["vertical_rate", int],
                           ["squawk", int],
                           ["alert_squawk_change", int],
                           ["emergency", int],
                           ["spi_ident", int],
                           ["ss_on_ground", int],
                           ], columns=["name", "dtype"])
FIELDS = DATA_SPECS['name'].tolist()
DTYPES = DATA_SPECS['dtype'].tolist()
NUM_FIELDS = len(FIELDS)


class SBS1Parser(object):
    """
    Handles the formatting and DataFrame appending for the SBS-1 data
    """
    def __init__(self, file_name):
        self.lock = threading.Lock()
        self.file_name = file_name
        self.df = pd.DataFrame(columns=FIELDS)
        self.last_seen_icao = None
        self.current_entry = None
        self.file_append_process = None

    def process(self, entry):
        msg_type = entry[0]
        if msg_type == "MSG":
            # Add the completed entry to the DataFrame when we see a new ICAO
            # Otherwise, keep merging in new messages for the current ICAO
            if entry[4] != self.last_seen_icao:
                # Append the completed entry to the DataFrame in a subprocess
                if self.current_entry is not None and\
                                sorted(self.current_entry.seen_message_types) in SBS1Entry.VALID_MESSAGE_SETS:
                    with self.lock:
                        current_df = pd.Series(data=self.current_entry.data,
                                               index=FIELDS)
                        self.df = self.df.append(current_df, ignore_index=True)
                # Regardless, create a new entry for the new ICAO
                self.current_entry = TransmissionMsgEntry(entry)
            else:
                # Merge in new data for the current ICAO
                self.current_entry.more_data(entry)
        # Update the last seen ICAO
        self.last_seen_icao = entry[4]

    def print_memory_usage(self):
        print("Processed ~{:.2f} MB, ~{:.2f} MB total".format(self.df.memory_usage(index=False).sum() >> 20,
                                                              os.path.getsize(self.file_name) >> 20))

    class WriteSBS1(threading.Thread):
        """
        Inner threading class to do file I/O in parallel
        """
        # TODO do this with the multiprocessing module for true parallel processing
        def __init__(self, lock, parser):
            super().__init__()
            self.lock = lock
            self.parser = parser

        def run(self):
            start_time = time.time()
            with self.lock:
                # Coerce data types
                for i, col in enumerate(self.parser.df.columns):
                    self.parser.df[col] = self.parser.df[col].astype(DTYPES[i])
                # Save off the file
                with pd.HDFStore(self.parser.file_name) as hdf:
                    hdf.put('/data/formatted', self.parser.df, format='table', data_columns=True)
            print("Saved data off in {:.2f} ms".format(time.time() - start_time))

        # def run(self):
        #     start_time = time.time()
        #     with self.lock:
        #         # Coerce data types
        #         for i, col in enumerate(self.parser.df.columns):
        #             self.parser.df[col] = self.parser.df[col].astype(DTYPES[i])
        #         # Save off the file
        #         pd.read_pickle(self.parser.file_name).append(self.parser.df).to_pickle(self.parser.file_name)
        #     print("Saved data off in {:.2f} ms".format(time.time() - start_time))

    def save_data(self):
        self.WriteSBS1(self.lock, self).start()


class SBS1Entry(object):
    # Static constant members
    EXPECTED_NUM_MSGS = 4
    VALID_MESSAGE_SETS = [[1, 2, 6],
                          [1, 3, 4, 6]]

    def __init__(self, data):
        self.data = ["", "", -1, -1, "", -1, "", "", "", "", "", -1, 0.0, 0.0, 0.0, 0.0, -1, -1,
                     False, False, False, False]
        self.unique_id = self.data[4] = data[4]
        self.message_type = self.data[0] = data[0]
        self.data[2] = SBS1Entry.sbs1_int(self.data[2])
        self.data[3] = SBS1Entry.sbs1_int(self.data[3])
        self.data[5] = SBS1Entry.sbs1_int(self.data[5])
        self.n_messages = 0
        self.seen_message_types = []

    @staticmethod
    def sbs1_int(value):
        return int(value) if type(value) is int or value.isnumeric() else -1

    @staticmethod
    def sbs1_boolean(value):
        return 1 if value == "-1" else 0


class TransmissionMsgEntry(SBS1Entry):
    def __init__(self, data):
        SBS1Entry.__init__(self, data)
        self.transmission_type = self.data[1]
        # Set the transmission type field to 0, since we will be merging
        # multiple messages together to form the complete entries.
        self.data[1] = "0"
        self.more_data(data)

    def more_data(self, data):
        transmission_type = int(data[1])
        if self.unique_id == data[4]:
            self.n_messages += 1
            self.seen_message_types.append(transmission_type)
        if transmission_type == 1:
            # MSG 1: ES Identification and Category Message
            self.data[10] = data[10]
        elif transmission_type == 3:
            # MSG 3: ES Airborne Position Message
            self.data[11] = int(data[11])
            self.data[14] = float(data[14])
            self.data[15] = float(data[15])
            self.data[18] = self.sbs1_boolean(data[18])
            self.data[19] = self.sbs1_boolean(data[19])
            self.data[20] = self.sbs1_boolean(data[20])
            self.data[21] = self.sbs1_boolean(data[21])
        elif transmission_type == 4:
            # MSG 4: ES Airborne Velocity Message
            self.data[12] = float(data[12])
            self.data[13] = float(data[13])
            self.data[16] = SBS1Entry.sbs1_int(data[16])
        elif transmission_type == 6:
            # MSG 6: Surveillance ID Message
            self.data[11] = int(data[11])
            self.data[17] = data[17]
            self.data[18] = self.sbs1_boolean(data[18])
            self.data[19] = self.sbs1_boolean(data[19])
            self.data[20] = self.sbs1_boolean(data[20])
            self.data[21] = self.sbs1_boolean(data[21])
