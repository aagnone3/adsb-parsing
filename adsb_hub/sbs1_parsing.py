# -*- coding: utf-8 -*-
"""
Connects to a socket serving ADS-B data in CSV format and continuously parses its output to a pandas DataFrame
"""
import os
import threading
import pandas as pd
import logging
import time
import socket

from .sbs1_protocol import SBS1Entry, TransmissionMsgEntry
from adsb_parsing.utils import RepeatedTimer, timestamped_file_name


class SBS1Parser(object):
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

    """
    Handles the formatting and DataFrame appending for the SBS-1 data
    """
    def __init__(self, file_name):
        self.lock = threading.Lock()
        self.file_name = file_name
        self.df = pd.DataFrame(columns=SBS1Parser.FIELDS)
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
                                               index=SBS1Parser.FIELDS)
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
                    self.parser.df[col] = self.parser.df[col].astype(SBS1Parser.DTYPES[i])
                # Save off the file
                with pd.HDFStore(self.parser.file_name) as hdf:
                    hdf.put('/data/formatted', self.parser.df, format='table', data_columns=True)
            print("Saved data off in {:.2f} ms".format(time.time() - start_time))

    def save_data(self):
        self.WriteSBS1(self.lock, self).start()


def start(timestamp, dump_path):
    # some initial settings
    server_ip = "174.49.117.143"
    server_port = 30004
    buffer_size = 4096
    current_mb_amount = 0.0
    status_interval_s = 45
    save_interval_s = 30

    logging.basicConfig(filename="collection_{}.log".format(timestamp),
                        level=logging.DEBUG)
    parser = SBS1Parser(dump_path)
    leftover = []

    # Set up timer interrupts for status announcing and data saving
    status_timer = RepeatedTimer(status_interval_s, parser.print_memory_usage)
    save_timer = RepeatedTimer(save_interval_s, parser.save_data)
    status_timer.start()
    save_timer.start()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((server_ip, server_port))
        while True:
            # Decode new UTF-8 data
            data = s.recv(buffer_size).decode()
            # Keep track of total data processed
            current_mb_amount += len(data) / 1e6
            # Split the new data into a 2-D array
            data = [d.split(',') for d in data.split('\r\n')]
            # Join the previous leftover data with the (incomplete) first entry
            data[0] = leftover + data[0]
            for d in data:
                if len(d) == SBS1Parser.NUM_FIELDS:
                    parser.process(d)
                else:
                    leftover = d

    status_timer.cancel()
    save_timer.cancel()
    parser.save_data()
