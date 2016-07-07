# -*- coding: utf-8 -*-
"""
Connects to a socket serving ADS-B data in CSV format and continuously parses its output to a pandas DataFrame
"""
import socket
from threading import Timer

from adsb_parsing.adsb_hub.sbs1_parsing import SBS1Parser, NUM_FIELDS

# Constants
SERVER_IP = "174.49.117.143"
SERVER_PORT = 30004
BUFFER_SIZE = 4096
DESIRED_MB_AMOUNT = 5
CURRENT_MB_AMOUNT = 0.0
OUT_FILE = "small.h5"
STATUS_INTERVAL_S = 45
SAVE_INTERVAL_S = 30


class RepeatedTimer(Timer):
    """
    See: https://hg.python.org/cpython/file/2.7/Lib/threading.py#l1079
    """
    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            self.function(*self.args, **self.kwargs)
        self.finished.set()


if __name__ == '__main__':
    parser = SBS1Parser(OUT_FILE)
    leftover = []

    # Set up timer interrupts for status announcing and data saving
    status_timer = RepeatedTimer(STATUS_INTERVAL_S, parser.print_memory_usage)
    save_timer = RepeatedTimer(SAVE_INTERVAL_S, parser.save_data)
    status_timer.start()
    save_timer.start()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((SERVER_IP, SERVER_PORT))
        while True:
            # Decode new UTF-8 data
            data = s.recv(BUFFER_SIZE).decode()
            # Keep track of total data processed
            CURRENT_MB_AMOUNT += len(data) / 1e6
            # Split the new data into a 2-D array
            data = [d.split(',') for d in data.split('\r\n')]
            # Join the previous leftover data with the (incomplete) first entry
            data[0] = leftover + data[0]
            for d in data:
                if len(d) == NUM_FIELDS:
                    parser.process(d)
                else:
                    leftover = d

    status_timer.cancel()
    save_timer.cancel()
    parser.save_data()
