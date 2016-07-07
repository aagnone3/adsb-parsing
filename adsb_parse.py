import sys

from virtual_radar_server import vrs_parsing
from adsb_hub import sbs1_parsing
from utils import timestamped_file_name

TIME_STR = timestamped_file_name()
DUMP_PATH = "adsb_exchange_data_{}.h5".format(TIME_STR).replace(" ", "_")

if __name__ == '__main__':
    parser = vrs_parsing if len(sys.argv) == 1 or sys.argv[1] == "1" else sbs1_parsing
    # FUTURE move start() functionalities into a shared class instead of different implementations for each data source
    parser.start(TIME_STR, DUMP_PATH)
