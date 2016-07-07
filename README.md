# ADSB Data Parsing

A collection of Python scripts which server to parse ADSB data commonly transmitted by aircraft. Currently, two sources of ADSB data are supported:
- [Virtual Radar Server](http://www.virtualradarserver.co.uk/) 
- [ADSB Hub](http://www.adsbhub.net/)


# Motivation

I came across [this site](https://www.flightradar24.com) and just got giddy. I now have a Raspberry Pi 3 feeding ADSB data to [ADSB hub](http://www.adsbhub.net/), which in returns gets me the combined feed from all similar data providers. But now we need to parse this data in order to do nerdy things with it! Bingo: here's the code.

# Installation

Clone and run! This is a lightweight repository that does not need any formal installation.

# Getting Started

Each submodule implements parsing for a data source. Simply import the desired submodule and call its start() function, providing a timestamp for unique file name creation and an output file name for the accumulated data.

```python
from adsb_parsing.virtual_radar_server import vrs_parsing as vrs
from adsb_parsing.utils import timestamped_file_name

TIME_STR = timestamped_file_name()
DUMP_PATH = "adsb_exchange_data_{}.h5".format(TIME_STR).replace(" ", "_")

if __name__ == '__main__':
    # FUTURE move start() functionalities into a shared class instead of different implementations for each data source
    vrs.start(TIME_STR, DUMP_PATH)
```

## Contributors

Currently me, myself, and I. I'm happy to enhance this project with others, don't hesitate to reach out!

## License

This software is released with the Apache License. Download it, use it, change it, share it. Just keep the license!
