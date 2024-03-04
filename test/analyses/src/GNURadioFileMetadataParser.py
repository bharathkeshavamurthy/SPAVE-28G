import pmt
import sys
import traceback
import numpy as np
from gnuradio import gr, blocks
from gnuradio.blocks import parse_file_metadata

filename = 'samples_with_metadata.log'
max_data_segments_to_read = 7344
print_output = True

fh = open(filename, "rb")
for ii in range(max_data_segments_to_read):
    header_str = fh.read(171)
    header = pmt.deserialize_str(header_str)
    print(f"\n=== Data segment {ii} ===")
    header_info = parse_file_metadata.parse_header(header, print_output)
    print(header_info)
fh.close()
