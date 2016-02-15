
import os
import sys
import csv
import copy
import re
from netCDF4 import Dataset
from uframe import UFrame
from dateutil import parser

_OOI_ARRAYS = {'CP' : 'Coastal_Pioneer',
    'CE' : 'Coastal_Endurance',
    'GP' : 'Global_Station_Papa',
    'GI' : 'Global_Irminger_Sea',
    'GA' : 'Global_Argentine_Basin',
    'GS' : 'Global_Southern_Ocean',
    'RS' : 'Cabled_Array'}
    
def csv2json(csv_filename):
    
    json_array = []
   
    # Check for 0 file size
    if os.stat(csv_filename).st_size == 0:
        sys.stderr.write('{:s}: Empty file\n'.format(csv_filename))
        return json_array

    try:
        fid = open(csv_filename, 'r')
    except IOError as e:
        sys.stderr.write('{:s}: {:s}\n'.format(csv_filename, e.strerror))
        return json_array
        
    csv_reader = csv.reader(fid)
    cols = csv_reader.next()
    col_range = range(0,len(cols))
    
    for r in csv_reader:
    
        if r[0].startswith('#'):
            continue
            
        stream_meta = {cols[i]:r[i] for i in col_range}
        
        json_array.append(stream_meta)
        
    fid.close()
    
    return json_array
    
def find_new_streams(master_streams, known_streams):
    
    new_streams = []
    for master_stream in master_streams:
        
        # Create a copy of the dict
        stream = copy.deepcopy(master_stream)
        
        if not known_streams:
            new_streams.append(stream)
            continue
            
        # Create a list of instruments for this sensor
        instruments = [s['sensor'] for s in known_streams]
        
        # See if the current instrument is in instruments
        if stream['sensor'] not in instruments:
            new_streams.append(stream)
            continue
            
    return new_streams

def create_data_request_url(uframe_base, stream_meta):
    
    tokens = stream_meta['sensor'].split('-')
    if len(tokens) != 4:
        sys.stderr.write('Invalid sensor/reference designator specified: {:s}\n'.format(stream_meta['sensor']))
        sys.stderr.flush()
        return None
        
    async_url = '{:s}/{:s}/{:s}/{:s}-{:s}/{:s}/{:s}?beginDT={:s}&endDT={:s}&limit=-1&execDPA=true&format=application/netcdf&include_provenance=true'.format(
        uframe_base.url,
        tokens[0],
        tokens[1],
        tokens[2],
        tokens[3],
        stream_meta['method'],
        stream_meta['stream'],
        stream_meta['beginTime'],
        stream_meta['endTime'])
        
    return async_url
    
def create_stream_metadata_url(uframe_base, stream_meta):
    
    tokens = stream_meta['sensor'].split('-')
    if len(tokens) != 4:
        sys.stderr.write('Invalid sensor/reference designator specified: {:s}\n'.format(stream_meta['sensor']))
        sys.stderr.flush()
        return None
        
    meta_url = '{:s}/{:s}/{:s}/{:s}-{:s}/metadata'.format(
        uframe_base.url,
        tokens[0],
        tokens[1],
        tokens[2],
        tokens[3])
        
    return meta_url
    
def write_streams_to_csv(streams, out_file):
    
    try:
        fid = open(out_file, 'w')
    except IOError as e:
        sys.stderr.write('{:s}: {:s}\n'.format(e.strerror, out_file))
        return 1
        
    csv_writer = csv.writer(fid)
    cols = streams[0].keys()
    csv_writer.writerow(cols)
    
    for s in streams:
        r = [s[c] for c in cols]
        csv_writer.writerow(r)
        
    fid.close()
    
    return 1
    
def build_async_query_from_stream_meta(uframe_base, streams, user=None):
    
    required_cols = ['stream',
        'beginTime',
        'endTime',
        'sensor',
        'method']
    
    async_urls = []
    for s in streams:
        
        has_cols = [c for c in required_cols if c in s.keys()]
        if len(has_cols) != len(required_cols):
            sys.stderr.write('Stream metadat is missing one or more required columns: {:s}-{:s}\n'.format(s['sensor'], s['stream']))
            sys.stderr.flush()
            continue
    
        tokens = s['sensor'].split('-')
        if len(tokens) != 4:
            sys.stderr.write('Invalid sensor/reference designator specified: {:s}\n'.format(s['sensor']))
            sys.stderr.flush()
            continue
           
        if not user:
	        async_url = '{:s}/{:s}/{:s}/{:s}-{:s}/{:s}/{:s}?beginDT={:s}&endDT={:s}&limit=-1&execDPA=true&format=application/netcdf&include_provenance=true'.format(
	            uframe_base.url,
	            tokens[0],
	            tokens[1],
	            tokens[2],
	            tokens[3],
	            s['method'],
	            s['stream'],
	            s['beginTime'],
	            s['endTime'])
        else:
	        async_url = '{:s}/{:s}/{:s}/{:s}-{:s}/{:s}/{:s}?beginDT={:s}&endDT={:s}&limit=-1&execDPA=true&format=application/netcdf&include_provenance=true&user={:s}'.format(
	            uframe_base.url,
	            tokens[0],
	            tokens[1],
	            tokens[2],
	            tokens[3],
	            s['method'],
	            s['stream'],
	            s['beginTime'],
	            s['endTime'],
                user)

            
        async_urls.append(async_url)
        
    return async_urls
    
def timestamp_nc_file(nc_file, dest_dir=None):
    
    if dest_dir and not os.path.exists(dest_dir):
        sys.stderr.write('Invalid destination specified: {:s}\n'.format(dest_dir))
        return None
        
    file_tokens = os.path.split(nc_file)
        
    # match reference designator
    ref_des_regexp = re.compile(r'^(deployment\d{1,})_(\w{1,}\-\w{1,}\-\w{1,}\-\w{1,}.*)\.nc')
    match = ref_des_regexp.search(file_tokens[1])
    if not match:
        sys.stderr.write('Failed to parse reference designator filename: {:s}\n'.format(nc_file))
        return None
        
    try:
        nci = Dataset(nc_file, 'r')
    except RuntimeError as e:
        sys.stderr.write('{:s}: {:s}\n'.format(e.message, nc_file))
        return None
    
    ts0 = re.sub('\-|:', '', nci.time_coverage_start[:19])
    ts1 = re.sub('\-|:', '', nci.time_coverage_end[:19])
    
    #nc_filename = '{:s}-{:s}-{:s}-{:s}.nc'.format(match.groups()[1], match.groups()[0], ts0, ts1)
    # time-index.nc
    #nc_filename = '{:s}-{:s}-{:s}.nc'.format(match.groups()[1][:-13], ts0, ts1)
    nc_filename = '{:s}-{:s}-{:s}.nc'.format(match.groups()[1], ts0, ts1)
    if dest_dir:
        ts_nc_file = os.path.join(dest_dir, nc_filename)
    else:
        ts_nc_file = nc_filename
    
    return ts_nc_file

def dir_from_request_meta(meta):

    destination = None

    required_keys = ['instrument',
        'stream',
        'telemetry']
    has_keys = [k for k in meta.keys() if k in required_keys]
    if len(has_keys) != len(required_keys):
        sys.stderr.write('Stream metadata is missing one or more required keys\n')
        return destination
        
    # Map the first 2 letters of instrument to the appropriate entry in _OOI_ARRAYS
    ooi_array = 'Unknown'
    if meta['instrument'][:2] in _OOI_ARRAYS.keys():
        ooi_array = _OOI_ARRAYS[meta['instrument'][:2]]
    
    instrument_tokens = meta['instrument'].split('-')
    if len(instrument_tokens) != 4:
        sys.stderr.write('Invalid reference designator: {:s}\n'.format(meta['instrument']))
        return destination
        
    platform = instrument_tokens[0]
    instrument_type = '{:s}-{:s}'.format(instrument_tokens[2], instrument_tokens[3])
    
    destination = '{:s}/{:s}/{:s}/{:s}/{:s}-{:s}-{:s}'.format(
        ooi_array,
        platform,
        instrument_type,
        meta['telemetry'],
        meta['instrument'],
        meta['stream'],
        meta['telemetry'])
    
    return destination
