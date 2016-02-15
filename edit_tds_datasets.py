#!/usr/bin/env python

import os
import sys
import argparse
import csv
import glob

def main(args):
    
    _OOI_ARRAYS = {'CP' : 'Coastal_Pioneer',
        'CE' : 'Coastal_Endurance',
        'GP' : 'Global_Station_Papa',
        'GI' : 'Global_Irminger_Sea',
        'GA' : 'Global_Argentine_Basin',
        'GS' : 'Global_Southern_Ocean',
        'RS' : 'Cabled_Array',
        'SS' : 'Shore Station'}
    array_names = _OOI_ARRAYS.keys()
        
    required_cols = ['reference designator',
        'stream',
        'telemetry']
        
    status = 0
    
    TDS_NC_ROOT = args.tdsroot
    if not TDS_NC_ROOT:
        # Make sure the ASYNC_TDS_NC_ROOT is set  in the user's environment if it
        # wasn't specified on the command line
        TDS_NC_ROOT = os.getenv('ASYNC_TDS_NC_ROOT')
        if not args.debug and not TDS_NC_ROOT:
            sys.stderr.write('ASYNC_TDS_NC_ROOT environment variable not set\n')
            sys.stderr.flush()
            return 1
        
    if not os.path.exists(TDS_NC_ROOT):
        sys.stderr.write('ASYNC_TDS_NC_ROOT is invalid: {:s}\n'.format(TDS_NC_ROOT))
        sys.stderr.flush()
        return 1
    
    # If delete or copy was specified, check args.location to ensure it is valid
    if args.move or args.copy:
        if not args.location:
            sys.stderr.write('No THREDDS destination specified\n')
            return status
        if not os.path.exists(args.location):
            sys.stderr.write('Invalid destination specified: {:s}\n'.format(args.location))
            return status
            
    # Open args.csv_file for reading    
    csv_file = args.csv_file
    fid = open(csv_file, 'r')
    csv_reader = csv.reader(fid)
    
    # First row is the header
    cols = csv_reader.next()
    fid.close()
    
    # convert the csv file rows to a dictionary
    streams = csv2json(csv_file)
    
    # Make sure the csv_file contains the appropriate headers
    if len(set(required_cols).intersection(streams[0].keys())) != len(required_cols):
        sys.stderr.write('Invalid csv file format\n')
        return status
        
    # The rest of the rows are datasets
    for stream in streams:
        
        r_tokens = stream['reference designator'].split('-')
        
        if r_tokens[0][:2] not in array_names:
            sys.stderr.write('{:s}: No array name found\n'.format(stream))
            continue
            
        rel_path = '{:s}/{:s}/{:s}-{:s}/{:s}/{:s}-{:s}-{:s}'.format(
            _OOI_ARRAYS[r_tokens[0][:2]],
            r_tokens[0],
            r_tokens[2],
            r_tokens[3],
            stream['telemetry'],
            stream['reference designator'],
            stream['stream'],
            stream['telemetry'])
            
        tds_path = os.path.join(TDS_NC_ROOT, rel_path)
        
        if not args.debug and not os.path.exists(tds_path):
            sys.stderr.write('Invalid THREDDS stream location: {:s}\n'.format(tds_path))
            continue
        
        sys.stdout.write('Valid THREDDS stream location: {:s}\n'.format(tds_path))
            
        if not args.delete and not args.copy and not args.move:
            continue
            
        if args.delete:
            sys.stdout.write('Deleting THREDDS stream: {:s}\n'.format(tds_path))
            # Remove all files first
            d_contents = glob.glob(os.path.join(tds_path, '*'))
            if not d_contents:
                sys.stderr.write('No files found: {:s}\n'.format(tds_path))
                continue
        elif args.copy:
            sys.stdout.write('Copying THREDDS stream to {:s}\n'.format(args.location))
        elif args.move:
            sys.stdout.write('Moving THREDDS stream to {:s}\n'.format(args.location))
    
    status = 1
              
    return status
    
def csv2json(csv_filename):
    
    json_array = []
    
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
            
        stream_meta = {cols[i].lower():r[i] for i in col_range}
        
        json_array.append(stream_meta)
        
    fid.close()
    
    return json_array
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('csv_file',
        help='Location of the csv file containing the datasets to operate on')
    arg_parser.add_argument('-d', '--delete',
        action='store_true',
        help='Delete directory tree')
    arg_parser.add_argument('-m', '--move',
        action='store_true',
        help='Move directory tree.  Must specify destination via --location option')
    arg_parser.add_argument('-c', '--copy',
        action='store_true',
        help='Verbose display')
    arg_parser.add_argument('--tdsroot',
        type=str,
        help='Location of the THREDDS root directory containing the source files')
    arg_parser.add_argument('--location',
        type=str,
        help='Location to move or copy directory tree')
    arg_parser.add_argument('-v', '--validate',
        action='store_true',
        help='Validate that the directory tree exists, but perform no action')
    arg_parser.add_argument('-x', '--debug',
        action='store_true',
        help='Debug mode.  Does not validate or perform operations')
    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
