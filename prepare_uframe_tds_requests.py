#!/usr/bin/env python

import os
import sys
import csv
import argparse
import copy
import requests
import datetime
from uframe import UFrame
from dateutil import parser
from tds import *

def main(args):
    
    # File locations from environment
    UFRAME_NC_ROOT = os.getenv('ASYNC_UFRAME_NC_ROOT')
    if not UFRAME_NC_ROOT:
        sys.stderr.write('ASYNC_UFRAME_NC_ROOT environment variable not set\n')
        sys.stderr.flush()
        return 1

    # Append the user directory
    UFRAME_NC_ROOT = os.path.join(UFRAME_NC_ROOT, args.user)
    if not os.path.exists(UFRAME_NC_ROOT):
        sys.stderr.write('ASYNC_UFRAME_NC_ROOT is invalid\n')
        sys.stderr.flush()
        return 1
        
    TDS_NC_ROOT = os.getenv('ASYNC_TDS_NC_ROOT')
    if not TDS_NC_ROOT:
        sys.stderr.write('ASYNC_TDS_NC_ROOT environment variable not set\n')
        sys.stderr.flush()
        return 1
    elif not os.path.exists(TDS_NC_ROOT):
        sys.stderr.write('ASYNC_TDS_NC_ROOT is invalid\n')
        sys.stderr.flush()
        return 1
        
    ASYNC_DATA_ROOT = os.getenv('ASYNC_DATA_HOME')
    if not ASYNC_DATA_ROOT:
        sys.stderr.write('ASYNC_DATA_HOME environment variable not set\n')
        sys.stderr.flush()
        return 1
    elif not os.path.exists(ASYNC_DATA_ROOT):
        sys.stderr.write('ASYNC_DATA_HOME is invalid\n')
        sys.stderr.flush()
        return 1
    
    # Create some directory names to look for the files we need
    KNOWN_STREAMS_ROOT = os.path.join(ASYNC_DATA_ROOT, 'known-streams')
    if not os.path.exists(KNOWN_STREAMS_ROOT):
        sys.stdout.write('Creating known-streams root: {:s}\n'.format(KNOWN_STREAMS_ROOT))
        try:
            os.mkdir(KNOWN_STREAMS_ROOT)
        except OSError as e:
            sys.stderr.write('Failed to create known-streams root: {:s}\n'.format(KNOWN_STREAMS_ROOT))
            sys.stderr.flush()
            return 1
            
    STREAMS_REQUEST_ROOT = os.path.join(ASYNC_DATA_ROOT, 'stream-requests')
    if not os.path.exists(STREAMS_REQUEST_ROOT):
        sys.stdout.write('Creating stream-request root: {:s}\n'.format(STREAMS_REQUEST_ROOT))
        try:
            os.mkdir(STREAMS_REQUEST_ROOT)
        except OSError as e:
            sys.stderr.write('Failed to create stream-request root: {:s}\n'.format(STREAMS_REQUEST_ROOT))
            sys.stderr.flush()
            return 1
            
    STREAMS_QUEUE_ROOT = os.path.join(ASYNC_DATA_ROOT, 'stream-queue')
    if not os.path.exists(STREAMS_QUEUE_ROOT):
        sys.stdout.write('Creating stream-queue root: {:s}\n'.format(STREAMS_QUEUE_ROOT))
        try:
            os.mkdir(STREAMS_QUEUE_ROOT)
        except OSError as e:
            sys.stderr.write('Failed to create stream-queue root: {:s}\n'.format(STREAMS_QUEUE_ROOT))
            sys.stderr.flush()
            return 1
            
    # Configure UFrame instance    
    if args.base_url:
        uframe_base = UFrame(base_url=args.base_url)
    else:
        uframe_env_url = os.getenv('UFRAME_BASE_URL')
        if uframe_env_url:
            uframe_base = UFrame(base_url=uframe_env_url)
        else:
            uframe_base = UFrame()
    
    master_streams_file = args.master_stream_csv
    if not os.path.exists(master_streams_file):
        sys.stderr.write('Invalid master stream file: {:s}\n'.format(master_streams_file))
        return 1
    
    # Split the master_stream_file into components    
    (fd,fn) = os.path.split(master_streams_file)
    
    # Load the stream_master_file csv and convert to dict
    sys.stdout.write('Reading master stream file: {:s}\n'.format(master_streams_file))
    master_streams = csv2json(master_streams_file)
    if not master_streams:
        sys.stderr.write('No streams found: {:s}\n'.format(master_streams_file))
        sys.stderr.flush()
        return 1
    
    # create the known stream file name
    fn_tokens = fn.split('-')
    known_streams_file = os.path.join(KNOWN_STREAMS_ROOT, '{:s}-known-meta.csv'.format(fn_tokens[0]))
    
    # create the stream requests file name
    stream_request_file = os.path.join(STREAMS_REQUEST_ROOT, '{:s}-urls.csv'.format(fn_tokens[0]))
        
    # Load the known_streams_file if it exists
    known_streams = []
    if os.path.exists(known_streams_file):
        sys.stdout.write('Reading known streams file: {:s}\n'.format(known_streams_file))
        known_streams = csv2json(known_streams_file)
    else:
        sys.stdout.write('No known streams file: {:s}\n'.format(known_streams_file))
        sys.stdout.write('Processing all streams from {:s}\n'.format(master_streams_file))
        sys.stdout.flush()
        
    # compare master_streams to known_streams to see if there any new streams to request
    new_streams = find_new_streams(master_streams, known_streams)
        
    sys.stdout.write('Found {:d} new streams\n'.format(len(new_streams)))
    sys.stdout.flush()
    
    for s in new_streams:
        sys.stdout.write('New stream: {:s}-{:s}\n'.format(s['sensor'], s['stream']))
        sys.stdout.flush()
    
    # If CHECK_UPDATES is True, iterate through each of the known_streams and send a metadata
#   # request to see if the dataset has been updated.    
    if args.update and known_streams:
        
        sys.stdout.write('Checking for updates to existing streams\n')
        sys.stdout.flush()
        
        for s in known_streams:
            
            meta_url = create_stream_metadata_url(uframe_base, s)
            if not meta_url:
                continue
                
            try:
                r = requests.get(meta_url)
                if r.status_code != 200:
                    sys.stderr.write('{:s}: {:s}\n'.format(r.reason, meta_url))
                    continue
            except requests.ConnectionError as e:
                sys.stderr.write('{:s}: {:s}\n'.format(e[0][1], meta_url))
                continue
                
            meta_streams = r.json()['times']
            stream_names = [m['stream'] for m in meta_streams]
            if s['stream'] not in stream_names:
                sys.stderr.write('{:s}: Stream not found: {:s}\n'.format(s['sensor'], s['stream']))
                continue
                
            i = stream_names.index(s['stream'])
            
            # Parse the beginTime and endTime for both s and meta_streams[i] to see
            # if any data has been added/removed
            st0 = parser.parse(s['beginTime'])
            st1 = parser.parse(s['endTime'])
            mt0 = parser.parse(meta_streams[i]['beginTime'])
            mt1 = parser.parse(meta_streams[i]['endTime'])
            
            if st0 != mt0 or st1 != mt1:
                sys.stdout.write('Stream updated: {:s}\n'.format(s['stream']))
                new_streams.append(s)
                
    # Merge known_streams and new_streams
    if new_streams:
        sys.stdout.write('Merging new and known streams\n')
        sys.stdout.flush()
        
    for s in new_streams:
        
        streams = ['{:s}-{:s}'.format(r['sensor'], r['stream']) for r in known_streams]
        
        if not streams:
            known_streams.append(s)
            continue
        
        stream_id = '{:s}-{:s}'.format(s['sensor'], s['stream'])
        if stream_id not in streams:
            known_streams.append(s)
            continue
            
        i = streams.index(stream_id)
        known_streams[i] = s
        
    # Write known_streams to the known_streams_file
    if not args.debug:
        sys.stdout.write('Saving new known streams: {:s}\n'.format(known_streams_file))
        sys.stdout.flush()
        status = write_streams_to_csv(known_streams, known_streams_file)
        if not status:
            return status
    
    # Write the new async requests to stream_request_file
    async_urls = build_async_query_from_stream_meta(uframe_base, new_streams, user=args.user)
    if async_urls:
        
        if args.debug:
            for url in async_urls:
                sys.stdout.write('DEBUG> async query: {:s}\n'.format(url))
                
        else:
            sys.stdout.write('Writing new asynchronous queries: {:s}\n'.format(stream_request_file))
            try:
                fid = open(stream_request_file, 'w')
                for url in async_urls:
                    fid.write('{:s}\n'.format(url))
                fid.close()
            except IOError as e:
                sys.stderr.write('{:s}: {:s}\n'.format(e.strerror, stream_request_file))
                return 1
            
    return 0
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('master_stream_csv',
        help='Filename containing stream request pieces.  Create this file using stream2ref_des_list.py')
    arg_parser.add_argument('--user',
        default='_nouser',
        help='Alternate user name (_nouser is <default>)')
    arg_parser.add_argument('--update',
        action='store_true',
        help='Check known streams for metadata updates')
    arg_parser.add_argument('-x', '--debug',
        dest='debug',
        action='store_true',
        help='Print completed request info, but do not move files')
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        help='Specify an alternate uFrame server URL. Must start with \'http://\'.')
    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
