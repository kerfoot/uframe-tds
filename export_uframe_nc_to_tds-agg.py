#!/usr/bin/env python

import os
import sys
import argparse
import csv
import glob
import shutil
from uframe import *
from tds import *

_OOI_ARRAYS = {'CP' : 'Coastal_Pioneer',
    'CE' : 'Coastal_Endurance',
    'GP' : 'Global_Station_Papa',
    'GI' : 'Global_Irminger_Sea',
    'GA' : 'Global_Argentine_Basin',
    'GS' : 'Global_Southern_Ocean',
    'RS' : 'Cabled_Array'}
    
def main(args):
    '''Check the status of queued UFrame requests.  No files are moved and no
    NCML aggregation files are written.  Use the -m or --move option to
    timestamp any generated NetCDF files to THREDDS and write the NCML
    aggregation files'''

    USER = args.user
    QUEUE_CSV = args.queue_csv
    
    # Make sure the queue file exists
    if not os.path.exists(QUEUE_CSV):
        sys.stderr.write('Invalid queue csv file: {:s}\n'.format(QUEUE_CSV))
        return 1
        
    # File locations from environment
    ASYNC_DATA_ROOT = os.getenv('ASYNC_DATA_HOME')
    if not ASYNC_DATA_ROOT:
        sys.stderr.write('ASYNC_DATA_ROOT environment variable not set\n')
        sys.stderr.flush()
        return 1
    elif not os.path.exists(ASYNC_DATA_ROOT):
        sys.stderr.write('ASYNC_DATA_ROOT is invalid: {:s}\n'.format(ASYNC_DATA_ROOT))
        sys.stderr.flush()
        return 1
        
    UFRAME_NC_ROOT = os.getenv('ASYNC_UFRAME_NC_ROOT')
    if not UFRAME_NC_ROOT:
        sys.stderr.write('ASYNC_UFRAME_NC_ROOT environment variable not set\n')
        sys.stderr.flush()
        return 1
        
    # Append the user directory
    UFRAME_NC_ROOT = os.path.join(UFRAME_NC_ROOT, USER)
    if not os.path.exists(UFRAME_NC_ROOT):
        sys.stderr.write('ASYNC_UFRAME_NC_ROOT is invalid: {:s}\n'.format(UFRAME_NC_ROOT))
        sys.stderr.flush()
        return 1
        
    TDS_NC_ROOT = os.getenv('ASYNC_TDS_NC_ROOT')
    if not TDS_NC_ROOT:
        sys.stderr.write('ASYNC_TDS_NC_ROOT environment variable not set\n')
        sys.stderr.flush()
        return 1
    elif not os.path.exists(TDS_NC_ROOT):
        sys.stderr.write('ASYNC_TDS_NC_ROOT is invalid: {:s}\n'.format(TDS_NC_ROOT))
        sys.stderr.flush()
        return 1

    #TDS_NCML_ROOT = os.getenv('ASYNC_TDS_NCML_ROOT')
    #if not TDS_NCML_ROOT:
    #    sys.stderr.write('ASYNC_TDS_NCML_ROOT environment variable not set\n')
    #    sys.stderr.flush()
    #    return 1
    #elif not os.path.exists(TDS_NCML_ROOT):
    #    sys.stderr.write('ASYNC_TDS_NCML_ROOT is invalid: {:s}\n'.format(TDS_NCML_ROOT))
    #    sys.stderr.flush()
    #    return 1

    NCML_TEMPLATE = os.path.join(ASYNC_DATA_ROOT, 'catalogs', 'stream-agg-template.ncml')
    if not os.path.exists(NCML_TEMPLATE):
        sys.stderr.write('NCML stream agg template: {:s}\n'.format(NCML_TEMPLATE))
        return 1
        
    sys.stdout.write('Async data home    : {:s}\n'.format(ASYNC_DATA_ROOT))
    sys.stdout.write('UFrame NetCDF Root : {:s}\n'.format(UFRAME_NC_ROOT))
    sys.stdout.write('THREDDS NetCDF Root: {:s}\n'.format(TDS_NC_ROOT))
    #sys.stdout.write('THREDDS NCML Root  : {:s}\n'.format(TDS_NCML_ROOT))
    sys.stdout.write('NCML Agg Template  : {:s}\n'.format(NCML_TEMPLATE))
   
    # Exit if we're just validating our environment setup (-v)
    if args.validate:
        return 0

    # Convert the queue_csv csv records to an array of dicts
    stream_requests = csv2json(QUEUE_CSV)
    if not stream_requests:
        return 0

    remaining_streams = []
    for stream in stream_requests:

        if 'tds_destination' not in stream.keys():
            stream['tds_destination'] = None

        sys.stdout.write('\nProcessing Stream: {:s}-{:s}\n'.format(stream['instrument'], stream['stream']))

        if stream['reason'].find('Complete') == 0:
            sys.stdout.write('Request already complete and available on thredds: {:s}\n'.format(stream['tds_destination']))
            continue
        elif not stream['requestUUID']:
            sys.stderr.write('Request failed (No requestUUID): {:s}\n'.format(stream['request_url']));
            stream['reason'] = 'No requestUUID created'
            remaining_streams.append(stream)
            continue

        sys.stdout.write('Request: {:s}\n'.format(stream['requestUUID']))

        # A UFrame request is complete when complete_file exists and contains the
        # string 'complete'.
        product_dir = os.path.join(UFRAME_NC_ROOT, stream['requestUUID'])
        complete_file = os.path.join(product_dir, 'status.txt')
        if not os.path.exists(complete_file):
            sys.stderr.write('Request not completed yet: {:s}\n'.format(stream['request_url']))
            # If it doesn't exist, add it to remaining streams
            stream['reason'] = 'In process'
            remaining_streams.append(stream)
            continue
        
        try:
            fid = open(complete_file, 'r')
        except IOError as e:
            sys.stderr.write('{:s}: {:s}\n'.format(complete_file, e.strerror))
            remaining_streams.append(stream)
            continue
        
        status = fid.readline()
        if status != 'complete':
            sys.stderr.write('Request not completed yet: {:s}\n'.format(stream['request_url']))
            # If it doesn't exist, add it to remaining streams
            stream['reason'] = 'In process'
            remaining_streams.append(stream)
        
        sys.stdout.write('NetCDF Source Directory: {:s}\n'.format(product_dir))
        product_dir_items = os.listdir(product_dir)
        nc_files = []
        for product_dir_item in product_dir_items:
            bin_dir = os.path.join(product_dir, product_dir_item)
            if not os.path.isdir(bin_dir):
                continue
            
            target_nc_files = glob.glob(os.path.join(bin_dir, '*{:s}.nc'.format(stream['stream'])))
            if not target_nc_files:
                continue
                
            for target_nc_file in target_nc_files:
                nc_files.append(target_nc_file)
                
        if not nc_files:
#            sys.stderr.write('No NetCDF product files found: {:s}\n'.format(product_dir))
            sys.stderr.write('No NetCDF files found\n')
            stream['reason'] = 'No NetCDF files found'
            remaining_streams.append(stream)
            continue
            
        # Create the name of the stream destination directory
        destination = dir_from_request_meta(stream)
        if not destination:
            sys.stderr.write('Cannot determine stream destination\n')
            continue
        
        # See if the fully qualified NetCDF stream destination directory needs to be created    
        stream_destination = os.path.join(TDS_NC_ROOT, destination)
        sys.stdout.write('NetCDF TDS Destination : {:s}\n'.format(stream_destination))
       
        # Add the tds_destination
        stream['tds_destination'] = stream_destination

        #NCML
        ncml_destination = stream_destination
        sys.stdout.write('NCML file destination  : {:s}\n'.format(ncml_destination))
        if not os.path.exists(stream_destination):
            
            if  not args.move:
                sys.stdout.write('DEBUG> Skipping creation of stream destination\n')
            else:
                sys.stdout.write('Creating stream destination: {:s}\n'.format(stream_destination))
                try:
                    os.makedirs(stream_destination)
                except OSError as e:
                    sys.stderr.write('{:s}\n'.format(e.strerror))
                    continue
        
        # See if the fully qualified NCML destination directory needs to be created    
        if not os.path.exists(ncml_destination):
            
            if not args.move:
                sys.stdout.write('DEBUG> Skipping creation of NCML destination\n')
            else:
                sys.stdout.write('Creating stream destination: {:s}\n'.format(ncml_destination))
                try:
                    os.makedirs(ncml_destination)
                except OSError as e:
                    sys.stderr.write('{:s}\n'.format(e.strerror))
                    continue
                    
        # Write the NCML aggregation file using NCML_TEMPLATE        
        dataset_id = '{:s}-{:s}-{:s}'.format(
            stream['instrument'],
            stream['stream'],
            stream['telemetry'])
        
        ncml_file = os.path.join(ncml_destination, '{:s}.ncml'.format(dataset_id))
        sys.stdout.write('NCML aggregation file: {:s}\n'.format(os.path.split(ncml_file)[1]))
        # Write the NCML file, using NCML_TEMPLATE, if it doesn't already exist
        if not os.path.exists(ncml_file):
            
            if not args.move:
                sys.stdout.write('DEBUG> Skipping NCML aggregation file creation\n')
            else:
                try:
                    sys.stdout.write('Loading NCML aggregation template: {:s}\n'.format(ncml_file))
                    template_fid = open(NCML_TEMPLATE, 'r')
                    ncml_template = template_fid.read()
                    template_fid.close()
                    
                    sys.stdout.write('Writing NCML aggregation file: {:s}\n'.format(ncml_file))
                    stream_ncml = ncml_template.format(dataset_id, stream_destination)
                    
                    ncml_fid = open(ncml_file, 'w')
                    ncml_fid.write(stream_ncml)
                    ncml_fid.close()
                except IOError as e:
                    sys.stderr.write('{:s}: {:s}\n'.format(e.filename, e.strerror))
                    continue
        
        #sys.stdout.write('Stopping before we do any damage')
        #continue
            
        # Rename the files and move them to TDS_NC_ROOT
        ts_nc_files = []
        for nc_file in nc_files:

            (nc_path, nc_filename) = os.path.split(nc_file)
            sys.stdout.write('UFrame NetCDF : {:s}/{:s}\n'.format(os.path.split(nc_path)[-1], nc_filename))

            # Timestamp the file but do not prepend a destination directory
            ts_nc_file = timestamp_nc_file(nc_file, dest_dir=None)
            if not ts_nc_file:
                sys.stderr.write('Failed to timestamp UFrame NetCDF file: {:s}\n'.format(nc_file))
                continue
            
            sys.stdout.write('THREDDS NetCDF: {:s}\n'.format(ts_nc_file))
            # Create the NetCDF destination file        
            tds_nc_file = os.path.join(stream_destination, ts_nc_file)
            ts_nc_files.append(tds_nc_file)

            # Skip moving the file if in debug mode, but tell me what the new
            # filename is
            if not args.move:
#                sys.stdout.write('DEBUG> Skipping file creation\n')
                continue
            
            # Move the file
            try:
                sys.stdout.write('Moving UFrame NetCDF file: {:s}\n'.format(nc_file))
                sys.stdout.write('Timestamp NetCDF file    : {:s}\n'.format(ts_nc_file))
                shutil.copyfile(nc_file, tds_nc_file)
            except IOError as e:
                sys.stderr.write('{:s}: {:s}\n'.format(e.strerr, ts_nc_file))
     
        ts_nc_files.sort()
        sys.stdout.write('Found {:d} files\n'.format(len(ts_nc_files)))
        for ts_nc_file in ts_nc_files:
            (ts_nc_dir, ts_nc_name) = os.path.split(ts_nc_file)
            sys.stdout.write('Timestamp NetCDF File: {:s}\n'.format(ts_nc_name))

        # Mark the request as complete if we've moved at least one NetCDF file
        # to stream_destination
        stream['reason'] = 'Complete'

        if args.delete:
            sys.stdout.write('Deleting UFrame product destination: {:s}\n'.format(product_dir))
            try:
                os.rmdir(product_dir)
            except OSError as e:
                sys.stderr.write('Failed to delete UFrame product destination: {:s} (Reason: {:s})\n'.format(product_dir, e.strerror))
                sys.stderr.flush()
                continue
                
    # Delete the request_queue file
    if not args.move:
        sys.stdout.write('DEBUG> Keeping stream request file: {:s}\n'.format(args.queue_csv))
    else:
        try:
            os.remove(args.queue_csv)
        except OSError as e:
            sys.stderr.write('Failed to remove request queue file: {:s} (Reason: {:s})\n'.format(args.queue_csv, e.strerror))
            sys.stderr.flush()
            return 1
                
    # Write updated requests back to args.queue_csv
    if not args.move:
        sys.stdout.write('DEBUG> Stream status:\n')
        csv_writer = csv.writer(sys.stdout)
    else:
        sys.stdout.write('Saving updated requests: {:s}\n'.format(args.queue_csv))
        try:
            fid = open(args.queue_csv, 'w')
            csv_writer = csv.writer(fid)
        except IOError as e:
            sys.stderr.write('{:s}: {:s}\n'.format(args.queue_csv, e.strerror))
            return 1
   
    # Write all requests back to the args.queue_csv
    cols = stream_requests[0].keys()
    csv_writer.writerow(cols)
    for stream in stream_requests:
        row = [stream[k] for k in cols]
        csv_writer.writerow(row)
                
    return 0
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('queue_csv',
        help='Filename containing queued request information.')
    arg_parser.add_argument('-u', '--user',
        default='_nouser',
        help='Alternate user name (_nouser is <default>)')
    arg_parser.add_argument('-k', '--keep',
        dest='delete',
        action='store_true',
        help='Keep original UFrame product destination')
    arg_parser.add_argument('-m', '--move',
        dest='move',
        action='store_true',
        help='Create NCML file and move NetCDF files to THREDDS');
    arg_parser.add_argument('-v', '--validate',
        dest='validate',
        action='store_true',
        help='Validate environment set up only.')
    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
