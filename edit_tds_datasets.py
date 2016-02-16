#!/usr/bin/env python

import os
import sys
import argparse
import csv
import glob
import shutil

def main(args):
    '''Parses CSV_FILE for records containing a reference designator, telemetry type
    and stream name.  Each record is used to create and validate a THREDDS stream location
    (.ncml and .nc files) and the list of files is printed to STDOUT.  Additional
    options are available to copy, move or delete the found files and resulting
    degenerate directory tree.'''
    
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
        
    status = 1
    
    TDS_NC_ROOT = args.tdsroot
    if not TDS_NC_ROOT:
        # Make sure the ASYNC_TDS_NC_ROOT is set  in the user's environment if it
        # wasn't specified on the command line
        TDS_NC_ROOT = os.getenv('ASYNC_TDS_NC_ROOT')
        if not TDS_NC_ROOT:
            sys.stderr.write('ASYNC_TDS_NC_ROOT environment variable not set\n')
            sys.stderr.flush()
            return 1
        
    if not os.path.exists(TDS_NC_ROOT):
        sys.stderr.write('ASYNC_TDS_NC_ROOT is invalid: {:s}\n'.format(TDS_NC_ROOT))
        sys.stderr.flush()
        return 1
    
    # If move or copy was specified, check args.location to ensure it is valid
    location = args.location
    if args.move or args.copy:
        if not args.location:
            sys.stderr.write('No THREDDS destination specified\n')
            return status
        if not os.path.exists(args.location):
            sys.stderr.write('Invalid destination specified: {:s}\n'.format(args.location))
            return status
                
    csv_file = args.csv_file
    # convert the csv file rows to a dictionary
    streams = csv2json(csv_file)
    if not streams:
        sys.stderr.write('No streams parsed from csv file: {:s}\n'.format(csv_file))
        return status
    
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
        
        if not os.path.exists(tds_path):
            sys.stderr.write('Invalid THREDDS stream location: {:s}\n'.format(tds_path))
            continue
        
        sys.stdout.write('Valid THREDDS stream location: {:s}\n'.format(tds_path))
          
        # Get the list of .ncml and .nc files in tds_path
        f_contents = glob.glob(os.path.join(tds_path, '*.*'))
        if not f_contents:
            sys.stderr.write('No files found: {:s}\n'.format(tds_path))
            continue
            
        if not args.delete and not args.copy and not args.move:
            sys.stdout.write('No file operations will be performed\n')
            # Print the list of files found, then skip since we're not operating on
            # these files
            for f in f_contents:
                sys.stdout.write('Found file: {:s}\n'.format(f))
                
            continue
            
        elif location and not os.path.exists(location):
            # Validate the specified destination root directory
            sys.stderr.write('Invalid destination root: {:s}\n'.format(location))
            return status
           
        # Copy, move or delete the stream tree if specified on the command line 
        if args.copy:
            sys.stdout.write('Copying THREDDS stream to {:s}\n'.format(location))
            
            # Create the rel_path under location
            new_location = os.path.join(location, rel_path)
            if not os.path.exists(new_location):
                try:
                    os.makedirs(new_location)
                except OSError as e:
                    sys.stderr.write('{:s}: {:s}\n'.format(e.strerror, new_location))
                    continue
                    
            for f in f_contents:
                # Copy the file only if it doesn't already exist
                dest_nc = os.path.join(new_location, os.path.basename(f))
                if os.path.exists(dest_nc):
                    sys.stderr.write('Destination file already exists: {:s}\n'.format(dest_nc))
                    continue
                try:
                    sys.stdout.write('Copying {:s}: {:s}\n'.format(os.path.basename(f), new_location))
                    shutil.copy(f, new_location)
                except IOError as e:
                    sys.stderr.write('{:s}: {:s}\n'.format(e.strerror, f))
                    continue
                
        elif args.move:
            sys.stdout.write('Moving THREDDS stream to {:s}\n'.format(location))
            # Create the rel_path under location
            new_location = os.path.join(location, rel_path)
            if not os.path.exists(new_location):
                try:
                    os.makedirs(new_location)
                except OSError as e:
                    sys.stderr.write('{:s}: {:s}\n'.format(e.strerror, new_location))
                    continue
            
            move_status = True        
            for f in f_contents:
                # Move the file only if it doesn't already exist
                dest_nc = os.path.join(new_location, os.path.basename(f))
                if os.path.exists(dest_nc):
                    sys.stderr.write('Destination file already exists: {:s}\n'.format(dest_nc))
                    continue
                try:
                    sys.stdout.write('Moving {:s}: {:s}\n'.format(os.path.basename(f), new_location))
                    shutil.move(f, new_location)
                except IOError as e:
                    sys.stderr.write('{:s}: {:s}\n'.format(e.strerror, f))
                    move_status = False
                    continue
                    
            if not move_status:
                sys.stderr.write('Keeping stream source (1 or more move errors)\n')
                continue
                
            # Prune the empty source directory if the move was successful
            deleted_dirs = prune_empty_directories(TDS_NC_ROOT, rel_path)
            for d in deleted_dirs:
                sys.stdout.write('Deleted directory: {:s}\n'.format(d))
            
        elif args.delete:
            sys.stdout.write('Deleting THREDDS stream: {:s}\n'.format(tds_path))
            
            # Remove all files first
            sys.stdout.write('Removing files...\n')
            for f in f_contents:
                try:
                    sys.stdout.write('Deleting file: {:s}\n'.format(f))
                    os.remove(f)
                except OSError as e:
                    sys.stderr.write('{:s}: {:s}\n'.format(e.strerror, f))
                    continue
            # Recursively delete the directories from the bottom up provided they
            # are empty
            sys.stdout.write('Pruning path: {:s}\n'.format(rel_path))
            deleted_dirs = prune_empty_directories(TDS_NC_ROOT, rel_path)
            for d in deleted_dirs:
                sys.stdout.write('Deleted directory: {:s}\n'.format(d))
    
    status = 0
              
    return status

def prune_empty_directories(root_dir, rel_path):
    '''Recursively remove empty child directories, from the bottom up.  Removed
    directories are relative (rel_path) to the root_dir'''
    
    deleted_dirs = []
    
    d_tokens = rel_path.split('/')
    while d_tokens:
        target_dir = os.path.join(*[root_dir, os.path.join(*d_tokens)])
        if not os.path.exists(target_dir):
            sys.stderr.write('Directory does not exist: {:s}\n'.format(target_dir))
            return deleted_dirs
            
        f_contents = os.listdir(target_dir)
        if f_contents:
            sys.stdout.write('Directory is not empty: {:s}\n'.format(target_dir))
            return deleted_dirs
            
        # Remove the empty directory
        try:
            os.rmdir(target_dir)
        except OSError as e:
            sys.stderr.write('{:s}: {:s}\n'.format(e.strerror, target_dir))
            break
            
        deleted_dirs.append(target_dir)
        
        # Remove the last element from d_tokens to go up one directory
        d_tokens.pop(-1)
        
    return deleted_dirs
        
def csv2json(csv_filename):
    '''Parses the comma-separated value file, in which the first row is a column
    header, and creates an array of dictionaries'''
    
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
        help='Location of the THREDDS root directory containing the source files.  Must be specified if ASYNC_TDS_NC_ROOT is not set')
    arg_parser.add_argument('--location',
        type=str,
        help='Location to move or copy directory tree.  Must be specified if the --copy or --move option is used')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
