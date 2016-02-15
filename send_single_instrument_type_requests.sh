#! /bin/bash --
#
# USAGE:
#

PATH=/Users/kerfoot/code/ooi/uframe-async-requests:${PATH}:/bin;

app=$(basename $0);

# Usage message
USAGE="
NAME
    $app - 

SYNOPSIS
    $app [h]

DESCRIPTION
    -h
        show help message
";

# Default values for options

# Process options
while getopts "h" option
do
    case "$option" in
        "h")
            echo -e "$USAGE";
            exit 0;
            ;;
        "?")
            echo -e "$USAGE" >&2;
            exit 1;
            ;;
    esac
done

# Remove option from $@
shift $((OPTIND-1));

REQUEST_ROOT=$ASYNC_DATA_HOME/stream-requests;
#REQUEST_DONE_ROOT=$REQUEST_ROOT/processed;
QUEUE_ROOT=$ASYNC_DATA_HOME/stream-queue;
if [ ! -d "$REQUEST_ROOT" ]
then
    echo "Invalid REQUEST csv directory: $REQUEST_ROOT" >&2;
    exit 1;
fi
#if [ ! -d "$REQUEST_DONE_ROOT" ]
#then
#    echo "Invalid REQUEST DONE csv directory: $REQUEST_DONE_ROOT" >&2;
#    exit 1;
#fi
if [ ! -d "$QUEUE_ROOT" ]
then
    echo "Invalid QUEUE csv directory: $QUEUE_ROOT" >&2;
    exit 1;
fi
INSTRUMENTS_FILE="${REQUEST_ROOT}/instrument-types.txt";
if [ ! -f "$INSTRUMENTS_FILE" ]
then
    echo "Invalid instrument types file: $INSTRUMENTS_FILE" >&2;
    exit 1;
fi

while read t
do
#    echo "TYPE: $t";
    url_file=$(ls $REQUEST_ROOT/$t*.csv 2> /dev/null | head -1); 
    if [ -z "$url_file" ]
    then
        echo "No request files for instrument type: $t" >&2;
        continue
    fi
    echo "Sending requests: $url_file";

    continue

    queue_csv="${QUEUE_ROOT}/$(basename $url_file .csv)-queue.csv";
    queue_stderr="${QUEUE_ROOT}/$(basename $url_file .csv)-queue.stderr";
#    echo "Queue csv : $queue_csv";
#    echo "STDERR csv: $queue_stderr";

#    send_async_requests_from_urlcsv.py $url_file > $queue_csv 2> $queue_stderr;

    sent_file="${url_file}.sent";
#    echo "Moving request csv: $url_file -> $sent_file";
#    mv $url_file $sent_file;
    
done < $INSTRUMENTS_FILE

