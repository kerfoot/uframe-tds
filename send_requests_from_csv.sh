#! /bin/bash --
#
# USAGE:
#
# ============================================================================
# $RCSfile$
# $Source$
# $Revision$
# $Date$
# $Author$
# $Name$
# ============================================================================
#

PATH=${HOME}/uframe-async-requests:${PATH}:/bin;

app=$(basename $0);

QUEUE_DIR="${ASYNC_DATA_HOME}/stream-queue";
PROCESSED_URLS_DIR="${ASYNC_DATA_HOME}/stream-requests/processed";

# Usage message
USAGE="
NAME
    $app - send UFrame data product requests

SYNOPSIS
    $app [h] file1[ file2 file..]

DESCRIPTION
    Send the UFrame data product requests contained in one or more text files
    and log the request queues and error messages to:

        $QUEUE_DIR

    Each specified file is moved to:

        $PROCESSED_URLS_DIR

    after all requests are sent

    IMPORTANT: You must set the ASYNC_DATA_HOME environment directory to a
    valid directory!

    -h
        show help message
";

# Default values for options

# Process options
while getopts "h:v" option
do
    case "$option" in
        "h")
            echo -e "$USAGE";
            exit 0;
            ;;
        "v")
            echo "Validating environment only!";
            VALIDATE=1;
            ;;
        "?")
            echo -e "$USAGE" >&2;
            exit 1;
            ;;
    esac
done

# Remove option from $@
shift $((OPTIND-1));

if [ -z "$ASYNC_DATA_HOME" ]
then
    echo "ASYNC_DATA_HOME not set" >&2;
    exit 1
elif [ ! -d "$ASYNC_DATA_HOME" ]
then
    echo "Invalid ASYNC_DATA_HOME directory: $ASYNC_DATA_HOME" >&2;
    exit 1
fi

QUEUE_DIR="${ASYNC_DATA_HOME}/stream-queue";
if [ ! -d "$QUEUE_DIR" ]
then
    echo "Invalid stream-queue directory: $QUEUE_DIR";
    exit 1;
fi

PROCESSED_URLS_DIR="${ASYNC_DATA_HOME}/stream-requests/processed";
if [ ! -d "$PROCESSED_URLS_DIR" ]
then
    echo "Invalid processed urls directory: $PROCESSED_URLS_DIR";
    exit 1;
fi

if [ -n "$VALIDATE" ]
then
    echo "Queue directory: $QUEUE_DIR";
    echo "Processed dir  : $PROCESSED_URLS_DIR";
    exit 0;
fi

if [ "$#" -eq 0 ]
then
    echo "No request csv file(s) specified" >&2;
    exit 1;
fi

for f in "$@"
do

    if [ ! -f "$f" ]
    then
        echo "Requests file does not exist: $f" >&2;
        continue
    fi

    echo "Sending requests: $f";
    
    output_template=$(basename $f .csv)-queue;
    csv_out=${QUEUE_DIR}/${output_template}.csv;
    stderr_log=${QUEUE_DIR}/${output_template}.stderr;

    echo "Queue file : $csv_out";
    echo "stderr file: $stderr_log";

    send_async_requests_from_urlcsv.py $f \
        > $QUEUE_DIR/$(basename $f .csv)-queue.csv \
        2> $QUEUE_DIR/$(basename $f .csv)-queue.stderr;

    if [ "$?" -eq 0 ]
    then
        echo "Moving $f to $PROCESSED_URLS_DIR";
        mv $f $PROCESSED_URLS_DIR;
    fi

done

