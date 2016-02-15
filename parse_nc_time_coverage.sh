#! /bin/bash
#

PATH=${PATH}:/bin;

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

if [ "$#" -eq 0 ]
then
    echo "No NetCDF files specified" >&2;
    exit 1;
fi

for nc in "$@"
do
#    echo "NC: $nc";
    
    nc_file=$(basename $nc .nc);
#    echo "NetCDF: $nc_file";

    ts0=$(echo $nc_file | gawk -F- '{print $(NF-1)}');
    ts1=$(echo $nc_file | gawk -F- '{print $NF}');
#    echo "TS0: $ts0";
#    echo "TS1: $ts1";

    if [ -z "$old_ts1" ]
    then
        old_ts1=$ts1;
        continue
    fi

    echo "$old_ts1,$ts0";
    old_ts1=$ts1;

done

