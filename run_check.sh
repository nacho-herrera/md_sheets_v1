#!/bin/bash

# EXIT IF ALREADY RUNNING
if pidof -o %PPID -x "$0"; then
        echo "EXIT: Already running"
        exit 1
fi

# RUN THE SCRIPT
SCRIPT="/home/md_sheets_epbg/python/opciones/check_status2.py"

$SCRIPT
#cat $SCRIPT

exit

