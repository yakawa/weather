#!/bin/bash

RSYNC=/usr/bin/rsync
USER=weather
HOST=$1

if [ $# -ne 1 ]; then 
  exit 1
fi

${RSYNC} -azcq --delete /home/DATA/outgoing/wrf ${USER}@${host}:/home/DATA/incoming/wrf
