#!/bin/bash

RSYNC=/usr/bin/rsync
USER=weather
HOSTS=("wrf001.hiyorimi.jp" "wrf-dev.hiyorimi.jp")

for host in "${HOSTS[@]}"
do
    ${RSYNC} -azcq /home/DATA/outgoing/wrf ${USER}@${host}:/home/DATA/incoming/wrf
done


    
