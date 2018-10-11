#!/bin/bash

function cleanup() {
   /usr/bin/find $1 -type f -mtime +$2| /usr/bin/xargs /bin/rm -rf
}

cleanup /home/weather/tmp 1
cleanup /home/weather/cache/JMAWIS/ 1
cleanup /home/weather/cache/GSM/ 7
cleanup /home/weather/cache/MSM/ 7
cleanup /home/weather/cache/JMAXML/ 2

