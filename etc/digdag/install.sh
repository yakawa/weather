#!/bin/bash

curl -o /usr/local/bin/digdag -L "https://dl.digdag.io/digdag-latest"
chmod +x /usr/local/bin/digdag
apt install openjdk-8-jre
cp digdag.service /etc/systemd/system/
mkdir /etc/digdag
mkdir /usr/local/share/digdag
cp digdag.conf /etc/digdag/
systemctl enable digdag
systemctl start digdag
