#!/bin/bash
sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install tensorflow==1.15.0 keras==2.2.5 "h5py<3.0.0"
sudo python3 -m pip install pincelate
# sudo python3 -m pip install pyspark
sudo aws s3 cp s3://prosodies/scripts/soundout.py /usr/local/lib/python3.6/site-packages/soundout.py
sudo chmod 777 /usr/local/lib/python3.6/site-packages/soundout.py