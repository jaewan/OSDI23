#! /bin/bash

FILESIZE=100M
#sync; echo 3 > /proc/sys/vm/drop_caches
sudo fio --name=global --ioengine=libaio --iodepth=1 --bs=4k --direct=1 --buffered=0 --size=$FILESIZE  --rw=write --name=job1 --ioengine=libaio --iodepth=1 --bs=4k --direct=1 --buffered=0 --size=$FILESIZE  --rw=read --name=job2 
