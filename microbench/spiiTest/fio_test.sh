#! /bin/bash

#sync; echo 3 > /proc/sys/vm/drop_caches
#sudo fio --name=write --ioengine=libaio --iodepth=1 --rw=write --bs=4k --direct=0 --buffered=1 --size=16G --numjobs=1
#sudo fio --name=write --ioengine=libaio --iodepth=1 --rw=write --bs=4k --direct=1 --buffered=0 --size=16G --numjobs=1
sudo fio --name=write --ioengine=libaio --iodepth=1 --rw=write --bs=4k --direct=1 --size=16G --numjobs=1
#sync; echo 3 > /proc/sys/vm/drop_caches
#sudo fio --name=write --ioengine=libaio --iodepth=1 --rw=write --bs=4k --direct=0 --buffered=1 --size=64G --numjobs=1
