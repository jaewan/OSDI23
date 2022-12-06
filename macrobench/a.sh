#! /bin/bash 

function Flush(){
	pushd /tmp/ray
	rm -rf session*
	rm -rf *log
	rm -rf /dev/shm/*
	rm -rf /ray_spill/*
	popd
	sleep 3
}

NUM_PARTITION=512
PARTITION_SIZE=1e8

#./../script/install/install_production_ray.sh
#./0.sh $NUM_PARTITION $PARTITION_SIZE max 1
#Flush
#./1.sh $NUM_PARTITION $PARTITION_SIZE max 1
#Flush
#./2.sh $NUM_PARTITION $PARTITION_SIZE max 1
#Flush

./../script/install/install_boa.sh
./d0.sh $NUM_PARTITION $PARTITION_SIZE max 1
Flush
./d1.sh $NUM_PARTITION $PARTITION_SIZE max 1
Flush
./d2.sh $NUM_PARTITION $PARTITION_SIZE max 1
Flush
./b0.sh $NUM_PARTITION $PARTITION_SIZE max 1
Flush
./b1.sh $NUM_PARTITION $PARTITION_SIZE max 1
Flush
./b2.sh $NUM_PARTITION $PARTITION_SIZE max 1
Flush
./e0.sh $NUM_PARTITION $PARTITION_SIZE max 1
Flush
./e1.sh $NUM_PARTITION $PARTITION_SIZE max 1
Flush
./e2.sh $NUM_PARTITION $PARTITION_SIZE max 1
Flush
