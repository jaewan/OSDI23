#! /bin/bash

################ System Variables ################ 
LOG_DIR=../../data/
TEST_FILE=pipeline.py
OBJECT_STORE_SIZE=4000000000
OBJECT_SIZE=100000000


BACKPRESSURE_THRESHOLD=0.8

function Test()
{
	BLOCKSPILL=$1
	BACKPRESSURE=$2
	DEADLOCK1=$3
	DEADLOCK2=$4
	RESULT_FILE=$LOG_DIR$5.csv
	test -f "$RESULT_FILE" && rm $RESULT_FILE
	echo "std,var,working_set,object_store_size,object_size,time" >>$RESULT_FILE
	for w in {1,2,4,8}
	do
		echo $5 -w $w
		#RAY_BACKEND_LOG_LEVEL=debug RAY_object_spilling_threshold=1.0  RAY_block_tasks_threshold=$BACKPRESSURE_THRESHOLD \
		RAY_object_spilling_threshold=1.0  RAY_block_tasks_threshold=$BACKPRESSURE_THRESHOLD \
		RAY_enable_BlockTasks=$BACKPRESSURE RAY_enable_BlockTasksSpill=$BLOCKSPILL \
		RAY_enable_Deadlock1=$DEADLOCK1 RAY_enable_Deadlock2=$DEADLOCK2 \
		python $TEST_FILE -w $w -r $RESULT_FILE  -o $OBJECT_STORE_SIZE -os $OBJECT_SIZE -t 10
	done
	#rm -rf /tmp/ray/*
}

#Test false false false false DFS
Test false true false false Backpressure_Performance
