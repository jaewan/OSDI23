#! /bin/bash

DEBUG=true

################ System Variables ################ 
LOG_DIR=../../data/deadlock_Performance_pipeline_
TEST_FILE=../../microbench/instantSubmission/pipeline.py
OBJECT_STORE_SIZE=4000000000
OBJECT_SIZE=100000000

################ Test Techniques ################ 
Production_RAY=false
DFS=false
CONSTANT_WAIT=false
DEADLOCK_ONE=false
DEADLOCK_TWO=false
EAGERSPILL=true

function Test()
{
	BLOCKSPILL=$1
	DEADLOCK1=$2
	DEADLOCK2=$3
	RESULT_FILE=$LOG_DIR$4.csv
	if $DEBUG;
	then
		#for w in {1,2,4,8}
		#do
		w=8
			let a=20000
			echo $4 -w $w wait time $a
			RAY_BACKEND_LOG_LEVEL=debug \
			RAY_object_spilling_threshold=1.0 RAY_spill_wait_time=$a RAY_enable_BlockTasksSpill=$BLOCKSPILL  RAY_enable_Deadlock1=$DEADLOCK1 RAY_enable_Deadlock2=$DEADLOCK2 RAY_enable_EagerSpill=true\
			python $TEST_FILE -w $w -o $OBJECT_STORE_SIZE -os $OBJECT_SIZE -t 1
		#done
	else
		test -f "$RESULT_FILE" && rm $RESULT_FILE
		echo "std,var,working_set,object_store_size,object_size,time" >>$RESULT_FILE
		for w in {1,2,4,8}
		do
			let a=20000
			echo $4 -w $w wait time $a
			RAY_object_spilling_threshold=1.0 RAY_spill_wait_time=$a RAY_enable_BlockTasksSpill=$BLOCKSPILL  RAY_enable_Deadlock1=$DEADLOCK1 RAY_enable_Deadlock2=$DEADLOCK2 \
			python $TEST_FILE -w $w -r $RESULT_FILE -o $OBJECT_STORE_SIZE -os $OBJECT_SIZE -t 5
		done
		rm -rf /tmp/ray/*
	fi
}

if $Production_RAY;
then
	Test false false false RAY
fi

if $DFS;
then
	Test false false false DFS
fi

if $CONSTANT_WAIT;
then
	Test true false false Constant_Wait
fi

if $DEADLOCK_ONE;
then
	Test true true false 1
fi

if $DEADLOCK_TWO;
then
 	Test true false true 2
fi

if $EAGERSPILL;
then
 	Test false false false EagerSpill 
fi

################ Plot Graph ################ 
#./plot.py

