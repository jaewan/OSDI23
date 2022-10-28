#! /bin/bash

################ System Variables ################ 
TEST_FILE=shuffle.py
OBJECT_STORE_SIZE=16000000000
OBJECT_SIZE=2000000000

################ Test Techniques ################ 
Production_RAY=false
DFS=true
CONSTANT_WAIT=false
DEADLOCK_ONE=false
DEADLOCK_TWO=false

function Test()
{
	BLOCKSPILL=$1
	DEADLOCK1=$2
	DEADLOCK2=$3
	w=2
		let a=25000
		echo $4 -w $w wait time $a
		RAY_BACKEND_LOG_LEVEL=debug \
		RAY_object_spilling_threshold=1.0 RAY_spill_wait_time=$a RAY_enable_BlockTasksSpill=$BLOCKSPILL  RAY_enable_Deadlock1=$DEADLOCK1 RAY_enable_Deadlock2=$DEADLOCK2 \
		python $TEST_FILE -w $w -o $OBJECT_STORE_SIZE -os $OBJECT_SIZE -t 1
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
