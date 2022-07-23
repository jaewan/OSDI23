#! /bin/bash

DEBUG=true

################ System Variables ################ 
APPLICATION=pipeline
LOG_DIR=../data/$APPLICATION\_
TEST_FILE=../microbench/instantSubmission/$APPLICATION.py
OBJECT_STORE_SIZE=4000000000
OBJECT_SIZE=100000000

################ Test Techniques ################ 
Production_RAY=false
OFFLINE=false
DFS=false
DFS_BACKPRESSURE=false
DFS_BLOCKSPILL=false
DFS_BACKPRESSURE_BLOCKSPILL=true

function Test()
{
	BACKPRESSURE=$1
	BLOCKSPILL=$2
	RESULT_FILE=$LOG_DIR$3.csv
	if $DEBUG;
	then
		#for w in {1,2,4,8}
		#do
		w=8
			let a=20000
			echo $3 -w $w 
			RAY_BACKEND_LOG_LEVEL=debug \
			RAY_enable_BlockTasks=$BACKPRESSURE RAY_block_tasks_threshold=1.0 RAY_object_spilling_threshold=1.0 RAY_spill_wait_time=$a RAY_enable_BlockTasksSpill=$BLOCKSPILL RAY_enable_Deadlock2=true \
			python $TEST_FILE -w $w -o $OBJECT_STORE_SIZE -os $OBJECT_SIZE -t 1 -nw 60
		#done
	else
		#test -f "$RESULT_FILE" && rm $RESULT_FILE
		#echo "std,var,working_set,object_store_size,object_size,time,num_spill_objs,spilled_size" >>$RESULT_FILE
		#for w in {1,2,4,8}
		#do
		w=8
			let a=50000
			echo $3 -w $w 
			RAY_enable_BlockTasks=$BACKPRESSURE RAY_block_tasks_threshold=1.0 RAY_object_spilling_threshold=1.0 RAY_spill_wait_time=$a RAY_enable_BlockTasksSpill=$BLOCKSPILL RAY_enable_Deadlock2=true \
			python $TEST_FILE -w $w -r $RESULT_FILE -o $OBJECT_STORE_SIZE -os $OBJECT_SIZE -t 5 -nw 60 
		#done
		rm -rf /tmp/ray/*
	fi
}

if $Production_RAY;
then
	Test false false RAY
fi

if $DFS;
then
	Test false false DFS
fi

if $DFS_BACKPRESSURE;
then
	Test true false DFS_Backpressure
fi

if $DFS_BLOCKSPILL;
then
	Test false true DFS_BlockSpill
fi

if $DFS_BACKPRESSURE_BLOCKSPILL;
then
 	Test true true DFS_Backpressure_BlockSpill_Deadlock
fi

################ Plot Graph ################ 
#./plot.py

