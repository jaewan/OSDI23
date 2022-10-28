#! /bin/bash

DEBUG=true

################ System Variables ################ 
APPLICATION=sort
LOG_DIR=../data/push_based_shuffle\_
TEST_FILE=../macrobench/$APPLICATION.py
NUM_PARTITIONS=10
PARITION_SIZE=1e7

################ Test Techniques ################ 
Production_RAY=false
DFS=true
DFS_EVICT=false
DFS_BACKPRESSURE=false
DFS_BLOCKSPILL=false
DFS_EVICT_BLOCKSPILL=false
DFS_BACKPRESSURE_BLOCKSPILL=false
EAGER_SPILL=true

function Test()
{
	BACKPRESSURE=$1
	BLOCKSPILL=$2
	EVICT=$3
	EAGERSPILL=$4
	RESULT_FILE=$LOG_DIR$5.csv
	if $DEBUG;
	then
		#for w in {1,2,4,8}
		#do
		w=3
			let a=20000
			echo $5 -w $w 
			RAY_BACKEND_LOG_LEVEL=debug \
			RAY_block_tasks_threshold=1.0 RAY_object_spilling_threshold=1.0 RAY_spill_wait_time=$a RAY_enable_Deadlock2=true \
			RAY_enable_BlockTasks=$BACKPRESSURE  RAY_enable_EvictTasks=$EVICT RAY_enable_BlockTasksSpill=$BLOCKSPILL RAY_enable_EagerSpill=$EAGERSPILL\
			RAY_DATASET_PUSH_BASED_SHUFFLE=1 \
			python $TEST_FILE --num-partitions=$NUM_PARTITIONS --partition-size=$PARITION_SIZE
		#done
	else
		test -f "$RESULT_FILE" && rm $RESULT_FILE
		echo "std,var,working_set,object_store_size,object_size,time,num_spill_objs,spilled_size" >>$RESULT_FILE
		for w in {1,2,4,8}
		do
			let a=50000
			echo $5 -w $w 
			RAY_block_tasks_threshold=1.0 RAY_object_spilling_threshold=1.0 RAY_spill_wait_time=$a RAY_enable_Deadlock2=true \
			RAY_enable_BlockTasks=$BACKPRESSURE  RAY_enable_EvictTasks=$EVICT RAY_enable_BlockTasksSpill=$BLOCKSPILL RAY_enable_EagerSpill=$EAGERSPILL\
			RAY_DATASET_PUSH_BASED_SHUFFLE=1 \
			python $TEST_FILE --num-partitions=$NUM_PARTITIONS --partition-size=$PARITION_SIZE
		done
		rm -rf /tmp/ray/*
	fi
}

if $Production_RAY;
then
	Test false false false false RAY
fi

if $DFS;
then
	Test false false false false DFS
fi


if $DFS_EVICT;
then
	Test false false true false DFS_Evict
fi

if $DFS_BACKPRESSURE;
then
	Test true false false false DFS_Backpressure
fi

if $DFS_BLOCKSPILL;
then
	Test false true false false DFS_BlockSpill
fi

if $DFS_EVICT_BLOCKSPILL;
then
	Test false true true false DFS_Evict_BlockSpill
fi

if $DFS_BACKPRESSURE_BLOCKSPILL;
then
 	Test true true false false DFS_Backpressure_BlockSpill_Deadlock
fi

if $EAGER_SPILL;
then
 	Test false false false true EagerSpill
fi

################ Plot Graph ################ 
if ! $DEBUG;
then
./plot.py $APPLICATION
fi
