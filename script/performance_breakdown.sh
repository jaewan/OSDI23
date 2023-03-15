#! /bin/bash

DEBUG=true

################ System Variables ################ 
APPLICATION=pipeline
LOG_DIR=../data/$APPLICATION/
TEST_FILE=../microbench/instantSubmission/$APPLICATION.py
OBJECT_STORE_SIZE=4000000000
OBJECT_SIZE=100000000
NUM_CPUS=32

################ Test Techniques ################ 
Production_RAY=false
OFFLINE=false
DFS=true
DFS_EVICT=false
DFS_BACKPRESSURE=false
DFS_BLOCKSPILL=false
DFS_EVICT_BLOCKSPILL=false
DFS_BACKPRESSURE_BLOCKSPILL=false
DFS_EAGERSPILL=false
MULTI_NODE=false

function Test()
{
	BACKPRESSURE=$1
	BLOCKSPILL=$2
	EVICT=$3
	EAGERSPILL=$4
	OFF=$5
	RESULT_FILE=$LOG_DIR$6.csv
	if $DEBUG;
	then
		#for w in {1,2,4,8}
		#do
		w=2
			let a=20000
			echo $6 -w $w 
			RAY_worker_lease_timeout_milliseconds=0 RAY_worker_cap_enabled=false \
			RAY_PROFILING=1\
			RAY_BACKEND_LOG_LEVEL=debug \
			RAY_block_tasks_threshold=1.0 RAY_object_spilling_threshold=1.0 RAY_spill_wait_time=$a RAY_enable_Deadlock2=true \
			RAY_enable_BlockTasks=$BACKPRESSURE  RAY_enable_EvictTasks=$EVICT RAY_enable_BlockTasksSpill=$BLOCKSPILL RAY_enable_EagerSpill=$EAGERSPILL\
			python $TEST_FILE -w $w -o $OBJECT_STORE_SIZE -os $OBJECT_SIZE -t 1 -nw 32 -m $MULTI_NODE
		#done
	else
		test -f "$RESULT_FILE" && rm $RESULT_FILE
		echo "std,var,working_set,object_store_size,object_size,time,num_spill_objs,spilled_size" >>$RESULT_FILE
		for w in {1,2,4,8}
		do
			let a=50000
			echo $6 -w $w 
			RAY_worker_lease_timeout_milliseconds=0 RAY_worker_cap_enabled=false \
			RAY_block_tasks_threshold=1.0 RAY_object_spilling_threshold=1.0 RAY_spill_wait_time=$a RAY_enable_Deadlock2=$BLOCKSPILL \
			RAY_enable_BlockTasks=$BACKPRESSURE  RAY_enable_EvictTasks=$EVICT RAY_enable_BlockTasksSpill=$BLOCKSPILL RAY_enable_EagerSpill=$EAGERSPILL\
			python $TEST_FILE -w $w -r $RESULT_FILE -o $OBJECT_STORE_SIZE -os $OBJECT_SIZE  -off $OFF -t 5 -nw 32 -m $MULTI_NODE
		done
		rm -rf /tmp/ray/*
	fi
}

if $MULTI_NODE;
then
	python multinode/wake_worker_node.py -nw $NUM_CPUS -o $OBJECT_STORE_SIZE
fi

if $Production_RAY;
then
	./../script/install/install_production_ray.sh
	Test false false false false false RAY
fi

if $DFS;
then
	#./../script/install/install_boa.sh
	Test false false false false false DFS
fi

if $OFFLINE;
then
	Test false false false false true OFFLINE
fi

if $DFS_EVICT;
then
	Test false false true false false DFS_Evict
fi

if $DFS_BACKPRESSURE;
then
	Test true false false false false DFS_Backpressure
fi

if $DFS_BLOCKSPILL;
then
	Test false true false false false DFS_BlockSpill
fi

if $DFS_EVICT_BLOCKSPILL;
then
	Test false true true false false DFS_Evict_BlockSpill
fi

if $DFS_BACKPRESSURE_BLOCKSPILL;
then
 	Test true true false false false DFS_Backpressure_BlockSpill_Deadlock
fi

if $DFS_EAGERSPILL;
then
 	Test false false false true false EagerSpill
fi

################ Plot Graph ################ 
<<comment
if ! $DEBUG;
then
./plot.py $APPLICATION
fi
comment
