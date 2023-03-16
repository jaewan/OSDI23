#! /bin/bash

DEBUG=true

################ System Variables ################ 
APPLICATION=pipeline
LOG_DIR=../data/$APPLICATION/
TEST_FILE=~/OSDI23/microbench/instantSubmission/$APPLICATION.py
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
MULTI_NODE=true

function Test()
{
	BACKPRESSURE=$1
	BLOCKSPILL=$2
	EVICT=$3
	EAGERSPILL=$4
	OFF=$5
	RESULT_FILE=$LOG_DIR$6.csv
	NUM_TRIAL=1

	export RAY_worker_lease_timeout_milliseconds=0
	export RAY_worker_cap_enabled=false 
	export RAY_block_tasks_threshold=1.0 
	export RAY_object_spilling_threshold=1.0 
	export RAY_enable_Deadlock2=$BLOCKSPILL
	export RAY_enable_BlockTasks=$BACKPRESSURE
	export RAY_enable_EvictTasks=$EVICT
	export RAY_enable_BlockTasksSpill=$BLOCKSPILL
	export RAY_enable_EagerSpill=$EAGERSPILL

	if $DEBUG;
	then
		export RAY_BACKEND_LOG_LEVEL=debug
		RESULT_FILE='~/OSDI/data/dummy.csv'
	else
		NUM_TRIAL=5
		test -f "$RESULT_FILE" && rm $RESULT_FILE
		echo "std,var,working_set,object_store_size,object_size,time,num_spill_objs,spilled_size" >>$RESULT_FILE
	fi
	for w in {1,2,4,8}
	do
		if $MULTI_NODE;
		then
			for ((n=0;n<$NUM_TRIAL;n++))
			do
				python multinode/wake_worker_node.py -nw $NUM_CPUS -o $OBJECT_STORE_SIZE -b $BACKPRESSURE -bs $BLOCKSPILL -e $EAGERSPILL
				ray job submit --working-dir ~/OSDI23/microbench/instantSubmission \
					~/OSDI23/script/submit_job.sh $TEST_FILE  $w $RESULT_FILE $OBJECT_SIZE $OFF $MULTI_NODE 
				python multinode/wake_worker_node.py -s true
				ray stop
			done
		else
			python $TEST_FILE -w $w -r $RESULT_FILE -o $OBJECT_STORE_SIZE -os $OBJECT_SIZE -t $NUM_TRIAL -nw 32 -m $MULTI_NODE
		fi
		rm -rf /tmp/ray/*
	done
}

if $Production_RAY;
then
	./../script/install/install_production_ray.sh
	Test false false false false false RAY
fi

if $DFS;
then
	#./../script/install/install_boa.sh
	rm -rf /tmp/ray
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
