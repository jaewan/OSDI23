#! /bin/bash

DEBUG=true

################ Test Techniques ################ 
Production_RAY=true
DFS_EVICT=false
DFS_BACKPRESSURE=false
DFS_BLOCKSPILL=false
DFS_EVICT_BLOCKSPILL=false
DFS_BACKPRESSURE_BLOCKSPILL=false
DFS_EAGERSPILL=false
COMPLETE_BOA=false
MULTI_NODE=true
n=$(python multinode/get_node_count.py 2>&1)
NUM_NODES=$(($n + 0))

################ System Variables ################ 
APPLICATION=video_processing
LOG_DIR=~/OSDI23/data/single_node/$APPLICATION/
if $MULTI_NODE;
then
	LOG_DIR=~/OSDI23/data/$APPLICATION/
fi
TEST_FILE=~/OSDI23/macrobench/video-processing/$APPLICATION.py
OBJECT_STORE_SIZE=16000000000
NUM_VIDEOS=14
NUM_CPUS=30

mkdir -p $LOG_DIR

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
		echo "mean_latency,max_latency,runtime,spilled_size,migration_count,object_store_size" >>$RESULT_FILE
	fi
	echo $APPLICATION 
	if $MULTI_NODE;
	then
		for ((n=0;n<$NUM_TRIAL;n++))
		do
			python multinode/wake_worker_node.py -nw $NUM_CPUS -o $OBJECT_STORE_SIZE -b $BACKPRESSURE -bs $BLOCKSPILL -e $EAGERSPILL
			ray job submit --working-dir ~/OSDI23/microbench/instantSubmission \
				~/OSDI23/script/multinode/submit_job.sh $TEST_FILE $RESULT_FILE $NUM_VIDEOS  $NUM_NODES
			python multinode/wake_worker_node.py -s true
			ray stop
		done
	else
		python $TEST_FILE -r $RESULT_FILE --num-videos=$NUM_VIDEOS --NUM_NODES=1 --max-frames=1800 --local
	fi
	rm -rf /tmp/ray/*
}

if $Production_RAY;
then
	#./../script/install/install_production_ray.sh
	Test false false false false false RAY
fi

if $DFS;
then
	#./../script/install/install_boa.sh
	rm -rf /tmp/ray
	Test false false false false false DFS
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

if $COMPLETE_BOA;
then
 	Test true true false true false BOA
fi
################ Plot Graph ################ 
<<comment
if ! $DEBUG;
then
./plot.py $APPLICATION
fi
comment
