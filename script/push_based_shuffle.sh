#! /bin/bash

DEBUG=true

################ Application Config ################ 
APP_SCHEDULING=0
PRODUCTION_DIR=~/production_ray/python/ray/
BOA_DIR=~/ray_memory_management/python/ray
BASE_DIR=~/OSDI23/macrobench/

################ System Variables ################ 
APPLICATION=sort
LOG_DIR=~/OSDI23/data/$APPLICATION/
TEST_FILE=$BASE_DIR$APPLICATION.py
OBJECT_STORE_SIZE=16000000000
NUM_CPUS=22
NUM_PARTITION=64
PARTITION_SIZE=5e7

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

function SetUp()
{
	BOA=$1

	case $APP_SCHEDULING in
		# App-level scheduling
		0)
			CODE_PATH=$BASE_DIR/code/application_scheduling/push_based_shuffle.py
			;;
		# App-level scheduling off ver1
		1)
			CODE_PATH=$BASE_DIR/code/application_scheduling_off_ver1/push_based_shuffle.py
			;;
		# App-level scheduling off ver2
		2)
			CODE_PATH=$BASE_DIR/code/application_scheduling_off_ver2/push_based_shuffle.py
			;;
	esac

	if $BOA;
	then
		cp $CODE_PATH $BOA_DIR/data/_internal/
		echo 'Setup push_based_shuffle to Boa python files'
	else
		cp $CODE_PATH $PRODUCTION_DIR/data/_internal/
		echo 'Setup push_based_shuffle to Production Ray python files'
	fi
}

function Test()
{
	BACKPRESSURE=$1
	BLOCKSPILL=$2
	EVICT=$3
	EAGERSPILL=$4
	RESULT_FILE=$LOG_DIR$5.csv
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
		rm /tmp/ray/*log
		export RAY_BACKEND_LOG_LEVEL=debug
		RESULT_FILE='~/OSDI/data/dummy.csv'
	else
		NUM_TRIAL=5
		#test -f "$RESULT_FILE" && rm $RESULT_FILE
		echo "time,num_spill_objs,spilled_size,migration_count,working_set,object_store_size,object_size,std,var" >>$RESULT_FILE
	fi
	echo $APPLICATION
	if $MULTI_NODE;
	then
		for ((n=0;n<$NUM_TRIAL;n++))
		do
			python multinode/wake_worker_node.py -nw $NUM_CPUS -o $OBJECT_STORE_SIZE -b $BACKPRESSURE -bs $BLOCKSPILL -e $EAGERSPILL -a $APP_SCHEDULING
			ray job submit --working-dir $BASE_DIR \
				~/OSDI23/script/multinode/submit_job.sh $TEST_FILE  $RESULT_FILE $MULTI_NODE $NUM_PARTITION $PARTITION_SIZE
			python multinode/wake_worker_node.py -s true
			ray stop
		done
	else
		python $TEST_FILE -r $RESULT_FILE -o $OBJECT_STORE_SIZE -nw $NUM_CPUS -m $MULTI_NODE \
		--num-partitions=$NUM_PARTITION --partition-size=$PARTITION_SIZE 
	fi
	#rm -rf /tmp/ray/*
}

if $Production_RAY;
then
	#./../script/install/install_production_ray.sh
	SetUp false
	echo "Running [Production Ray] with Application-level Scheduling: $APP_SCHEDULING"
	Test false false false false RAY
fi

if $DFS;
then
	#./../script/install/install_boa.sh
	rm -rf /tmp/ray
	SetUp true
	echo "Running [BOA-DFS Ray] with Application-level Scheduling: $APP_SCHEDULING"
	Test false false false false DFS
fi

if $DFS_EVICT;
then
	SetUp true
	echo "Running [BOA-DFS-Evict Ray] with Application-level Scheduling: $APP_SCHEDULING"
	Test false false true false DFS_Evict
fi

if $DFS_BACKPRESSURE;
then
	SetUp true
	echo "Running [BOA-DFS-Backpressure Ray] with Application-level Scheduling: $APP_SCHEDULING"
	Test true false false false DFS_Backpressure
fi

if $DFS_BLOCKSPILL;
then
	SetUp true
	echo "Running [BOA-DFS-BlockSpill Ray] with Application-level Scheduling: $APP_SCHEDULING"
	Test false true false false DFS_BlockSpill
fi

if $DFS_BACKPRESSURE_BLOCKSPILL;
then
	SetUp true
	echo "Running [BOA-DFS-Backpressure-BlockSpill Ray] with Application-level Scheduling: $APP_SCHEDULING"
 	Test true true false false DFS_Backpressure_BlockSpill_Deadlock
fi

if $DFS_EAGERSPILL;
then
	SetUp true
	echo "Running [BOA-DFS-Eagerspill Ray] with Application-level Scheduling: $APP_SCHEDULING"
 	Test false false false true EagerSpill
fi

################ Plot Graph ################ 
<<comment
if ! $DEBUG;
then
./plot.py $APPLICATION
fi
comment
