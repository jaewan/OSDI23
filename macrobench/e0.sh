#! /bin/bash 

################ Application Config ################ 
DEBUG_MODE=false
APP_SCHEDULING=0
PRODUCTION=false
DFS=false
EAGERSPILL=true
DFS_BACKPRESSURE_BLOCKSPILL=false

################ System Variables ################ 
PRODUCTION_DIR=/home/ubuntu/production_ray/python/ray/
#PRODUCTION_DIR=/home/ubuntu/.local/lib/python3.8/site-packages/ray
BOA_DIR=/home/ubuntu/ray_memory_management/python/ray
LOG_DIR=../data/$3/
NUM_PARTITION=$1
PARTITION_SIZE=$2
TRIALS=$4

function SetUp()
{
	BOA=$1

	case $APP_SCHEDULING in
		# App-level scheduling
		0)
			CODE_PATH=code/application_scheduling/push_based_shuffle.py
			;;
		# App-level scheduling off ver1
		1)
			CODE_PATH=code/application_scheduling_off_ver1/push_based_shuffle.py
			;;
		# App-level scheduling off ver2
		2)
			CODE_PATH=code/application_scheduling_off_ver2/push_based_shuffle.py
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

function Run()
{
	RESULT_PATH=$LOG_DIR$1$APP_SCHEDULING.csv
	eagerspill=$2
	BACKPRESSURE=$3

	NUM_TRIAL=$TRIALS
	DEBUG=info
	if $DEBUG_MODE;
	then
		rm /tmp/ray/*log
		DEBUG=debug
		NUM_TRIAL=1
		RESULT_PATH="../data/dummy.csv"
	else
		#test -f "$RESULT_PATH" && rm $RESULT_PATH
		echo "runtime,spilled_amount,spilled_objects,write_throughput,restored_amount,restored_objects,read_throughput" >> $RESULT_PATH
		echo "Append mode"
	fi

	for (( i=0; i<$NUM_TRIAL; i++))
	do
		RAY_DATASET_PUSH_BASED_SHUFFLE=1 RAY_BACKEND_LOG_LEVEL=$DEBUG RAY_enable_EagerSpill=$eagerspill \
		RAY_enable_BlockTasks=$BACKPRESSURE RAY_enable_BlockTasksSpill=$eagerspil\
		python sort.py --num-partitions=$NUM_PARTITION --partition-size=$PARTITION_SIZE -r $RESULT_PATH 
	done
}

if $PRODUCTION;
then
	SetUp false
	echo "Running [Production Ray] with Application-level Scheduling: $APP_SCHEDULING"
	Run RAY false false
fi

if $DFS;
then
	SetUp true
	echo "Running [BOA-DFS Ray] with Application-level Scheduling: $APP_SCHEDULING"
	Run DFS false false
fi

if $DFS_BACKPRESSURE_BLOCKSPILL;
then
	SetUp true
	echo "Running [BOA-DFS-Backpressure Ray] with Application-level Scheduling: $APP_SCHEDULING"
	Run DFS_Backpressure false true
fi

if $EAGERSPILL;
then
	SetUp true
	echo "Running [BOA-DFS-EagerSpill Ray] with Application-level Scheduling: $APP_SCHEDULING"
	Run DFS_EagerSpill true false
fi
