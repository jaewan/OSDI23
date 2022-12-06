#! /bin/bash 

################ Application Config ################ 
APP_SCHEDULING=0
PRODUCTION=false
DFS=false
DFS_BACKPRESSURE_BLOCKSPILL=false
EAGERSPILL=true

################ System Variables ################ 
PRODUCTION_DIR=/home/ubuntu/production_ray/python/ray/
#PRODUCTION_DIR=/home/ubuntu/.local/lib/python3.8/site-packages/ray
BOA_DIR=/home/ubuntu/ray_memory_management/python/ray
LOG_DIR=../data/config/

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

	NUM_TRIAL=3

	#test -f "$RESULT_PATH" && rm $RESULT_PATH
	#echo "object_store_size,num_workers,num_partitions,partition_size,runtime" >> $RESULT_PATH

	#for oss in {500000000,1000000000,4000000000,9000000000}
	for oss in {500000000,1000000000}
	do
		for nw in {8,16}
		do
			for np in {320,512}
			do
				for ps in {1e7,5e7,1e8}
				do
					for (( i=0; i<$NUM_TRIAL; i++))
					do
						RAY_BACKEND_LOG_LEVEL=debug RAY_DATASET_PUSH_BASED_SHUFFLE=1 RAY_enable_EagerSpill=$eagerspill \
						RAY_enable_BlockTasks=$BACKPRESSURE RAY_enable_BlockTasksSpill=$eagerspill \
						python sort.py --num-partitions=$np --partition-size=$ps --NUM_WORKER=$nw --OBJECT_STORE_SIZE=$oss -r $RESULT_PATH 
						rm /tmp/ray/*log
						rm -rf /tmp/ray/session*
					done
				done
			done
		done
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
	Run DFS_Backpressure_EagerSpill true true 
	./a.sh
fi
