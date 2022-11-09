#! /bin/bash 

################ Application Config ################ 
DEBUG_MODE=true
APP_SCHEDULING=true
PRODUCTION=false
DFS=true
EAGERSPILL=false

################ System Variables ################ 
PRODUCTION_DIR=/home/ubuntu/.local/lib/python3.8/site-packages/ray
BOA_DIR=/home/ubuntu/ray_memory_management/python/ray
NUM_PARTITION=6

function SetUp()
{
	BOA=$1

	if $APP_SCHEDULING;
	then
		CODE_PATH=code/application_scheduling/push_based_shuffle.py
	else
		CODE_PATH=code/application_scheduling_off/push_based_shuffle.py
	fi

	if $BOA;
	then
		cp $CODE_PATH $BOA_DIR/data/_internal/
		echo 'Setup push_based_shuffle to Boa python files'
	else
		cp code/client_mode_hook.py $PRODUCTION_DIR/_private/
		cp $CODE_PATH $PRODUCTION_DIR/data/_internal/
		echo 'Setup push_based_shuffle to Production Ray python files'
	fi

}

function Run()
{
	eagerspill=$1
	NUM_TRIAL=10
	DEBUG=info
	if $DEBUG_MODE;
	then
		DEBUG=debug
		NUM_TRIAL=1
	fi

	for i in {1..$NUM_TRIAL}
	do
		RAY_DATASET_PUSH_BASED_SHUFFLE=1 RAY_BACKEND_LOG_LEVEL=$DEBUG RAY_enable_EagerSpill=$eagerspill \
		python sort.py --num-partitions=$NUM_PARTITION --partition-size=1e7
	done
}

if $PRODUCTION;
then
	SetUp false
	echo "Running [Production Ray] with Application-level Scheduling: $APP_SCHEDULING"
	Run false
fi

if $DFS;
then
	SetUp true
	echo "Running [BOA-DFS Ray] with Application-level Scheduling: $APP_SCHEDULING"
	Run false
fi

if $EAGERSPILL;
then
	SetUp true
	echo "Running [BOA-DFS-EagerSpill Ray] with Application-level Scheduling: $APP_SCHEDULING"
	Run true
fi
