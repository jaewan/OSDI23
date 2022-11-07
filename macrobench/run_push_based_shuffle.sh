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
NUM_PARTITION=32

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
	if $DEBUG_MODE;
	then
		DEBUG=debug
	fi

	for i in {1..2}
	do
		RAY_DATASET_PUSH_BASED_SHUFFLE=1 RAY_BACKEND_LOG_LEVEL=$DEBUG RAY_enable_EagerSpill=$eagerspill \
		python sort.py --num-partitions=$NUM_PARTITION --partition-size=1e7
	done
}

if $PRODUCTION;
then
	SetUp false
	Run false
fi

if $DFS;
then
	SetUp true
	Run false
fi

if $EAGERSPILL;
then
	SetUp true
	Run true
fi
