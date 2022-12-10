#! /bin/bash 

################ System Config ################ 
DEBUG_MODE=false
PRODUCTION=false
DFS=false
EAGERSPILL=true

################ Application Config ################ 
BATCH_SIZE=100
BATCH_INTERVAL=0
NUM_BATCHES=1
OBJECT_STORE_SIZE=12000000000

function Run()
{
	eagerspill=$1

	NUM_TRIAL=5
	if $DEBUG_MODE;
	then
		for (( i=0; i<$NUM_TRIAL; i++))
		do
			RAY_BACKEND_LOG_LEVEL=debug RAY_ENSEMBLE_SERVE=true RAY_enable_EagerSpill=$eagerspill \
			RAY_enable_BlockTasks=$eagerspill RAY_enable_BlockTasksSpill=$eagerspill RAY_enable_DeadLock2=$eagerspill \
			python ensemble_serving.py -bs $BATCH_SIZE
		done
	else
		for (( i=0; i<1; i++))
		do
			RAY_ENSEMBLE_SERVE=true RAY_enable_EagerSpill=$eagerspill \
			RAY_enable_BlockTasks=false RAY_enable_BlockTasksSpill=$eagerspill RAY_enable_DeadLock2=$eagerspill \
			python ensemble_serving.py -nb $NUM_BATCHES -bi $BATCH_INTERVAL -bs $BATCH_SIZE -o $OBJECT_STORE_SIZE
		done
	fi

}

./load_images.sh

if $PRODUCTION;
then
	echo "Running [Production Ray] Ensemble Serving"
#	./../../script/install/install_production_ray.sh
	Run false
fi

if $DFS;
then
	echo "Running [BoA DFS] Ensemble Serving"
	#./../../script/install/install_boa.sh
	Run false
fi

if $EAGERSPILL;
then
	echo "Running [BoA EagerSpill] Ensemble Serving"
	#./../../script/install/install_boa.sh
	Run true
fi
