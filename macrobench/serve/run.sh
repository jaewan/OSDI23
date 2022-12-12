#! /bin/bash 

################ System Config ################ 
DEBUG_MODE=false
PRODUCTION=false
DFS=true
EAGERSPILL=false

################ Application Config ################ 
BATCH_SIZE=500
BATCH_INTERVAL=0
NUM_BATCHES=1
OBJECT_STORE_SIZE=8000000000

LOG_DIR=../../data/ensemble_serve/simulate/

function Run()
{
	RESULT_PATH=$LOG_DIR$1.csv
	eagerspill=$2

	NUM_TRIAL=10
	if $DEBUG_MODE;
	then
		for (( i=0; i<1; i++))
		do
			RAY_BACKEND_LOG_LEVEL=debug RAY_ENSEMBLE_SERVE=true RAY_enable_EagerSpill=$eagerspill \
			RAY_enable_BlockTasks=false RAY_enable_BlockTasksSpill=false RAY_enable_DeadLock2=false \
			python ensemble_serving.py -nb $NUM_BATCHES -bi $BATCH_INTERVAL -bs $BATCH_SIZE -o $OBJECT_STORE_SIZE 
		done
	else
		echo "runtime,num_batches,batch_size,batch_interval,models_run,spilled,restored,object_store_size" >> $RESULT_PATH
			for (( i=0; i<$NUM_TRIAL; i++))
			do
				RAY_ENSEMBLE_SERVE=true RAY_enable_EagerSpill=$eagerspill \
				RAY_enable_BlockTasks=true RAY_enable_BlockTasksSpill=false RAY_enable_DeadLock2=false \
				python ensemble_serving.py -nb $NUM_BATCHES -bi $BATCH_INTERVAL -bs $BATCH_SIZE -o $OBJECT_STORE_SIZE -r $RESULT_PATH
			done
	fi

}

./load_images.sh

if $PRODUCTION;
then
	echo "Running [Production Ray] Ensemble Serving"
	./../../script/install/install_production_ray.sh
	Run RAY false
fi

if $DFS;
then
	echo "Running [BoA DFS] Ensemble Serving"
	./../../script/install/install_boa.sh
	Run Backpressure false
fi

if $EAGERSPILL;
then
	echo "Running [BoA EagerSpill] Ensemble Serving"
	#./../../script/install/install_boa.sh
	Run EagerSpill true
fi
