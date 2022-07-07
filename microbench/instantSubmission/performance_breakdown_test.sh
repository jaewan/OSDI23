#! /bin/bash
# Performance Breakdown for each techniques (DFS, BackPressure, BlockSpill)
# 16 Workers

BASE_DIR=/home/ubuntu/ray/NSDI23
LOG_DIR=data
LOG_PATH=$BASE_DIR/$LOG_DIR

NUM_WORKER=8
OBJECT_SIZE=2000000000
OBJECT_STORE_SIZE=$NUM_WORKER*$OBJECT_SIZE

BACKPRESSURE=false
BLOCKSPILL=false


function BaseRuns()
{
	TEST_FILE=$1
	RESULT_FILE=$2
	test -f "$RESULT_FILE" && rm $RESULT_FILE
	echo "base_std, ray_std, base_var, ray_var, working_set_ratio, object_store_size,object_size,baseline_pipeline,ray_pipeline" >>$RESULT_FILE
	for w in 1 2 4 8 16
	do
		for o in 1000000000 4000000000 8000000000 #((o=$OBJECT_STORE_SIZE; o<=$OBJECT_STORE_SIZE_MAX; o += $OBJECT_STORE_SIZE_INCREASE))
		do
			for ((os=10000000; os<=160000000; os *= 2))
			do
				echo -n -e "$TEST_FILE -w $w -o $o -os $os\n"
				RAY_object_spilling_threshold=1.0 python $TEST_FILE -w $w -o $o -os $os -r $RESULT_FILE 
				rm -rf /tmp/ray/*
			done
		done
	done
}

function ParallelShuffleTest()
{
	BaseRuns test_parallel_shuffle.py $1
}

function ScatterGatherTest()
{
	BaseRuns test_scatter_gather.py $1
}
function ShuffleTest_backup()
{
	BaseRuns test_shuffle.py $1
}

function ShuffleTest_backup()
{
	RESULT_FILE=$1
	test -f "$RESULT_FILE" && rm $RESULT_FILE
	echo "base_std, ray_std, base_var, ray_var, working_set_ratio, object_store_size,object_size,baseline_pipeline,ray_pipeline" >>$RESULT_FILE
	for w in 1 2 
	do
		for o in 1000000000 4000000000  8000000000 
		do
			for ((os=10000000; os<=160000000; os *= 2))
			do
				echo -n -e "test_shuffle.py -w $w -o $o -os $os\n"
				RAY_object_spilling_threshold=1.0 python test_shuffle.py -w $w -o $o -os $os -r $RESULT_FILE 
				rm -rf /tmp/ray/*
			done
		done
	done
}

function PipelineTest()
{
	RESULT_FILE=$1
	test -f "$RESULT_FILE" && rm $RESULT_FILE
	#echo "base_std, ray_std, base_var, ray_var, working_set_ratio, num_stages, object_store_size,object_size,baseline_pipeline,ray_pipeline" >>$RESULT_FILE
	echo "ray_std, ray_var, working_set_ratio, num_stages, object_store_size,object_size,ray_pipeline" >>$RESULT_FILE
	for w in 1 2 4 8 16
	do
		echo -n -e "RAY_enable_BlockTasks=$BACKPRESSURE RAY_enable_BlockTasksSpill=$BLOCKSPILL test_pipeline.py -w $w -o $o -os $OBJECT_SIZE\n"
		#RAY_enable_BlockTasks=true python test_pipeline.py -w $w -o $o -os $os -r $RESULT_FILE 
		RAY_enable_BlockTasks=$BACKPRESSURE RAY_enable_BlockTasksSpill=$BLOCKSPILL RAY_object_spilling_threshold=1.0 python test_pipeline.py -w $w -o $OBJECT_STORE_SIZE -os $OBJECT_SIZE -r $RESULT_FILE 
		rm -rf /tmp/ray/*
	done
}

pushd $BASE_DIR
mkdir -p $LOG_DIR
popd

#-----------------------------------------------------------------------
#Production Ray
#PipelineTest $LOG_PATH/pipeline_production.csv
#ShuffleTest $LOG_PATH/shuffle_production.csv
#ScatterGatherTest $LOG_PATH/scatter_gather_production.csv
#ParallelShuffleTest $LOG_PATH/parallel_shuffle_production.csv
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
#Scheduled
#./../scripts/install_scheduler_ray.sh
PipelineTest $LOG_PATH/pipeline_dfs.csv
#ShuffleTest $LOG_PATH/shuffle_dfs.csv
#ScatterGatherTest $LOG_PATH/scatter_gather_dfs.csv
#ParallelShuffleTest $LOG_PATH/parallel_shuffle_dfs.csv
#-----------------------------------------------------------------------
