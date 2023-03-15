#! /bin/bash

################ System Variables ################ 
PORT=6379
HEAD=true
HEAD_ADDR=35.199.189.234
NUM_CPUS=16
OBJECT_STORE_MEMORY_SIZE=2000000000

while getopts n:o: flag
do
    case "${flag}" in
        n) NUM_CPUS=${OPTARG};;
        o) OBJECT_STORE_MEMORY_SIZE=${OPTARG};;
    esac
done

function Start_Ray()
{
	RAY_UP_COMMAND="ray start --address=$HEAD_ADDR:$PORT --system-config='{"object_spilling_config":"{\"type\":\"filesystem\",\"params\":{\"directory_path\":\"${RAY_SPILL_DIR}\"}}"}'"
	if $HEAD;
	then
		RAY_UP_COMMAND="ray start --head --port=$PORT"
	fi
	$RAY_UP_COMMAND --num-cpus $NUM_CPUS --object-store-memory $OBJECT_STORE_MEMORY_SIZE

}

rm -rf /tmp/ray
Start_Ray
