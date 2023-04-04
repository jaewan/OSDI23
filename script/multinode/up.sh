#! /bin/bash

################ System Variables ################ 
PORT=6379
HEAD_ADDR=104.199.120.75
NUM_CPUS=16
OBJECT_STORE_MEMORY_SIZE=2000000000
HEAD=false
if [[ "$HOSTNAME" == *"head"* ]]; then
	HEAD=true
fi

while getopts n:o: flag
do
    case "${flag}" in
        n) NUM_CPUS=${OPTARG};;
        o) OBJECT_STORE_MEMORY_SIZE=${OPTARG};;
    esac
done

function Start_Ray()
{
	RAY_UP_COMMAND="ray start --address=$HEAD_ADDR:$PORT"
	if $HEAD;
	then
		RAY_UP_COMMAND="ray start --head --port=$PORT --system-config={"
		RAY_UP_COMMAND+='"object_spilling_config":"{\"type\":\"filesystem\",\"params\":{\"directory_path\":\"/ray_spill\"}}"}'
	fi
	$RAY_UP_COMMAND --num-cpus $NUM_CPUS --object-store-memory $OBJECT_STORE_MEMORY_SIZE

}

rm -rf /tmp/ray
Start_Ray
