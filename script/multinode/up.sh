#! /bin/bash

PORT=6379
HEAD=true
HEAD_ADDR=35.199.189.234
NUM_CPUS=4
OBJECT_STORE_MEMORY_SIZE=1000000000

function Start_Ray()
{
	RAY_UP_COMMAND="ray start --address=$HEAD_ADDR:$PORT"
	if $HEAD;
	then
		RAY_UP_COMMAND="ray start --head --port=$PORT"
	fi
	$RAY_UP_COMMAND --system-config='{"object_spilling_config":"{\"type\":\"filesystem\",\"params\":{\"directory_path\":\"${RAY_SPILL_DIR}\"}}"}'\
		--num-cpus $NUM_CPUS --object-store-memory $OBJECT_STORE_MEMORY_SIZE

}

Start_Ray
