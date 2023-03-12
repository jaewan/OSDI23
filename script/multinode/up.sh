#! /bin/bash

PORT=6379
HEAD=true
HEAD_ADDR=35.199.189.234

function Start_Ray()
{
	RAY_UP_COMMAND="ray start --address=$HEAD_ADDR:$PORT"
	if $HEAD;
	then
		RAY_UP_COMMAND="ray start --head --port=$PORT"
	fi
	$RAY_UP_COMMAND --system-config=‘{“object_spilling_config”:“{\“type\“:\“filesystem\“,\“params\“:{\“directory_path\“:\“$(RAY_SPILL_DIR)\“}}“}’
}

#Start_Ray
echo $RAY_SPILL_DIR
