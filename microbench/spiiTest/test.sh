#! /bin/bash

OBJECT_SIZE=100000000

function Test()
{
	for o in {16000000000,32000000000} #{1000000000,4000000000,16000000000,32000000000,64000000000}
	do
		echo Spill $o 
		RAY_BACKEND_LOG_LEVEL=debug \
		RAY_object_spilling_threshold=1.0 \
		python spill_test.py -o $o -os $OBJECT_SIZE
		python ~/parse_spill_time.py
	done
	#rm -rf /tmp/ray/*
}

Test 
