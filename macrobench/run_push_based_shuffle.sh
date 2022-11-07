#! /bin/bash

APP_SCHEDULING=false
PRODUCTION=true
EAGERSPILL=false
################ System Variables ################ 
NUM_PARTITION=320

if $PRODUCTION;
then
	cp code/client_mode_hook.py /home/ubuntu/.local/lib/python3.8/site-packages/ray/_private/
	if $APP_SCHEDULING;
	then
		cp code/application_scheduling/push_based_shuffle.py /home/ubuntu/.local/lib/python3.8/site-packages/ray/data/_internal/
	else
		cp code/application_scheduling_off/push_based_shuffle.py /home/ubuntu/.local/lib/python3.8/site-packages/ray/data/_internal/
		echo 'Copied app-level scheudling off push_based_shuffle'
	fi

	for i in {1..1}
		do
		RAY_DATASET_PUSH_BASED_SHUFFLE=1 python sort.py --num-partitions=$NUM_PARTITION --partition-size=1e7
		done
fi

if $EAGERSPILL;
then
	for i in {1..3}
	do
	RAY_BACKEND_LOG_LEVEL=debug RAY_DATASET_PUSH_BASED_SHUFFLE=1 RAY_enable_EagerSpill=true python sort.py --num-partitions=$NUM_PARTITION --partition-size=1e7
done
fi
