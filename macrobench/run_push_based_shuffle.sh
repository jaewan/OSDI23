#! /bin/bash

################ System Variables ################ 
PRODUCTION=false
EAGERSPILL=true

if $PRODUCTION;
then
	RAY_DATASET_PUSH_BASED_SHUFFLE=1 python sort.py --num-partitions=10 --partition-size=1e7
fi

if $EAGERSPILL;
then
	RAY_BACKEND_LOG_LEVEL=debug RAY_DATASET_PUSH_BASED_SHUFFLE=1 RAY_enable_EagerSpill=trutrue python sort.py --num-partitions=10 --partition-size=1e7
fi
