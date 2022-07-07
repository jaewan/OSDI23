#! /bin/bash

for ((o=1000000000; o<=32000000000; o*=2))
	do
	echo python spill_time_measure.py -o $o 
	RAY_record_ref_creation_sites=1 python spill_time_measure.py -o $o 
done
