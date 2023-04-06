#! /bin/bash

case "${1}" in
    *sort.py*)  #Push-based Shuffle
	  echo "Submitting Push Based Shuffle"
	  python $1 -r $2 -m $3 --num-partitions=$4 --partition-size=$5 ;;
    *) # microbenchmark	
      echo "Submitting Microbenchmark"
	  python $1 -w $2 -r $3 -o $4 -os $5  -off $6 -m $7
      exit
esac
