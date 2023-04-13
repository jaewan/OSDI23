#! /bin/bash

case "${1}" in
    *video_processing.py*)  #Video Processing
	  echo "Submitting Video Processing"
	  python $1 -r $2 -m $3 --num-videos=$4 --max-frames=1800 --output latency_output.txt;;
    *sort.py*)  #Push-based Shuffle
	  echo "Submitting Push Based Shuffle"
	  python $1 -r $2 -m $3 --num-partitions=$4 --partition-size=$5 ;;
    *) # microbenchmark	
      echo "Submitting Microbenchmark"
	  python $1 -w $2 -r $3 -o $4 -os $5  -off $6 -m $7
      exit
esac

