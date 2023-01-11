#! /bin/bash

echo "Non Eager Spill Latency"
python eager_spill.py
echo "Eager Spill Latency"
RAY_enable_EagerSpill=true python eager_spill.py
