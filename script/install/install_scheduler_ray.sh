#! /bin/bash

echo "Install Memory Scheduler Ray\n"
pushd /home/ubuntu/ray_memory_management/python
pip install -e . --verbose
popd
