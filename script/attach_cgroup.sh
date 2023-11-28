#! /bin/bash

# Usage: ./attach_cgroup.sh $$

RAYMEMDIR="/sys/fs/cgroup/memory/raymemory"

echo $1 | sudo tee $RAYMEMDIR/cgroup.procs

