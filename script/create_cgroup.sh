#! /bin/bash

MEM_LIMIT=4000000000
RAYMEMDIR="/sys/fs/cgroup/memory/raymemory"

# Checking if /sys/fs/cgroup is mounted
if !(mount | grep cgroup | grep tmpfs)
then
	echo "cgroups support is not enabled!!"
	exit
fi

# Create memory subdirectory if not exist
MEM_SUBDIR="/sys/fs/cgroup/memory"
if !([ -d $MEM_SUBDIR ]);
then
	sudo mkdir $MEM_SUBDIR
	sudo mount -t cgroup -o memory cgroup_memory $MEM_SUBDIR

	# Creating a new group
	sudo mkdir $RAYMEMDIR
	echo $MEM_LIMIT | sudo tee $RAYMEMDIR/memory.limit_in_bytes
fi
