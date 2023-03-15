#! /bin/bash 

# Mounting local storage instance
RAY_SPILL_DIR="/ray_spill"

if df -T -h | grep $RAY_SPILL_DIR;
then
	exit
fi

if !([ -d $RAY_SPILL_DIR ]);
then
	sudo mkdir $RAY_SPILL_DIR
fi

MOUNT_DEV=/dev/nvme1n1

test -b $MOUNT_DEV  || MOUNT_DEV=/dev/sdb
test -b $MOUNT_DEV  || { echo "Unknown Device, Stop mounting" ; exit 0;}

sudo umount $RAY_SPILL_DIR
sudo mkfs.ext4 $MOUNT_DEV 
sudo mount -t ext4 $MOUNT_DEV $RAY_SPILL_DIR

# Setting env variable
sudo chown $USER $RAY_SPILL_DIR 
if !(grep $RAY_SPILL_DIR /etc/fstab);
then
	echo "${MOUNT_DEV} ${RAY_SPILL_DIR} auto noatime 0 0" | sudo tee -a /etc/fstab
fi
if !(grep $RAY_SPILL_DIR ~/.bashrc);
then
	echo "export RAY_SPILL_DIR='${RAY_SPILL_DIR}'" | tee -a ~/.bashrc
	echo "${PWD}/mount_local_storage_instance.sh" | tee -a ~/.bashrc
fi
source ~/.bashrc
