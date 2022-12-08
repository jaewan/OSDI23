#! /bin/bash 

# Mounting local storage instance
RAY_SPILL_DIR="/ray_spill"

if df -T -h | grep $RAY_SPILL_DIR;
then
	exit
fi

if !(test -f $RAY_SPILL_DIR);
then
		sudo mkdir $RAY_SPILL_DIR
fi


sudo umount $RAY_SPILL_DIR
sudo mkfs.ext4 /dev/nvme1n1
sudo mount -t ext4 /dev/nvme1n1 $RAY_SPILL_DIR

# Setting env variable
sudo chown $USER $RAY_SPILL_DIR 
if !(grep $RAY_SPILL_DIR /etc/fstab);
then
	echo "/dev/nvme1n1 ${RAY_SPILL_DIR} auto noatime 0 0" | sudo tee -a /etc/fstab
fi
if !(grep $RAY_SPILL_DIR ~/.bashrc);
then
	echo "export RAY_SPILL_DIR='${RAY_SPILL_DIR}'" | tee -a ~/.bashrc
	echo "${PWD}/mount_local_storage_instance.sh" | tee -a ~/.bashrc
	source ~/.bashrc
fi
