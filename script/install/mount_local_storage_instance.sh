# Mountint local storage instance
RAY_SPILL_DIR="/ray_spill"
sudo mkdir $RAY_SPILL_DIR
sudo mkfs.ext4 /dev/nvme1n1
sudo mount -t ext4 /dev/nvme1n1 $RAY_SPILL_DIR

# Setting env variable
sudo chown $USER $RAY_SPILL_DIR 
echo "/dev/nvme1n1 ${RAY_SPILL_DIR} auto noatime 0 0" | sudo tee -a /etc/fstab
echo "export RAY_SPILL_DIR='${RAY_SPILL_DIR}'" | tee -a ~/.bashrc
source ~/.bashrc
