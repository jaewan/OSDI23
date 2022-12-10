#! /bin/bash 

sudo add-apt-repository ppa:graphics-drivers/ppa
sudo apt install ubuntu-drivers-common
# Manually install by finding driver with sudo ubuntu-drivers devices and install recommended like
# sudo apt install nvidia-driver-550
sudo ubuntu-drivers autoinstall
sudo reboot
