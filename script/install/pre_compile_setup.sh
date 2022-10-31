#gcc version 9
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
sudo apt-get update
sudo apt-get install -y build-essential curl gcc-9 g++-9 pkg-config psmisc unzip
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 90 \
              --slave /usr/bin/g++ g++ /usr/bin/g++-9 \
              --slave /usr/bin/gcov gcov /usr/bin/gcov-9

#Python setup
sudo apt install python-is-python3
sudo apt install python3-pip
python -m pip install --upgrade pip wheel

#Nodejs
wget https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh
chmod +x install.sh
./install.sh
rm install.sh
#need to reopen the terminal
nvm install 14
nvm use 14

#install bazel
wget https://github.com/bazelbuild/bazel/releases/download/5.3.2/bazel-5.3.2-installer-darwin-arm64.sh
chmod +x bazel-5.3.2-installer-darwin-arm64.sh
rm bazel-5.3.2-installer-darwin-arm64.sh
./bazel-5.3.2-installer-darwin-arm64.sh --user
