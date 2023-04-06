#gcc version 9
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
sudo apt-get update
sudo apt-get install -y build-essential curl gcc-9 g++-9 pkg-config psmisc unzip
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 90 \
              --slave /usr/bin/g++ g++ /usr/bin/g++-9 \
              --slave /usr/bin/gcov gcov /usr/bin/gcov-9

#Python setup
echo "Install Python"
sudo apt install python-is-python3
sudo apt install python3-pip
python -m pip install --upgrade pip wheel
pip install termcolor
pip install psutil
pip install pyarrow
pip install pandas
pip install tqdm
pip install matplotlib

pip install ray
pip uninstall ray

#Ray Serve
pip install fastapi
pip install uvicorn
pip install transformers
pip install torch torchvision
pip install aiorwlock
pip install sentencepiece
pip install datasets
pip install --upgrade requests
pip install opencensus
pip install prometheus_client
pip install gpustat
pip install aiohttp_cors

#Nodejs
echo "Install Nodejs"
wget https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh
chmod +x install.sh
./install.sh
rm install.sh
source ~/.bashrc
#need to reopen the terminal
nvm install 14
nvm use 14
rm bazel-5.3.2-installer-linux-x86_64.sh

#install bazel
echo "Install Bazel"
wget https://github.com/bazelbuild/bazel/releases/download/5.3.2/bazel-5.3.2-installer-linux-x86_64.sh
chmod +x bazel-5.3.2-installer-linux-x86_64.sh
./bazel-5.3.2-installer-linux-x86_64.sh --user
export PATH="$PATH:$HOME/bin"

# Get Boa
pushd  ~/
git clone https://github.com/jaewan/ray_memory_management.git
cd ray_memory_management
# if this server has less memory than (core count * 2)GB, uncomment the following line
#echo "build --local_ram_resources=HOST_RAM*.5 --local_cpu_resources=HOST_CPUS-2" | tee -a .bazelrc
git checkout eager-spill
cd python/ray/dashboard/client
npm install && npm ci && npm run build
popd

# Get production Ray 2.2.0
pushd  ~/
git clone https://github.com/ray-project/ray.git
mv ray production_ray
cd production_ray
# if this server has less memory than (core count * 2)GB, uncomment the following line
#echo "build --local_ram_resources=HOST_RAM*.5 --local_cpu_resources=HOST_CPUS-2" | tee -a .bazelrc
git checkout releases/2.2.0
cd python/ray/dashboard/client
npm install && npm ci && npm run build
popd
