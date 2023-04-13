#gcc version 9
nvm install 14
nvm use 14
# Get Boa
pushd  ~/
git clone https://github.com/jaewan/ray_memory_management.git
cd ray_memory_management
# if this server has less memory than (core count * 2)GB, uncomment the following line
#echo "build --local_ram_resources=HOST_RAM --local_cpu_resources=HOST_CPUS-16" | tee -a .bazelrc
git checkout eager-spill
cd python
pip install -e . 
cd ray/dashboard/client
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
#cd python
#pip install -e . 
#cd ray/dashboard/client
#npm install && npm ci && npm run build
popd
