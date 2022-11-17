#! /bin/bash

echo "Install Production Ray"
pip uninstall ray --y
pushd /home/ubuntu/production_ray/python
pip install -e . --verbose
popd
