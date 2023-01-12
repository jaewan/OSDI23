#! /bin/bash

echo "Install Production Ray"
pip uninstall ray --y
pushd $HOME/production_ray/python
pip install -e . --verbose
popd
