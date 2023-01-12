#! /bin/bash

echo "Install Boa Ray"
pushd $HOME/ray_memory_management/python
pip uninstall ray --y
pip install -e . --verbose
popd
