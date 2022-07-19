#! /bin/bash

echo "Install Production Ray\n"
pip uninstall ray --Y
pip install ray==1.12 
