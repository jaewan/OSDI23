#! /bin/bash 

DEBUG_MODE=false

data/load_images.sh
if DEBUG_MODE;
then 
	RAY_BACKEND_LOG_LEVEL=debug RAY_ENSEMBLE_SERVE=true python ensemble_serving.py
else
	RAY_ENSEMBLE_SERVE=true python ensemble_serving.py
fi
