#!/bin/bash

# Run process_data method
echo "Running process_data..."
python code/src/run.py process_data --cfg code/config/config.yaml --dataset news --dirout outputs

# Run process_data_all method
echo "Running process_data_all..."
python code/src/run.py process_data_all --cfg code/config/config.yaml --dataset news --dirout outputs

