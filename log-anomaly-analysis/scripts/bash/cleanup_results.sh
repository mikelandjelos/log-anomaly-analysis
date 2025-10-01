#!/bin/bash

# Script to clean up the ./data/results directory

RESULTS_DIR="./data/results"

echo "Cleaning up $RESULTS_DIR directory..."

if [ -d "$RESULTS_DIR" ]; then
    rm -rf "$RESULTS_DIR"/*
    echo "Cleaned up $RESULTS_DIR"
else
    echo "Directory $RESULTS_DIR does not exist"
fi
