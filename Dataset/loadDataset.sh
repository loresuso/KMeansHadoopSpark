#!/bin/bash

PROJECT_PATH="/user/progetto/"

hadoop fs -mkdir /user
hadoop fs -mkdir ${PROJECT_PATH}
hadoop fs -copyFromLocal $(ls -1 *-*-*-dataset.txt) ${PROJECT_PATH}

echo "Done"
