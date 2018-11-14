#!/bin/bash

# App properties
APP_NAME=ClickinBad
APP_VERSION=1.0

# Whole file name
APP_EXECUTABLE=$APP_NAME-$APP_VERSION.jar

# Checking the parameters
PROGRAM_FILE=$0
TRAINING_FILE=$1
DATA_FILE=$2

if [ -z $TRAINING_FILE ] || [ -z $DATA_FILE ]; then
	echo "Command usage: "$PROGRAM_FILE" <traninig-file-name> <data-file-name>"
else
	java -Xmn1G -Xmx2G -jar $APP_EXECUTABLE $TRAINING_FILE $DATA_FILE
fi