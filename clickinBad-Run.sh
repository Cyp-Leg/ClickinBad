#!/bin/bash

# App properties
APP_NAME=ClickinBad
APP_VERSION=1.0
MAIN_CLASS=Main

# Whole file name
APP_EXECUTABLE=$APP_NAME-$APP_VERSION.jar

# Checking the parameters
PROGRAM_FILE=$0
TRAINING_FILE=$1
DATA_FILE=$2

if [ -z $TRAINING_FILE ] || [ -z $DATA_FILE ]; then
	echo "Command usage: "$PROGRAM_FILE" <traninig-file-name> <data-file-name>"
else
	# Remove the manifest (no main class detected because of a bug)
	zip -d $APP_EXECUTABLE META-INF/MANIFEST.MF >> /dev/null
	java -Xmn1G -Xmx2G -cp $APP_EXECUTABLE $MAIN_CLASS $TRAINING_FILE $DATA_FILE
fi