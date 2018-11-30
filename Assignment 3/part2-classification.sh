#!/bin/bash

python $SCRIPTSPATH/data_download.py
python $SCRIPTSPATH/MultipleQuarters.py

#find .

if [ $? -eq 0 ]
then
  echo "Successfully created files"
else
  echo "Could not create file" >&2
fi