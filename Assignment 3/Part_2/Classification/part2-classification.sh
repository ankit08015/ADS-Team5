#!/bin/bash

python data-download.py

#find .

if [ $? -eq 0 ]
then
  echo "Successfully created files"
else
  echo "Could not create file" >&2
fi