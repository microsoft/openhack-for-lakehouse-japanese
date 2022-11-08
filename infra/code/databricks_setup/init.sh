#!/bin/bash


# Check if user is logged in
if [[ -n $(az account show 2> /dev/null) ]];then
    echo "user is already logged in"
else
    { echo "Please login via the Azure CLI: "; az login; }
fi


