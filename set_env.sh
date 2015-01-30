#!/usr/bin/env bash
# first get th dir where the install script is placed
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
export LIBS3A=$DIR
export PYTHONPATH=${LIBS3A}/python:${PYTHONPATH}

# make sure right git version is sourced for githookcontroller
if [[ ":$PATH:" == *":/cvmfs/cms.cern.ch/slc6_amd64_gcc481/external/git/1.8.3.1-cms/bin/"* ]]; then
    export PATH=/cvmfs/cms.cern.ch/slc6_amd64_gcc481/external/git/1.8.3.1-cms/bin/:$PATH
fi

# make sure githookcontroller is installed
if [ ! -e "./.git/hooks/githookcontroller.py" ]; then
    echo "Did not find githookcontroller in .git/hooks"
    echo "Please make sure you installed the repo correctly"
fi
